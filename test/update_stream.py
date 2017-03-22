#!/usr/bin/env python

import logging
import time
import unittest

import environment
import tablet
import utils
from vtdb import dbexceptions
from vtdb import proto3_encoding
from vtdb import vtgate_client
from vtproto import query_pb2
from vtproto import topodata_pb2
from mysql_flavor import mysql_flavor
from protocols_flavor import protocols_flavor
from vtgate_gateway_flavor.gateway import vtgate_gateway_flavor

# global flag to control which type of replication we use.
use_rbr = False

master_tablet = tablet.Tablet()
replica_tablet = tablet.Tablet()

# master_start_position has the replication position before we start
# doing anything to the master database. It is used by test_ddl to
# make sure we see DDLs.
master_start_position = None

_create_vt_insert_test = '''create table if not exists vt_insert_test (
id bigint auto_increment,
msg varchar(64),
primary key (id)
) Engine=InnoDB'''

_create_vt_a = '''create table if not exists vt_a (
eid bigint,
id int,
primary key(eid, id)
) Engine=InnoDB'''

_create_vt_b = '''create table if not exists vt_b (
eid bigint,
name varchar(128),
foo varbinary(128),
primary key(eid, name)
) Engine=InnoDB'''


def _get_master_current_position():
  return mysql_flavor().master_position(master_tablet)


def _get_repl_current_position():
  return mysql_flavor().master_position(replica_tablet)


def setUpModule():
  global master_start_position

  try:
    environment.topo_server().setup()

    # start mysql instance external to the test
    setup_procs = [master_tablet.init_mysql(use_rbr=use_rbr),
                   replica_tablet.init_mysql(use_rbr=use_rbr)]
    utils.wait_procs(setup_procs)

    # start a vtctld so the vtctl insert commands are just RPCs, not forks
    utils.Vtctld().start()

    # Start up a master mysql and vttablet
    logging.debug('Setting up tablets')
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])
    master_tablet.init_tablet('replica', 'test_keyspace', '0', tablet_index=0)
    replica_tablet.init_tablet('replica', 'test_keyspace', '0', tablet_index=1)
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)
    master_tablet.create_db('vt_test_keyspace')
    master_tablet.create_db('other_database')
    replica_tablet.create_db('vt_test_keyspace')
    replica_tablet.create_db('other_database')

    master_tablet.start_vttablet(wait_for_state=None)
    replica_tablet.start_vttablet(wait_for_state=None)
    master_tablet.wait_for_vttablet_state('NOT_SERVING')
    replica_tablet.wait_for_vttablet_state('NOT_SERVING')

    utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/0',
                     master_tablet.tablet_alias], auto_log=True)

    utils.wait_for_tablet_type(replica_tablet.tablet_alias, 'replica')
    master_tablet.wait_for_vttablet_state('SERVING')
    replica_tablet.wait_for_vttablet_state('SERVING')

    # reset counter so tests don't assert
    tablet.Tablet.tablets_running = 0

    master_start_position = _get_master_current_position()
    master_tablet.mquery('vt_test_keyspace', _create_vt_insert_test)
    master_tablet.mquery('vt_test_keyspace', _create_vt_a)
    master_tablet.mquery('vt_test_keyspace', _create_vt_b)

    utils.run_vtctl(['ReloadSchemaKeyspace', 'test_keyspace'])
    utils.run_vtctl(['RebuildVSchemaGraph'])

    utils.VtGate().start(tablets=[master_tablet, replica_tablet])
    utils.vtgate.wait_for_endpoints('test_keyspace.0.master', 1)
    utils.vtgate.wait_for_endpoints('test_keyspace.0.replica', 1)

    # Wait for the master and slave tablet's ReloadSchema to have worked.
    # Note we don't specify a keyspace name, there is only one, vschema
    # will just use that single keyspace.
    timeout = 10
    while True:
      try:
        utils.vtgate.execute('select count(1) from vt_insert_test',
                             tablet_type='master')
        utils.vtgate.execute('select count(1) from vt_insert_test',
                             tablet_type='replica')
        break
      except protocols_flavor().client_error_exception_type():
        logging.exception('query failed')
        timeout = utils.wait_step('slave tablet having correct schema', timeout)
        # also re-run ReloadSchema on slave, it case the first one
        # didn't get the replicated table.
        utils.run_vtctl(['ReloadSchema', replica_tablet.tablet_alias])

  except:
    tearDownModule()
    raise


def tearDownModule():
  utils.required_teardown()
  if utils.options.skip_teardown:
    return
  logging.debug('Tearing down the servers and setup')
  tablet.Tablet.tablets_running = 2
  tablet.kill_tablets([master_tablet, replica_tablet])
  teardown_procs = [master_tablet.teardown_mysql(),
                    replica_tablet.teardown_mysql()]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server().teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()
  master_tablet.remove_tree()
  replica_tablet.remove_tree()


class TestUpdateStream(unittest.TestCase):
  _populate_vt_insert_test = [
      "insert into vt_insert_test (msg) values ('test %s')" % x
      for x in xrange(4)]

  def _populate_vt_a(self, count):
    return ['insert into vt_a (eid, id) values (%d, %d)' % (x, x)
            for x in xrange(count + 1) if x > 0]

  def _populate_vt_b(self, count):
    return [
        "insert into vt_b (eid, name, foo) values (%d, 'name %s', 'foo %s')" %
        (x, x, x) for x in xrange(count)]

  def _get_vtgate_stream_conn(self):
    protocol, addr = utils.vtgate.rpc_endpoint(python=True)
    return vtgate_client.connect(protocol, addr, 30.0)

  def _exec_vt_txn(self, query_list):
    protocol, addr = utils.vtgate.rpc_endpoint(python=True)
    vtgate_conn = vtgate_client.connect(protocol, addr, 30.0)
    cursor = vtgate_conn.cursor(
        tablet_type='master', keyspace='test_keyspace',
        shards=['0'], writable=True)
    cursor.begin()
    for query in query_list:
      cursor.execute(query, {})
    cursor.commit()
    return

  def test_stream_parity(self):
    """Tests parity of streams between master and replica for the same writes.

    Also tests transactions are retrieved properly.
    """

    timeout = 30
    while True:
      master_position = _get_master_current_position()
      replica_position = _get_repl_current_position()
      if master_position == replica_position:
        break
      timeout = utils.wait_step(
          '%s == %s' % (master_position, replica_position),
          timeout
      )
    logging.debug('run_test_stream_parity starting @ %s',
                  master_position)
    self._exec_vt_txn(self._populate_vt_a(15))
    self._exec_vt_txn(self._populate_vt_b(14))
    self._exec_vt_txn(['delete from vt_a'])
    self._exec_vt_txn(['delete from vt_b'])

    # get master events
    master_conn = self._get_vtgate_stream_conn()
    master_events = []
    for event, resume_timestamp in master_conn.update_stream(
        'test_keyspace', topodata_pb2.MASTER,
        event=query_pb2.EventToken(shard='0', position=master_position),
        shard='0'):
      logging.debug('Got master event(%d): %s', resume_timestamp, event)
      master_events.append(event)
      if len(master_events) == 4:
        break
    master_conn.close()

    # get replica events
    replica_conn = self._get_vtgate_stream_conn()
    replica_events = []
    for event, resume_timestamp in replica_conn.update_stream(
        'test_keyspace', topodata_pb2.REPLICA,
        event=query_pb2.EventToken(shard='0', position=replica_position),
        shard='0'):
      logging.debug('Got slave event(%d): %s', resume_timestamp, event)
      replica_events.append(event)
      if len(replica_events) == 4:
        break
    replica_conn.close()

    # and compare
    if len(master_events) != len(replica_events):
      logging.debug(
          'Test Failed - # of records mismatch, master %s replica %s',
          master_events, replica_events)
    for master_event, replica_event in zip(master_events, replica_events):
      # The timestamp is from when the event was written to the binlogs.
      # the master uses the timestamp of when it wrote it originally,
      # the slave of when it applied the logs. These can differ and make this
      # test flaky. So we just blank them out, easier. We really want to
      # compare the replication positions.
      master_event.event_token.timestamp = 123
      replica_event.event_token.timestamp = 123
      self.assertEqual(
          master_event, replica_event,
          "Test failed, data mismatch - master '%s' and replica '%s'" %
          (master_event, replica_event))
    logging.debug('Test Writes: PASS')

  def test_ddl(self):
    """Asks for all statements since we started, find the DDL."""
    start_position = master_start_position
    logging.debug('test_ddl: starting @ %s', start_position)
    master_conn = self._get_vtgate_stream_conn()
    found = False
    for event, _ in master_conn.update_stream(
        'test_keyspace', topodata_pb2.MASTER,
        event=query_pb2.EventToken(shard='0', position=start_position),
        shard='0'):
      for statement in event.statements:
        if statement.sql == _create_vt_insert_test:
          found = True
          break
      break
    master_conn.close()
    self.assertTrue(found, "didn't get right sql")

  def test_set_insert_id(self):
    start_position = _get_master_current_position()
    self._exec_vt_txn(
        ['SET INSERT_ID=1000000'] + self._populate_vt_insert_test)
    logging.debug('test_set_insert_id: starting @ %s', start_position)
    master_conn = self._get_vtgate_stream_conn()
    expected_id = 1000000
    for event, _ in master_conn.update_stream(
        'test_keyspace', topodata_pb2.MASTER,
        event=query_pb2.EventToken(shard='0', position=start_position),
        shard='0'):
      for statement in event.statements:
        fields, rows = proto3_encoding.convert_stream_event_statement(statement)
        self.assertEqual(fields[0], 'id')
        self.assertEqual(rows[0][0], expected_id)
        expected_id += 1
      break
    if expected_id != 1000004:
      self.fail('did not get my four values!')
    master_conn.close()

  def test_database_filter(self):
    start_position = _get_master_current_position()
    master_tablet.mquery('other_database', _create_vt_insert_test)
    self._exec_vt_txn(self._populate_vt_insert_test)
    logging.debug('test_database_filter: starting @ %s', start_position)
    master_conn = self._get_vtgate_stream_conn()
    for event, _ in master_conn.update_stream(
        'test_keyspace', topodata_pb2.MASTER,
        event=query_pb2.EventToken(shard='0', position=start_position),
        shard='0'):
      for statement in event.statements:
        self.assertNotEqual(statement.category, 2,  # query_pb2.StreamEvent.DDL
                            "query using other_database wasn't filtered out")
      break
    master_conn.close()

  def test_service_switch(self):
    """tests the service switch from disable -> enable -> disable."""
    # make the replica spare
    utils.run_vtctl(['ChangeSlaveType', replica_tablet.tablet_alias, 'spare'])
    utils.wait_for_tablet_type(replica_tablet.tablet_alias, 'spare')

    # Check UpdateStreamState is disabled.
    v = utils.get_vars(replica_tablet.port)
    if v['UpdateStreamState'] != 'Disabled':
      self.fail("Update stream service should be 'Disabled' but is '%s'" %
                v['UpdateStreamState'])

    start_position = _get_repl_current_position()

    # Make sure we can't start a new request to vttablet directly.
    _, stderr = utils.run_vtctl(['VtTabletUpdateStream',
                                 '-position', start_position,
                                 replica_tablet.tablet_alias],
                                expect_fail=True)
    self.assertIn('operation not allowed in state NOT_SERVING', stderr)

    # Make sure we can't start a new request through vtgate.
    replica_conn = self._get_vtgate_stream_conn()
    try:
      for event, resume_timestamp in replica_conn.update_stream(
          'test_keyspace', topodata_pb2.REPLICA,
          event=query_pb2.EventToken(shard='0', position=start_position),
          shard='0'):
        self.assertFail('got event(%d): %s' % (resume_timestamp, str(event)))
      self.assertFail('update_stream terminated with no exception')
    except dbexceptions.DatabaseError as e:
      self.assertIn(vtgate_gateway_flavor().no_tablet_found_message(), str(e))

    # Go back to replica.
    utils.run_vtctl(
        ['ChangeSlaveType', replica_tablet.tablet_alias, 'replica'])
    utils.wait_for_tablet_type(replica_tablet.tablet_alias, 'replica')

    # Check UpdateStreamState is enabled.
    v = utils.get_vars(replica_tablet.port)
    if v['UpdateStreamState'] != 'Enabled':
      self.fail("Update stream service should be 'Enabled' but is '%s'" %
                v['UpdateStreamState'])

  def test_event_token(self):
    """Checks the background binlog monitor thread works."""
    timeout = 10
    while True:
      replica_position = _get_repl_current_position()
      value = None
      v = utils.get_vars(replica_tablet.port)
      if 'EventTokenPosition' in v:
        value = v['EventTokenPosition']
      if value == replica_position:
        logging.debug('got expected EventTokenPosition vars: %s', value)
        ts = v['EventTokenTimestamp']
        now = long(time.time())
        self.assertTrue(ts >= now - 120,
                        'EventTokenTimestamp is too old: %d < %d' %
                        (ts, now-120))
        self.assertTrue(ts <= now,
                        'EventTokenTimestamp is too recent: %d > %d' %(ts, now))
        break
      timeout = utils.wait_step(
          'EventTokenPosition must be up to date but got %s (expected %s)' %
          (value, replica_position), timeout)

    # With vttablet up to date, test a vttablet query returns the EventToken.
    qr = replica_tablet.execute('select * from vt_insert_test',
                                execute_options='include_event_token:true ')
    logging.debug('Got result: %s', qr)
    self.assertIn('extras', qr)
    self.assertIn('event_token', qr['extras'])
    self.assertEqual(qr['extras']['event_token']['position'], replica_position)

    # Same thing through vtgate
    qr = utils.vtgate.execute('select * from vt_insert_test',
                              tablet_type='replica',
                              execute_options='include_event_token:true ')
    logging.debug('Got result: %s', qr)
    self.assertIn('extras', qr)
    self.assertIn('event_token', qr['extras'])
    self.assertEqual(qr['extras']['event_token']['position'], replica_position)

    # Make sure the compare_event_token flag works, by sending a very
    # old timestamp, or a timestamp in the future.
    qr = replica_tablet.execute(
        'select * from vt_insert_test',
        execute_options='compare_event_token: <timestamp:123 > ')
    self.assertIn('extras', qr)
    self.assertIn('fresher', qr['extras'])
    self.assertTrue(qr['extras']['fresher'])

    future_timestamp = long(time.time()) + 100
    qr = replica_tablet.execute(
        'select * from vt_insert_test',
        execute_options='compare_event_token: <timestamp:%d > ' %
        future_timestamp)
    self.assertTrue(qr['extras'] is None)

    # Same thing through vtgate
    qr = utils.vtgate.execute(
        'select * from vt_insert_test', tablet_type='replica',
        execute_options='compare_event_token: <timestamp:123 > ')
    self.assertIn('extras', qr)
    self.assertIn('fresher', qr['extras'])
    self.assertTrue(qr['extras']['fresher'])

    future_timestamp = long(time.time()) + 100
    qr = utils.vtgate.execute(
        'select * from vt_insert_test', tablet_type='replica',
        execute_options='compare_event_token: <timestamp:%d > ' %
        future_timestamp)
    self.assertTrue(qr['extras'] is None)

    # Make sure the compare_event_token flag works, by sending a very
    # old timestamp, or a timestamp in the future, when combined with
    # include_event_token flag.
    qr = replica_tablet.execute('select * from vt_insert_test',
                                execute_options='include_event_token:true '
                                'compare_event_token: <timestamp:123 > ')
    self.assertIn('extras', qr)
    self.assertIn('fresher', qr['extras'])
    self.assertTrue(qr['extras']['fresher'])
    self.assertIn('event_token', qr['extras'])
    self.assertEqual(qr['extras']['event_token']['position'], replica_position)

    future_timestamp = long(time.time()) + 100
    qr = replica_tablet.execute('select * from vt_insert_test',
                                execute_options='include_event_token:true '
                                'compare_event_token: <timestamp:%d > ' %
                                future_timestamp)
    self.assertNotIn('fresher', qr['extras'])
    self.assertIn('event_token', qr['extras'])
    self.assertEqual(qr['extras']['event_token']['position'], replica_position)

    # Same thing through vtgate
    qr = utils.vtgate.execute('select * from vt_insert_test',
                              tablet_type='replica',
                              execute_options='include_event_token:true '
                              'compare_event_token: <timestamp:123 > ')
    self.assertIn('extras', qr)
    self.assertIn('fresher', qr['extras'])
    self.assertTrue(qr['extras']['fresher'])
    self.assertIn('event_token', qr['extras'])
    self.assertEqual(qr['extras']['event_token']['position'], replica_position)

    future_timestamp = long(time.time()) + 100
    qr = utils.vtgate.execute('select * from vt_insert_test',
                              tablet_type='replica',
                              execute_options='include_event_token:true '
                              'compare_event_token: <timestamp:%d > ' %
                              future_timestamp)
    self.assertNotIn('fresher', qr['extras'])
    self.assertIn('event_token', qr['extras'])
    self.assertEqual(qr['extras']['event_token']['position'], replica_position)

  def test_update_stream_interrupt(self):
    """Checks that a running query is terminated on going non-serving."""
    # Make sure the replica is replica type.
    utils.run_vtctl(
        ['ChangeSlaveType', replica_tablet.tablet_alias, 'replica'])
    logging.debug('sleeping a bit for the replica action to complete')
    utils.wait_for_tablet_type(replica_tablet.tablet_alias, 'replica', 30)

    # Save current position, insert some data.
    start_position = _get_repl_current_position()
    logging.debug('test_update_stream_interrupt starting @ %s', start_position)
    self._exec_vt_txn(self._populate_vt_a(1))
    self._exec_vt_txn(['delete from vt_a'])

    # Start an Update Stream from the slave. When we get the data, go to spare.
    # That should interrupt the streaming RPC.
    replica_conn = self._get_vtgate_stream_conn()
    first = True
    txn_count = 0
    try:
      for event, resume_timestamp in replica_conn.update_stream(
          'test_keyspace', topodata_pb2.REPLICA,
          event=query_pb2.EventToken(shard='0', position=start_position),
          shard='0'):
        logging.debug('test_update_stream_interrupt got event(%d): %s',
                      resume_timestamp, event)
        if first:
          utils.run_vtctl(
              ['ChangeSlaveType', replica_tablet.tablet_alias, 'spare'])
          utils.wait_for_tablet_type(replica_tablet.tablet_alias, 'spare', 30)
          first = False
        else:
          if event.event_token.position:
            txn_count += 1

      self.assertFail('update_stream terminated with no exception')
    except dbexceptions.DatabaseError as e:
      self.assertIn('context canceled', str(e))
    self.assertFalse(first)

    logging.debug('Streamed %d transactions before exiting', txn_count)
    replica_conn.close()

  def test_log_rotation(self):
    start_position = _get_master_current_position()
    logging.debug('test_log_rotation: starting @ %s', start_position)
    position = start_position
    master_tablet.mquery('vt_test_keyspace', 'flush logs')
    self._exec_vt_txn(self._populate_vt_a(15))
    self._exec_vt_txn(['delete from vt_a'])
    master_conn = self._get_vtgate_stream_conn()
    master_txn_count = 0
    logs_correct = False
    for event, _ in master_conn.update_stream(
        'test_keyspace', topodata_pb2.MASTER,
        event=query_pb2.EventToken(shard='0', position=start_position),
        shard='0'):
      if event.event_token.position:
        master_txn_count += 1
        position = event.event_token.position
        if mysql_flavor().position_after(position, start_position):
          logs_correct = True
          logging.debug('Log rotation correctly interpreted')
          break
        if master_txn_count == 2:
          self.fail('ran out of logs')
    if not logs_correct:
      self.fail("Flush logs didn't get properly interpreted")
    master_conn.close()

  def test_timestamp_start_current_log(self):
    """Test we can start binlog streaming from the current binlog.

    Order of operation:
    - Insert something in the binlogs for tablet vt_a then delete it.
    - Get the current timestamp.
    - Wait for 4 seconds for the timestamp to change for sure.
    - Insert something else in vt_b and delete it.
    - Then we stream events starting at the original timestamp + 2, we
    should get only the vt_b events.
    """
    self._test_timestamp_start(rotate_before_sleep=False,
                               rotate_after_sleep=False)

  def test_timestamp_start_rotated_log_before_sleep(self):
    """Test we can start binlog streaming from the current rotated binlog.

    Order of operation:
    - Insert something in the binlogs for tablet vt_a then delete it.
    - Rotate the logs.
    - Get the current timestamp.
    - Wait for 4 seconds for the timestamp to change for sure.
    - Insert something else in vt_b and delete it.
    - Then we stream events starting at the original timestamp + 2, we
    should get only the vt_b events.

    In this test case, the current binlogs have a starting time stamp
    that is smaller than what we ask for, so it should just stay on it.
    """
    self._test_timestamp_start(rotate_before_sleep=True,
                               rotate_after_sleep=False)

  def test_timestamp_start_rotated_log_after_sleep(self):
    """Test we can start binlog streaming from the previous binlog.

    Order of operation:
    - Insert something in the binlogs for tablet vt_a then delete it.
    - Get the current timestamp.
    - Wait for 4 seconds for the timestamp to change for sure.
    - Rotate the logs.
    - Insert something else in vt_b and delete it.
    - Then we stream events starting at the original timestamp + 2, we
    should get only the vt_b events.

    In this test case, the current binlogs have a starting time stamp
    that is 2s higher than what we ask for, so it should go back to
    the previous binlog.
    """
    self._test_timestamp_start(rotate_before_sleep=False,
                               rotate_after_sleep=True)

  def _test_timestamp_start(self,
                            rotate_before_sleep=False,
                            rotate_after_sleep=False):
    """Common function for timestamp tests."""
    # Insert something in the binlogs for tablet vt_a then delete it.
    self._exec_vt_txn(self._populate_vt_a(1))
    self._exec_vt_txn(['delete from vt_a'])

    # (optional) Rotate the logs
    if rotate_before_sleep:
      master_tablet.mquery('vt_test_keyspace', 'flush logs')

    # Get the current timestamp.
    starting_timestamp = long(time.time())
    logging.debug('test_timestamp_start_current_log: starting @ %d',
                  starting_timestamp)

    # Wait for 4 seconds for the timestamp to change for sure.
    time.sleep(4)

    # (optional) Rotate the logs
    if rotate_after_sleep:
      master_tablet.mquery('vt_test_keyspace', 'flush logs')

    # Insert something else in vt_b and delete it.
    self._exec_vt_txn(self._populate_vt_b(1))
    self._exec_vt_txn(['delete from vt_b'])

    # make sure we only get events related to vt_b.
    master_conn = self._get_vtgate_stream_conn()
    count = 0
    for (event, resume_timestamp) in master_conn.update_stream(
        'test_keyspace', topodata_pb2.MASTER,
        timestamp=starting_timestamp+2,
        shard='0'):
      logging.debug('_test_timestamp_start: got event: %s @ %d',
                    str(event), resume_timestamp)
      # we might get a couple extra events from the rotation, ignore these.
      if not event.statements:
        continue
      if event.statements[0].category == 0:  # Statement.Category.Error
        continue
      self.assertEqual(event.statements[0].table_name, 'vt_b',
                       'got wrong event: %s' % str(event))
      count += 1
      if count == 2:
        break
    master_conn.close()

  def test_timestamp_start_too_old(self):
    """Ask the server to start streaming from a timestamp 4h ago."""
    starting_timestamp = long(time.time()) - 4*60*60
    master_conn = self._get_vtgate_stream_conn()
    try:
      for (event, resume_timestamp) in master_conn.update_stream(
          'test_keyspace', topodata_pb2.MASTER,
          timestamp=starting_timestamp,
          shard='0'):
        self.assertFail('got an event: %s %d' % (str(event), resume_timestamp))
    except dbexceptions.QueryNotServed as e:
      self.assertIn('cannot find relevant binlogs on this server',
                    str(e))


if __name__ == '__main__':
  utils.main()
