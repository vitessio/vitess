#!/usr/bin/env python

import logging
import traceback
import threading
import unittest

import environment
import tablet
import utils
from vtdb import dbexceptions
from vtdb import update_stream
from vtdb import vtgate_client
from mysql_flavor import mysql_flavor
from protocols_flavor import protocols_flavor

master_tablet = tablet.Tablet()
replica_tablet = tablet.Tablet()
master_host = 'localhost:%d' % master_tablet.port

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
    setup_procs = [master_tablet.init_mysql(),
                   replica_tablet.init_mysql()]
    utils.wait_procs(setup_procs)

    # start a vtctld so the vtctl insert commands are just RPCs, not forks
    utils.Vtctld().start()

    # Start up a master mysql and vttablet
    logging.debug('Setting up tablets')
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])
    master_tablet.init_tablet('master', 'test_keyspace', '0', tablet_index=0)
    replica_tablet.init_tablet('replica', 'test_keyspace', '0', tablet_index=1)
    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)
    utils.validate_topology()
    master_tablet.create_db('vt_test_keyspace')
    master_tablet.create_db('other_database')
    replica_tablet.create_db('vt_test_keyspace')
    replica_tablet.create_db('other_database')

    master_tablet.start_vttablet(wait_for_state=None)
    replica_tablet.start_vttablet(wait_for_state=None)
    master_tablet.wait_for_vttablet_state('SERVING')
    replica_tablet.wait_for_vttablet_state('NOT_SERVING')

    for t in [master_tablet, replica_tablet]:
      t.reset_replication()
    utils.run_vtctl(['InitShardMaster', 'test_keyspace/0',
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

    utils.run_vtctl(['ReloadSchema', master_tablet.tablet_alias])
    utils.run_vtctl(['ReloadSchema', replica_tablet.tablet_alias])
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

  def _get_master_stream_conn(self):
    protocol, endpoint = master_tablet.update_stream_python_endpoint()
    return update_stream.connect(protocol, endpoint, 30)

  def _get_replica_stream_conn(self):
    protocol, endpoint = replica_tablet.update_stream_python_endpoint()
    return update_stream.connect(protocol, endpoint, 30)

  def _test_service_disabled(self):
    # it looks like update stream would be re-enabled automatically
    # because of vttablet health check
    return
    start_position = _get_repl_current_position()
    logging.debug('_test_service_disabled starting @ %s', start_position)
    self._exec_vt_txn(self._populate_vt_insert_test)
    self._exec_vt_txn(['delete from vt_insert_test'])
    utils.run_vtctl(['ChangeSlaveType', replica_tablet.tablet_alias, 'spare'])
    utils.wait_for_tablet_type(replica_tablet.tablet_alias, 'spare')
    logging.debug('dialing replica update stream service')
    replica_conn = self._get_replica_stream_conn()
    try:
      for _ in replica_conn.stream_update(start_position):
        break
    except dbexceptions.DatabaseError as e:
      self.assertIn('update stream service is not enabled', str(e))
    replica_conn.close()

    v = utils.get_vars(replica_tablet.port)
    if v['UpdateStreamState'] != 'Disabled':
      self.fail("Update stream service should be 'Disabled' but is '%s'" %
                v['UpdateStreamState'])

  def perform_writes(self, count):
    for _ in xrange(count):
      self._exec_vt_txn(self._populate_vt_insert_test)
      self._exec_vt_txn(['delete from vt_insert_test'])

  def _test_service_enabled(self):
    # it looks like update stream would be re-enabled automatically
    # because of vttablet health check
    return
    start_position = _get_repl_current_position()
    logging.debug('_test_service_enabled starting @ %s', start_position)
    utils.run_vtctl(
        ['ChangeSlaveType', replica_tablet.tablet_alias, 'replica'])
    logging.debug('sleeping a bit for the replica action to complete')
    utils.wait_for_tablet_type(replica_tablet.tablet_alias, 'replica', 30)
    thd = threading.Thread(target=self.perform_writes, name='write_thd',
                           args=(100,))
    thd.daemon = True
    thd.start()
    replica_conn = self._get_replica_stream_conn()

    try:
      for stream_event in replica_conn.stream_update(start_position):
        if stream_event.category == update_stream.StreamEvent.DML:
          logging.debug('Test Service Enabled: Pass')
          break
    except dbexceptions.DatabaseError as e:
      self.fail('Exception in getting stream from replica: %s\n Traceback %s' %
                (str(e), traceback.format_exc()))
    thd.join(timeout=30)
    replica_conn.close()

    v = utils.get_vars(replica_tablet.port)
    if v['UpdateStreamState'] != 'Enabled':
      self.fail("Update stream service should be 'Enabled' but is '%s'" %
                v['UpdateStreamState'])
    self.assertIn('SE_DML', v['UpdateStreamEvents'])
    self.assertIn('SE_POS', v['UpdateStreamEvents'])

    logging.debug('Testing enable -> disable switch starting @ %s',
                  start_position)
    replica_conn = self._get_replica_stream_conn()
    first = True
    txn_count = 0
    try:
      for stream_event in replica_conn.stream_update(start_position):
        if first:
          utils.run_vtctl(
              ['ChangeSlaveType', replica_tablet.tablet_alias, 'spare'])
          utils.wait_for_tablet_type(replica_tablet.tablet_alias, 'spare', 30)
          first = False
        else:
          if stream_event.category == update_stream.StreamEvent.POS:
            txn_count += 1
        # FIXME(alainjobart) gasp, the test fails but we don't assert?
        logging.debug('Test Service Switch: FAIL')
        replica_conn.close()
        return
    except dbexceptions.DatabaseError as e:
      self.assertEqual(
          'Fatal Service Error: Disconnecting because the Update Stream '
          'service has been disabled',
          str(e))
    except Exception as e:
      logging.error('Exception: %s', str(e))
      logging.error('Traceback: %s', traceback.format_exc())
      self.fail("Update stream returned error '%s'" % str(e))
    logging.debug('Streamed %d transactions before exiting', txn_count)
    replica_conn.close()

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

    global master_start_position

    timeout = 30
    while True:
      master_start_position = _get_master_current_position()
      replica_start_position = _get_repl_current_position()
      if master_start_position == replica_start_position:
        break
      timeout = utils.wait_step(
          '%s == %s' % (master_start_position, replica_start_position),
          timeout
      )
    logging.debug('run_test_stream_parity starting @ %s',
                  master_start_position)
    self._exec_vt_txn(self._populate_vt_a(15))
    self._exec_vt_txn(self._populate_vt_b(14))
    self._exec_vt_txn(['delete from vt_a'])
    self._exec_vt_txn(['delete from vt_b'])
    master_conn = self._get_master_stream_conn()
    master_events = []
    for stream_event in master_conn.stream_update(master_start_position):
      master_events.append(stream_event)
      if stream_event.category == update_stream.StreamEvent.POS:
        break
    replica_events = []
    replica_conn = self._get_replica_stream_conn()
    for stream_event in replica_conn.stream_update(replica_start_position):
      replica_events.append(stream_event)
      if stream_event.category == update_stream.StreamEvent.POS:
        break
    if len(master_events) != len(replica_events):
      logging.debug(
          'Test Failed - # of records mismatch, master %s replica %s',
          master_events, replica_events)
    for master_val, replica_val in zip(master_events, replica_events):
      master_data = master_val.__dict__
      replica_data = replica_val.__dict__
      # the timestamp is from when the event was written to the binlogs.
      # the master uses the timestamp of when it wrote it originally,
      # the slave of when it applied the logs. These can differ and make this
      # test flaky. So we just blank them out, easier. We really want to
      # compare the replication positions.
      master_data['timestamp'] = 'XXX'
      replica_data['timestamp'] = 'XXX'
      self.assertEqual(
          master_data, replica_data,
          "Test failed, data mismatch - master '%s' and replica position '%s'" %
          (master_data, replica_data))
    master_conn.close()
    replica_conn.close()
    logging.debug('Test Writes: PASS')

  def test_ddl(self):
    start_position = master_start_position
    logging.debug('test_ddl: starting @ %s', start_position)
    master_conn = self._get_master_stream_conn()
    for stream_event in master_conn.stream_update(start_position):
      self.assertEqual(stream_event.sql, _create_vt_insert_test,
                       "DDL didn't match original")
      master_conn.close()
      return
    self.fail("didn't get right sql")

  def test_set_insert_id(self):
    start_position = _get_master_current_position()
    self._exec_vt_txn(
        ['SET INSERT_ID=1000000'] + self._populate_vt_insert_test)
    logging.debug('test_set_insert_id: starting @ %s', start_position)
    master_conn = self._get_master_stream_conn()
    expected_id = 1000000
    for stream_event in master_conn.stream_update(start_position):
      if stream_event.category == update_stream.StreamEvent.POS:
        break
      self.assertEqual(stream_event.fields[0], 'id')
      self.assertEqual(stream_event.rows[0][0], expected_id)
      expected_id += 1
    if expected_id != 1000004:
      self.fail('did not get my four values!')
    master_conn.close()

  def test_database_filter(self):
    start_position = _get_master_current_position()
    master_tablet.mquery('other_database', _create_vt_insert_test)
    self._exec_vt_txn(self._populate_vt_insert_test)
    logging.debug('test_database_filter: starting @ %s', start_position)
    master_conn = self._get_master_stream_conn()
    for stream_event in master_conn.stream_update(start_position):
      if stream_event.category == update_stream.StreamEvent.POS:
        break
      self.assertNotEqual(
          stream_event.category, update_stream.StreamEvent.DDL,
          "query using other_database wasn't filted out")
    master_conn.close()

  def test_service_switch(self):
    """tests the service switch from disable -> enable -> disable."""
    self._test_service_disabled()
    self._test_service_enabled()
    # The above tests leaves the service in disabled state, hence enabling it.
    utils.run_vtctl(
        ['ChangeSlaveType', replica_tablet.tablet_alias, 'replica'])
    utils.wait_for_tablet_type(replica_tablet.tablet_alias, 'replica', 30)

  def test_log_rotation(self):
    start_position = _get_master_current_position()
    position = start_position
    master_tablet.mquery('vt_test_keyspace', 'flush logs')
    self._exec_vt_txn(self._populate_vt_a(15))
    self._exec_vt_txn(['delete from vt_a'])
    master_conn = self._get_master_stream_conn()
    master_txn_count = 0
    logs_correct = False
    for stream_event in master_conn.stream_update(start_position):
      if stream_event.category == update_stream.StreamEvent.POS:
        master_txn_count += 1
        position = mysql_flavor(
        ).position_append(position, stream_event.transaction_id)
        if mysql_flavor().position_after(position, start_position):
          logs_correct = True
          logging.debug('Log rotation correctly interpreted')
          break
        if master_txn_count == 2:
          self.fail('ran out of logs')
    if not logs_correct:
      self.fail("Flush logs didn't get properly interpreted")
    master_conn.close()

if __name__ == '__main__':
  utils.main()
