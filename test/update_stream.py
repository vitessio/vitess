#!/usr/bin/python

import warnings
# Dropping a table inexplicably produces a warning despite
# the "IF EXISTS" clause. Squelch these warnings.
warnings.simplefilter("ignore")

import logging
import os
import time
import traceback
import threading
import unittest

import MySQLdb

import tablet
import utils
from vtdb import update_stream_service
from vtdb import vtclient

master_tablet = tablet.Tablet(62344)
replica_tablet = tablet.Tablet(62345)
master_host = "localhost:%u" % master_tablet.port
replica_host = "localhost:%u" % replica_tablet.port
GLOBAL_MASTER_START_POSITION = None

def _get_master_current_position():
  res = utils.mysql_query(62344, 'vt_test_keyspace', 'show master status')
  start_position = update_stream_service.Coord(res[0][0], res[0][1])
  return start_position.__dict__


def _get_repl_current_position():
  conn = MySQLdb.Connect(user='vt_dba',
                         unix_socket=os.path.join(utils.vtdataroot, 'vt_%010d/mysql.sock' % 62345),
                         db='vt_test_keyspace')
  cursor = MySQLdb.cursors.DictCursor(conn)
  cursor.execute('show master status')
  res = cursor.fetchall()
  slave_dict = res[0]
  master_log = slave_dict['File']
  master_pos = slave_dict['Position']
  start_position = update_stream_service.Coord(master_log, master_pos)
  return start_position.__dict__


def setUpModule():
  try:
    utils.zk_setup()

  # start mysql instance external to the test
    setup_procs = [master_tablet.init_mysql(),
                   replica_tablet.init_mysql()
                   ]
    utils.wait_procs(setup_procs)
    setup_tablets()
  except:
    tearDownModule()
    raise


def tearDownModule():
  if utils.options.skip_teardown:
    return
  logging.debug("Tearing down the servers and setup")
  teardown_procs = [master_tablet.teardown_mysql(),
                    replica_tablet.teardown_mysql()]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  utils.zk_teardown()
  tablet.Tablet.tablets_running = 2
  master_tablet.kill_vttablet()
  replica_tablet.kill_vttablet()
  utils.kill_sub_processes()
  utils.remove_tmp_files()
  master_tablet.remove_tree()
  replica_tablet.remove_tree()

def setup_tablets():
  # Start up a master mysql and vttablet
  logging.debug("Setting up tablets")
  utils.run_vtctl('CreateKeyspace test_keyspace')
  master_tablet.init_tablet('master', 'test_keyspace', '0')
  utils.run_vtctl('RebuildShardGraph test_keyspace/0')
  utils.run_vtctl('RebuildKeyspaceGraph test_keyspace')
  utils.validate_topology()

  setup_schema()
  replica_tablet.create_db('vt_test_keyspace')
  #master_tablet.start_vttablet(auth=True)
  master_tablet.start_vttablet()

  replica_tablet.init_tablet('idle', 'test_keyspace', start=True)
  snapshot_dir = os.path.join(utils.vtdataroot, 'snapshot')
  utils.run("mkdir -p " + snapshot_dir)
  utils.run("chmod +w " + snapshot_dir)
  utils.run_vtctl('Clone -force %s %s' %
                  (master_tablet.tablet_alias, replica_tablet.tablet_alias))


  utils.run_vtctl('Ping test_nj-0000062344')
  utils.run_vtctl('SetReadWrite ' + master_tablet.tablet_alias)
  utils.check_db_read_write(62344)

  utils.validate_topology()
  utils.run_vtctl('Ping test_nj-0000062345')

  # reset counter so tests don't assert
  tablet.Tablet.tablets_running = 0

def setup_schema():
  master_tablet.create_db('vt_test_keyspace')
  global GLOBAL_MASTER_START_POSITION
  GLOBAL_MASTER_START_POSITION = _get_master_current_position()
  master_tablet.mquery('vt_test_keyspace', _create_vt_insert_test)
  master_tablet.mquery('vt_test_keyspace', _create_vt_a)
  master_tablet.mquery('vt_test_keyspace', _create_vt_b)
  master_tablet.mquery('vt_test_keyspace', _create_vt_c)


class TestUpdateStream(unittest.TestCase):

  def _get_master_stream_conn(self):
    #return update_stream_service.UpdateStreamConnection(master_host, 30, user="ala", password="ma kota")
    return update_stream_service.UpdateStreamConnection(master_host, 30)

  def _get_replica_stream_conn(self):
    #return update_stream_service.UpdateStreamConnection(replica_host, 30, user="ala", password="ma kota")
    return update_stream_service.UpdateStreamConnection(replica_host, 30)


  def _test_service_disabled(self):
    start_position = _get_repl_current_position()
    logging.debug("_test_service_disabled starting @ %s" % start_position)
    self._exec_vt_txn(master_host, _populate_vt_insert_test)
    self._exec_vt_txn(master_host, ['delete from vt_insert_test',])
    utils.run_vtctl('ChangeSlaveType test_nj-0000062345 spare')
    #  time.sleep(20)
    replica_conn = self._get_replica_stream_conn()
    logging.debug("dialing replica update stream service")
    replica_conn.dial()
    try:
      binlog_pos, data, err = replica_conn.stream_start(start_position)
    except Exception, e:
      logging.debug(str(e))
      if str(e) == "Update stream service is not enabled yet":
        logging.debug("Test Service Disabled: Pass")
      else:
        self.fail("Test Service Disabled: Fail - did not throw the correct exception")

    v = utils.get_vars(replica_tablet.port)
    if v['UpdateStreamState']['Current'] != 'Disabled':
      self.fail("Update stream service should be 'Disabled' but is '%s'" % v['UpdateStreamState']['Current'])

  def perform_writes(self, count):
    for i in xrange(count):
      self._exec_vt_txn(master_host, _populate_vt_insert_test)
      self._exec_vt_txn(master_host, ['delete from vt_insert_test',])


  def _test_service_enabled(self):
    start_position = _get_repl_current_position()
    logging.debug("_test_service_enabled starting @ %s" % start_position)
    utils.run_vtctl('ChangeSlaveType test_nj-0000062345 replica')
    logging.debug("sleeping a bit for the replica action to complete")
    time.sleep(10)
    thd = threading.Thread(target=self.perform_writes, name='write_thd', args=(400,))
    thd.daemon = True
    thd.start()
    replica_conn = self._get_replica_stream_conn()
    replica_conn.dial()

    try:
      binlog_pos, data, err = replica_conn.stream_start(start_position)
      if err:
        raise utils.TestError("Update stream returned error '%s'", err)
      for i in xrange(10):
        binlog_pos, data, err = replica_conn.stream_next()
        if err:
          raise utils.TestError("Update stream returned error '%s'", err)
        if data['SqlType'] == 'COMMIT' and utils.options.verbose == 2:
          logging.debug("Test Service Enabled: Pass")
          break
    except Exception, e:
      raise utils.TestError("Exception in getting stream from replica: %s\n Traceback %s",str(e), traceback.print_exc())
    thd.join(timeout=30)

    v = utils.get_vars(replica_tablet.port)
    if v['UpdateStreamState']['Current'] != 'Enabled':
      self.fail("Update stream service should be 'Enabled' but is '%s'" % v['UpdateStreamState']['Current'] )

    logging.debug("Testing enable -> disable switch starting @ %s" % start_position)
    replica_conn = self._get_replica_stream_conn()
    replica_conn.dial()
    disabled_err = False
    txn_count = 0
    try:
      binlog_pos, data, err = replica_conn.stream_start(start_position)
      utils.run_vtctl('ChangeSlaveType test_nj-0000062345 spare')
      #logging.debug("Sleeping a bit for the spare action to complete")
      #time.sleep(20)
      while(1):
        binlog_pos, data, err = replica_conn.stream_next()
        if err is not None and err == "Fatal Service Error: Disconnecting because the Update Stream service has been disabled":
          disabled_err = True
          break
        if data is not None and data['SqlType'] == 'COMMIT':
          txn_count +=1

      if not disabled_err:
        logging.error("Test Service Switch: FAIL")
        return
    except Exception, e:
      logging.error("Exception: %s", str(e))
      logging.error("Traceback: %s", traceback.print_exc())
      raise utils.TestError("Update stream returned error '%s'", str(e))
    logging.debug("Streamed %d transactions before exiting" % txn_count)

  def _vtdb_conn(self, host):
    return vtclient.connect(host, 'test_keyspace', '0', 2)

  def _exec_vt_txn(self, host, query_list=None):
    if not query_list:
      return
    vtdb_conn = self._vtdb_conn(host)
    vtdb_cursor = vtdb_conn.cursor()
    vtdb_conn.begin()
    for q in query_list:
      vtdb_cursor.execute(q, {})
    vtdb_conn.commit()

  #The function below checks the parity of streams received
  #from master and replica for the same writes. Also tests
  #transactions are retrieved properly.
  def test_stream_parity(self):
    master_start_position = _get_master_current_position()
    replica_start_position = _get_repl_current_position()
    logging.debug("run_test_stream_parity starting @ %s" % master_start_position)
    master_txn_count = 0
    replica_txn_count = 0
    self._exec_vt_txn(master_host, _populate_vt_a(15))
    self._exec_vt_txn(master_host, _populate_vt_b(14))
    self._exec_vt_txn(master_host, ['delete from vt_a',])
    self._exec_vt_txn(master_host, ['delete from vt_b',])
    master_conn = self._get_master_stream_conn()
    master_conn.dial()
    master_tuples = []
    binlog_pos, data, err = master_conn.stream_start(master_start_position)
    if err:
      raise utils.TestError("Update stream returned error '%s'", err)
    master_tuples.append((binlog_pos, data))
    for i in xrange(21):
      binlog_pos, data, err = master_conn.stream_next()
      if err:
        raise utils.TestError("Update stream returned error '%s'", err)
      master_tuples.append((binlog_pos, data))
      if data['SqlType'] == 'COMMIT':
        master_txn_count +=1
        break
    replica_tuples = []
    replica_conn = self._get_replica_stream_conn()
    replica_conn.dial()
    binlog_pos, data, err = replica_conn.stream_start(replica_start_position)
    if err:
      raise utils.TestError("Update stream returned error '%s'", err)
    replica_tuples.append((binlog_pos, data))
    for i in xrange(21):
      binlog_pos, data, err = replica_conn.stream_next()
      if err:
        raise utils.TestError("Update stream returned error '%s'", err)
      replica_tuples.append((binlog_pos, data))
      if data['SqlType'] == 'COMMIT':
        replica_txn_count +=1
        break
    if len(master_tuples) != len(replica_tuples):
      logging.debug("Test Failed - # of records mismatch, master %s replica %s" % (master_tuples, replica_tuples))
    for master_val, replica_val in zip(master_tuples, replica_tuples):
      master_data = master_val[1]
      replica_data = replica_val[1]
      self.assertEqual(master_data, replica_data, "Test failed, data mismatch - master '%s' and replica position '%s'" % (master_data, replica_data))
    logging.debug("Test Writes: PASS")


  def test_ddl(self):
    global GLOBAL_MASTER_START_POSITION
    start_position = GLOBAL_MASTER_START_POSITION
    logging.debug("run_test_ddl: starting @ %s" % start_position)
    master_conn = self._get_master_stream_conn()
    master_conn.dial()
    binlog_pos, data, err = master_conn.stream_start(start_position)
    if err:
      raise utils.TestError("Update stream returned error '%s'", err)

    if data['Sql'] != _create_vt_insert_test.replace('\n', ''):
      raise utils.TestError("Test Failed: DDL %s didn't match the original %s" % (data['Sql'], _create_vt_insert_test))
    logging.debug("Test DDL: PASS")

  #This tests the service switch from disable -> enable -> disable
  def test_service_switch(self):
    self._test_service_disabled()
    self._test_service_enabled()
    # The above tests leaves the service in disabled state, hence enabling it.
    utils.run_vtctl('ChangeSlaveType test_nj-0000062345 replica')

  def test_log_rotation(self):
    start_position = _get_master_current_position()
    master_tablet.mquery('vt_test_keyspace', "flush logs")
    self._exec_vt_txn(master_host, _populate_vt_a(15))
    self._exec_vt_txn(master_host, ['delete from vt_a',])
    master_conn = self._get_master_stream_conn()
    master_conn.dial()
    binlog_pos, data, err = master_conn.stream_start(start_position)
    if err:
      raise utils.TestError("Update stream returned error '%s'", err)
    master_txn_count = 0
    logs_correct = False
    while master_txn_count <=2:
      binlog_pos, data, err = master_conn.stream_next()
      if err:
        raise utils.TestError("Update stream returned error '%s'", err)
      if start_position['Position']['MasterFilename'] < binlog_pos['Position']['MasterFilename']:
        logs_correct = True
        logging.debug("Log rotation correctly interpreted")
        break
      if data['SqlType'] == 'COMMIT':
        master_txn_count +=1
    if not logs_correct:
      self.fail("Flush logs didn't get properly interpreted")

_create_vt_insert_test = '''create table vt_insert_test (
id bigint auto_increment,
msg varchar(64),
primary key (id)
) Engine=InnoDB'''

_populate_vt_insert_test = [
    "insert into vt_insert_test (msg) values ('test %s')" % x
    for x in xrange(4)]

_create_vt_a = '''create table vt_a (
eid bigint,
id int,
primary key(eid, id)
) Engine=InnoDB'''

def _populate_vt_a(count):
  return ["insert into vt_a (eid, id) values (%d, %d)" % (x, x)
          for x in xrange(count+1) if x >0]

_create_vt_b = '''create table vt_b (
eid bigint,
name varchar(128),
foo varbinary(128),
primary key(eid, name)
) Engine=InnoDB'''

def _populate_vt_b(count):
  return ["insert into vt_b (eid, name, foo) values (%d, 'name %s', 'foo %s')" % (x, x, x)
          for x in xrange(count)]

_create_vt_c = '''create table vt_c (
eid bigint auto_increment,
id int default 1,
name varchar(128) default 'name',
foo varchar(128),
primary key(eid, id, name)
) Engine=InnoDB'''

if __name__ == '__main__':
  utils.main()
