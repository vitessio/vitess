#!/usr/bin/python

import warnings
# Dropping a table inexplicably produces a warning despite
# the "IF EXISTS" clause. Squelch these warnings.
warnings.simplefilter("ignore")

import json
from optparse import OptionParser
import os
import shlex
import shutil
import signal
import socket
from subprocess import check_call, Popen, CalledProcessError, PIPE
import sys
import tablet
import time
import traceback
import threading

import MySQLdb

import utils

from vtdb import update_stream_service
from vtdb import vt_occ2

devnull = open('/dev/null', 'w')
vttop = os.environ['VTTOP']
vtroot = os.environ['VTROOT']
vtdataroot = os.environ.get('VTDATAROOT', '/vt')
hostname = socket.gethostname()
master_host = "localhost:6700"
replica_host = "localhost:6701"

master_tablet = tablet.Tablet(62344, 6700, 3700)
replica_tablet = tablet.Tablet(62345, 6701, 3702)
GLOBAL_MASTER_START_POSITION = None

class Position(object):
  RelayFilename = ""
  RelayPosition = 0
  MasterFilename = ""
  MasterPosition = 0

  def __init__(self, **kargs):
    for k, v in kargs.iteritems():
      self.__dict__[k] = v

  def encode_json(self):
    return json.dumps(self.__dict__)

  def decode_json(self, position):
    self.__dict__ = json.loads(position)
    return self


def _get_master_current_position():
  res = utils.mysql_query(62344, 'vt_test_keyspace', 'show master status')
  start_position = Position(MasterFilename=res[0][0], MasterPosition=res[0][1]).encode_json()
  return start_position


def _get_repl_current_position():
  conn = MySQLdb.Connect(user='vt_dba',
                         unix_socket=os.path.join(vtdataroot, 'vt_%010d/mysql.sock' % 62345),
                         db='vt_test_keyspace')
  cursor = MySQLdb.cursors.DictCursor(conn)
  cursor.execute('show slave status')
  res = cursor.fetchall()
  slave_dict = res[0]
  master_log = slave_dict['Relay_Master_Log_File']
  master_pos = slave_dict['Exec_Master_Log_Pos']
  relay_log = slave_dict['Relay_Log_File']
  relay_pos = slave_dict['Relay_Log_Pos']
  start_position = Position(MasterFilename=master_log, MasterPosition=master_pos, RelayFilename=relay_log, RelayPosition=relay_pos).encode_json()
  return start_position


def setup():
  utils.zk_setup()
  utils.prog_compile(['mysqlctl',
                      'vtaction',
                      'vtctl',
                      'vttablet',
                      ])

  # start mysql instance external to the test
  setup_procs = [master_tablet.start_mysql(),
                 replica_tablet.start_mysql()
                ]
  utils.wait_procs(setup_procs)
  setup_tablets()

def teardown():
  if utils.options.skip_teardown:
    return
  if utils.options.verbose:
    print "Tearing down the servers and setup"
  teardown_procs = [master_tablet.teardown_mysql(),
                    replica_tablet.teardown_mysql()]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  utils.zk_teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()
  master_tablet.remove_tree()
  replica_tablet.remove_tree()

def setup_tablets():
  # Start up a master mysql and vttablet
  if utils.options.verbose:
    print "Setting up tablets"
  utils.run_vtctl('CreateKeyspace -force /zk/global/vt/keyspaces/test_keyspace')
  master_tablet.init_tablet('master', 'test_keyspace', '0')
  utils.run_vtctl('RebuildShardGraph /zk/global/vt/keyspaces/test_keyspace/shards/0')
  utils.run_vtctl('RebuildKeyspaceGraph /zk/global/vt/keyspaces/test_keyspace')
  utils.run_vtctl('Validate /zk/global/vt/keyspaces')

  setup_schema()
  replica_tablet.create_db('vt_test_keyspace')
  #master_tablet.start_vttablet(auth=True)
  master_tablet.start_vttablet()

  replica_tablet.init_tablet('idle', start=True)
  snapshot_dir = os.path.join(vtdataroot, 'snapshot')
  utils.run("mkdir -p " + snapshot_dir)
  utils.run("chmod +w " + snapshot_dir)
  utils.run_vtctl('Clone -force %s %s' %
                  (master_tablet.zk_tablet_path, replica_tablet.zk_tablet_path))


  utils.run_vtctl('Ping /zk/test_nj/vt/tablets/0000062344')
  utils.run_vtctl('SetReadWrite ' + master_tablet.zk_tablet_path)
  utils.check_db_read_write(62344)

  utils.run_vtctl('Validate /zk/global/vt/keyspaces')
  utils.run_vtctl('Ping /zk/test_nj/vt/tablets/0000062345')


def setup_schema():
  master_tablet.create_db('vt_test_keyspace')
  global GLOBAL_MASTER_START_POSITION
  GLOBAL_MASTER_START_POSITION = _get_master_current_position()
  master_tablet.mquery('vt_test_keyspace', create_vt_insert_test)
  master_tablet.mquery('vt_test_keyspace', create_vt_a)
  master_tablet.mquery('vt_test_keyspace', create_vt_b)
  master_tablet.mquery('vt_test_keyspace', create_vt_c)


def _get_stream_connections():
  master_conn = _get_master_stream_conn()
  replica_conn =  _get_replica_stream_conn()
  return master_conn, replica_conn

def _get_master_stream_conn():
  #return update_stream_service.UpdateStreamConnection(master_host, 30, user="ala", password="ma kota")
  return update_stream_service.UpdateStreamConnection(master_host, 30)

def _get_replica_stream_conn():
  #return update_stream_service.UpdateStreamConnection(replica_host, 30, user="ala", password="ma kota")
  return update_stream_service.UpdateStreamConnection(replica_host, 30)


@utils.test_case
def run_test_service_disabled():
  start_position = _get_repl_current_position()
  if utils.options.verbose:
    print "run_test_service_disabled starting @ %s" % start_position
  _exec_vt_txn(master_host, 'vt_test_keyspace', populate_vt_insert_test)
  _exec_vt_txn(master_host, 'vt_test_keyspace', ['delete from vt_insert_test',])
  utils.run_vtctl('ChangeSlaveType /zk/test_nj/vt/tablets/0000062345 spare')
  time.sleep(20)
  replica_conn = _get_replica_stream_conn()
  replica_conn.dial()
  try:
    binlog_pos, data, err = replica_conn.stream_start(start_position)
  except Exception, e:
    if str(e) == "Update stream service is not enabled yet":
      if utils.options.verbose:
        print "Test Service Disabled: Pass"
    else:
      raise "Test Service Disabled: Fail - did not throw the correct exception"

def perform_writes(count):
  for i in xrange(count):
    _exec_vt_txn(master_host, 'vt_test_keyspace', populate_vt_insert_test)
    _exec_vt_txn(master_host, 'vt_test_keyspace', ['delete from vt_insert_test',])


@utils.test_case
def run_test_service_enabled():
  start_position = _get_repl_current_position()
  if utils.options.verbose:
    print "run_test_service_enabled starting @ %s" % start_position
  utils.run_vtctl('ChangeSlaveType /zk/test_nj/vt/tablets/0000062345 replica')
  if utils.options.verbose:
    print "sleeping a bit for the replica action to complete"
  time.sleep(30)
  thd = threading.Thread(target=perform_writes, name='write_thd', args=(400,))
  thd.daemon = True
  thd.start()
  replica_conn = _get_replica_stream_conn()
  replica_conn.dial()

  try:
    binlog_pos, data, err = replica_conn.stream_start(start_position)
    if err:
      print "Test Service Enabled: Fail"
      return
    for i in xrange(10):
      binlog_pos, data, err = replica_conn.stream_next()
      if err:
        print "Test Service Enabled: Fail"
        return
      if data['SqlType'] == 'COMMIT' and utils.options.verbose:
        print "Test Service Enabled: Pass"
        break
  except Exception, e:
    print "Exception: %s" % str(e)
    print traceback.print_exc()
  thd.join(timeout=30)

  if utils.options.verbose:
    print "Testing enable -> disable switch starting @ %s" % start_position
  replica_conn = _get_replica_stream_conn()
  replica_conn.dial()
  disabled_err = False
  txn_count = 0
  try:
    binlog_pos, data, err = replica_conn.stream_start(start_position)
    utils.run_vtctl('ChangeSlaveType /zk/test_nj/vt/tablets/0000062345 spare')
    if utils.options.verbose:
      print "Sleeping a bit for the spare action to complete"
    time.sleep(20)
    while(1):
      binlog_pos, data, err = replica_conn.stream_next()
      if err != None and err == "Disconnecting because the Update Stream service has been disabled":
        disabled_err = True
        break
      if data['SqlType'] == 'COMMIT':
        txn_count +=1

    if not disabled_err:
      print "Test Service Switch: FAIL"
      return
  except Exception, e:
    print "Exception: %s" % str(e)
    print traceback.print_exc()
  if utils.options.verbose:
    print "Streamed %d transactions before exiting" % txn_count

def _vtdb_conn(host, dbname):
  return vt_occ2.connect(host, 2, dbname=dbname)

def _exec_vt_txn(host, dbname, query_list=None):
  if not query_list:
    return
  vtdb_conn = _vtdb_conn(host, dbname)
  vtdb_cursor = vtdb_conn.cursor()
  vtdb_cursor.execute('begin', {})
  for q in query_list:
    vtdb_cursor.execute(q, {})
  vtdb_cursor.execute('commit', {})

#The function below checks the parity of streams received
#from master and replica for the same writes. Also tests
#transactions are retrieved properly.
@utils.test_case
def run_test_stream_parity():
  master_start_position = _get_master_current_position()
  replica_start_position = _get_repl_current_position()
  if utils.options.verbose:
    print "run_test_stream_parity starting @ %s" % master_start_position
  master_txn_count = 0
  replica_txn_count = 0
  _exec_vt_txn(master_host, 'vt_test_keyspace', populate_vt_a(15))
  _exec_vt_txn(master_host, 'vt_test_keyspace', populate_vt_b(14))
  _exec_vt_txn(master_host, 'vt_test_keyspace', ['delete from vt_a',])
  _exec_vt_txn(master_host, 'vt_test_keyspace', ['delete from vt_b',])
  master_conn = _get_master_stream_conn()
  master_conn.dial()
  master_tuples = []
  binlog_pos, data, err = master_conn.stream_start(master_start_position)
  if err:
    print "Test Failed %s" % err
    return
  master_tuples.append((binlog_pos, data))
  for i in xrange(21):
    binlog_pos, data, err = master_conn.stream_next()
    if err:
      print "Test Failed %s" % err
      return
    master_tuples.append((binlog_pos, data))
    if data['SqlType'] == 'COMMIT':
      master_txn_count +=1
      break
  replica_tuples = []
  replica_conn = _get_replica_stream_conn()
  replica_conn.dial()
  binlog_pos, data, err = replica_conn.stream_start(replica_start_position)
  if err:
    print "Test Failed, err: %s" % err
    return
  replica_tuples.append((binlog_pos, data))
  for i in xrange(21):
    binlog_pos, data, err = replica_conn.stream_next()
    if err:
      print "Test Failed, err %s" % err
      return
    replica_tuples.append((binlog_pos, data))
    if data['SqlType'] == 'COMMIT':
      replica_txn_count +=1
      break
  if len(master_tuples) != len(replica_tuples):
    print "Test Failed - # of records from master doesn't match replica"
    print len(master_tuples), len(replica_tuples)
  for i, val in enumerate(master_tuples):
    decoded_replica_pos = Position().decode_json(val[0]['Position'])
    decoded_master_pos = Position().decode_json(val[0]['Position'])
    if decoded_replica_pos.MasterFilename != decoded_master_pos.MasterFilename or \
      decoded_replica_pos.MasterPosition != decoded_master_pos.MasterPosition:
      print "Test Failed, master data: %s replica data: %s" % (val[1], replica_tuples[i][1])
    if val[1] != replica_tuples[i][1]:
      print "Test Failed, master data: %s replica data: %s" % (val[1], replica_tuples[i][1])
  if utils.options.verbose:
    print "Test Writes: PASS"


@utils.test_case
def run_test_ddl():
  global GLOBAL_MASTER_START_POSITION
  start_position = GLOBAL_MASTER_START_POSITION
  if utils.options.verbose:
    print "run_test_ddl: starting @ %s" % start_position
  master_conn = _get_master_stream_conn()
  master_conn.dial()
  binlog_pos, data, err = master_conn.stream_start(start_position)
  if err:
    print "Test Failed, err: %s" % err
    return

  decoded_start_pos = Position().decode_json(start_position)
  decoded_binlog_pos = Position().decode_json(binlog_pos['Position'])
  if decoded_start_pos.MasterFilename != decoded_binlog_pos.MasterFilename and decoded_start_pos.MasterPosition != decoded_start_pos.MasterPosition:
    print "Test Failed: Received position %s doesn't match the start position %s" % (pos, start_position)
    return
  if data['Sql'] != create_vt_insert_test.replace('\n', ''):
    print "Test Failed: DDL %s didn't match the original %s" % (data['Sql'], create_vt_insert_test)
    return
  if utils.options.verbose:
    print "Test DDL: PASS"

#This tests the service switch from disable -> enable -> disable
def run_test_service_switch():
  run_test_service_disabled()
  run_test_service_enabled()

@utils.test_case
def run_test_log_rotation():
  start_position = _get_master_current_position()
  decoded_start_position = Position().decode_json(start_position)
  master_tablet.mquery('vt_test_keyspace', "flush logs")
  _exec_vt_txn(master_host, 'vt_test_keyspace', populate_vt_a(15))
  _exec_vt_txn(master_host, 'vt_test_keyspace', ['delete from vt_a',])
  master_conn = _get_master_stream_conn()
  master_conn.dial()
  binlog_pos, data, err = master_conn.stream_start(start_position)
  if err:
    print "Encountered error in fetching stream: %s" % err
    return
  master_txn_count = 0
  while master_txn_count <=2:
    binlog_pos, data, err = master_conn.stream_next()
    if err:
      print "Encountered error in fetching stream: %s" % err
      return
    decoded_pos = Position().decode_json(binlog_pos['Position'])
    if decoded_start_position.MasterFilename < decoded_pos.MasterFilename:
      if utils.options.verbose:
        print "Log rotation correctly interpreted"
      break
    if data['SqlType'] == 'COMMIT':
      master_txn_count +=1


def run_all():
  run_test_service_switch()
  #The above test leaves the service in disabled state, hence enabling it.
  utils.run_vtctl('ChangeSlaveType /zk/test_nj/vt/tablets/0000062345 replica')
  if utils.options.verbose:
    print "Sleeping a bit for the action to complete"
  time.sleep(20)
  run_test_ddl()
  run_test_stream_parity()
  run_test_log_rotation()

create_vt_insert_test = '''create table vt_insert_test (
id bigint auto_increment,
msg varchar(64),
primary key (id)
) Engine=InnoDB'''

populate_vt_insert_test = [
    "insert into vt_insert_test (msg) values ('test %s')" % x
    for x in xrange(4)]

create_vt_a = '''create table vt_a (
eid bigint,
id int,
primary key(eid, id)
) Engine=InnoDB'''

def populate_vt_a(count):
  return ["insert into vt_a (eid, id) values (%d, %d)" % (x, x)
    for x in xrange(count+1) if x >0]

create_vt_b = '''create table vt_b (
eid bigint,
name varchar(128),
foo varbinary(128),
primary key(eid, name)
) Engine=InnoDB'''

def populate_vt_b(count):
  return ["insert into vt_b (eid, name, foo) values (%d, 'name %s', 'foo %s')" % (x, x, x)
    for x in xrange(count)]

create_vt_c = '''create table vt_c (
eid bigint auto_increment,
id int default 1,
name varchar(128) default 'name',
foo varchar(128),
primary key(eid, id, name)
) Engine=InnoDB'''

def populate_vt_c(count):
  return ["insert into vt_c (foo) values ('foo %s')" % x
    for x in xrange(count)]


create_vt_select_test = '''create table vt_select_test (
id bigint auto_increment,
msg varchar(64),
primary key (id)
) Engine=InnoDB'''

populate_vt_select_test = [
    "insert into vt_select_test (msg) values ('test %s')" % x
    for x in xrange(4)]


def main():
  parser = OptionParser()
  parser.add_option('-v', '--verbose', action='store_true')
  parser.add_option('-d', '--debug', action='store_true')
  parser.add_option('--skip-teardown', action='store_true')
  (utils.options, args) = parser.parse_args()

  if not args:
    args = ['run_all']

  try:
    if args[0] != 'teardown':
      setup()
      if args[0] != 'setup':
        for arg in args:
          globals()[arg]()
          print "GREAT SUCCESS"
  except KeyboardInterrupt:
    pass
  except utils.Break:
    utils.options.skip_teardown = True
  finally:
    teardown()


if __name__ == '__main__':
  main()
