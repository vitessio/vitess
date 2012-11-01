#!/usr/bin/python

import json
from optparse import OptionParser
import os
import socket
from subprocess import check_call, Popen, CalledProcessError, PIPE
import tempfile

from zk import zkocc

import utils
import tablet

vttop = os.environ['VTTOP']
vtroot = os.environ['VTROOT']
hostname = socket.gethostname()

# range 0000000000000000 - 8000000000000000
shard_0_master = tablet.Tablet()
shard_0_replica  = tablet.Tablet()
# range 8000000000000000 - FFFFFFFFFFFFFFFF
shard_1_master = tablet.Tablet()
shard_1_replica  = tablet.Tablet()

def setup():
  utils.prog_compile(['zkocc'])
  utils.zk_setup()

  setup_procs = [
      shard_0_master.start_mysql(),
      shard_0_replica.start_mysql(),
      shard_1_master.start_mysql(),
      shard_1_replica.start_mysql(),
      ]
  utils.wait_procs(setup_procs)

def teardown():
  if utils.options.skip_teardown:
    return

  teardown_procs = [
      shard_0_master.teardown_mysql(),
      shard_0_replica.teardown_mysql(),
      shard_1_master.teardown_mysql(),
      shard_1_replica.teardown_mysql(),
      ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  utils.zk_teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  shard_0_master.remove_tree()
  shard_0_replica.remove_tree()
  shard_1_master.remove_tree()
  shard_1_replica.remove_tree()

# both shards will have similar tables, but with different column order,
# so we can test column mismatches by doing a 'select *',
# and also check the good case by doing a 'select id, msg'
create_vt_select_test = '''create table vt_select_test (
id bigint not null,
msg varchar(64),
primary key (id)
) Engine=InnoDB'''

create_vt_select_test_reverse = '''create table vt_select_test (
msg varchar(64),
id bigint not null,
primary key (id)
) Engine=InnoDB'''

def check_rows(to_look_for, driver="vtdb"):
  out, err = utils.vttablet_query(0, "/zk/test_nj/vt/ns/test_keyspace/master", "select id, msg from vt_select_test", driver=driver, verbose=True)
  for pattern in to_look_for:
    if err.find(pattern) == -1:
      print "vttablet_query returned:"
      print out
      print err
      raise utils.TestError('wrong vtclient2 output, missing: ' + pattern)
  if utils.options.verbose:
    print out, err

def check_rows_schema_diff(driver):
  out, err = utils.vttablet_query(0, "/zk/test_nj/vt/ns/test_keyspace/master", "select * from vt_select_test", driver=driver, verbose=False, raise_on_error=False)
  if (err.find("column[0] name mismatch: id != msg") == -1 and
      err.find("column[0] name mismatch: msg != id") == -1):
    print "vttablet_query returned:"
    print out
    print err
    raise utils.TestError('wrong vtclient2 output, missing "name mismatch" of some kind')
  if utils.options.verbose:
    print out, err


def run_test_sharding():

  utils.run_vtctl('-force CreateKeyspace /zk/global/vt/keyspaces/test_keyspace')

  shard_0_master.init_tablet( 'master',  'test_keyspace', '0000000000000000-8000000000000000', key_end='8000000000000000')
  shard_0_replica.init_tablet('replica', 'test_keyspace', '0000000000000000-8000000000000000', key_end='8000000000000000')
  shard_1_master.init_tablet( 'master',  'test_keyspace', '8000000000000000-FFFFFFFFFFFFFFFF', key_start='8000000000000000')
  shard_1_replica.init_tablet('replica', 'test_keyspace', '8000000000000000-FFFFFFFFFFFFFFFF', key_start='8000000000000000')

  utils.run_vtctl('RebuildShardGraph /zk/global/vt/keyspaces/test_keyspace/shards/0000000000000000-8000000000000000')
  utils.run_vtctl('RebuildShardGraph /zk/global/vt/keyspaces/test_keyspace/shards/8000000000000000-FFFFFFFFFFFFFFFF')

  utils.run_vtctl('RebuildKeyspaceGraph /zk/global/vt/keyspaces/test_keyspace')

  # run checks now before we start the tablets
  utils.zk_check()

  # create databases and schema so tablets are good to go
  shard_0_master.populate('vt_test_keyspace', create_vt_select_test)
  shard_0_replica.populate('vt_test_keyspace', create_vt_select_test)
  shard_1_master.populate('vt_test_keyspace', create_vt_select_test_reverse)
  shard_1_replica.populate('vt_test_keyspace', create_vt_select_test_reverse)

  # start the tablets
  shard_0_master.start_vttablet()
  shard_0_replica.start_vttablet()
  shard_1_master.start_vttablet()
  shard_1_replica.start_vttablet()

  # start zkocc, we'll use it later
  zkocc = utils.run_bg(vtroot+'/bin/zkocc -port=14850 test_nj')

  utils.run_vtctl('-force ReparentShard /zk/global/vt/keyspaces/test_keyspace/shards/0000000000000000-8000000000000000 ' + shard_0_master.zk_tablet_path)
  utils.run_vtctl('-force ReparentShard /zk/global/vt/keyspaces/test_keyspace/shards/8000000000000000-FFFFFFFFFFFFFFFF ' + shard_1_master.zk_tablet_path)

  # insert some values directly (db is RO after minority reparent)
  # FIXME(alainjobart) these values don't match the shard map
  utils.run_vtctl('SetReadWrite ' + shard_0_master.zk_tablet_path)
  utils.run_vtctl('SetReadWrite ' + shard_1_master.zk_tablet_path)
  shard_0_master.mquery('vt_test_keyspace', "insert into vt_select_test (id, msg) values (1, 'test 1')", write=True)
  shard_1_master.mquery('vt_test_keyspace', "insert into vt_select_test (id, msg) values (10, 'test 10')", write=True)

  utils.zk_check(ping_tablets=True)

  utils.pause("Before the sql scatter query")

  # note the order of the rows is not guaranteed, as the go routines
  # doing the work can go out of order
  check_rows(["Index\tid\tmsg",
              "1\ttest 1",
              "10\ttest 10"])

  # write a value, re-read them all
  out, err = utils.vttablet_query(3803, "/zk/test_nj/vt/ns/test_keyspace/master", "insert into vt_select_test (id, msg) values (2, 'test 2')", driver="vtdb", verbose=True)
  print out
  print err

  check_rows(["Index\tid\tmsg",
              "1\ttest 1",
              "2\ttest 2",
              "10\ttest 10"])

  # make sure the '2' value was written on first shard
  rows = shard_0_master.mquery('vt_test_keyspace', "select id, msg from vt_select_test order by id")
  if (len(rows) != 2 or \
        rows[0][0] != 1 or \
        rows[1][0] != 2):
    print "mysql_query returned:", rows
    raise utils.TestError('wrong mysql_query output')

  utils.pause("After db writes")

  # now use zkocc or streaming or both for the same query
  check_rows(["Index\tid\tmsg",
              "1\ttest 1",
              "2\ttest 2",
              "10\ttest 10"],
             driver="vtdb-zkocc")
  check_rows(["Index\tid\tmsg",
              "1\ttest 1",
              "2\ttest 2",
              "10\ttest 10"],
             driver="vtdb-streaming")
  check_rows(["Index\tid\tmsg",
              "1\ttest 1",
              "2\ttest 2",
              "10\ttest 10"],
             driver="vtdb-zkocc-streaming")

  # make sure the schema checking works
  check_rows_schema_diff("vtdb-zkocc")
  check_rows_schema_diff("vtdb")

  # throw in some schema validation step
  # we created the schema differently, so it should show
  utils.run_vtctl('ValidateSchemaShard /zk/global/vt/keyspaces/test_keyspace/shards/0000000000000000-8000000000000000')
  utils.run_vtctl('ValidateSchemaShard /zk/global/vt/keyspaces/test_keyspace/shards/8000000000000000-FFFFFFFFFFFFFFFF')
  out, err = utils.run_vtctl('ValidateSchemaKeyspace /zk/global/vt/keyspaces/test_keyspace', trap_output=True, raise_on_error=False)
  if (err.find("/zk/test_nj/vt/tablets/0000062344 and /zk/test_nj/vt/tablets/0000062346 disagree on schema for table vt_select_test") == -1 or \
      err.find("/zk/test_nj/vt/tablets/0000062344 and /zk/test_nj/vt/tablets/0000062347 disagree on schema for table vt_select_test") == -1):
        raise utils.TestError('wrong ValidateSchemaKeyspace output: ' + err)

  utils.kill_sub_process(zkocc)
  shard_0_master.kill_vttablet()
  shard_0_replica.kill_vttablet()
  shard_1_master.kill_vttablet()
  shard_1_replica.kill_vttablet()

def run_all():
  run_test_sharding()

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
