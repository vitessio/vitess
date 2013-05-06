#!/usr/bin/python

import os
import socket

import utils
import tablet

from vtdb import tablet2

# range "" - 80
shard_0_master = tablet.Tablet()
shard_0_replica = tablet.Tablet()
# range 80 - ""
shard_1_master = tablet.Tablet()
shard_1_replica = tablet.Tablet()

def setup():
  utils.zk_setup()

  setup_procs = [
      shard_0_master.init_mysql(),
      shard_0_replica.init_mysql(),
      shard_1_master.init_mysql(),
      shard_1_replica.init_mysql(),
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
  out, err = utils.vtclient2(0, "/zk/test_nj/vt/ns/test_keyspace/master", "select id, msg from vt_select_test", driver=driver, verbose=True)
  for pattern in to_look_for:
    if pattern not in err:
      print "vtclient2 returned:"
      print out
      print err
      raise utils.TestError('wrong vtclient2 output, missing: ' + pattern)
  if utils.options.verbose:
    print out, err

def check_rows_schema_diff(driver):
  out, err = utils.vtclient2(0, "/zk/test_nj/vt/ns/test_keyspace/master", "select * from vt_select_test", driver=driver, verbose=False, raise_on_error=False)
  if "column[0] name mismatch: id != msg" not in err and \
      "column[0] name mismatch: msg != id" not in err:
    print "vtclient2 returned:"
    print out
    print err
    raise utils.TestError('wrong vtclient2 output, missing "name mismatch" of some kind')
  if utils.options.verbose:
    print out, err


@utils.test_case
def run_test_sharding():

  utils.run_vtctl('CreateKeyspace -force /zk/global/vt/keyspaces/test_keyspace')

  shard_0_master.init_tablet( 'master',  'test_keyspace', '-80')
  shard_0_replica.init_tablet('replica', 'test_keyspace', '-80')
  shard_1_master.init_tablet( 'master',  'test_keyspace', '80-')
  shard_1_replica.init_tablet('replica', 'test_keyspace', '80-')

  utils.run_vtctl('RebuildShardGraph /zk/global/vt/keyspaces/test_keyspace/shards/*', auto_log=True)

  utils.run_vtctl('RebuildKeyspaceGraph /zk/global/vt/keyspaces/*', auto_log=True)

  # run checks now before we start the tablets
  utils.zk_check()

  # create databases
  shard_0_master.create_db('vt_test_keyspace')
  shard_0_replica.create_db('vt_test_keyspace')
  shard_1_master.create_db('vt_test_keyspace')
  shard_1_replica.create_db('vt_test_keyspace')

  # start the tablets
  shard_0_master.start_vttablet()
  shard_0_replica.start_vttablet()
  shard_1_master.start_vttablet()
  shard_1_replica.start_vttablet()

  # apply the schema on the first shard through vtctl, so all tablets
  # are the same (replication is not enabled yet, so allow_replication=false
  # is just there to be tested)
  utils.run_vtctl(['ApplySchema',
                   '-stop-replication',
                   '-sql=' + create_vt_select_test.replace("\n", ""),
                   shard_0_master.zk_tablet_path])
  utils.run_vtctl(['ApplySchema',
                   '-stop-replication',
                   '-sql=' + create_vt_select_test.replace("\n", ""),
                   shard_0_replica.zk_tablet_path])

  # start zkocc, we'll use it later
  zkocc = utils.zkocc_start()

  utils.run_vtctl('ReparentShard -force /zk/global/vt/keyspaces/test_keyspace/shards/-80 ' + shard_0_master.zk_tablet_path, auto_log=True)
  utils.run_vtctl('ReparentShard -force /zk/global/vt/keyspaces/test_keyspace/shards/80- ' + shard_1_master.zk_tablet_path, auto_log=True)

  # apply the schema on the second shard using a simple schema upgrade
  utils.run_vtctl(['ApplySchemaShard',
                   '-simple',
                   '-sql=' + create_vt_select_test_reverse.replace("\n", ""),
                   '/zk/global/vt/keyspaces/test_keyspace/shards/80-'])

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
  utils.vtclient2(3803, "/zk/test_nj/vt/ns/test_keyspace/master", "insert into vt_select_test (id, msg) values (:keyspace_id, 'test 2')", bindvars='{"keyspace_id": 2}', driver="vtdb", verbose=True)
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
  utils.run_vtctl('ValidateSchemaShard /zk/global/vt/keyspaces/test_keyspace/shards/-80')
  utils.run_vtctl('ValidateSchemaShard /zk/global/vt/keyspaces/test_keyspace/shards/80-')
  out, err = utils.run_vtctl('ValidateSchemaKeyspace /zk/global/vt/keyspaces/test_keyspace', trap_output=True, raise_on_error=False)
  if "/zk/test_nj/vt/tablets/0000062344 and /zk/test_nj/vt/tablets/0000062346 disagree on schema for table vt_select_test:\nCREATE TABLE" not in err or \
      "/zk/test_nj/vt/tablets/0000062344 and /zk/test_nj/vt/tablets/0000062347 disagree on schema for table vt_select_test:\nCREATE TABLE" not in err:
        raise utils.TestError('wrong ValidateSchemaKeyspace output: ' + err)

  # and create zkns on this complex keyspace, make sure a few files are created
  utils.run_vtctl('ExportZknsForKeyspace /zk/global/vt/keyspaces/test_keyspace')
  out, err = utils.run(utils.vtroot+'/bin/zk ls -R /zk/test_nj/zk?s/vt/test_keysp*', trap_output=True)
  lines = out.splitlines()
  for base in ['-80', '80-']:
    for db_type in ['master', 'replica']:
      for sub_path in ['', '.vdns', '/0', '/_vtocc.vdns']:
        expected = '/zk/test_nj/zkns/vt/test_keyspace/' + base + '/' + db_type + sub_path
        if expected not in lines:
          raise utils.TestError('missing zkns part:\n%s\nin:%s' %(expected, out))

  # now try to connect using the python client and shard-aware connection
  # to both shards
  # FIXME(alainjobart) get key_range from the topology
  # when emd_topology.py is exported to this depot
  conn = tablet2.TabletConnection("localhost:%u" % shard_0_master.port,
                                  "test_keyspace", "-80", 10.0)
  conn.dial()
  (results, rowcount, lastrowid, fields) = conn._execute("select id, msg from vt_select_test order by id", {})
  if (len(results) != 2 or \
        results[0][0] != 1 or \
        results[1][0] != 2):
    print "conn._execute returned:", results
    raise utils.TestError('wrong conn._execute output')
  conn = tablet2.TabletConnection("localhost:%u" % shard_1_master.port,
                                  "test_keyspace", "80-", 10.0)
  conn.dial()
  (results, rowcount, lastrowid, fields) = conn._execute("select id, msg from vt_select_test order by id", {})
  if (len(results) != 1 or \
        results[0][0] != 10):
    print "conn._execute returned:", results
    raise utils.TestError('wrong conn._execute output')

  # try to connect with bad shard
  try:
    conn = tablet2.TabletConnection("localhost:%u" % shard_0_master.port,
                                    "test_keyspace", "-90", 10.0)
    conn.dial()
    raise utils.TestError('expected an exception')
  except Exception as e:
    if "fatal: Shard mismatch, expecting -80, received -90" not in str(e):
      raise utils.TestError('unexpected exception: ' + str(e))

  utils.kill_sub_process(zkocc)
  shard_0_master.kill_vttablet()
  shard_0_replica.kill_vttablet()
  shard_1_master.kill_vttablet()
  shard_1_replica.kill_vttablet()

def run_all():
  run_test_sharding()

def main():
  args = utils.get_args()

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
  except utils.TestError as e:
    for arg in e.args:
      print arg
  finally:
    teardown()


if __name__ == '__main__':
  main()
