#!/usr/bin/python
#
# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import time

import utils
import tablet

# initial shards
# range "" - 80
shard_0_master = tablet.Tablet()
shard_0_replica = tablet.Tablet()
# range 80 - ""
shard_1_master = tablet.Tablet()
shard_1_replica = tablet.Tablet()

# split shards
# range 80 - C0
shard_2_master = tablet.Tablet()
shard_2_replica = tablet.Tablet()
# range C0 - ""
shard_3_master = tablet.Tablet()
shard_3_replica = tablet.Tablet()


def setup():
  utils.zk_setup()

  setup_procs = [
      shard_0_master.init_mysql(),
      shard_0_replica.init_mysql(),
      shard_1_master.init_mysql(),
      shard_1_replica.init_mysql(),
      shard_2_master.init_mysql(),
      shard_2_replica.init_mysql(),
      shard_3_master.init_mysql(),
      shard_3_replica.init_mysql(),
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
      shard_2_master.teardown_mysql(),
      shard_2_replica.teardown_mysql(),
      shard_3_master.teardown_mysql(),
      shard_3_replica.teardown_mysql(),
      ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  utils.zk_teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  shard_0_master.remove_tree()
  shard_0_replica.remove_tree()
  shard_1_master.remove_tree()
  shard_1_replica.remove_tree()
  shard_2_master.remove_tree()
  shard_2_replica.remove_tree()
  shard_3_master.remove_tree()
  shard_3_replica.remove_tree()

# create_schema will create the same schema on the keyspace
# then insert some values
def create_schema():
  create_table_template = '''create table %s (
id bigint auto_increment,
msg varchar(64),
keyspace_id bigint(20) unsigned not null, 
primary key (id),
index by_msg (msg)
) Engine=InnoDB'''
  create_view_template = '''create view %s(id, msg, keyspace_id) as select id, msg, keyspace_id from %s'''

  utils.run_vtctl(['ApplySchemaKeyspace',
                   '-simple',
                   '-sql=' + create_table_template % ("resharding1"),
                   'test_keyspace'],
                  auto_log=True)
  utils.run_vtctl(['ApplySchemaKeyspace',
                   '-simple',
                   '-sql=' + create_table_template % ("resharding2"),
                   'test_keyspace'],
                  auto_log=True)
  utils.run_vtctl(['ApplySchemaKeyspace',
                   '-simple',
                   '-sql=' + create_view_template % ("view1", "resharding1"),
                   'test_keyspace'],
                  auto_log=True)
  shard_0_master.mquery('vt_test_keyspace', 'insert into resharding1(id, msg, keyspace_id) values(1, "msg1", 0x1000000000000000)', write=True)
  shard_1_master.mquery('vt_test_keyspace', 'insert into resharding1(id, msg, keyspace_id) values(2, "msg2", 0x9000000000000000)', write=True)
  shard_1_master.mquery('vt_test_keyspace', 'insert into resharding1(id, msg, keyspace_id) values(3, "msg3", 0xD000000000000000)', write=True)

def wait_for_binlog_server_state(tablet, expected, timeout=5.0):
    while True:
      v = utils.get_vars(tablet.port)
      if v == None:
        utils.debug("  vttablet not answering at /debug/vars, waiting...")
      else:
        if 'BinlogServerRpcService' not in v:
          utils.debug("  vttablet not exporting BinlogServerRpcService, waiting...")
        else:
          s = v['BinlogServerRpcService']['States']['Current']
          if s != expected:
            utils.debug("  vttablet's binlog server in state %s != %s" % (s, expected))
          else:
            break

      utils.debug("sleeping a bit while we wait")
      time.sleep(0.1)
      timeout -= 0.1
      if timeout <= 0:
        raise utils.TestError("timeout waiting for binlog server state %s" % expected)
    utils.debug("tablet %s binlog service is in state %s" % (tablet.tablet_alias, expected))

def run_test_resharding():
  utils.run_vtctl('CreateKeyspace test_keyspace')

  shard_0_master.init_tablet( 'master',  'test_keyspace', '-80')
  shard_0_replica.init_tablet('replica', 'test_keyspace', '-80')
  shard_1_master.init_tablet( 'master',  'test_keyspace', '80-')
  shard_1_replica.init_tablet('replica', 'test_keyspace', '80-')

  utils.run_vtctl('RebuildShardGraph /zk/global/vt/keyspaces/test_keyspace/shards/*', auto_log=True)

  utils.run_vtctl('RebuildKeyspaceGraph /zk/global/vt/keyspaces/test_keyspace', auto_log=True)

  # create databases so vttablet can start behaving normally
  shard_0_master.create_db('vt_test_keyspace')
  shard_0_replica.create_db('vt_test_keyspace')
  shard_1_master.create_db('vt_test_keyspace')
  shard_1_replica.create_db('vt_test_keyspace')

  # start the tablets
  shard_0_master.start_vttablet()
  shard_0_replica.start_vttablet()
  shard_1_master.start_vttablet()
  shard_1_replica.start_vttablet()

  # reparent to make the tablets work
  for t in [shard_0_master, shard_0_replica, shard_1_master, shard_1_replica]:
    t.reset_replication()
  utils.run_vtctl('ReparentShard -force test_keyspace/-80 ' + shard_0_master.tablet_alias, auto_log=True)
  utils.run_vtctl('ReparentShard -force test_keyspace/80- ' + shard_1_master.tablet_alias, auto_log=True)

  # create the tables
  create_schema()

  # create the split shards
  shard_2_master.init_tablet( 'master',  'test_keyspace', '80-C0')
  shard_2_replica.init_tablet('spare', 'test_keyspace', '80-C0')
  shard_3_master.init_tablet( 'master',  'test_keyspace', 'C0-')
  shard_3_replica.init_tablet('spare', 'test_keyspace', 'C0-')

  # start vttablet on the split shards (no db created,
  # so they're all not serving)
  shard_2_master.start_vttablet(wait_for_state='CONNECTING')
  shard_2_replica.start_vttablet(wait_for_state='NOT_SERVING')
  shard_3_master.start_vttablet(wait_for_state='CONNECTING')
  shard_3_replica.start_vttablet(wait_for_state='NOT_SERVING')

  utils.run_vtctl('RebuildShardGraph /zk/global/vt/keyspaces/test_keyspace/shards/*', auto_log=True)

  utils.run_vtctl('RebuildKeyspaceGraph -use-served-types test_keyspace', auto_log=True)
  check_srv_keyspace('test_nj', 'test_keyspace',
                     'Partitions(master): -80 80-\n' +
                     'Partitions(rdonly): -80 80-\n' +
                     'Partitions(replica): -80 80-\n' +
                     'TabletTypes: master,replica')

  # take the snapshot for the split
  utils.run_vtctl('MultiSnapshot --spec=80-C0- %s keyspace_id' % (shard_1_replica.tablet_alias), auto_log=True)

  # wait for tablet's binlog server service to be enabled after snapshot,
  # and check all the others while we're at it
  wait_for_binlog_server_state(shard_1_master, "Disabled")
  wait_for_binlog_server_state(shard_1_replica, "Enabled")

  # perform the restore.
  utils.run_vtctl(['ShardMultiRestore', '-strategy=populateBlpRecovery', 'test_keyspace/80-C0', shard_1_replica.tablet_alias], auto_log=True)
  utils.run_vtctl(['ShardMultiRestore', '-strategy=populateBlpRecovery', 'test_keyspace/C0-', shard_1_replica.tablet_alias], auto_log=True)

  # now filtered replication should just work, need to test it.

  # now serve rdonly from the split shards
  utils.run_vtctl('SetShardServedTypes test_keyspace/80- master,replica')
  utils.run_vtctl('SetShardServedTypes test_keyspace/80-C0 rdonly')
  utils.run_vtctl('SetShardServedTypes test_keyspace/C0- rdonly')
  utils.run_vtctl('RebuildKeyspaceGraph -use-served-types test_keyspace', auto_log=True)
  check_srv_keyspace('test_nj', 'test_keyspace',
                     'Partitions(master): -80 80-\n' +
                     'Partitions(rdonly): -80 80-C0 C0-\n' +
                     'Partitions(replica): -80 80-\n' +
                     'TabletTypes: master,replica')

  # then serve replica from the split shards
  utils.run_vtctl('SetShardServedTypes test_keyspace/80- master')
  utils.run_vtctl('SetShardServedTypes test_keyspace/80-C0 replica,rdonly')
  utils.run_vtctl('SetShardServedTypes test_keyspace/C0- replica,rdonly')
  utils.run_vtctl('RebuildKeyspaceGraph -use-served-types test_keyspace', auto_log=True)
  check_srv_keyspace('test_nj', 'test_keyspace',
                     'Partitions(master): -80 80-\n' +
                     'Partitions(rdonly): -80 80-C0 C0-\n' +
                     'Partitions(replica): -80 80-C0 C0-\n' +
                     'TabletTypes: master,replica')

  # then serve master from the split shards
  utils.run_vtctl('SetShardServedTypes test_keyspace/80-')
  utils.run_vtctl('SetShardServedTypes test_keyspace/80-C0 master,replica,rdonly')
  utils.run_vtctl('SetShardServedTypes test_keyspace/C0- master,replica,rdonly')
  utils.run_vtctl('RebuildKeyspaceGraph -use-served-types test_keyspace', auto_log=True)
  check_srv_keyspace('test_nj', 'test_keyspace',
                     'Partitions(master): -80 80-C0 C0-\n' +
                     'Partitions(rdonly): -80 80-C0 C0-\n' +
                     'Partitions(replica): -80 80-C0 C0-\n' +
                     'TabletTypes: master,replica')

  # kill everything
  for t in [shard_0_master, shard_0_replica, shard_1_master, shard_1_replica,
            shard_2_master, shard_2_replica, shard_3_master, shard_3_replica]:
    t.kill_vttablet()

def check_srv_keyspace(cell, keyspace, expected):
  ks = utils.zk_cat_json('/zk/%s/vt/ns/%s' % (cell, keyspace))
  result = ""
  for tablet_type in sorted(ks['Partitions'].keys()):
    result += "Partitions(%s):" % tablet_type
    partition = ks['Partitions'][tablet_type]
    for shard in partition['Shards']:
      result = result + " %s-%s" % (shard['KeyRange']['Start'],
                                    shard['KeyRange']['End'])
    result += "\n"
  result += "TabletTypes: " + ",".join(sorted(ks['TabletTypes']))
  utils.debug("Cell %s keyspace %s has data:\n%s" % (cell, keyspace, result))
  if result != expected:
    raise utils.TestError("***** Expected srv keyspace:\n%s\nbut got:\n%s\n" %
                          (expected, result))

def run_all():
  run_test_resharding()

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
