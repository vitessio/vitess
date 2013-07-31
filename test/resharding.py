#!/usr/bin/python
#
# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# FIXME(alainjobart) This test does not pass. It is a work in progress
# for resharding.

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
  utils.run_vtctl('ReparentShard -force test_keyspace/-80 ' + shard_0_master.tablet_alias, auto_log=True)
  utils.run_vtctl('ReparentShard -force test_keyspace/80- ' + shard_1_master.tablet_alias, auto_log=True)

  # create the split shards
  shard_2_master.init_tablet( 'master',  'test_keyspace', '80-C0')
  shard_2_replica.init_tablet('spare', 'test_keyspace', '80-C0')
  shard_3_master.init_tablet( 'master',  'test_keyspace', 'C0-')
  shard_3_replica.init_tablet('spare', 'test_keyspace', 'C0-')

  utils.run_vtctl('RebuildShardGraph /zk/global/vt/keyspaces/test_keyspace/shards/*', auto_log=True)

  utils.run_vtctl('RebuildKeyspaceGraph -use-served-types test_keyspace', auto_log=True)
  check_srv_keyspace('test_nj', 'test_keyspace',
                     'master: -80 80-\n' +
                     'rdonly: -80 80-\n' +
                     'replica: -80 80-\n')

  # now serve rdonly from the split shards
  utils.run_vtctl('SetShardServedTypes test_keyspace/80- master,replica')
  utils.run_vtctl('SetShardServedTypes test_keyspace/80-C0 rdonly')
  utils.run_vtctl('SetShardServedTypes test_keyspace/C0- rdonly')
  utils.run_vtctl('RebuildKeyspaceGraph -use-served-types test_keyspace', auto_log=True)
  check_srv_keyspace('test_nj', 'test_keyspace',
                     'master: -80 80-\n' +
                     'rdonly: -80 80-C0 C0-\n' +
                     'replica: -80 80-\n')

  # then serve replica from the split shards
  utils.run_vtctl('SetShardServedTypes test_keyspace/80- master')
  utils.run_vtctl('SetShardServedTypes test_keyspace/80-C0 replica,rdonly')
  utils.run_vtctl('SetShardServedTypes test_keyspace/C0- replica,rdonly')
  utils.run_vtctl('RebuildKeyspaceGraph -use-served-types test_keyspace', auto_log=True)
  check_srv_keyspace('test_nj', 'test_keyspace',
                     'master: -80 80-\n' +
                     'rdonly: -80 80-C0 C0-\n' +
                     'replica: -80 80-C0 C0-\n')

  # then serve master from the split shards
  utils.run_vtctl('SetShardServedTypes test_keyspace/80-')
  utils.run_vtctl('SetShardServedTypes test_keyspace/80-C0 master,replica,rdonly')
  utils.run_vtctl('SetShardServedTypes test_keyspace/C0- master,replica,rdonly')
  utils.run_vtctl('RebuildKeyspaceGraph -use-served-types test_keyspace', auto_log=True)
  check_srv_keyspace('test_nj', 'test_keyspace',
                     'master: -80 80-C0 C0-\n' +
                     'rdonly: -80 80-C0 C0-\n' +
                     'replica: -80 80-C0 C0-\n')

  # kill everything
  shard_0_master.kill_vttablet()
  shard_0_replica.kill_vttablet()
  shard_1_master.kill_vttablet()
  shard_1_replica.kill_vttablet()

def check_srv_keyspace(cell, keyspace, expected):
  ks = utils.zk_cat_json('/zk/%s/vt/ns/%s' % (cell, keyspace))
  result = ""
  for tablet_type in sorted(ks['Partitions'].keys()):
    result += tablet_type + ":"
    partition = ks['Partitions'][tablet_type]
    for shard in partition['Shards']:
      result = result + " %s-%s" % (shard['KeyRange']['Start'],
                                    shard['KeyRange']['End'])
    result += "\n"
  utils.debug("Cell %s keyspace %s has data:\n%s" % (cell, keyspace, result))
  if result != expected:
    raise utils.TestError("expected srv keyspace:\n%s\nbut got:\n%s\n" %
                          expected, result)

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
