#!/usr/bin/python
#
# Copyright 2014, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# This test is not automated, it depends on flushing the Linux OS buffer cache,
# which can only be done by root. Also, it is fairly big and runs for a long time.
# On a server with a SSD drive, it takes:
# - 96s to insert the data on the master
# - 30s to clone the master to a replica
# - then we change random data for 30s on the master (with replication stopped)
# - then we run primecache in the background (optional)
# - and we see how long it takes to catch up:
#   - 29s without primecache
#   - 19s with primecache at 4 db connections
#   - <17s with primecache at 8 db connections, not much less with 16 connections.

import logging
import random
import time
import unittest

import environment
import utils
import tablet

# tablets
master = tablet.Tablet()
replica = tablet.Tablet()

def setUpModule():
  try:
    environment.topo_server_setup()

    setup_procs = [
        master.init_mysql(),
        replica.init_mysql(),
        ]
    utils.wait_procs(setup_procs)
  except:
    tearDownModule()
    raise

def tearDownModule():
  if utils.options.skip_teardown:
    return

  teardown_procs = [
      master.teardown_mysql(),
      replica.teardown_mysql(),      ]
  utils.wait_procs(teardown_procs, raise_on_error=False)

  environment.topo_server_teardown()
  utils.kill_sub_processes()
  utils.remove_tmp_files()

  master.remove_tree()
  replica.remove_tree()

class TestPrimeCache(unittest.TestCase):

  ROW_COUNT = 100000
  CHANGE_DURATION = 30

  def _create_data(self):
    create_table_template = '''create table lots_of_data(
id bigint auto_increment,
ts datetime,
msg varchar(4096),
primary key (id)
) Engine=InnoDB'''
    utils.run_vtctl(['ApplySchemaKeyspace',
                     '-simple',
                     '-sql=' + create_table_template,
                     'test_keyspace'],
                    auto_log=True)

    start = time.time()
    for i in xrange(100):
      for j in xrange(self.ROW_COUNT / 100):
        master.mquery('vt_test_keyspace', 'insert into lots_of_data(msg, ts) values(repeat("a", 4096), now())', write=True)
      logging.info("Inserted %u%% of the data", i)
    logging.info("It took %g seconds to insert data" % (time.time() - start))

  # _change_random_data will change random data in the data set
  # on the master, for up to CHANGE_DURATION seconds
  def _change_random_data(self):
    logging.info("Starting to change data for %us on the master",
                 self.CHANGE_DURATION)
    start = time.time()
    random.seed()

    count = 0
    while True:
      queries = []
      count += 100
      for i in xrange(100):
        index = random.randrange(self.ROW_COUNT)
        queries.append('update lots_of_data set ts=now() where id=%u' % index)
      master.mquery('vt_test_keyspace', queries, write=True)

      if time.time() - start > self.CHANGE_DURATION:
        break
    logging.info("Changed %u rows", count)

  def catch_up(self):
    start = time.time()
    time.sleep(5) # no need to start too early
    while True:
      s = replica.mquery('', 'show slave status')
      sbm = s[0][32]
      if sbm is not None and sbm == 0:
        logging.info("It took %g seconds to catch up" % (time.time() - start))
        return
      time.sleep(0.1)

  def test_primecache(self):
    utils.run_vtctl(['CreateKeyspace', 'test_keyspace'])

    master.init_tablet( 'master',  'test_keyspace', '0')
    replica.init_tablet('idle')

    utils.run_vtctl(['RebuildKeyspaceGraph', 'test_keyspace'], auto_log=True)

    master.create_db('vt_test_keyspace')
    master.start_vttablet(wait_for_state=None)
    replica.start_vttablet(wait_for_state=None)

    master.wait_for_vttablet_state('SERVING')
    replica.wait_for_vttablet_state('NOT_SERVING') # DB doesn't exist

    self._create_data()

    # we use clone to not prime the mysql cache on the slave db
    utils.run_vtctl(['Clone', '-force', '-server-mode',
                     master.tablet_alias, replica.tablet_alias],
                    auto_log=True)

    # sync the buffer cache, and clear it. This will prompt for user's password
    utils.run(['sync'])
    utils.run(['sudo', 'bash', '-c', 'echo 1 > /proc/sys/vm/drop_caches'])

    # we can now change data on the master for 30s, while slave is stopped.
    # master's binlog will be in OS buffer cache now.
    replica.mquery('', 'slave stop')
    self._change_random_data()

    use_primecache = True # easy to test without
    if use_primecache:
      # starting vtprimecache, sleeping for a couple seconds
      args = [environment.binary_path('vtprimecache'),
              '-db-config-dba-uname', 'vt_dba',
              '-db-config-dba-charset', 'utf8',
              '-db-config-dba-dbname', 'vt_test_keyspace',
              '-db-config-app-uname', 'vt_app',
              '-db-config-app-charset', 'utf8',
              '-db-config-app-dbname', 'vt_test_keyspace',
              '-db-config-repl-uname', 'vt_repl',
              '-db-config-repl-charset', 'utf8',
              '-db-config-repl-dbname', 'vt_test_keyspace',
              '-mycnf_file', replica.tablet_dir+'/my.cnf',
              '-log_dir', environment.vtlogroot,
              '-worker_count', '4',
              '-alsologtostderr',
             ]
      vtprimecache = utils.run_bg(args)
      time.sleep(2)

    # start slave, see how longs it takes to catch up on replication
    replica.mquery('', 'slave start')
    self.catch_up()

    if use_primecache:
      # TODO(alainjobart): read and check stats
      utils.kill_sub_process(vtprimecache)

    tablet.kill_tablets([master, replica])

if __name__ == '__main__':
  utils.main()
