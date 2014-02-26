#!/usr/bin/python
#
# Copyright 2014, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

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

    utils.pause("now is a good time to clear the cache, run:   sync ; sudo bash -c \"echo 1 > /proc/sys/vm/drop_caches\"")

    replica.mquery('', 'slave stop')

    self._change_random_data()
    logging.info("google3 primecache: ~/alainjobart-yt-main/google3/blaze-bin/video/youtube/db/primecache --mysql_socket /mnt/ssd/alainjobart/vt/vt_0000062345/mysql.sock --mysql_bin_path /mnt/ssd/alainjobart/git/dist/mysql/bin --binlog_path /mnt/ssd/alainjobart/vt/vt_0000062345/relay-logs --prime_user vt_dba --alsologtostderr")
    logging.info("vtprimecache: vtprimecache -db-config-dba-uname vt_dba -db-config-dba-charset utf8 -db-config-d-dbname vt_test_keyspace -db-config-app-uname vt_app -db-config-app-charset utf8 -db-config-app-dbname vt_test_keyspace -db-config-repl-uname vt_repl -db-config-repl-charset utf8 -db-config-repl-dbname vt_test_keyspace -mycnf_file /mnt/ssd/alainjobart/vt/vt_0000062345/my.cnf -alsologtostderr")
    utils.pause("after data changed, can start primecache")

    replica.mquery('', 'slave start')

    # see how longs it takes to catch up on replication
    self.catch_up()

    tablet.kill_tablets([master, replica])

if __name__ == '__main__':
  utils.main()
