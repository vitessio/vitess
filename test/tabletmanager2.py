#!/usr/bin/env python

# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# vim: tabstop=8 expandtab shiftwidth=2 softtabstop=2

import MySQLdb
import logging
import re
import unittest
import os
import environment
import tablet
import utils
import time
from utils import TestError
from vtproto.tabletmanagerdata_pb2 import LockTablesRequest, UnlockTablesRequest, StopSlaveRequest, \
    MasterPositionRequest, StartSlaveUntilAfterRequest

# regexp to check if the tablet status page reports healthy,
# regardless of actual replication lag
healthy_expr = re.compile(r'Current status: <span.+?>healthy')

_create_vt_insert_test = '''create table vt_insert_test (
  id bigint auto_increment,
  msg varchar(64),
  primary key (id)
  ) Engine=InnoDB'''


class TestServiceTestOfTabletManager(unittest.TestCase):
    """This tests the locking functionality by running rpc calls against the tabletmanager,
    in contrast to testing the tabletmanager through the vtcl"""

    replica = tablet.Tablet(62344)
    master = tablet.Tablet(62044)

    def setUp(self):
        try:
            os.makedirs(environment.tmproot)
        except OSError:
            # directory already exists
            pass

        try:
            topo_flavor = environment.topo_server().flavor()
            if topo_flavor == 'zk2':
                # This is a one-off test to make sure our 'zk2' implementation
                # behave with a server that is not DNS-resolveable.
                environment.topo_server().setup(add_bad_host=True)
            else:
                environment.topo_server().setup()

            # start mysql instance external to the test
            setup_procs = [
                self.replica.init_mysql(),
                self.master.init_mysql(),
            ]
            utils.Vtctld().start()
            logging.debug(utils.vtctld_connection)
            utils.wait_procs(setup_procs)

            for t in self.master, self.replica:
                t.create_db('vt_test_keyspace')

            self.master.init_tablet('replica', 'test_keyspace', '0', start=True)
            self.replica.init_tablet('replica', 'test_keyspace', '0', start=True)
            utils.run_vtctl(['InitShardMaster', '-force', 'test_keyspace/0',
                             self.master.tablet_alias])
            self.master.mquery('vt_test_keyspace', _create_vt_insert_test)
            for t in [self.master, self.replica]:
                t.set_semi_sync_enabled(master=False, slave=False)
        except Exception as e:
            logging.exception(e)
            self.tearDown()

    def tearDown(self):
        try:
            for t in self.master, self.replica:
                t.kill_vttablet()
            tablet.Tablet.check_vttablet_count()
            environment.topo_server().wipe()
            for t in [self.master, self.replica]:
                t.reset_replication()
                t.set_semi_sync_enabled(master=False, slave=False)
                t.clean_dbs()
        finally:
            utils.required_teardown()

            if utils.options.skip_teardown:
                return

            teardown_procs = [
                self.master.teardown_mysql(),
                self.replica.teardown_mysql(),
            ]
            utils.wait_procs(teardown_procs, raise_on_error=False)

            environment.topo_server().teardown()
            utils.kill_sub_processes()
            utils.remove_tmp_files()

            self.replica.remove_tree()
            self.master.remove_tree()

    def _write_data_to_master(self):
        """Write a single row to the master"""
        self.master.mquery('vt_test_keyspace', "insert into vt_insert_test (msg) values ('test')", write=True)

    def _check_data_on_replica(self, count, msg):
        """Check that the specified tablet has the expected number of rows."""
        timeout = 3
        while True:
            try:
                result = self.replica.mquery(
                    'vt_test_keyspace', 'select count(*) from vt_insert_test')
                if result[0][0] == count:
                    break
            except MySQLdb.DatabaseError:
                # ignore exceptions, we'll just timeout (the tablet creation
                # can take some time to replicate, and we get a 'table vt_insert_test
                # does not exist exception in some rare cases)
                logging.exception('exception waiting for data to replicate')
            timeout = utils.wait_step(msg, timeout)

    def test_lock_and_unlock(self):
        """Test the lock ability by locking a replica and asserting it does not see changes"""
        # first make sure that our writes to the master make it to the replica
        self._write_data_to_master()
        self._check_data_on_replica(1, "replica getting the data")

        # now lock the replica
        tablet_manager = self.replica.tablet_manager()
        tablet_manager.LockTables(LockTablesRequest())

        # make sure that writing to the master does not show up on the replica while locked
        self._write_data_to_master()
        with self.assertRaises(TestError):
            self._check_data_on_replica(2, "the replica should not see these updates")

        # finally, make sure that unlocking the replica leads to the previous write showing up
        tablet_manager.UnlockTables(UnlockTablesRequest())
        self._check_data_on_replica(2, "after unlocking the replica, we should see these updates")

    def test_unlock_when_we_dont_have_a_lock(self):
        """Unlocking when we do not have a valid lock should lead to an exception being raised"""
        # unlock the replica
        tablet_manager = self.replica.tablet_manager()
        with self.assertRaises(Exception):
            tablet_manager.UnlockTables(UnlockTablesRequest())

    def test_start_slave_until_after(self):
        """Test by writing three rows, noting the gtid after each, and then replaying them one by one"""
        self.replica.start_vttablet()
        self.master.start_vttablet()

        # first we stop replication to the replica, so we can move forward step by step.
        replica_tablet_manager = self.replica.tablet_manager()
        replica_tablet_manager.StopSlave(StopSlaveRequest())

        master_tablet_manager = self.master.tablet_manager()
        self._write_data_to_master()
        pos1 = master_tablet_manager.MasterPosition(MasterPositionRequest())

        self._write_data_to_master()
        pos2 = master_tablet_manager.MasterPosition(MasterPositionRequest())

        self._write_data_to_master()
        pos3 = master_tablet_manager.MasterPosition(MasterPositionRequest())

        # Now, we'll resume stepwise position by position and make sure that we see the expected data
        self._check_data_on_replica(0, "no data has yet reached the replica")

        # timeout is given in nanoseconds. we want to wait no more than 10 seconds
        timeout = int(10 * 1e9)

        replica_tablet_manager.StartSlaveUntilAfter(
            StartSlaveUntilAfterRequest(position=pos1.position, wait_timeout=timeout))
        self._check_data_on_replica(1, "first row is now visible")

        replica_tablet_manager.StartSlaveUntilAfter(
            StartSlaveUntilAfterRequest(position=pos2.position, wait_timeout=timeout))
        self._check_data_on_replica(2, "second row is now visible")

        replica_tablet_manager.StartSlaveUntilAfter(
            StartSlaveUntilAfterRequest(position=pos3.position, wait_timeout=timeout))
        self._check_data_on_replica(3, "third row is now visible")

    def test_lock_and_timeout(self):
        """Test that the lock times out and updates can be seen even though nothing is unlocked"""

        # first make sure that our writes to the master make it to the replica
        self._write_data_to_master()
        self._check_data_on_replica(1, "replica getting the data")

        # now lock the replica
        tablet_manager = self.replica.tablet_manager()
        tablet_manager.LockTables(LockTablesRequest())

        # make sure that writing to the master does not show up on the replica while locked
        self._write_data_to_master()
        with self.assertRaises(TestError):
            self._check_data_on_replica(2, "the replica should not see these updates")

        # the tests sets the lock timeout to 5 seconds, so sleeping 10 should be safe
        time.sleep(10)

        self._check_data_on_replica(2, "the replica should now see these updates")

        # finally, trying to unlock should clearly tell us we did not have the lock
        with self.assertRaises(Exception):
            tablet_manager.UnlockTables(UnlockTablesRequest())


if __name__ == '__main__':
    utils.main()
