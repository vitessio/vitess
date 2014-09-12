#!/usr/bin/python
#
# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import unittest

import vtdb_test
import utils

from vtdb import cursor
from vtdb import dbexceptions
from vtdb import vtgate

def setUpModule():
  vtdb_test.setUpModule()

def tearDownModule():
  vtdb_test.tearDownModule()


class TestVtgateFunctions(vtdb_test.TestTabletFunctions):
  def test_status(self):
    self.assertIn('</html>', utils.get_status(vtdb_test.vtgate_port))

# FIXME(shrutip): this class needs reworking once
# the error handling is resolved the right way at vtgate binary.
class TestFailures(unittest.TestCase):
  def setUp(self):
    self.shard_index = 0
    self.master_tablet = vtdb_test.shard_0_master
    self.replica_tablet = vtdb_test.shard_0_replica

  def test_tablet_restart_read(self):
    try:
      replica_conn = vtdb_test.get_connection(db_type='replica', shard_index=self.shard_index)
    except Exception, e:
      self.fail("Connection to shard %s replica failed with error %s" % (vtdb_test.shard_names[self.shard_index], str(e)))
    self.replica_tablet.kill_vttablet()
    with self.assertRaises(dbexceptions.DatabaseError):
      replica_conn._execute("select 1 from vt_insert_test", {})
    proc = self.replica_tablet.start_vttablet()
    try:
      results = replica_conn._execute("select 1 from vt_insert_test", {})
    except Exception, e:
      self.fail("Communication with shard %s replica failed with error %s" % (vtdb_test.shard_names[self.shard_index], str(e)))

  def test_vtgate_restart_read(self):
    try:
      replica_conn = vtdb_test.get_connection(db_type='replica', shard_index=self.shard_index)
    except Exception, e:
      self.fail("Connection to shard %s replica failed with error %s" % (vtdb_test.shard_names[self.shard_index], str(e)))
    utils.vtgate_kill(vtdb_test.vtgate_server)
    with self.assertRaises(dbexceptions.OperationalError):
      replica_conn._execute("select 1 from vt_insert_test", {})
    vtdb_test.vtgate_server, vtdb_test.vtgate_port = utils.vtgate_start(vtdb_test.vtgate_port)
    try:
      results = replica_conn._execute("select 1 from vt_insert_test", {})
    except Exception, e:
      self.fail("Communication with shard %s replica failed with error %s" % (vtdb_test.shard_names[self.shard_index], str(e)))


  def test_tablet_restart_stream_execute(self):
    try:
      replica_conn = vtdb_test.get_connection(db_type='replica', shard_index=self.shard_index)
    except Exception, e:
      self.fail("Connection to %s replica failed with error %s" % (vtdb_test.shard_names[self.shard_index], str(e)))
    stream_cursor = cursor.StreamCursor(replica_conn)
    self.replica_tablet.kill_vttablet()
    with self.assertRaises(dbexceptions.DatabaseError):
      stream_cursor.execute("select * from vt_insert_test", {})
    proc = self.replica_tablet.start_vttablet()
    self.replica_tablet.wait_for_vttablet_state('SERVING')
    try:
      stream_cursor.execute("select * from vt_insert_test", {})
    except Exception, e:
      self.fail("Communication with shard0 replica failed with error %s" %
                str(e))

  def test_vtgate_restart_stream_execute(self):
    try:
      replica_conn = vtdb_test.get_connection(db_type='replica', shard_index=self.shard_index)
    except Exception, e:
      self.fail("Connection to %s replica failed with error %s" % (vtdb_test.shard_names[self.shard_index], str(e)))
    stream_cursor = cursor.StreamCursor(replica_conn)
    utils.vtgate_kill(vtdb_test.vtgate_server)
    with self.assertRaises(dbexceptions.OperationalError):
      stream_cursor.execute("select * from vt_insert_test", {})
    vtdb_test.vtgate_server, vtdb_test.vtgate_port = utils.vtgate_start(vtdb_test.vtgate_port)
    try:
      stream_cursor.execute("select * from vt_insert_test", {})
    except Exception, e:
      self.fail("Communication with shard0 replica failed with error %s" %
                str(e))

  # vtgate begin doesn't make any back-end connections to
  # vttablet so the kill and restart shouldn't have any effect.
  def test_tablet_restart_begin(self):
    try:
      master_conn = vtdb_test.get_connection(db_type='master')
    except Exception, e:
      self.fail("Connection to shard0 master failed with error %s" % str(e))
    self.master_tablet.kill_vttablet()
    master_conn.begin()
    proc = self.master_tablet.start_vttablet()
    master_conn.begin()
    # this succeeds only if retry_count > 0
    master_conn._execute("delete from vt_insert_test", {})
    master_conn.commit()

  def test_vtgate_restart_begin(self):
    try:
      master_conn = vtdb_test.get_connection(db_type='master')
    except Exception, e:
      self.fail("Connection to shard0 master failed with error %s" % str(e))
    utils.vtgate_kill(vtdb_test.vtgate_server)
    with self.assertRaises(dbexceptions.OperationalError):
      master_conn.begin()
    vtdb_test.vtgate_server, vtdb_test.vtgate_port = utils.vtgate_start(vtdb_test.vtgate_port)
    master_conn.begin()

  def test_tablet_fail_write(self):
    try:
      master_conn = vtdb_test.get_connection(db_type='master')
    except Exception, e:
      self.fail("Connection to shard0 master failed with error %s" % str(e))
    with self.assertRaises(dbexceptions.DatabaseError):
      master_conn.begin()
      self.master_tablet.kill_vttablet()
      master_conn._execute("delete from vt_insert_test", {})
      master_conn.commit()
    proc = self.master_tablet.start_vttablet()
    master_conn.begin()
    master_conn._execute("delete from vt_insert_test", {})
    master_conn.commit()

  def test_vtgate_fail_write(self):
    try:
      master_conn = vtdb_test.get_connection(db_type='master')
    except Exception, e:
      self.fail("Connection to shard0 master failed with error %s" % str(e))
    with self.assertRaises(dbexceptions.OperationalError):
      master_conn.begin()
      utils.vtgate_kill(vtdb_test.vtgate_server)
      master_conn._execute("delete from vt_insert_test", {})
      master_conn.commit()
    vtdb_test.vtgate_server, vtdb_test.vtgate_port = utils.vtgate_start(vtdb_test.vtgate_port)
    try:
      master_conn = vtdb_test.get_connection(db_type='master')
    except Exception, e:
      self.fail("Connection to shard0 master failed with error %s" % str(e))
    master_conn.begin()
    master_conn._execute("delete from vt_insert_test", {})
    master_conn.commit()

  # test timeout between py client and vtgate
  # the default timeout is 10 seconds
  def test_vtgate_timeout(self):
    try:
      replica_conn = vtdb_test.get_connection(db_type='replica', shard_index=self.shard_index)
    except Exception, e:
      self.fail("Connection to shard0 replica failed with error %s" % str(e))
    with self.assertRaises(dbexceptions.TimeoutError):
      replica_conn._execute("select sleep(12) from dual", {})

    try:
      master_conn = vtdb_test.get_connection(db_type='master')
    except Exception, e:
      self.fail("Connection to shard0 master failed with error %s" % str(e))
    with self.assertRaises(dbexceptions.TimeoutError):
      master_conn._execute("select sleep(12) from dual", {})

  # test timeout between vtgate and vttablet
  # the default timeout is 5 seconds
  def test_tablet_timeout(self):
    try:
      replica_conn = vtdb_test.get_connection(db_type='replica', shard_index=self.shard_index)
    except Exception, e:
      self.fail("Connection to shard0 replica failed with error %s" % str(e))
    with self.assertRaises(dbexceptions.DatabaseError):
      replica_conn.begin()
      replica_conn._execute("select sleep(7) from dual", {})

    try:
      master_conn = vtdb_test.get_connection(db_type='master')
    except Exception, e:
      self.fail("Connection to shard0 master failed with error %s" % str(e))
    with self.assertRaises(dbexceptions.DatabaseError):
      master_conn.begin()
      master_conn._execute("select sleep(7) from dual", {})

  def test_restart_mysql_failure(self):
    try:
      replica_conn = vtdb_test.get_connection(db_type='replica', shard_index=self.shard_index)
    except Exception, e:
      self.fail("Connection to shard0 replica failed with error %s" % str(e))
    utils.wait_procs([self.replica_tablet.shutdown_mysql(),])
    with self.assertRaises(dbexceptions.DatabaseError):
      replica_conn._execute("select 1 from vt_insert_test", {})
    utils.wait_procs([self.replica_tablet.start_mysql(),])
    self.replica_tablet.kill_vttablet()
    self.replica_tablet.start_vttablet()
    self.replica_tablet.wait_for_vttablet_state('SERVING')
    replica_conn._execute("select 1 from vt_insert_test", {})

  # FIXME(shrutip): this test is basically just testing that
  # txn pool full error doesn't get thrown anymore with vtgate.
  # vtgate retries for this condition. Not a very high value
  # test at this point, could be removed if there is coverage at vtgate level.
  def test_retry_txn_pool_full(self):
    master_conn = vtdb_test.get_connection(db_type='master')
    master_conn._execute("set vt_transaction_cap=1", {})
    master_conn.begin()
    master_conn2 = vtdb_test.get_connection(db_type='master')
    master_conn2.begin()
    master_conn.commit()
    master_conn._execute("set vt_transaction_cap=20", {})
    master_conn.begin()
    master_conn._execute("delete from vt_insert_test", {})
    master_conn.commit()


class TestAuthentication(vtdb_test.TestAuthentication):
  pass


# this test is just re-running an entire vtdb_test.py with a
# client type VTGate
if __name__ == '__main__':
  vtdb_test.vtgate_protocol = 'v1bson'
  vtdb_test.conn_class = vtgate.VtgateConnection
  utils.main()
