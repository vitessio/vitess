#!/usr/bin/env python
#
# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import logging
import vertical_split
import utils

from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import vtgatev2

def setUpModule():
  vertical_split.setUpModule()

def tearDownModule():
  vertical_split.tearDownModule()

class TestVerticalSplitVTGate(vertical_split.TestVerticalSplit):
  def _vtdb_conn(self):
    conn = vtgatev2.VTGateConnection(self.vtgate_addrs['_vt'][0], 30)
    conn.dial()
    return conn

  def _insert_values(self, table, count, db_type='master', keyspace='source_keyspace'):
    result = self.insert_index
    conn = self._vtdb_conn()
    cursor = conn.cursor(None, conn, keyspace, db_type, keyranges=[keyrange.KeyRange(keyrange_constants.NON_PARTIAL_KEYRANGE)], writable=True)
    for i in xrange(count):
      conn.begin()
      cursor.execute("insert into %s (id, msg) values(%u, 'value %u')" % (
          table, self.insert_index, self.insert_index), {})
      conn.commit()
      self.insert_index += 1
    conn.close()
    return result

  def _check_client_conn_redirection(self, source_ks, destination_ks, db_types, servedfrom_db_types, moved_tables=None):
    # check that the ServedFrom indirection worked correctly.
    if moved_tables is None:
      moved_tables = []
    conn = self._vtdb_conn()
    for db_type in servedfrom_db_types:
      for tbl in moved_tables:
        try:
          rows = conn._execute("select * from %s" % tbl, {}, destination_ks, db_type, keyranges=[keyrange.KeyRange(keyrange_constants.NON_PARTIAL_KEYRANGE)])
          logging.debug("Select on %s.%s returned %d rows" % (db_type, tbl, len(rows)))
        except Exception, e:
          self.fail("Execute failed w/ exception %s" % str(e))

  def _check_stats(self):
    v = utils.get_vars(self.vtgate_port)
    self.assertEqual(v['VttabletCall']['Histograms']['Execute.source_keyspace.0.replica']['Count'], 2, "unexpected value for VttabletCall(Execute.source_keyspace.0.replica) inside %s" % str(v))
    self.assertEqual(v['VtgateApi']['Histograms']['ExecuteKeyRanges.destination_keyspace.master']['Count'], 6, "unexpected value for VtgateApi(ExecuteKeyRanges.destination_keyspace.master) inside %s" % str(v))
    self.assertEqual(len(v['VtgateApiErrorCounts']), 0, "unexpected errors for VtgateApiErrorCounts inside %s" % str(v))
    self.assertEqual(v['EndpointCount']['test_nj.source_keyspace.0.master'], 1, "unexpected EndpointCount inside %s" % str(v))
    self.assertEqual(v['DegradedEndpointCount']['test_nj.source_keyspace.0.master'], 0, "unexpected DegradedEndpointCount inside %s" % str(v))

if __name__ == '__main__':
  vertical_split.vtgate_protocol = vertical_split.VTGATE_PROTOCOL_V1BSON
  utils.main()
