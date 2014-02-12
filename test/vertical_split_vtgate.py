#!/usr/bin/python
#
# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import logging
import vertical_split
import utils

def setUpModule():
  vertical_split.setUpModule()

def tearDownModule():
  vertical_split.tearDownModule()

class TestVerticalSplitVTGate(vertical_split.TestVerticalSplit):
  def _check_client_conn_redirection(self, source_ks, destination_ks, db_types, servedfrom_db_types, moved_tables=None):
    # check that the ServedFrom indirection worked correctly.
    if moved_tables is None:
      moved_tables = []
    for db_type in servedfrom_db_types:
      conn = self._vtdb_conn(db_type, keyspace=destination_ks)
      for tbl in moved_tables:
        try:
          rows = conn._execute("select * from %s" % tbl, {})
          logging.debug("Select on %s.%s returned %d rows" % (db_type, tbl, len(rows)))
        except Exception, e:
          self.fail("Execute failed w/ exception %s" % str(e))

    # check that the connection to db_type for destination keyspace works too.
    for db_type in db_types:
      dest_conn = self._vtdb_conn(db_type, keyspace=destination_ks)
      self.assertEqual(dest_conn.db_params['keyspace'], destination_ks)


if __name__ == '__main__':
  vertical_split.vtgate_protocol = vertical_split.VTGATE_PROTOCOL_V1BSON
  utils.main()


