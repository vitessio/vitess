# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

from vtdb import base_cursor


class Cursor(base_cursor.BaseListCursor):

  def __init__(self, connection, tablet_type, single_db=False, twopc=False):
    super(Cursor, self).__init__(single_db=single_db, twopc=twopc)
    self._conn = connection
    self.tablet_type = tablet_type

  def execute(self, sql, bind_variables):
    self._clear_list_state()
    if self._handle_transaction_sql(sql):
      return
    self.results, self.rowcount, self.lastrowid, self.description = (
        self.connection._execute(sql, bind_variables, self.tablet_type))
    return self.rowcount


class StreamCursor(base_cursor.BaseStreamCursor):

  def __init__(self, connection, tablet_type):
    super(StreamCursor, self).__init__()
    self._conn = connection
    self.tablet_type = tablet_type

  def execute(self, sql, bind_variables, **kargs):
    self._clear_stream_state()
    self.generator, self.description = self.connection._stream_execute(
        sql, bind_variables, self.tablet_type)
    return 0
