# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

from vtdb import base_cursor
from vtdb import dbexceptions


class Cursor(base_cursor.BaseListCursor):

  def __init__(self, connection, tablet_type):
    self._conn = connection
    self.tablet_type = tablet_type
    self.lastrowid = None
    self.rowcount = 0
    self.results = None
    self.description = None
    self.index = None

  def close(self):
    self.results = None

  def execute(self, sql, bind_variables):
    self.rowcount = 0
    self.results = None
    self.description = None
    self.lastrowid = None
    if self._handle_transaction_sql(sql, effective_caller_id=None):
      return
    self.results, self.rowcount, self.lastrowid, self.description = (
        self._conn._execute(sql, bind_variables, self.tablet_type))
    self.index = 0
    return self.rowcount


class StreamCursor(base_cursor.BaseStreamCursor):

  def __init__(self, connection, tablet_type):
    self._conn = connection
    self.tablet_type = tablet_type
    self.fetchmany_done = False
    self.description = None
    self.generator = None

  def execute(self, sql, bind_variables, **kargs):
    self.description = None
    result = self._conn._stream_execute(
        sql, bind_variables, self.tablet_type)
    self._parse_stream_execute_result(self._conn, result)
    return 0
