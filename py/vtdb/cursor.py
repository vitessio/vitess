# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

from vtdb import base_cursor
from vtdb import dbexceptions


class BaseCursor(base_cursor.BaseListCursor):

  def __init__(self, connection):
    self._conn = connection
    self.description = None
    self.index = None
    self.lastrowid = None
    self.results = None
    self.rowcount = 0

  def close(self):
    self._conn = None
    self.results = None

  # pass kargs here in case higher level APIs need to push more data through
  # for instance, a key value for shard mapping
  def _execute(self, sql, bind_variables, **kargs):
    self.rowcount = 0
    self.results = None
    self.description = None
    self.lastrowid = None
    effective_caller_id = kargs.get('effective_caller_id')
    if self._handle_transaction_sql(sql, effective_caller_id):
      return
    self.results, self.rowcount, self.lastrowid, self.description = (
        self._conn._execute(sql, bind_variables, **kargs))
    self.index = 0
    return self.rowcount


# A simple cursor intended for attaching to a single tablet server.
class TabletCursor(BaseCursor):

  def execute(self, sql, bind_variables=None):
    return self._execute(sql, bind_variables)


class BatchCursor(BaseCursor):

  def __init__(self, connection):
    # rowset is [(results, rowcount, lastrowid, fields),]
    self.rowsets = None
    self.query_list = []
    self.bind_vars_list = []
    BaseCursor.__init__(self, connection)

  def execute(self, sql, bind_variables=None):
    self.query_list.append(sql)
    self.bind_vars_list.append(bind_variables)

  def flush(self, as_transaction=False):
    self.rowsets = self._conn._execute_batch(self.query_list,
                                                  self.bind_vars_list,
                                                  as_transaction)
    self.query_list = []
    self.bind_vars_list = []


# just used for batch items
class BatchQueryItem(object):

  def __init__(self, sql, bind_variables, key, keys):
    self.sql = sql
    self.bind_variables = bind_variables
    self.key = key
    self.keys = keys


class StreamCursor(base_cursor.BaseStreamCursor):

  def __init__(self, connection):
    self._conn = connection
    self.conversions = None
    self.description = None
    self.generator = None
    self.index = None
    self.fetchmany_done = False

  def close(self):
    self._conn = None

  # pass kargs here in case higher level APIs need to push more data through
  # for instance, a key value for shard mapping
  def execute(self, sql, bind_variables, **kargs):
    self.description = None
    result = self._conn._stream_execute(sql, bind_variables, **kargs)
    self._parse_stream_execute_result(self._conn, result)
    return 0
