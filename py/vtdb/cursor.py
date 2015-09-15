# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

from vtdb import base_cursor


class BaseCursor(base_cursor.BaseListCursor):

  def __init__(self, connection):
    super(BaseCursor, self).__init__()
    self._conn = connection

  # pass kargs here in case higher level APIs need to push more data through
  # for instance, a key value for shard mapping
  def _execute(self, sql, bind_variables, **kargs):
    self._clear_list_state()
    # FIXME: Remove effective_caller_id from interface.
    effective_caller_id = kargs.get('effective_caller_id')
    if effective_caller_id:
      self.set_effective_caller_id(effective_caller_id)
    if self._handle_transaction_sql(sql):
      return
    self.results, self.rowcount, self.lastrowid, self.description = (
        self.connection._execute(sql, bind_variables, **kargs))
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
    self.rowsets = self.connection._execute_batch(
        self.query_list, self.bind_vars_list, as_transaction)
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
    super(StreamCursor, self).__init__()
    self._conn = connection

  # pass kargs here in case higher level APIs need to push more data through
  # for instance, a key value for shard mapping
  def execute(self, sql, bind_variables, **kargs):
    self._clear_stream_state()
    self.generator, self.description = self.connection._stream_execute(
        sql, bind_variables, **kargs)
    return 0
