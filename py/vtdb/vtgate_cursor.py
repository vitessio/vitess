# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

"""VTGateCursor, BatchVTGateCursor, and StreamVTGateCursor."""

import itertools
import operator
import re

from vtdb import base_cursor
from vtdb import dbexceptions

write_sql_pattern = re.compile(r'\s*(insert|update|delete)', re.IGNORECASE)


def ascii_lower(string):
    """Lower-case, but only in the ASCII range."""
    return string.encode('utf8').lower().decode('utf8')


class VTGateCursorMixin(object):

  def connection_list(self):
    return [self._conn]

  def is_writable(self):
    return self._writable


class VTGateCursor(base_cursor.BaseListCursor, VTGateCursorMixin):
  """A cursor for execute statements to VTGate.

  Results are stored as a list.
  """

  def __init__(
      self, connection, keyspace, tablet_type, keyspace_ids=None,
      keyranges=None, writable=False):
    super(VTGateCursor, self).__init__()
    self._conn = connection
    self._writable = writable
    self.description = None
    self.index = None
    self.keyranges = keyranges
    self.keyspace = keyspace
    self.keyspace_ids = keyspace_ids
    self.lastrowid = None
    self.results = None
    self.routing = None
    self.rowcount = 0
    self.tablet_type = tablet_type

  # pass kargs here in case higher level APIs need to push more data through
  # for instance, a key value for shard mapping
  def execute(self, sql, bind_variables, **kargs):
    self._clear_list_state()
    # FIXME: Remove effective_caller_id from interface.
    effective_caller_id = kargs.get('effective_caller_id')
    if effective_caller_id:
      self.set_effective_caller_id(effective_caller_id)
    if self._handle_transaction_sql(sql):
      return
    write_query = bool(write_sql_pattern.match(sql))
    # NOTE: This check may also be done at higher layers but adding it
    # here for completion.
    if write_query:
      if not self.is_writable():
        raise dbexceptions.DatabaseError('DML on a non-writable cursor', sql)

    self.results, self.rowcount, self.lastrowid, self.description = (
        self.connection._execute(
            sql,
            bind_variables,
            self.keyspace,
            self.tablet_type,
            keyspace_ids=self.keyspace_ids,
            keyranges=self.keyranges,
            not_in_transaction=not self.is_writable(),
            effective_caller_id=self.effective_caller_id))
    return self.rowcount

  def execute_entity_ids(
      self, sql, bind_variables, entity_keyspace_id_map, entity_column_name,
      effective_caller_id=None):
    # FIXME: Remove effective_caller_id from interface.
    self._clear_list_state()

    # This is by definition a scatter query, so raise exception.
    write_query = bool(write_sql_pattern.match(sql))
    if write_query:
      raise dbexceptions.DatabaseError(
          'execute_entity_ids is not allowed for write queries')
    # FIXME: Remove effective_caller_id from interface.
    if effective_caller_id is not None:
      self.set_effective_caller_id(effective_caller_id)
    self.results, self.rowcount, self.lastrowid, self.description = (
        self.connection._execute_entity_ids(
            sql,
            bind_variables,
            self.keyspace,
            self.tablet_type,
            entity_keyspace_id_map,
            entity_column_name,
            not_in_transaction=not self.is_writable(),
            effective_caller_id=self.effective_caller_id))
    return self.rowcount

  def fetch_aggregate_function(self, func):
    return func(row[0] for row in self.fetchall())

  def fetch_aggregate(self, order_by_columns, limit):
    sort_columns = []
    desc_columns = []
    for order_clause in order_by_columns:
      if type(order_clause) in (tuple, list):
        sort_columns.append(order_clause[0])
        if ascii_lower(order_clause[1]) == 'desc':
          desc_columns.append(order_clause[0])
      else:
        sort_columns.append(order_clause)
    # sort the rows and then trim off the prepended sort columns

    if sort_columns:
      sorted_rows = list(sort_row_list_by_columns(
          self.fetchall(), sort_columns, desc_columns))[:limit]
    else:
      sorted_rows = itertools.islice(self.fetchall(), limit)
    neutered_rows = [row[len(order_by_columns):] for row in sorted_rows]
    return neutered_rows


class BatchVTGateCursor(VTGateCursor):
  """Batch Cursor for VTGate.

  This cursor allows 'n' queries to be executed against
  'm' keyspace_ids. For writes though, it maybe preferable
  to only execute against one keyspace_id.

  This only supports keyspace_ids right now since that is what
  the underlying vtgate server supports.
  """

  def __init__(self, connection, tablet_type, writable=False):
    # rowset is [(results, rowcount, lastrowid, fields),]
    self.rowsets = None
    self.query_list = []
    self.bind_vars_list = []
    self.keyspace_list = []
    self.keyspace_ids_list = []
    super(BatchVTGateCursor, self).__init__(
        connection, '', tablet_type, writable=writable)

  def execute(self, sql, bind_variables, keyspace, keyspace_ids):
    self.query_list.append(sql)
    self.bind_vars_list.append(bind_variables)
    self.keyspace_list.append(keyspace)
    self.keyspace_ids_list.append(keyspace_ids)

  def flush(self, as_transaction=False, effective_caller_id=None):
    # FIXME: Remove effective_caller_id from interface.
    if effective_caller_id is not None:
      self.set_effective_caller_id(effective_caller_id)
    self.rowsets = self.connection._execute_batch(
        self.query_list,
        self.bind_vars_list,
        self.keyspace_list,
        self.keyspace_ids_list,
        self.tablet_type,
        as_transaction,
        self.effective_caller_id)
    self.query_list = []
    self.bind_vars_list = []
    self.keyspace_list = []
    self.keyspace_ids_list = []


class StreamVTGateCursor(base_cursor.BaseStreamCursor, VTGateCursorMixin):
  """A cursor for streaming statements to VTGate.

  Results are returned as a generator.
  """

  def __init__(
      self, connection, keyspace, tablet_type, keyspace_ids=None,
      keyranges=None, writable=False):
    super(StreamVTGateCursor, self).__init__()
    self._conn = connection
    self._writable = writable
    self.keyranges = keyranges
    self.keyspace = keyspace
    self.keyspace_ids = keyspace_ids
    self.routing = None
    self.tablet_type = tablet_type

  def is_writable(self):
    return self._writable

  # pass kargs here in case higher level APIs need to push more data through
  # for instance, a key value for shard mapping
  def execute(self, sql, bind_variables, **kargs):
    if self._writable:
      raise dbexceptions.ProgrammingError('Streaming query cannot be writable')
    self._clear_stream_state()
    # FIXME: Remove effective_caller_id from interface.
    effective_caller_id = kargs.get('effective_caller_id')
    if effective_caller_id is not None:
      self.set_effective_caller_id(effective_caller_id)
    self.generator, self.description = self.connection._stream_execute(
        sql,
        bind_variables,
        self.keyspace,
        self.tablet_type,
        keyspace_ids=self.keyspace_ids,
        keyranges=self.keyranges,
        not_in_transaction=not self.is_writable(),
        effective_caller_id=self.effective_caller_id)
    return 0


# assumes the leading columns are used for sorting
def sort_row_list_by_columns(row_list, sort_columns=(), desc_columns=()):
  for column_index, column_name in reversed(
      [x for x in enumerate(sort_columns)]):
    og = operator.itemgetter(column_index)
    if type(row_list) != list:
      row_list = sorted(
          row_list, key=og, reverse=bool(column_name in desc_columns))
    else:
      row_list.sort(key=og, reverse=bool(column_name in desc_columns))
  return row_list
