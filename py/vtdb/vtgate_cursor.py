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

"""VTGateCursor, and StreamVTGateCursor."""

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
      self, connection, tablet_type, keyspace=None,
      shards=None, keyspace_ids=None, keyranges=None,
      writable=False, as_transaction=False, single_db=False,
      twopc=False):
    """Init VTGateCursor.

    Args:
      connection: A PEP0249 connection object.
      tablet_type: Str tablet_type.
      keyspace: Str keyspace or None if batch API will be used.
      shards: List of strings.
      keyspace_ids: Struct('!Q').packed keyspace IDs.
      keyranges: Str keyranges.
      writable: True if writable.
      as_transaction: True if an executemany call is its own transaction.
      single_db: True if single db transaction is needed.
      twopc: True if 2-phase commit is needed.
    """
    super(VTGateCursor, self).__init__(single_db=single_db, twopc=twopc)
    self._conn = connection
    self._writable = writable
    self.description = None
    self.index = None
    self.keyspace = keyspace
    self.shards = shards
    self.keyspace_ids = keyspace_ids
    self.keyranges = keyranges
    self.lastrowid = None
    self.results = None
    self.routing = None
    self.rowcount = 0
    self.tablet_type = tablet_type
    self.as_transaction = as_transaction
    self._clear_batch_state()

  # pass kwargs here in case higher level APIs need to push more data through
  # for instance, a key value for shard mapping
  def execute(self, sql, bind_variables, **kwargs):
    """Perform a query, return the number of rows affected."""
    self._clear_list_state()
    self._clear_batch_state()
    if self._handle_transaction_sql(sql):
      return
    entity_keyspace_id_map = kwargs.pop('entity_keyspace_id_map', None)
    entity_column_name = kwargs.pop('entity_column_name', None)
    write_query = bool(write_sql_pattern.match(sql))
    # NOTE: This check may also be done at higher layers but adding it
    # here for completion.
    if write_query:
      if not self.is_writable():
        raise dbexceptions.ProgrammingError('DML on a non-writable cursor', sql)
      if entity_keyspace_id_map:
        raise dbexceptions.ProgrammingError(
            'entity_keyspace_id_map is not allowed for write queries')
    # FIXME(alainjobart): the entity_keyspace_id_map should be in the
    # cursor, same as keyspace_ids, shards, keyranges, to avoid this hack.
    if entity_keyspace_id_map:
      shards = None
      keyspace_ids = None
      keyranges = None
    else:
      shards = self.shards
      keyspace_ids = self.keyspace_ids
      keyranges = self.keyranges
    self.results, self.rowcount, self.lastrowid, self.description = (
        self.connection._execute(  # pylint: disable=protected-access
            sql,
            bind_variables,
            tablet_type=self.tablet_type,
            keyspace_name=self.keyspace,
            shards=shards,
            keyspace_ids=keyspace_ids,
            keyranges=keyranges,
            entity_keyspace_id_map=entity_keyspace_id_map,
            entity_column_name=entity_column_name,
            not_in_transaction=not self.is_writable(),
            effective_caller_id=self.effective_caller_id,
            **kwargs))
    return self.rowcount

  def fetch_aggregate_function(self, func):
    return func(row[0] for row in self.fetchall())

  def fetch_aggregate(self, order_by_columns, limit):
    """Fetch from many shards, sort, then remove sort columns.

    A scatter query may return up to limit rows. Sort all results
    manually order them, and return the first rows.

    This is a special-use function.

    Args:
      order_by_columns: The ORDER BY clause. Each element is either a
        column, [column, 'ASC'], or [column, 'DESC'].
      limit: Int limit.

    Returns:
      Smallest rows, with up to limit items. First len(order_by_columns)
        columns are stripped.
    """
    sort_columns = []
    desc_columns = []
    for order_clause in order_by_columns:
      if isinstance(order_clause, (tuple, list)):
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

  def _clear_batch_state(self):
    """Clear state that allows traversal to next query's results."""
    self.result_sets = []
    self.result_set_index = None

  def close(self):
    super(VTGateCursor, self).close()
    self._clear_batch_state()

  def executemany(self, sql, params_list, **kwargs):
    """Execute multiple statements in one batch.

    This adds len(params_list) result_sets to self.result_sets.  Each
    result_set is a (results, rowcount, lastrowid, fields) tuple.

    Each call overwrites the old result_sets. After execution, nextset()
    is called to move the fetch state to the start of the first
    result set.

    Args:
      sql: The sql text, with %(format)s-style tokens. May be None.
      params_list: A list of the keyword params that are normally sent
        to execute. Either the sql arg or params['sql'] must be defined.
      **kwargs: passed as is to connection._execute_batch.
    """
    if sql:
      sql_list = [sql] * len(params_list)
    else:
      sql_list = [params.get('sql') for params in params_list]
    bind_variables_list = [params['bind_variables'] for params in params_list]
    keyspace_list = [params['keyspace'] for params in params_list]
    keyspace_ids_list = [params.get('keyspace_ids') for params in params_list]
    shards_list = [params.get('shards') for params in params_list]
    self._clear_batch_state()
    #  Find other _execute_batch calls in test code.
    self.result_sets = self.connection._execute_batch(  # pylint: disable=protected-access
        sql_list, bind_variables_list, keyspace_list, keyspace_ids_list,
        shards_list,
        self.tablet_type, self.as_transaction, self.effective_caller_id,
        **kwargs)
    self.nextset()

  def nextset(self):
    """Move the fetch state to the start of the next result set.

    self.(results, rowcount, lastrowid, description) will be set to
    the next result_set, and the fetch-commands will work on this
    result set.

    Returns:
      True if another result set exists, False if not.
    """
    if self.result_set_index is None:
      self.result_set_index = 0
    else:
      self.result_set_index += 1

    self._clear_list_state()
    if self.result_set_index < len(self.result_sets):
      self.results, self.rowcount, self.lastrowid, self.description = (
          self.result_sets[self.result_set_index])
      return True
    else:
      self._clear_batch_state()
      return None


class StreamVTGateCursor(base_cursor.BaseStreamCursor, VTGateCursorMixin):

  """A cursor for streaming statements to VTGate.

  Results are returned as a generator.
  """

  def __init__(
      self, connection, tablet_type, keyspace=None,
      shards=None, keyspace_ids=None,
      keyranges=None, writable=False):
    super(StreamVTGateCursor, self).__init__()
    self._conn = connection
    self._writable = writable
    self.keyspace = keyspace
    self.shards = shards
    self.keyspace_ids = keyspace_ids
    self.keyranges = keyranges
    self.routing = None
    self.tablet_type = tablet_type

  def is_writable(self):
    return self._writable

  # pass kwargs here in case higher level APIs need to push more data through
  # for instance, a key value for shard mapping
  def execute(self, sql, bind_variables, **kwargs):
    """Start a streaming query."""
    if self._writable:
      raise dbexceptions.ProgrammingError('Streaming query cannot be writable')
    self._clear_stream_state()
    self.generator, self.description = self.connection._stream_execute(  # pylint: disable=protected-access
        sql,
        bind_variables,
        tablet_type=self.tablet_type,
        keyspace_name=self.keyspace,
        shards=self.shards,
        keyspace_ids=self.keyspace_ids,
        keyranges=self.keyranges,
        not_in_transaction=not self.is_writable(),
        effective_caller_id=self.effective_caller_id,
        **kwargs)
    return 0


def sort_row_list_by_columns(row_list, sort_columns=(), desc_columns=()):
  """Sort by leading sort columns by stable-sorting in reverse-index order."""
  for column_index, column_name in reversed(
      [x for x in enumerate(sort_columns)]):
    og = operator.itemgetter(column_index)
    if not isinstance(row_list, list):
      row_list = sorted(
          row_list, key=og, reverse=bool(column_name in desc_columns))
    else:
      row_list.sort(key=og, reverse=bool(column_name in desc_columns))
  return row_list
