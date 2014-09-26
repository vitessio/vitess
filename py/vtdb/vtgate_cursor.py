# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import itertools
import re

from vtdb import cursor
from vtdb import dbexceptions
from vtdb import keyrange_constants
from vtdb import topology


write_sql_pattern = re.compile('\s*(insert|update|delete)', re.IGNORECASE)


class VTGateCursor(object):
  arraysize = 1
  lastrowid = None
  rowcount = 0
  results = None
  _conn = None
  description = None
  index = None
  keyspace = None
  tablet_type = None
  keyspace_ids = None
  keyranges = None
  _writable = None

  def __init__(self, connection, keyspace, tablet_type, keyspace_ids=None, keyranges=None, writable=False):
    self._conn = connection
    self.keyspace = keyspace
    self.tablet_type = tablet_type
    self.keyspace_ids = keyspace_ids
    self.keyranges = keyranges
    self._writable = writable

  def connection_list(self):
    return [self._conn]

  def close(self):
    self.results = None

  def is_writable(self):
    return self._writable

  def commit(self):
    return self._conn.commit()

  def begin(self):
    return self._conn.begin()

  def rollback(self):
    return self._conn.rollback()

  # pass kargs here in case higher level APIs need to push more data through
  # for instance, a key value for shard mapping
  def execute(self, sql, bind_variables, **kargs):
    self.rowcount = 0
    self.results = None
    self.description = None
    self.lastrowid = None

    sql_check = sql.strip().lower()
    if sql_check == 'begin':
      self.begin()
      return
    elif sql_check == 'commit':
      self.commit()
      return
    elif sql_check == 'rollback':
      self.rollback()
      return

    write_query = bool(write_sql_pattern.match(sql))
    # NOTE: This check may also be done at high-layers but adding it here for completion.
    if write_query:
      if not self.is_writable():
        raise dbexceptions.DatabaseError('DML on a non-writable cursor', sql)

      # FIXME(shrutip): these checks maybe better on vtgate server.
      if topology.is_sharded_keyspace(self.keyspace, self.tablet_type):
        if self.keyspace_ids is None or len(self.keyspace_ids) != 1:
          raise dbexceptions.ProgrammingError('DML on zero or multiple keyspace ids is not allowed: %r'
                                              % self.keyspace_ids)
      else:
        if not self.keyranges or str(self.keyranges[0]) != keyrange_constants.NON_PARTIAL_KEYRANGE:
          raise dbexceptions.ProgrammingError('Keyrange not correct for non-sharded keyspace: %r'
                                              % self.keyranges)

    self.results, self.rowcount, self.lastrowid, self.description = self._conn._execute(
        sql,
        bind_variables,
        self.keyspace,
        self.tablet_type,
        keyspace_ids=self.keyspace_ids,
        keyranges=self.keyranges)
    self.index = 0
    return self.rowcount

  def execute_entity_ids(self, sql, bind_variables, entity_keyspace_id_map, entity_column_name):
    self.rowcount = 0
    self.results = None
    self.description = None
    self.lastrowid = None

    # This is by definition a scatter query, so raise exception.
    write_query = bool(write_sql_pattern.match(sql))
    if write_query:
      raise dbexceptions.DatabaseError('execute_entity_ids is not allowed for write queries')

    self.results, self.rowcount, self.lastrowid, self.description = self._conn._execute_entity_ids(
        sql,
        bind_variables,
        self.keyspace,
        self.tablet_type,
        entity_keyspace_id_map,
        entity_column_name)
    self.index = 0
    return self.rowcount


  def fetchone(self):
    if self.results is None:
      raise dbexceptions.ProgrammingError('fetch called before execute')

    if self.index >= len(self.results):
      return None
    self.index += 1
    return self.results[self.index-1]

  def fetchmany(self, size=None):
    if self.results is None:
      raise dbexceptions.ProgrammingError('fetch called before execute')

    if self.index >= len(self.results):
      return []
    if size is None:
      size = self.arraysize
    res = self.results[self.index:self.index+size]
    self.index += size
    return res

  def fetchall(self):
    if self.results is None:
      raise dbexceptions.ProgrammingError('fetch called before execute')
    return self.fetchmany(len(self.results)-self.index)

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
      sorted_rows = list(sort_row_list_by_columns(self.fetchall(), sort_columns, desc_columns))[:limit]
    else:
      sorted_rows = itertools.islice(self.fetchall(), limit)
    neutered_rows = [row[len(order_by_columns):] for row in sorted_rows]
    return neutered_rows

  def callproc(self):
    raise dbexceptions.NotSupportedError

  def executemany(self, *pargs):
    raise dbexceptions.NotSupportedError

  def nextset(self):
    raise dbexceptions.NotSupportedError

  def setinputsizes(self, sizes):
    pass

  def setoutputsize(self, size, column=None):
    pass

  @property
  def rownumber(self):
    return self.index

  def __iter__(self):
    return self

  def next(self):
    val = self.fetchone()
    if val is None:
      raise StopIteration
    return val


class BatchVTGateCursor(VTGateCursor):
  def __init__(self, connection, keyspace, tablet_type, keyspace_ids=None,
               keyranges=None, writable=False):
    # rowset is [(results, rowcount, lastrowid, fields),]
    self.rowsets = None
    self.query_list = []
    self.bind_vars_list = []
    VTGateCursor.__init__(self, connection, keyspace, tablet_type,
                          keyspace_ids=keyspace_ids, writable=writable)

  def execute(self, sql, bind_variables=None):
    self.query_list.append(sql)
    self.bind_vars_list.append(bind_variables)

  def flush(self):
    self.rowsets = self._conn._execute_batch(self.query_list,
                                              self.bind_vars_list,
                                              self.keyspace,
                                              self.tablet_type,
                                              self.keyspace_ids)
    self.query_list = []
    self.bind_vars_list = []


class StreamVTGateCursor(VTGateCursor):
  arraysize = 1
  conversions = None
  connection = None
  description = None
  index = None
  fetchmany_done = False

  def __init__(self, connection, keyspace, tablet_type, keyspace_ids=None, keyranges=None, writable=False):
    VTGateCursor.__init__(self, connection, keyspace, tablet_type, keyspace_ids=keyspace_ids, keyranges=keyranges)

  # pass kargs here in case higher level APIs need to push more data through
  # for instance, a key value for shard mapping
  def execute(self, sql, bind_variables, **kargs):
    if self._writable:
      raise dbexceptions.ProgrammingError('Streaming query cannot be writable')

    self.description = None
    x, y, z, self.description = self._conn._stream_execute(
        sql,
        bind_variables,
        self.keyspace,
        self.tablet_type,
        keyspace_ids=self.keyspace_ids,
        keyranges=self.keyranges)
    self.index = 0
    return 0

  def fetchone(self):
    if self.description is None:
      raise dbexceptions.ProgrammingError('fetch called before execute')

    self.index += 1
    return self._conn._stream_next()

  # fetchmany can be called until it returns no rows. Returning less rows
  # than what we asked for is also an indication we ran out, but the cursor
  # API in PEP249 is silent about that.
  def fetchmany(self, size=None):
    if size is None:
      size = self.arraysize
    result = []
    if self.fetchmany_done:
      self.fetchmany_done = False
      return result
    for i in xrange(size):
      row = self.fetchone()
      if row is None:
        self.fetchmany_done = True
        break
      result.append(row)
    return result

  def fetchall(self):
    result = []
    while True:
      row = self.fetchone()
      if row is None:
        break
      result.append(row)
    return result

  def callproc(self):
    raise dbexceptions.NotSupportedError

  def executemany(self, *pargs):
    raise dbexceptions.NotSupportedError

  def nextset(self):
    raise dbexceptions.NotSupportedError

  def setinputsizes(self, sizes):
    pass

  def setoutputsize(self, size, column=None):
    pass

  @property
  def rownumber(self):
    return self.index

  def __iter__(self):
    return self

  def next(self):
    val = self.fetchone()
    if val is None:
      raise StopIteration
    return val


# assumes the leading columns are used for sorting
def sort_row_list_by_columns(row_list, sort_columns=(), desc_columns=()):
  for column_index, column_name in reversed([x for x in enumerate(sort_columns)]):
    og = operator.itemgetter(column_index)
    if type(row_list) != list:
      row_list = sorted(
          row_list, key=og, reverse=bool(column_name in desc_columns))
    else:
      row_list.sort(key=og, reverse=bool(column_name in desc_columns))
  return row_list
