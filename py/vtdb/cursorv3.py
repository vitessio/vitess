# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

from vtdb import cursor
from vtdb import dbexceptions


class Cursor(object):
  _conn = None
  tablet_type = None
  arraysize = 1
  lastrowid = None
  rowcount = 0
  results = None
  description = None
  index = None

  def __init__(self, connection, tablet_type):
    self._conn = connection
    self.tablet_type = tablet_type

  def close(self):
    self.results = None

  def commit(self):
    return self._conn.commit()

  def begin(self):
    return self._conn.begin()

  def rollback(self):
    return self._conn.rollback()

  def execute(self, sql, bind_variables):
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

    self.results, self.rowcount, self.lastrowid, self.description = self._conn._execute(
        sql,
        bind_variables,
        self.tablet_type)
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


class StreamCursor(Cursor):
  arraysize = 1
  conversions = None
  connection = None
  description = None
  index = None
  fetchmany_done = False

  def execute(self, sql, bind_variables, **kargs):
    self.description = None
    x, y, z, self.description = self._conn._stream_execute(
        sql,
        bind_variables,
        self.tablet_type)
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
