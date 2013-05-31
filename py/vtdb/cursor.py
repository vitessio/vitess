# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

from vtdb import dbexceptions

class BaseCursor(object):
  arraysize = 1
  lastrowid = None
  rowcount = 0
  results = None
  connection = None
  description = None
  index = None

  def __init__(self, connection):
    self.connection = connection

  def close(self):
    self.connection = None
    self.results = None

  # pass kargs here in case higher level APIs need to push more data through
  # for instance, a key value for shard mapping
  def _execute(self, sql, bind_variables, **kargs):
    self.rowcount = 0
    self.results = None
    self.description = None
    self.lastrowid = None

    self.results, self.rowcount, self.lastrowid, self.description = self.connection._execute(sql, bind_variables, **kargs)
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

# A simple cursor intended for attaching to a single tablet server.
class TabletCursor(BaseCursor):
  def execute(self, sql, bind_variables=None):
    return self._execute(sql, bind_variables)


class BatchCursor(BaseCursor):
  def __init__(self, connection):
    self.exec_list = []
    BaseCursor.__init__(self, connection)

  def execute(self, sql, bind_variables=None, key=None, keys=None):
    self.exec_list.append(BatchQueryItem(sql, bind_variables, key, keys))

  def flush(self):
    self.rowcount = self.connection._exec_batch(self.exec_list)
    self.exec_list = []


# just used for batch items
class BatchQueryItem(object):
  def __init__(self, sql, bind_variables, key, keys):
    self.sql = sql
    self.bind_variables = bind_variables
    self.key = key
    self.keys = keys

class StreamCursor(object):
  arraysize = 1
  conversions = None
  connection = None
  description = None
  index = None

  def __init__(self, connection):
    self.connection = connection

  def close(self):
    self.connection = None

  # pass kargs here in case higher level APIs need to push more data through
  # for instance, a key value for shard mapping
  def execute(self, sql, bind_variables, **kargs):
    self.description = None
    _, _, _, self.description = self.connection._stream_execute(sql, bind_variables, **kargs)
    self.index = 0
    return 0

  def fetchone(self):
    if self.description is None:
      raise dbexceptions.ProgrammingError('fetch called before execute')

    self.index += 1
    return self.connection._stream_next()

  # fetchmany can be called until it returns no rows. Returning less rows
  # than what we asked for is also an indication we ran out. But the cursor
  # may not know that.
  def fetchmany(self, size=None):
    if size is None:
      size = self.arraysize
    result = []
    for i in xrange(size):
      row = self.fetchone()
      if row is None:
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
