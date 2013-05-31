
from vtdb import cursor

# Standard cursor when connecting to a sharded backend.
class ShardedCursor(object):
  arraysize = 1
  lastrowid = None
  rowcount = 0
  results = None
  sconn = None
  description = None
  index = None

  def __init__(self, sconn):
    self.sconn = sconn

  def close(self):
    self.sconn = None
    self.results = None

  def execute(self, sql, bind_variables, keyspace_ids=()):
    self.rowcount = 0
    self.results = None
    self.description = None
    self.lastrowid = None

    self.results, self.rowcount, self.lastrowid, self.description = self.sconn._execute_for_keyspace_ids(
        sql, bind_variables, keyspace_id_list=keyspace_id_list)
    self.index = 0
    return self.rowcount

  def fetchone(self):
    try:
      return self.fetchmany(size=1)[0]
    except IndexError:
      return None

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


class SharededStreamCursor(object):
  def execute(self, sql, bind_vars, keyspace_ids=()):
    self.description = None
    self.index = 0
    raise NotImplementedError, 'convert keyspace_ids to shard_idxs'
    results, self.rowcount, self.lastrowid, self.description = self.sconn._stream_execute(sql, bind_vars, shard_idx_list=shard_idx_list)
    return self.rowcount

  def fetchone(self):
    if not self._stream_fields:
      raise dbexceptions.ProgrammingError('fetch called before execute')

    self.index += 1
    return self.sconn._stream_next()

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

  # fetchall() does not make a lot of sense with an unbounded stream, but included for completeness.
  def fetchall(self):
    result = []
    while True:
      row = self.fetchone()
      if row is None:
        break
      result.append(row)
    return result
