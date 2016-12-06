"""Base classes for cursors.

These classes centralize common code.
"""

from vtdb import dbexceptions


class BasePEP0249Cursor(object):
  """Cursor with common PEP0249 implementations."""

  def __init__(self):
    self._clear_common_state()
    self._conn = None

  def callproc(self):
    """For PEP 0249."""
    raise dbexceptions.NotSupportedError

  def executemany(self, sql, params_list):
    """For PEP 0249."""
    _ = sql, params_list
    raise dbexceptions.NotSupportedError

  def nextset(self):
    """For PEP 0249."""
    raise dbexceptions.NotSupportedError

  def setinputsizes(self, sizes):
    """For PEP 0249."""
    _ = sizes

  def setoutputsize(self, size, column=None):
    """For PEP 0249."""
    _ = size, column

  @property
  def rownumber(self):
    return self.index

  def __iter__(self):
    """For PEP 0249: To make cursors compatible to the iteration protocol."""
    return self

  def next(self):
    """For PEP 0249."""
    val = self.fetchone()
    if val is None:
      raise StopIteration
    return val

  def close(self):
    """For PEP 0249."""
    raise NotImplementedError

  def fetchone(self):
    """For PEP 0249."""
    raise NotImplementedError

  def fetchmany(self, size=None):
    """For PEP 0249."""
    raise NotImplementedError

  def fetchall(self):
    """For PEP 0249."""
    raise NotImplementedError

  def _clear_common_state(self):
    self.index = 0

  @property
  def connection(self):
    if not self._conn:
      raise dbexceptions.ProgrammingError(
          'Cannot use closed cursor %s.' % self.__class__)
    return self._conn


class BaseListCursor(BasePEP0249Cursor):
  """Base cursor where results are stored as a list.

  Execute call should return a (results, rowcount, lastrowid,
  description) tuple. The fetch commands traverse self.results.
  """
  arraysize = 1

  def __init__(self, single_db=False, twopc=False):
    super(BaseListCursor, self).__init__()
    self._clear_list_state()
    self.effective_caller_id = None
    self.single_db = single_db
    self.twopc = twopc

  def _clear_list_state(self):
    self._clear_common_state()
    self.description = None
    self.lastrowid = None
    self.rowcount = None
    self.results = None

  def set_effective_caller_id(self, effective_caller_id):
    """Set the effective caller id that will be used in upcoming calls."""
    self.effective_caller_id = effective_caller_id

  def begin(self):
    return self.connection.begin(
        effective_caller_id=self.effective_caller_id,
        single_db=self.single_db)

  def commit(self):
    return self.connection.commit(self.twopc)

  def rollback(self):
    return self.connection.rollback()

  def _check_fetch(self):
    if self.results is None:
      raise dbexceptions.ProgrammingError('Fetch called before execute.')

  def _handle_transaction_sql(self, sql):
    sql_check = sql.strip().lower()
    if sql_check == 'begin':
      self.begin()
      return True
    elif sql_check == 'commit':
      self.commit()
      return True
    elif sql_check == 'rollback':
      self.rollback()
      return True
    else:
      return False

  def close(self):
    self._clear_list_state()
    self._conn = None

  def fetchone(self):
    self._check_fetch()
    if self.index >= len(self.results):
      return None
    self.index += 1
    return self.results[self.index - 1]

  def fetchmany(self, size=None):
    self._check_fetch()
    if self.index >= len(self.results):
      return []
    if size is None:
      size = self.arraysize
    res = self.results[self.index:self.index + size]
    self.index += size
    return res

  def fetchall(self):
    self._check_fetch()
    return self.fetchmany(len(self.results) - self.index)


class BaseStreamCursor(BasePEP0249Cursor):
  """Base cursor where results are returned as a generator.

  This supports large queries. An execute call returns a (generator,
  description) pair. The fetch functions read items from the generator
  until it is exhausted.
  """

  arraysize = 1

  def __init__(self):
    super(BaseStreamCursor, self).__init__()
    self._clear_stream_state()
    self.effective_caller_id = None

  def set_effective_caller_id(self, effective_caller_id):
    """Set the effective caller id that will be used in upcoming calls."""
    self.effective_caller_id = effective_caller_id

  def _clear_stream_state(self):
    self._clear_common_state()
    self.description = None
    self.generator = None

  def fetchone(self):
    if self.description is None:
      raise dbexceptions.ProgrammingError('Fetch called before execute.')
    self.index += 1
    try:
      return self.generator.next()
    except StopIteration:
      return None

  # fetchmany can be called until it returns no rows. Returning less rows
  # than what we asked for is also an indication we ran out, but the cursor
  # API in PEP249 is silent about that.
  def fetchmany(self, size=None):
    if size is None:
      size = self.arraysize
    result = []
    for _ in xrange(size):
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

  def close(self):
    if self.generator:
      self.generator.close()
    self._clear_stream_state()
    self._conn = None
