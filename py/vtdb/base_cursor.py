"""Base classes for cursors.

These classes centralize common code.
"""

import logging

from vtdb import dbexceptions

class BasePEP0249Cursor(object):
  """Cursor with common PEP0249 implementations."""

  def callproc(self):
    """For PEP 0249."""
    raise dbexceptions.NotSupportedError

  def executemany(self, *pargs):
    """For PEP 0249."""
    _ = pargs
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


class BaseListCursor(BasePEP0249Cursor):
  """Base cursor where results are stored as a list.

  An execute call to self._conn returns a (results, rowcount,
  lastrowid, description) tuple. The fetch commands walk self.results.
  """
  arraysize = 1

  def begin(self, effective_caller_id=None):
    return self._conn.begin(effective_caller_id)

  def commit(self):
    return self._conn.commit()

  def rollback(self):
    return self._conn.rollback()

  def _check_fetch(self):
    if self.results is None:
      raise dbexceptions.ProgrammingError('fetch called before execute')

  def _handle_transaction_sql(self, sql, effective_caller_id):
    sql_check = sql.strip().lower()
    if sql_check == 'begin':
      self.begin(effective_caller_id)
      return True
    elif sql_check == 'commit':
      self.commit()
      return True
    elif sql_check == 'rollback':
      self.rollback()
      return True
    else:
      return False

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

  def _parse_stream_execute_result(self, connection, result):
    if connection.stream_execute_returns_generator:
      self.generator, self.description = result
    else:
      # Deprecate this code path after all client code is updated.
      _, _, _, self.description = result
      self.generator = None
    self.index = 0

  def fetchone(self):
    if self.description is None:
      raise dbexceptions.ProgrammingError('fetch called before execute')
    self.index += 1
    if self.generator:
      try:
        return self.generator.next()
      except StopIteration:
        return None
    else:
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
    for _ in xrange(size):
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

  def close(self):
    if self.generator:
      self.generator.close()
      self.generator = None
