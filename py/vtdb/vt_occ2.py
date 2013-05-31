import errno
import logging
import re
import time
import socket

from vtdb import tablet3 as tablet2
from vtdb import dbapi
from vtdb import dbexceptions


RECONNECT_DELAY = 0.002

def reconnect(method):
  def _run_with_reconnect(self, *args, **kargs):
    attempt = 0
    while True:
      try:
        return method(self, *args, **kargs)
      except (tablet2.RetryError, tablet2.TimeoutError) as e:
        while True:
          attempt += 1
          if attempt >= self.max_attempts:
            raise tablet2.FatalError(*e.args)
          try:
            time.sleep(RECONNECT_DELAY)
            self.dial()
            if isinstance(e, tablet2.TimeoutError):
              raise e
            break
          except dbexceptions.OperationalError as dial_error:
            logging.warning('error dialing vtocc on %s %s (%s)',
                            method.__name__, self.addr, dial_error)
  return _run_with_reconnect


# Provide compatibility with the MySQLdb query param style and prune bind_vars
class VtOCCConnection(tablet2.TabletConnection):
  max_attempts = 2

  # Number of seconds after which we consider a connection permanently dead.
  @property
  def max_recovery_time(self):
    return self.timeout * 2

  # Track failures so that connections don't go on trying to recover
  # forever. This is the time that a connection first experienced a
  # failure after presumably being healthy.
  _time_failed = 0

  @reconnect
  def begin(self):
    result = tablet2.TabletConnection.begin(self)
    self._time_failed = 0
    return result

  def commit(self):
    result = tablet2.TabletConnection.commit(self)
    self._time_failed = 0
    return result

  @reconnect
  def _execute(self, sql, bind_variables):
    sql, bind_variables = dbapi.prepare_query_bind_vars(sql, bind_variables)
    result = tablet2.TabletConnection._execute(self, sql, bind_variables)
    self._time_failed = 0
    return result

  @reconnect
  def _execute_batch(self, sql_list, bind_variables_list):
    sane_sql_list = []
    sane_bind_vars_list = []
    for sql, bind_variables in zip(sql_list, bind_variables_list):
      sane_sql, sane_bind_vars = dbapi.prepare_query_bind_vars(sql, bind_variables)
      sane_sql_list.append(sane_sql)
      sane_bind_vars_list.append(sane_bind_vars)

    result = tablet2.TabletConnection._execute_batch(self, sane_sql_list, sane_bind_vars_list)
    self._time_failed = 0
    return result

  @reconnect
  def _stream_execute(self, sql, bind_variables):
    sql, bind_variables = dbapi.prepare_query_bind_vars(sql, bind_variables)
    result = tablet2.TabletConnection._stream_execute(self, sql, bind_variables)
    self._time_failed = 0
    return result


def connect(*pargs, **kargs):
  conn = VtOCCConnection(*pargs, **kargs)
  conn.dial()
  return conn
