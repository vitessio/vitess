"""Database Context sets the environment for accessing database via vtgate.

This modules has classes and methods that initialize the vitess system.
This includes -
* DatabaseContext is a global object that sets the mode of operation,
logging handlers, connection to VTGate etc.
* Error logging helpers
* Database decorators that set the local context for a set of
related queries that help with query routing and transaction management.

"""

# Copyright 2013 Google Inc. All Rights Reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import contextlib
import functools
import logging

from vtdb import dbexceptions
from vtdb import shard_constants
from vtdb import vtdb_logger
from vtdb import vtgatev2


#TODO: verify that these values make sense.
DEFAULT_CONNECTION_TIMEOUT = 5.0
DEFAULT_QUERY_TIMEOUT = 15.0

__app_read_only_mode_method = lambda:False
__vtgate_connect_method = vtgatev2.connect

class DatabaseContext(object):
  """Global Database Context for client db operations via VTGate.

  This is the main entry point for db access. Responsibilities include -
  * Setting the operational environment.
  * Manages connection to VTGate.
  * Manages the database transaction.
  * Error logging and handling.

  Attributes:
    lag_tolerant_mode: This directs all replica traffic to batch replicas.
    This is done for applications that have a OLAP workload and also higher tolerance
    for replication lag.
    vtgate_addrs: vtgate server endpoints
    master_access_disabled: Disallow master access for application running in non-master
    capable cells.
    event_logger: Logs events and errors of note. Defaults to vtdb_logger.
    transaction_stack_depth: This allows nesting of transactions and makes
    commit rpc to VTGate when the outer-most commits.
    vtgate_connection: Connection to VTGate.
  """

  def __init__(self, vtgate_addrs=None, lag_tolerant_mode=False, master_access_disabled=False):
    self.vtgate_addrs = vtgate_addrs
    self.lag_tolerant_mode = lag_tolerant_mode
    self.master_access_disabled = master_access_disabled
    self.vtgate_connection = None
    self.change_master_read_to_replica = False
    self._transaction_stack_depth = 0
    self.connection_timeout = DEFAULT_CONNECTION_TIMEOUT
    self.query_timeout = DEFAULT_QUERY_TIMEOUT
    self.event_logger = vtdb_logger.get_logger()
    self._tablet_type = None
    self._write_query_count = 0

  @property
  def tablet_type(self):
    return self._tablet_type

  @property
  def in_transaction(self):
    return self._transaction_stack_depth > 0

  @property
  def in_db_operation(self):
    return (self._tablet_type is not None)

  def get_vtgate_connection(self):
    """Returns the cached vtgate connection or creates a new one.

    Transactions and some of the consistency guarantees rely on vtgate
    connections being sticky hence this class caches the connection.
    """
    if self.vtgate_connection is not None and not self.vtgate_connection.is_closed():
      return self.vtgate_connection

    #TODO: the connect method needs to be extended to include query n txn timeouts as well
    #FIXME: what is the best way of passing other params ?
    connect_method = get_vtgate_connect_method()
    self.vtgate_connection = connect_method(self.vtgate_addrs, self.connection_timeout)
    return self.vtgate_connection

  def degrade_master_read_to_replica(self):
    self.change_master_read_to_replica = True

  def start_transaction(self):
    conn = self.get_vtgate_connection()
    if self._transaction_stack_depth == 0:
      conn.begin()
    self._transaction_stack_depth += 1

  def commit(self):
    if self._transaction_stack_depth:
      self._transaction_stack_depth -= 1

    if self._transaction_stack_depth != 0:
      return

    if self.vtgate_connection is None:
      return
    self.vtgate_connection.commit()
    self._write_query_count = 0

  def rollback(self):
    self._transaction_stack_depth = 0
    try:
      if self.vtgate_connection is not None:
        self.vtgate_connection.rollback()
      self._write_query_count = 0
    except dbexceptions.OperationalError:
      self.vtgate_connection.close()
      self.vtgate_connection = None
    except Exception as e:
      raise

  def close(self):
    if self._transaction_stack_depth:
      self.rollback()
    self.vtgate_connection.close()

  def read_from_master_setup(self):
    self._tablet_type = shard_constants.TABLET_TYPE_MASTER
    if self.master_access_disabled:
      raise dbexceptions.Error("Master access is disabled.")
    if app_read_only_mode() and self.change_master_read_to_replica:
      self._tablet_type = shard_constants.TABLET_TYPE_REPLICA

  def read_from_replica_setup(self):
    self._tablet_type = shard_constants.TABLET_TYPE_REPLICA

    # During a write transaction, all reads are promoted to
    # read from master.
    if self._transaction_stack_depth > 0:
      self._tablet_type = shard_constants.TABLET_TYPE_MASTER
    elif self.lag_tolerant_mode:
      self._tablet_type = shard_constants.TABLET_TYPE_BATCH

  def write_transaction_setup(self):
    if self.master_access_disabled:
      raise dbexceptions.Error("Cannot write, master access is disabled.")
    self._tablet_type = shard_constants.TABLET_TYPE_MASTER

  def close_db_operation(self):
    self._tablet_type = None

  def create_cursor(self, writable, table_class, **kwargs):
    if not self.in_db_operation:
      raise dbexceptions.ProgrammingError(
          "Cannot execute queries outside db operations context.")
    cursor = table_class.create_vtgate_cursor(self.get_vtgate_connection(),
                                            self.tablet_type,
                                            writable, **kwargs)
    if writable:
      #if self._write_query_count == 0:
      #  cursor.begin()
      self._write_query_count += 1

    return cursor


class DBOperationBase(object):
  def __init__(self, db_context):
    self.dc = db_context
    self.writable = False

  def get_cursor(self):
    return functools.partial(self.dc.create_cursor, self.writable)


class ReadFromMaster(DBOperationBase):
  def __enter__(self):
    self.dc.read_from_master_setup()
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    self.dc.close_db_operation()
    if exc_type is None:
      return True
    if isinstance(exc_type, dbexceptions.OperationalError):
      self.dc.event_logger.vtgatev2_exception(exc_value)
      self.dc.vtgate_connection.close()
      self.dc.vtgate_connection = None


class ReadFromReplica(DBOperationBase):
  def __enter__(self):
    self.dc.read_from_replica_setup()
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    self.dc.close_db_operation()
    if exc_type is None:
      return True
    if isinstance(exc_type, dbexceptions.OperationalError):
      self.dc.event_logger.vtgatev2_exception(exc_value)
      self.dc.vtgate_connection.close()
      self.dc.vtgate_connection = None


class WriteTransaction(DBOperationBase):
  def __enter__(self):
    self.writable = True
    self.dc.write_transaction_setup()
    self.dc.start_transaction()
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    self.dc.close_db_operation()
    if exc_type is None:
      self.dc.commit()
      return True

    if isinstance(exc_type, dbexceptions.OperationalError):
      self.dc.vtgate_connection.close()
      self.dc.vtgate_connection = None
    else:
      if self.dc.vtgate_connection is not None:
        self.dc.rollback()
      if isinstance(exc_type, dbexceptions.IntegrityError):
        self.dc.event_logger.integrity_error(exc_value)
      else:
        self.dc.event_logger.vtgatev2_exception(exc_value)


def read_from_master(method):
  def _read_from_master(*pargs, **kargs):
    dc = open_context()
    with ReadFromMaster(dc) as context:
      cursor_method = context.get_cursor()
      if pargs[1:]:
        return method(cursor_method, *pargs[1:], **kargs)
      else:
        return method(cursor_method, **kargs)
  return _read_from_master


__database_context = None

def open_context(*pargs, **kargs):
  """Returns the existing global database context or creates a new one."""
  global __database_context

  if __database_context is None:
    __database_context = DatabaseContext(*pargs, **kargs)
  return __database_context


def close():
  """Close the global database context and close any open connections."""
  global __database_context
  if __database_context is not None:
    __database_context.close()
    __database_context = None


def register_app_read_only_mode_method(func):
  global __app_read_only_mode_method
  __app_read_only_mode_method = func


def app_read_only_mode():
  global __app_read_only_mode_method
  return __app_read_only_mode_method()


def register_create_vtgate_connection_method(connect_method):
  global __vtgate_connect_method
  __vtgate_connect_method = connect_method

def get_vtgate_connect_method():
  global __vtgate_connect_method
  return __vtgate_connect_method
