# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import logging
import time

from vtdb import cursor
from vtdb import dbapi
from vtdb import dbexceptions
from vtdb import tablet
from vtdb import topo_utils
from vtdb import topology
from vtdb import vtdb_logger

RECONNECT_DELAY = 0.002 # 2 ms
BEGIN_RECONNECT_DELAY = 0.2 # 200 ms
MAX_RETRY_ATTEMPTS = 2


def get_vt_connection_params_list(topo_client, keyspace, shard, db_type,
                                  timeout, encrypted, user, password):
  return topo_utils.get_db_params_for_tablet_conn(topo_client, keyspace, shard,
                                                  db_type, timeout, encrypted,
                                                  user, password)


def reconnect(method):
  def _run_with_reconnect(self, *args, **kargs):
    attempt = 0
    while True:
      try:
        return method(self, *args, **kargs)
      except (dbexceptions.RetryError, dbexceptions.FatalError, dbexceptions.TxPoolFull) as e:
        attempt += 1
        # Execution attempt failed with OperationalError, re-read the keyspace.
        if not isinstance(e, dbexceptions.TxPoolFull):
          self.resolve_topology()

        if attempt >= self.max_attempts or self.in_txn:
          self.close()
          vtdb_logger.get_logger().vtclient_exception(self.keyspace, self.shard, self.db_type, e)
          raise dbexceptions.FatalError(*e.args)
        if method.__name__ == 'begin':
          time.sleep(BEGIN_RECONNECT_DELAY)
        else:
          time.sleep(RECONNECT_DELAY)
        if not isinstance(e, dbexceptions.TxPoolFull):
          logging.info("Attempting to reconnect, %d", attempt)
          self.close()
          self.connect()
          logging.info("Successfully reconnected to %s", str(self.conn))
        else:
          logging.info("Waiting to retry for dbexceptions.TxPoolFull to %s, attempt %d", str(self.conn), attempt)
  return _run_with_reconnect


# Provide compatibility with the MySQLdb query param style and prune bind_vars
class VtOCCConnection(object):
  cursorclass = cursor.TabletCursor

  def __init__(self, topo_client, keyspace, shard, db_type, timeout, user=None,
               password=None, encrypted=False, keyfile=None, certfile=None,
               vtgate_protocol='v0', vtgate_addrs=None):
    self.topo_client = topo_client
    self.keyspace = keyspace
    self.shard = str(shard)
    self.db_type = db_type
    self.timeout = timeout
    self.user = user
    self.password = password
    self.encrypted = encrypted
    self.keyfile = keyfile
    self.certfile = certfile
    self.vtgate_protocol = vtgate_protocol
    self.vtgate_addrs = vtgate_addrs
    self.conn = None
    self.max_attempts = MAX_RETRY_ATTEMPTS
    self.conn_db_params = None
    self.in_txn = False

  def __str__(self):
    return str(self.conn)

  @property
  def db_params(self):
    return self.conn_db_params

  def close(self):
    if self.conn:
      self.conn.close()

  def connect(self):
    try:
      return self._connect()
    except dbexceptions.OperationalError as e:
      vtdb_logger.get_logger().vtclient_exception(self.keyspace, self.shard, self.db_type, e)
      raise

  def _connect(self):
    db_key = "%s.%s.%s" % (self.keyspace, self.shard, self.db_type)
    db_params_list = get_vt_connection_params_list(self.topo_client,
                                                   self.keyspace,
                                                   self.shard,
                                                   self.db_type,
                                                   self.timeout,
                                                   self.encrypted,
                                                   self.user,
                                                   self.password)
    if not db_params_list:
      # no valid end-points were found, re-read the keyspace
      self.resolve_topology()
      raise dbexceptions.OperationalError("empty db params list - no db instance available for key %s" % db_key)
    db_exception = None
    host_addr = None
    # no retries here, since there is a higher level retry with reconnect.
    for params in db_params_list:
      try:
        db_params = params.copy()
        host_addr = db_params['addr']
        self.conn = tablet.TabletConnection(**db_params)
        self.conn.dial()
        self.conn_db_params = db_params
        return self.conn
      except Exception as e:
        db_exception = e
        logging.warning('db connection failed: %s %s, %s', db_key, host_addr, e)
        # vttablet threw an Operational Error on connect, re-read the keyspace
        if isinstance(e, dbexceptions.OperationalError):
          self.resolve_topology()

    raise dbexceptions.OperationalError(
      'unable to create vt connection', db_key, host_addr, db_exception)

  def cursor(self, cursorclass=None, **kargs):
    return (cursorclass or self.cursorclass)(self, **kargs)

  @reconnect
  def begin(self):
    result = self.conn.begin()
    self.in_txn = True
    return result

  def commit(self):
    result = self.conn.commit()
    self.in_txn = False
    return result

  def rollback(self):
    result = self.conn.rollback()
    self.in_txn = False
    return result

  @reconnect
  def _execute(self, sql, bind_variables):
    sql, bind_variables = dbapi.prepare_query_bind_vars(sql, bind_variables)
    try:
      result = self.conn._execute(sql, bind_variables)
    except dbexceptions.IntegrityError as e:
      vtdb_logger.get_logger().integrity_error(e)
      raise
    return result

  @reconnect
  def _execute_batch(self, sql_list, bind_variables_list):
    sane_sql_list = []
    sane_bind_vars_list = []
    for sql, bind_variables in zip(sql_list, bind_variables_list):
      sane_sql, sane_bind_vars = dbapi.prepare_query_bind_vars(sql, bind_variables)
      sane_sql_list.append(sane_sql)
      sane_bind_vars_list.append(sane_bind_vars)

    try:
      result = self.conn._execute_batch(sane_sql_list, sane_bind_vars_list)
    except dbexceptions.IntegrityError as e:
      vtdb_logger.get_logger().integrity_error(e)
      raise

    return result

  @reconnect
  def _stream_execute(self, sql, bind_variables):
    sql, bind_variables = dbapi.prepare_query_bind_vars(sql, bind_variables)
    result = self.conn._stream_execute(sql, bind_variables)
    return result

  def _stream_next(self):
    return self.conn._stream_next()

  # This function clears the cached value for the keyspace
  # and re-reads it from the toposerver once per 'n' secs.
  def resolve_topology(self):
    topology.refresh_keyspace(self.topo_client, self.keyspace)
