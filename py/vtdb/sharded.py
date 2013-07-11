# A client that abstracts shard placement.

import os
import time

from net import gorpc
from vtdb import cursor
from vtdb import dbapi
from vtdb import dbexceptions
from vtdb import keyspace
from vtdb import tablet3
from zk import zkns_query
from zk import zkocc

# zkocc_addrs - bootstrap addresses for resolving other endpoints
# local_cell - name of the local cell
# keyspace_name - as in /zk/local/vt/ns/<keyspace_name>
# db_type - master/replica/rdonly
def connect(zkocc_addrs, local_cell, keyspace_name, db_type, use_streaming, timeout, user, password, dbname):
  zk_client = zkocc.ZkOccConnection(zkocc_addrs, local_cell, timeout)
  return ShardedClient(zk_client, keyspace_name, db_type, use_streaming, timeout, user, password, dbname)

# Track the connections and statement issues with a transaction context.
# This is fundamentally a misnomer since we do not support transactions between shards.
#
class Txn(object):
  conns = []
  stmts = []

class ShardedClient(object):
  zkocc_client = None
  keyspace_name = ''
  keyspace = None
  db_type = ''
  db_name = ''
  use_streaming = False
  user = ''
  password = ''
  timeout = 0
  max_attempts = 2
  reconnect_delay = 0.005

  cursorclass = cursor.ShardedCursor

  def __init__(self, zkocc_client, keyspace_name, db_type, use_streaming, timeout, user, password, dbname):
    self.zkocc_client = zkocc_client
    self.keyspace_name = keyspace_name
    self.db_type = db_type
    self.use_streaming = use_streaming
    self.timeout = timeout
    self.user = user
    self.password = password
    self.dbname = dbname
    self.txn = None
    self.keyspace = keyspace.read_keyspace(self.zkocc_client, keyspace_name)
    self.conns = [None] * self.keyspace.shard_count
    self._streaming_shard_list = [] # shards with active streams

  def _dial_shard(self, shard_idx):
    shard_name = self.keyspace.shard_names[shard_idx]
    name_path = os.path.join(keyspace.ZK_KEYSPACE_PATH, self.keyspace_name, shard_name, self.db_type)
    addrs = zkns_query.lookup_name(zkocc_client, name_path)
    for addr in addrs:
      tablet_conn = tablet3.TabletConnection(addr, self.keyspace_name, shard_name, self.timeout, self.user, self.password)
      try:
        tablet_conn.dial()
        self.conns[shard_idx] = tablet_conn
        return tablet_conn
      except dbexceptions.OperationalError:
        # FIXME(msolomon) Implement retry deadline.
        pass
    raise dbexceptions.OperationalError('no tablet available for shard', name_path)

  def begin(self):
    if self.txn is not None:
      raise dbexceptions.ProgrammingError('nested transaction are not supported')
    self.txn = Txn()

  # Fast-fail on the first commit, but otherwise raise a PartialCommitError.
  # NOTE: Multi-db commits should be rare, so no need to do this in parallel yet.
  def commit(self):
    try:
      if self.txn:
        err_conns = []
        for i, conn in enumerate(self.txn.conns):
          if conn.is_closed():
            err_conns.append(conn)
        if err_conns:
          raise dbexceptions.OperationalError('tablets offline', [str(x) for x in err_conn])
        for i, conn in enumerate(self.txn.conns):
          try:
            conn.commit()
          except dbexceptions.DatabaseError as e:
            err_conns.append(conn)
            # If our first commit fails, just raise the error after rolling back
            # everything else.
            if i == 0:
              try:
                self.rollback()
              except dbexceptions.DatabaseError:
                pass
              raise e
        if err_conns and len(err_conns) != len(self.txn.conns):
          raise dbexceptions.PartialCommitError(err_conns)
    except dbexceptions.DatabaseError:
      # If a DatabaseError occurred, scan for dead connections and remove
      # them so they will be recreated.
      for i, conn in enumerate(self.conns):
        if conn.is_closed():
          self.conns[i] = None
    finally:
      self.txn = None

  # NOTE: Multi-db rollbacks should be rare, so no need to do this in parallel yet.
  def rollback(self):
    try:
      if self.txn:
        for conn in self.txn.conns:
          try:
            conn.rollback()
          except dbexceptions.DatabaseError:
            logging.warning('rollback failed: %s', conn)
    finally:
      self.txn = None

  def _execute_on_shards(self, query, bind_vars, shard_idx_list):
    # FIXME(msolomon) This needs to be parallel, without threads?
    raise NotImplementedError
    for i in shard_idx_list:
      self._execute_on_shards(query, bind_vars, i)

  def _stream_execute_on_shards(self, query, bind_vars, shard_idx_list):
    self._streaming_shard_list = shard_idx_list
    # FIXME(msolomon) This needs to be parallel, without threads?
    raise NotImplementedError
    for i in shard_idx_list:
      self._execute_on_shards(query, bind_vars, i)

  def _stream_next_on_shards(self, shard_idx_list=None):
    if shard_idx_list is None:
      shard_idx_list = self.streaming_shard_list
    # FIXME(msolomon) This needs to be parallel, without threads?
    raise NotImplementedError
    for i in shard_idx_list:
      self._stream_next_on_shard(i)

  def _execute_for_keyspace_ids(self, query, bind_vars, keyspace_id_list):
    shard_idx_list = list(set([self.keyspace.keyspace_id_to_shard_index(k)
                               for k in keyspace_id_list]))
    return self._execute_on_shards(query, bind_vars, shard_idx_list)

  def _stream_execute_for_keyspace_ids(self, query, bind_vars, keyspace_id_list):
    shard_idx_list = list(set([self.keyspace.keyspace_id_to_shard_index(k)
                               for k in keyspace_id_list]))
    return self._stream_execute_on_shards(query, bind_vars, shard_idx_list)

  def _begin(self, shard_idx):
    for x in xrange(self.max_attempts):
      try:
        conn = self.conns[shard_idx]
        if conn is None:
          conn = self._dial_shard(shard_idx)
        return conn.begin()
      except dbexceptions.OperationalError as e:
        # Tear down regardless of the precise failure.
        self.conns[shard_idx] = None
        if isinstance(e, tablet3.TimeoutError):
          # On any timeout let the error bubble up and just redial next time.
          raise e

        if isinstance(e, tablet3.RetryError):
          # Give the tablet a moment to restart itself. This isn't
          # strictly necessary since there is a significant chance you
          # will end up talking to another host.
          time.sleep(self.reconnect_delay)
    raise dbexceptions.OperationalError('tablets unreachable', self.keyspace_name, shard_idx, self.db_type)

  def _execute_on_shard(self, query, bind_vars, shard_idx):
    query, bind_vars = dbapi.prepare_query_bind_vars(query, bind_vars)
    for x in xrange(self.max_attempts):
      try:
        conn = self.conns[shard_idx]
        if conn is None:
          conn = self._dial_shard(shard_idx)

        if self.txn:
          self.txn.stmts.append(query)
          if conn not in self.txt.conns:
            # Defer the begin until we actually issue a statement.
            conn.begin()
            self.txt.conns.append(conn)

        return conn._execute(query, bind_vars)
      except dbexceptions.OperationalError as e:
        # Tear down regardless of the precise failure.
        self.conns[shard_idx] = None
        if isinstance(e, tablet3.TimeoutError):
          # On any timeout let the error bubble up and just redial next time.
          raise e

        if isinstance(e, tablet3.RetryError):
          # Give the tablet a moment to restart itself. This isn't
          # strictly necessary since there is a significant chance you
          # will end up talking to another host.
          time.sleep(self.reconnect_delay)
    raise dbexceptions.OperationalError('tablets unreachable', self.keyspace_name, shard_idx, self.db_type)


  def _execute_batch(self, query_list, bind_vars_list, shard_idx):
    new_query_list = []
    new_bind_vars_list = []
    for query, bind_vars in zip(query_list, bind_vars_list):
      query, bind_vars = dbapi.prepare_query_bind_vars(query, bind_vars)
      new_query_list.append(query)
      new_bind_vars_list.append(bind_vars)
    query_list = new_query_list
    bind_vars_list = new_bind_vars_list

    for x in xrange(self.max_attempts):
      try:
        conn = self.conns[shard_idx]
        if conn is None:
          conn = self._dial_shard(shard_idx)

        return conn._execute_batch(query_list, bind_vars_list)
      except dbexceptions.OperationalError as e:
        # Tear down regardless of the precise failure.
        self.conns[shard_idx] = None
        if isinstance(e, tablet3.TimeoutError):
          # On any timeout let the error bubble up and just redial next time.
          raise e

        if isinstance(e, tablet3.RetryError):
          # Give the tablet a moment to restart itself. This isn't
          # strictly necessary since there is a significant chance you
          # will end up talking to another host.
          time.sleep(self.reconnect_delay)
    raise dbexceptions.OperationalError('tablets unreachable', self.keyspace_name, shard_idx, self.db_type)


  def _stream_execute_on_shard(self, query, bind_vars, shard_idx):
    query, bind_vars = dbapi.prepare_query_bind_vars(query, bind_vars)
    for x in xrange(self.max_attempts):
      try:
        conn = self.conns[shard_idx]
        if conn is None:
          conn = self._dial_shard(shard_idx)

        return conn._stream_execute(self, query, bind_vars)
      except dbexceptions.OperationalError as e:
        # Tear down regardless of the precise failure.
        self.conns[shard_idx] = None
        if isinstance(e, tablet3.TimeoutError):
          # On any timeout let the error bubble up and just redial next time.
          raise e

        if isinstance(e, tablet3.RetryError):
          # Give the tablet a moment to restart itself. This isn't
          # strictly necessary since there is a significant chance you
          # will end up talking to another host.
          time.sleep(self.reconnect_delay)
    raise dbexceptions.OperationalError('tablets unreachable', self.keyspace_name, shard_idx, self.db_type)

  def _stream_next_on_shard(self, shard_idx):
    # NOTE(msolomon) This action cannot be retried.
    try:
      conn = self.conns[shard_idx]
      if conn is None:
        conn = self._dial_shard(shard_idx)

      return conn._stream_next()
    except dbexceptions.OperationalError:
      # Tear down regardless of the precise failure.
      self.conns[shard_idx] = None
      raise

  def cursor(self, cursorclass=None):
    return (cursorclass or self.cursorclass)(self)

  def __enter__(self):
    return self.cursor()

  def __exit__(self, exc, value, tb):
    if exc:
      self.rollback()
    else:
      self.commit()
