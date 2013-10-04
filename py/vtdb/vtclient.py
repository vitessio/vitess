import logging
import time

from vtdb import cursor
from vtdb import dbapi
from vtdb import dbexceptions
from vtdb import tablet
from vtdb import topology

RECONNECT_DELAY = 0.002 # 2 ms
BEGIN_RECONNECT_DELAY = 0.2 # 200 ms
MAX_RETRY_ATTEMPTS = 10

def get_vt_connection_params_list(zkocc_client, keyspace, shard, db_type, timeout, encrypted, user, password):
  db_params_list = []
  db_key = "%s.%s.%s" % (keyspace, shard, db_type)
  # the list of end_points is randomly shuffled
  end_points = topology.get_host_port_by_name(zkocc_client, db_key+":_vtocc", encrypted)
  for host, port, encrypted in end_points:
    vt_params = dict()
    vt_params['keyspace'] = keyspace
    vt_params['shard'] = shard
    vt_params['addr'] = "%s:%s" % (host, port)
    vt_params['timeout'] = timeout
    vt_params['encrypted'] = encrypted
    vt_params['user'] = user
    vt_params['password'] = password
    db_params_list.append(vt_params)
  return db_params_list

def reconnect(method):
  def _run_with_reconnect(self, *args, **kargs):
    attempt = 0
    while True:
      try:
        return method(self, *args, **kargs)
      except (tablet.RetryError, tablet.FatalError, tablet.TxPoolFull) as e:
        attempt += 1
        if attempt >= self.max_attempts or self.in_txn:
          self.close()
          raise tablet.FatalError(*e.args)
        if method.__name__ == 'begin':
          time.sleep(BEGIN_RECONNECT_DELAY)
        else:
          time.sleep(RECONNECT_DELAY)
        if not isinstance(e, tablet.TxPoolFull):
          logging.info("Attempting to reconnect, %d", attempt)
          self.close()
          self.connect()
          logging.info("Successfully reconnected to %s", str(self.conn))
        else:
          logging.info("Waiting to retry for tablet.TxPoolFull to %s, attempt %d", str(self.conn), attempt)
  return _run_with_reconnect

# Provide compatibility with the MySQLdb query param style and prune bind_vars
class VtOCCConnection(object):
  cursorclass = cursor.TabletCursor

  def __init__(self, zkocc_client, keyspace, shard, db_type, timeout, user=None, password=None, encrypted=False, keyfile=None, certfile=None):
    self.zkocc_client = zkocc_client
    self.keyspace = keyspace
    self.shard = str(shard)
    self.db_type = db_type
    self.timeout = timeout
    self.user = user
    self.password = password
    self.encrypted = encrypted
    self.keyfile = keyfile
    self.certfile = certfile
    self.conn = None
    self.max_attempts = MAX_RETRY_ATTEMPTS
    self.conn_db_params = None
    self.in_txn = False

  @property
  def db_params(self):
    return self.conn_db_params

  def close(self):
    if self.conn:
      self.conn.close()

  def connect(self):
    db_key = "%s.%s.%s" % (self.keyspace, self.shard, self.db_type)
    db_params_list = get_vt_connection_params_list(self.zkocc_client, self.keyspace, self.shard, self.db_type, self.timeout, self.encrypted, self.user, self.password)
    if not db_params_list:
      raise dbexceptions.OperationalError("empty db params list - no db instance available for key %s" % db_key)
    db_exception = None
    # no retries here, since there is a higher level retry with reconnect.
    for params in db_params_list:
      try:
        db_params = params.copy()
        self.conn = tablet.TabletConnection(**db_params)
        self.conn.dial()
        self.conn_db_params = db_params
        return self.conn
      except Exception as e:
        db_exception = e
        logging.warning('db connection failed: %s %s, %s', db_key, db_params['addr'], e)

    raise dbexceptions.OperationalError(
      'unable to create vt connection', db_key, db_params['addr'], db_exception)

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
    result = self.conn._execute(sql, bind_variables)
    return result

  @reconnect
  def _execute_batch(self, sql_list, bind_variables_list):
    sane_sql_list = []
    sane_bind_vars_list = []
    for sql, bind_variables in zip(sql_list, bind_variables_list):
      sane_sql, sane_bind_vars = dbapi.prepare_query_bind_vars(sql, bind_variables)
      sane_sql_list.append(sane_sql)
      sane_bind_vars_list.append(sane_bind_vars)

    result = self.conn._execute_batch(sane_sql_list, sane_bind_vars_list)
    return result

  @reconnect
  def _stream_execute(self, sql, bind_variables):
    sql, bind_variables = dbapi.prepare_query_bind_vars(sql, bind_variables)
    result = self.conn._stream_execute(sql, bind_variables)
    return result

  def _stream_next(self):
    return self.conn._stream_next()
