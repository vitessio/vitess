# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

from itertools import izip
import logging
import re

from net import bsonrpc
from net import gorpc
from vtdb import dbexceptions
from vtdb import field_types


_errno_pattern = re.compile('\(errno (\d+)\)')


def convert_exception(exc, *args):
  new_args = exc.args + args
  if isinstance(exc, gorpc.TimeoutError):
    return dbexceptions.TimeoutError(new_args)
  elif isinstance(exc, gorpc.AppError):
    msg = str(exc[0]).lower()
    if msg.startswith('retry'):
      return dbexceptions.RetryError(new_args)
    if msg.startswith('fatal'):
      return dbexceptions.FatalError(new_args)
    if msg.startswith('tx_pool_full'):
      return dbexceptions.TxPoolFull(new_args)
    match = _errno_pattern.search(msg)
    if match:
      mysql_errno = int(match.group(1))
      if mysql_errno == 1062:
        return dbexceptions.IntegrityError(new_args)
      # TODO(sougou/liguo): remove this case once servers are deployed
      elif mysql_errno == 1290 and 'read-only' in msg:
        return dbexceptions.RetryError(new_args)
    return dbexceptions.DatabaseError(new_args)
  elif isinstance(exc, gorpc.ProgrammingError):
    return dbexceptions.ProgrammingError(new_args)
  elif isinstance(exc, gorpc.GoRpcError):
    return dbexceptions.FatalError(new_args)
  return exc


# A simple, direct connection to the vttablet query server.
# This is shard-unaware and only handles the most basic communication.
# If something goes wrong, this object should be thrown away and a new one instantiated.
class TabletConnection(object):
  transaction_id = 0
  session_id = 0
  _stream_fields = None
  _stream_conversions = None
  _stream_result = None
  _stream_result_index = None

  def __init__(self, addr, tablet_type, keyspace, shard, timeout, user=None, password=None, encrypted=False, keyfile=None, certfile=None):
    self.addr = addr
    self.tablet_type = tablet_type
    self.keyspace = keyspace
    self.shard = shard
    self.timeout = timeout
    self.client = bsonrpc.BsonRpcClient(addr, timeout, user, password, encrypted=encrypted, keyfile=keyfile, certfile=certfile)

  def __str__(self):
    return '<TabletConnection %s %s %s/%s>' % (self.addr, self.tablet_type, self.keyspace, self.shard)

  def dial(self):
    try:
      if self.session_id:
        self.client.close()
        # This will still allow the use of the connection - a second
        # redial will succeed. This is more a hint that you are doing
        # it wrong and misunderstanding the life cycle of a
        # TabletConnection.
        #raise dbexceptions.ProgrammingError('attempting to reuse TabletConnection')

      self.client.dial()
      params = {'Keyspace': self.keyspace, 'Shard': self.shard}
      response = self.client.call('SqlQuery.GetSessionId', params)
      self.session_id = response.reply['SessionId']
    except gorpc.GoRpcError as e:
      raise convert_exception(e, str(self))

  def close(self):
    # rollback if possible, but ignore failures
    try:
      self.rollback()
    except Exception:
      pass
    self.session_id = 0
    self.client.close()

  def is_closed(self):
    return self.client.is_closed()

  def _make_req(self):
    return {'TransactionId': self.transaction_id,
            'SessionId': self.session_id}

  def begin(self):
    if self.transaction_id:
      raise dbexceptions.NotSupportedError('Nested transactions not supported')
    req = self._make_req()
    try:
      response = self.client.call('SqlQuery.Begin', req)
      self.transaction_id = response.reply['TransactionId']
    except gorpc.GoRpcError as e:
      raise convert_exception(e, str(self))

  def commit(self):
    if not self.transaction_id:
      return

    req = self._make_req()
    # NOTE(msolomon) Unset the transaction_id irrespective of the RPC's
    # response. The intent of commit is that no more statements can be made on
    # this transaction, so we guarantee that. Transient errors between the
    # db and the client shouldn't affect this part of the bookkeeping.
    # Do this after fill_session, since this is a critical part.
    self.transaction_id = 0

    try:
      response = self.client.call('SqlQuery.Commit', req)
      return response.reply
    except gorpc.GoRpcError as e:
      raise convert_exception(e, str(self))

  def rollback(self):
    if not self.transaction_id:
      return

    req = self._make_req()
    # NOTE(msolomon) Unset the transaction_id irrespective of the RPC. If the
    # RPC fails, the client will still choose a new transaction_id next time
    # and the tablet server will eventually kill the abandoned transaction on
    # the server side.
    self.transaction_id = 0

    try:
      response = self.client.call('SqlQuery.Rollback', req)
      return response.reply
    except gorpc.GoRpcError as e:
      raise convert_exception(e, str(self))

  def _execute(self, sql, bind_variables):
    new_binds = field_types.convert_bind_vars(bind_variables)
    req = self._make_req()
    req['Sql'] = sql
    req['BindVariables'] = new_binds

    fields = []
    conversions = []
    results = []
    try:
      response = self.client.call('SqlQuery.Execute', req)
      reply = response.reply

      for field in reply['Fields']:
        fields.append((field['Name'], field['Type']))
        conversions.append(field_types.conversions.get(field['Type']))

      for row in reply['Rows']:
        results.append(tuple(_make_row(row, conversions)))

      rowcount = reply['RowsAffected']
      lastrowid = reply['InsertId']
    except gorpc.GoRpcError as e:
      raise convert_exception(e, str(self), sql, bind_variables)
    except:
      logging.exception('gorpc low-level error')
      raise
    return results, rowcount, lastrowid, fields

  def _execute_batch(self, sql_list, bind_variables_list):
    query_list = []
    for sql, bind_vars in zip(sql_list, bind_variables_list):
      query = {}
      query['Sql'] = sql
      query['BindVariables'] = field_types.convert_bind_vars(bind_vars)
      query_list.append(query)

    rowsets = []

    try:
      req = self._make_req()
      req['Queries'] = query_list
      response = self.client.call('SqlQuery.ExecuteBatch', req)
      for reply in response.reply['List']:
        fields = []
        conversions = []
        results = []
        rowcount = 0

        for field in reply['Fields']:
          fields.append((field['Name'], field['Type']))
          conversions.append(field_types.conversions.get(field['Type']))

        for row in reply['Rows']:
          results.append(tuple(_make_row(row, conversions)))

        rowcount = reply['RowsAffected']
        lastrowid = reply['InsertId']
        rowsets.append((results, rowcount, lastrowid, fields))
    except gorpc.GoRpcError as e:
      raise convert_exception(e, str(self), sql_list, bind_variables_list)
    except:
      logging.exception('gorpc low-level error')
      raise
    return rowsets

  # we return the fields for the response, and the column conversions
  # the conversions will need to be passed back to _stream_next
  # (that way we avoid using a member variable here for such a corner case)
  def _stream_execute(self, sql, bind_variables):
    new_binds = field_types.convert_bind_vars(bind_variables)
    req = self._make_req()
    req['Sql'] = sql
    req['BindVariables'] = new_binds

    self._stream_fields = []
    self._stream_conversions = []
    self._stream_result = None
    self._stream_result_index = 0
    try:
      self.client.stream_call('SqlQuery.StreamExecute', req)
      first_response = self.client.stream_next()
      reply = first_response.reply

      for field in reply['Fields']:
        self._stream_fields.append((field['Name'], field['Type']))
        self._stream_conversions.append(field_types.conversions.get(field['Type']))
    except gorpc.GoRpcError as e:
      raise convert_exception(e, str(self), sql, bind_variables)
    except:
      logging.exception('gorpc low-level error')
      raise
    return None, 0, 0, self._stream_fields

  def _stream_next(self):
    # Terminating condition
    if self._stream_result_index is None:
      return None

    # See if we need to read more or whether we just pop the next row.
    if self._stream_result is None :
      try:
        self._stream_result = self.client.stream_next()
        if self._stream_result is None:
          self._stream_result_index = None
          return None
      except gorpc.GoRpcError as e:
        raise convert_exception(e, str(self))
      except:
        logging.exception('gorpc low-level error')
        raise

    row = tuple(_make_row(self._stream_result.reply['Rows'][self._stream_result_index], self._stream_conversions))

    # If we are reading the last row, set us up to read more data.
    self._stream_result_index += 1
    if self._stream_result_index == len(self._stream_result.reply['Rows']):
      self._stream_result = None
      self._stream_result_index = 0

    return row

def _make_row(row, conversions):
  converted_row = []
  for conversion_func, field_data in izip(conversions, row):
    if field_data is None:
      v = None
    elif conversion_func:
      v = conversion_func(field_data)
    else:
      v = field_data
    converted_row.append(v)
  return converted_row


def connect(*pargs, **kargs):
  conn = TabletConnection(*pargs, **kargs)
  conn.dial()
  return conn
