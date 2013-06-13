# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

from itertools import izip
import logging

from net import bsonrpc
from net import gorpc
from vtdb import cursor
from vtdb import dbexceptions
from vtdb import field_types


# A simple, direct connection to the voltron query server.
# This is shard-unaware and only handles the most basic communication.
class TabletConnection(object):
  transaction_id = 0
  session_id = 0
  cursorclass = cursor.TabletCursor

  def __init__(self, addr, keyspace, shard, timeout, user=None, password=None, encrypted=False, keyfile=None, certfile=None):
    self.addr = addr
    self.keyspace = keyspace
    self.shard = shard
    self.timeout = timeout
    self.client = bsonrpc.BsonRpcClient(addr, timeout, user, password, encrypted=encrypted, keyfile=keyfile, certfile=certfile)

  def dial(self):
    try:
      if self.session_id:
        self.client.close()
      self.client.dial()
      params = {'Keyspace': self.keyspace, 'Shard': self.shard}
      response = self.client.call('SqlQuery.GetSessionId', params)
      self.session_id = response.reply['SessionId']
    except gorpc.GoRpcError as e:
      raise dbexceptions.OperationalError(*e.args)

  def close(self):
    self.transaction_id = 0
    self.session_id = 0
    self.client.close()

  def _make_req(self):
    return {'TransactionId': self.transaction_id,
            'ConnectionId': 0,
            'SessionId': self.session_id}

  def begin(self):
    if self.transaction_id:
      raise dbexceptions.NotSupportedError('Nested transactions not supported')
    req = self._make_req()
    try:
      response = self.client.call('SqlQuery.Begin', req)
      self.transaction_id = response.reply['TransactionId']
    except gorpc.GoRpcError as e:
      raise dbexceptions.OperationalError(*e.args)

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
      raise dbexceptions.OperationalError(*e.args)

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
      raise dbexceptions.OperationalError(*e.args)

  def cursor(self, cursorclass=None, **kargs):
    return (cursorclass or self.cursorclass)(self, **kargs)

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
      raise dbexceptions.OperationalError(*e.args)
    except:
      logging.exception('gorpc low-level error')
      raise
    return results, rowcount, lastrowid, fields

  def _execute_batch(self, sql_list, bind_variables_list):
    query_list = []
    for sql, bind_vars in zip(sql_list, bind_variables_list):
      req = self._make_req()
      req['Sql'] = sql
      req['BindVariables'] = field_types.convert_bind_vars(bind_vars)
      query_list.append(req)

    rowsets = []

    try:
      req = {'List': query_list}
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
      raise dbexceptions.OperationalError(*e.args)
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

    fields = []
    conversions = []
    try:
      self.client.stream_call('SqlQuery.StreamExecute', req)
      first_response = self.client.stream_next()
      reply = first_response.reply

      for field in reply['Fields']:
        fields.append((field['Name'], field['Type']))
        conversions.append(field_types.conversions.get(field['Type']))

    except gorpc.GoRpcError as e:
      raise dbexceptions.OperationalError(*e.args)
    except:
      logging.exception('gorpc low-level error')
      raise
    return fields, conversions, None, 0

  # the calls to _stream_next will have the following states:
  # conversions, None, 0       (that will trigger asking for one result)
  # conversions, result, 1     (the next call will just return the 2nd row)
  # conversions, result, 2
  # ...
  # conversions, result, len(result)-1
  # conversions, None, 0       (that will trigger asking for one more result)
  # ...
  # conversions, last result, len(last result)-1
  #                            (asking for next result will return None)
  # conversions, None, None    (this is then stable and stays that way)
  # conversions, None, None
  #
  # the StreamCursor in cursor.py is a good implementation of this API.
  def _stream_next(self, conversions, query_result, index):

    # if index is None, it means we're done (because _stream_next
    # returned None, see 7 lines below here)
    if index is None:
      return None, None, None

    # see if we need to read more
    if query_result is None:
      try:
        query_result = self.client.stream_next()
        if query_result is None:
          return None, None, None
      except gorpc.GoRpcError as e:
        raise dbexceptions.OperationalError(*e.args)
      except:
        logging.exception('gorpc low-level error')
        raise

    result = tuple(_make_row(query_result.reply['Rows'][index], conversions))

    index += 1
    if index == len(query_result.reply['Rows']):
      query_result = None
      index = 0

    return result, query_result, index

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
