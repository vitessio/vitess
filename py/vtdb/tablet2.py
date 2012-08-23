# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

from itertools import izip
import hmac
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
  default_cursorclass = cursor.TabletCursor

  def __init__(self, addr, dbname, timeout, user=None, password=None):
    self.addr = addr
    self.dbname = dbname
    self.timeout = timeout

    if bool(user) != bool(password):
      raise ValueError("You must provide either both or none of user and password.")
    self.user = user
    self.password = password
    self.use_auth = bool(user)

    self.client = bsonrpc.BsonRpcClient(self.uri, self.timeout)
    self.cursorclass = self.default_cursorclass

  def dial(self):
    if self.client:
      self.client.close()
    self.transaction_id = 0
    self.session_id = 0
    if self.use_auth:
      self.authenticate()

  # You need to obtain and set the session_id for things to work.
  def set_session_id(self, session_id):
    self.session_id = session_id

  def authenticate(self):
    challenge = self.client.call('AuthenticatorCRAMMD5.GetNewChallenge', "").reply['Challenge']
    # CRAM-MD5 authentication.
    proof = self.user + " " + hmac.HMAC(self.password, challenge).hexdigest()
    self.client.call('AuthenticatorCRAMMD5.Authenticate', {"Proof": proof})

  @property
  def uri(self):
    if self.use_auth:
      return 'http://%s/_bson_rpc_/auth' % self.addr
    return 'http://%s/_bson_rpc_' % self.addr

  def close(self):
    self.rollback()
    self.client.close()

  __del__ = close

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
      # FIXME(sougou): Temp hack for backward compatibility
      if type(response.reply) == dict:
        self.transaction_id = respponse.reply["TransactionId"]
      else:
        self.transaction_id = response.reply
    except gorpc.GoRpcError, e:
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
    except gorpc.GoRpcError, e:
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
    except gorpc.GoRpcError, e:
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
    except gorpc.GoRpcError, e:
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
      req = {"List": query_list}
      response = self.client.call('SqlQuery.ExecuteBatch', req)
      for reply in response.reply["List"]:
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
    except gorpc.GoRpcError, e:
      raise dbexceptions.OperationalError(*e.args)
    except:
      logging.exception('gorpc low-level error')
      raise
    return rowsets


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
