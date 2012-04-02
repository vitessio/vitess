# Copyright 2012, Google Inc.
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:

#     * Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above
# copyright notice, this list of conditions and the following disclaimer
# in the documentation and/or other materials provided with the
# distribution.
#     * Neither the name of Google Inc. nor the names of its
# contributors may be used to endorse or promote products derived from
# this software without specific prior written permission.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

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
  default_cursorclass = cursor.TabletCursor

  def __init__(self, addr, dbname, timeout):
    self.addr = addr
    self.dbname = dbname
    self.timeout = timeout
    self.client = bsonrpc.BsonRpcClient(self.uri, self.timeout)
    self.cursorclass = self.default_cursorclass

  def dial(self):
    if self.client:
      self.client.close()
    self.transaction_id = 0
    self.session_id = 0

  # You need to obtain and set the session_id for things to work.
  def set_session_id(self, session_id):
    self.session_id = session_id

  @property
  def uri(self):
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
