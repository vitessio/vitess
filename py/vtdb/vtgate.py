# Copyright 2013 Google Inc. All Rights Reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

from itertools import izip
import logging
import re

from net import bsonrpc
from net import gorpc
from vtdb import cursor
from vtdb import dbexceptions
from vtdb import field_types

# This is the shard name for when the keyrange covers the entire space
# for unsharded database.
SHARD_ZERO = "0"


_errno_pattern = re.compile('\(errno (\d+)\)')
# Map specific errors to specific classes.
_errno_map = {
    1062: dbexceptions.IntegrityError,
}


def convert_exception(exc, *args):
  new_args = exc.args + args
  if isinstance(exc, gorpc.TimeoutError):
    return dbexceptions.TimeoutError(new_args)
  elif isinstance(exc, gorpc.DeadlineExceededError):
    return dbexceptions.DeadlineExceededError(new_args)
  elif isinstance(exc, gorpc.AppError):
    msg = str(exc[0]).lower()
    if msg.startswith('request_backlog'):
      return dbexceptions.RequestBacklog(new_args)
    match = _errno_pattern.search(msg)
    if match:
      mysql_errno = int(match.group(1))
      return _errno_map.get(mysql_errno, dbexceptions.DatabaseError)(new_args)
    return dbexceptions.DatabaseError(new_args)
  elif isinstance(exc, gorpc.ProgrammingError):
    return dbexceptions.ProgrammingError(new_args)
  elif isinstance(exc, gorpc.GoRpcError):
    return dbexceptions.FatalError(new_args)
  return exc


# A simple, direct connection to the vttablet query server.
# This is shard-unaware and only handles the most basic communication.
# If something goes wrong, this object should be thrown away and a new one instantiated.
class VtgateConnection(object):
  session = None
  tablet_type = None
  cursorclass = cursor.TabletCursor
  _stream_fields = None
  _stream_conversions = None
  _stream_result = None
  _stream_result_index = None

  def __init__(self, addr, tablet_type, keyspace, shard, deadline, socket_timeout=1, user=None, password=None, encrypted=False, keyfile=None, certfile=None):
    self.addr = addr
    self.tablet_type = tablet_type
    self.keyspace = keyspace
    self.shard = shard
    self.deadline = deadline
    self.socket_timeout = socket_timeout
    self.client = bsonrpc.BsonRpcClient(addr, deadline, socket_timeout, user, password, encrypted=encrypted, keyfile=keyfile, certfile=certfile)

  def __str__(self):
    return '<VtgateConnection %s %s %s/%s>' % (self.addr, self.tablet_type, self.keyspace, self.shard)

  def dial(self):
    try:
      if not self.is_closed():
        self.close()
      self.client.dial()
    except gorpc.GoRpcError as e:
      raise convert_exception(e, str(self))

  def close(self):
    if self.session:
      self.rollback()
    self.client.close()

  def is_closed(self):
    return self.client.is_closed()

  def begin(self):
    try:
      response = self.client.call('VTGate.Begin', None)
      self.session = response.reply
    except gorpc.GoRpcError as e:
      raise convert_exception(e, str(self))

  def commit(self):
    try:
      session = self.session
      self.session = None
      self.client.call('VTGate.Commit', session)
    except gorpc.GoRpcError as e:
      raise convert_exception(e, str(self))

  def rollback(self):
    try:
      session = self.session
      self.session = None
      self.client.call('VTGate.Rollback', session)
    except gorpc.GoRpcError as e:
      raise convert_exception(e, str(self))

  def cursor(self, cursorclass=None, **kargs):
    return (cursorclass or self.cursorclass)(self, **kargs)

  def _add_session(self, req):
    if self.session:
      req['Session'] = self.session

  def _update_session(self, response):
    if 'Session' in response.reply and response.reply['Session']:
      self.session = response.reply['Session']

  def _execute(self, sql, bind_variables):
    new_binds = field_types.convert_bind_vars(bind_variables)
    req = {
        'Sql': sql,
        'BindVariables': new_binds,
        'Keyspace': self.keyspace,
        'TabletType': self.tablet_type,
        'Shards': [self.shard],
    }
    self._add_session(req)

    fields = []
    conversions = []
    results = []
    rowcount = 0
    lastrowid = 0
    try:
      response = self.client.call('VTGate.ExecuteShard', req)
      self._update_session(response)
      reply = response.reply
      # TODO(sougou): Simplify this check after all servers are deployed
      if 'Error' in response.reply and response.reply['Error']:
        raise gorpc.AppError(response.reply['Error'], 'VTGate.ExecuteShard')

      if 'Result' in reply:
        res = reply['Result']
        for field in res['Fields']:
          fields.append((field['Name'], field['Type']))
          conversions.append(field_types.conversions.get(field['Type']))

        for row in res['Rows']:
          results.append(tuple(_make_row(row, conversions)))

        rowcount = res['RowsAffected']
        lastrowid = res['InsertId']
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
      req = {
          'Queries': query_list,
          'Keyspace': self.keyspace,
          'TabletType': self.tablet_type,
          'Shards': [self.shard],
      }
      self._add_session(req)
      response = self.client.call('VTGate.ExecuteBatchShard', req)
      self._update_session(response)
      if 'Error' in response.reply and response.reply['Error']:
        raise gorpc.AppError(response.reply['Error'], 'VTGate.ExecuteBatchShard')
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
    req = {
        'Sql': sql,
        'BindVariables': new_binds,
        'Keyspace': self.keyspace,
        'TabletType': self.tablet_type,
        'Shards': [self.shard],
    }
    self._add_session(req)

    self._stream_fields = []
    self._stream_conversions = []
    self._stream_result = None
    self._stream_result_index = 0
    try:
      self.client.stream_call('VTGate.StreamExecuteShard', req)
      first_response = self.client.stream_next()
      reply = first_response.reply['Result']

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
    while self._stream_result is None:
      try:
        self._stream_result = self.client.stream_next()
        if self._stream_result is None:
          self._stream_result_index = None
          return None
        # A session message, if any comes separately with no rows
        # TODO(sougou) get rid of this check. After all the server
        # changes, there will always be a 'Session' in the reply.
        if 'Session' in self._stream_result.reply and self._stream_result.reply['Session']:
          self.session = self._stream_result.reply['Session']
          self._stream_result = None
          continue
      except gorpc.GoRpcError as e:
        raise convert_exception(e, str(self))
      except:
        logging.exception('gorpc low-level error')
        raise

    row = tuple(_make_row(self._stream_result.reply['Result']['Rows'][self._stream_result_index], self._stream_conversions))

    # If we are reading the last row, set us up to read more data.
    self._stream_result_index += 1
    if self._stream_result_index == len(self._stream_result.reply['Result']['Rows']):
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
  conn = VtgateConnection(*pargs, **kargs)
  conn.dial()
  return conn
