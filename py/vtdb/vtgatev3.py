# Copyright 2013 Google Inc. All Rights Reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

from itertools import izip
import logging
import re

from net import bsonrpc
from net import gorpc
from vtdb import cursorv3
from vtdb import dbexceptions
from vtdb import field_types
from vtdb import vtdb_logger


_errno_pattern = re.compile(r'\(errno (\d+)\)')


def log_exception(method):
  """Decorator for logging the exception from vtgatev2.

  The convert_exception method interprets and recasts the exceptions
  raised by lower-layer. The inner function calls the appropriate vtdb_logger
  method based on the exception raised.

  Args:
    method: Method that takes exc, *args, where exc is an exception raised
      by calling code, args are additional args for the exception.

  Returns:
    Decorated method.
  """
  def _log_exception(exc, *args):
    logger_object = vtdb_logger.get_logger()

    new_exception = method(exc, *args)

    if isinstance(new_exception, dbexceptions.IntegrityError):
      logger_object.integrity_error(new_exception)
    else:
      logger_object.vtgatev2_exception(new_exception)
    return new_exception
  return _log_exception


def handle_app_error(exc_args):
  msg = str(exc_args[0]).lower()
  if msg.startswith('request_backlog'):
    return dbexceptions.RequestBacklog(exc_args)
  match = _errno_pattern.search(msg)
  if match:
    mysql_errno = int(match.group(1))
    # Prune the error message to truncate the query string
    # returned by mysql as it contains bind variables.
    if mysql_errno == 1062:
      parts = _errno_pattern.split(msg)
      pruned_msg = msg[:msg.find(parts[2])]
      new_args = (pruned_msg,) + tuple(exc_args[1:])
      return dbexceptions.IntegrityError(new_args)
  return dbexceptions.DatabaseError(exc_args)


@log_exception
def convert_exception(exc, *args):
  new_args = exc.args + args
  if isinstance(exc, gorpc.TimeoutError):
    return dbexceptions.TimeoutError(new_args)
  elif isinstance(exc, gorpc.AppError):
    return handle_app_error(new_args)
  elif isinstance(exc, gorpc.ProgrammingError):
    return dbexceptions.ProgrammingError(new_args)
  elif isinstance(exc, gorpc.GoRpcError):
    return dbexceptions.FatalError(new_args)
  return exc


def _create_req(sql, new_binds, tablet_type, not_in_transaction):
  new_binds = field_types.convert_bind_vars(new_binds)
  req = {
      'Sql': sql,
      'BindVariables': new_binds,
      'TabletType': tablet_type,
      'NotInTransaction': not_in_transaction,
  }
  return req


class VTGateConnection(object):
  """This utilizes the V3 API of VTGate."""

  def __init__(self, addr, timeout, user=None, password=None,
               keyfile=None, certfile=None):
    # TODO: Merge. This is very similar to vtgatev2.
    self.session = None
    self.addr = addr
    self.user = user
    self.password = password
    self.keyfile = keyfile
    self.certfile = certfile
    self.timeout = timeout
    self.client = self._create_client()
    self.logger_object = vtdb_logger.get_logger()

  def _create_client(self):
    # TODO: Merge. This is very similar to vtgatev2.
    return bsonrpc.BsonRpcClient(
        self.addr, self.timeout, self.user, self.password,
        keyfile=self.keyfile, certfile=self.certfile)

  def _get_client(self):
    """Get current client or create a new one and connect."""
    # TODO: Merge. This is very similar to vtgatev2.
    if not self.client:
      self.client = self._create_client()
      try:
        self.client.dial()
      except gorpc.GoRpcError as e:
        raise convert_exception(e, str(self))
    return self.client

  def __str__(self):
    return '<VTGateConnection %s >' % self.addr

  def dial(self):
    # TODO: Merge. This is very similar to vtgatev2.
    try:
      if not self.is_closed():
        self.close()
      self._get_client().dial()
    except gorpc.GoRpcError as e:
      raise convert_exception(e, str(self))

  def close(self):
    # TODO: Merge. This is very similar to vtgatev2.
    if self.session:
      self.rollback()
    if self.client:
      self.client.close()

  def is_closed(self):
    # TODO: Merge. This is very similar to vtgatev2.
    return not self.client or self.client.is_closed()

  def cursor(self, *pargs, **kwargs):
    cursorclass = kwargs.pop('cursorclass', None) or cursorv3.Cursor
    return cursorclass(self, *pargs, **kwargs)

  def begin(self, effective_caller_id=None):
    _ = effective_caller_id  # TODO: Pass effective_caller_id through.
    try:
      response = self._get_client().call('VTGate.Begin', None)
      self.session = response.reply
    except gorpc.GoRpcError as e:
      raise convert_exception(e, str(self))

  def commit(self):
    try:
      session = self.session
      self.session = None
      self._get_client().call('VTGate.Commit', session)
    except gorpc.GoRpcError as e:
      raise convert_exception(e, str(self))

  def rollback(self):
    try:
      session = self.session
      self.session = None
      self._get_client().call('VTGate.Rollback', session)
    except gorpc.GoRpcError as e:
      raise convert_exception(e, str(self))

  def _add_session(self, req):
    if self.session:
      req['Session'] = self.session

  def _update_session(self, response):
    if response.reply.get('Session'):
      self.session = response.reply['Session']

  def _execute(
      self, sql, bind_variables, tablet_type, not_in_transaction=False):
    req = _create_req(sql, bind_variables, tablet_type, not_in_transaction)
    self._add_session(req)

    fields = []
    conversions = []
    results = []
    rowcount = 0
    lastrowid = 0
    try:
      response = self._get_client().call('VTGate.Execute', req)
      self._update_session(response)
      reply = response.reply
      if response.reply.get('Error'):
        raise gorpc.AppError(response.reply['Error'], 'VTGate.Execute')

      if reply.get('Result'):
        res = reply['Result']
        for field in res['Fields']:
          fields.append((field['Name'], field['Type']))
          conversions.append(field_types.conversions.get(field['Type']))

        for row in res['Rows']:
          results.append(tuple(_make_row(row, conversions)))

        rowcount = res['RowsAffected']
        lastrowid = res['InsertId']
    except gorpc.GoRpcError as e:
      self.logger_object.log_private_data(bind_variables)
      raise convert_exception(e, str(self), sql)
    except Exception:
      logging.exception('gorpc low-level error')
      raise
    return results, rowcount, lastrowid, fields

  def _execute_batch(
      self, sql_list, bind_variables_list, tablet_type, as_transaction):
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
          'TabletType': tablet_type,
          'AsTransaction': as_transaction,
      }
      self._add_session(req)
      response = self._get_client().call('VTGate.ExecuteBatch', req)
      self._update_session(response)
      if response.reply.get('Error'):
        raise gorpc.AppError(response.reply['Error'], 'VTGate.ExecuteBatch')
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
      self.logger_object.log_private_data(bind_variables_list)
      raise convert_exception(e, str(self), sql_list)
    except Exception:
      logging.exception('gorpc low-level error')
      raise
    return rowsets

  def _stream_execute(
      self, sql, bind_variables, tablet_type, not_in_transaction=False):
    req = _create_req(sql, bind_variables, tablet_type, not_in_transaction)
    self._add_session(req)

    rpc_client = self._get_client()

    stream_fields = []
    stream_conversions = []
    try:
      rpc_client.stream_call('VTGate.StreamExecute', req)
      first_response = rpc_client.stream_next()
      reply = first_response.reply['Result']

      for field in reply['Fields']:
        stream_fields.append((field['Name'], field['Type']))
        stream_conversions.append(
            field_types.conversions.get(field['Type']))
    except gorpc.GoRpcError as e:
      self.logger_object.log_private_data(bind_variables)
      raise convert_exception(e, str(self), sql)
    except Exception:
      logging.exception('gorpc low-level error')
      raise

    # Take the BsonRpcClient from VTGateConnection. The row_generator
    # will manage the BsonRpcClient. This VTGateConnection will connect
    # to a new client if needed.
    self.client = None

    def row_generator():
      # TODO: Merge. This is very similar to vtgatev2.
      try:
        while True:
          try:
            stream_result = rpc_client.stream_next()
            if stream_result is None:
              break
            # A session message, if any comes separately with no rows.
            # I am not sure if we can ignore this.
            if stream_result.reply.get('Session'):
              self.session = stream_result.reply['Session']
            else:
              for result_item in stream_result.reply['Result']['Rows']:
                yield tuple(_make_row(result_item, stream_conversions))
          except gorpc.GoRpcError as e:
            raise convert_exception(e, str(self))
          except Exception:
            logging.exception('gorpc low-level error')
            raise
      finally:
        rpc_client.close()

    return row_generator(), stream_fields


def _make_row(row, conversions):
  # TODO: Merge. This is very similar to vtgatev2.
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


def connect(*pargs, **kwargs):
  conn = VTGateConnection(*pargs, **kwargs)
  conn.dial()
  return conn
