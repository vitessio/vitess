# Copyright 2013 Google Inc. All Rights Reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import logging
import re

from net import gorpc
from vtdb import bson_vtgate_client
from vtdb import cursorv3
from vtdb import dbexceptions
from vtdb import field_types
from vtdb import vtdb_logger


_errno_pattern = re.compile(r'\(errno (\d+)\)')


def log_exception(method):
  """Decorator for logging the exception from vtgatev2.

  The convert_gorpc_exception method interprets and recasts the exceptions
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


def _create_req(sql, new_binds, tablet_type, not_in_transaction):
  new_binds = field_types.convert_bind_vars(new_binds)
  req = {
      'Sql': sql,
      'BindVariables': new_binds,
      'TabletType': tablet_type,
      'NotInTransaction': not_in_transaction,
  }
  return req


class VTGateConnection(bson_vtgate_client.BsonVtgateClient):
  """This utilizes the V3 API of VTGate."""

  cursor_cls = cursorv3.Cursor

  def __init__(self, addr, timeout, user=None, password=None,
               keyfile=None, certfile=None):
    super(VTGateConnection, self).__init__(
        addr, timeout, user, password, keyfile, certfile)
    self.session = None
    self.logger_object = vtdb_logger.get_logger()

  @log_exception
  def convert_gorpc_exception(self, exc, *args):
    new_args = exc.args + (str(self),) + args
    if isinstance(exc, gorpc.TimeoutError):
      return dbexceptions.TimeoutError(new_args)
    elif isinstance(exc, gorpc.AppError):
      return handle_app_error(new_args)
    elif isinstance(exc, gorpc.ProgrammingError):
      return dbexceptions.ProgrammingError(new_args)
    elif isinstance(exc, gorpc.GoRpcError):
      return dbexceptions.FatalError(new_args)
    return exc

  def __str__(self):
    return '<VTGateConnection %s >' % self.addr

  def in_transaction(self):
    return bool(self.session)

  def begin(self, effective_caller_id=None):
    _ = effective_caller_id  # TODO: Pass effective_caller_id through.
    try:
      response = self._get_client().call('VTGate.Begin', None)
      self.session = response.reply
    except gorpc.GoRpcError as e:
      raise self.convert_gorpc_exception(e)

  def commit(self):
    try:
      self._get_client().call('VTGate.Commit', self.session)
    except gorpc.GoRpcError as e:
      raise self.convert_gorpc_exception(e)
    finally:
      self.session = None

  def rollback(self):
    try:
      self._get_client().call('VTGate.Rollback', self.session)
    except gorpc.GoRpcError as e:
      raise self.convert_gorpc_exception(e)
    finally:
      self.session = None

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
          results.append(tuple(self._make_row(row, conversions)))

        rowcount = res['RowsAffected']
        lastrowid = res['InsertId']
    except gorpc.GoRpcError as e:
      self.logger_object.log_private_data(bind_variables)
      raise self.convert_gorpc_exception(e, sql)
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
          results.append(tuple(self._make_row(row, conversions)))

        rowcount = reply['RowsAffected']
        lastrowid = reply['InsertId']
        rowsets.append((results, rowcount, lastrowid, fields))
    except gorpc.GoRpcError as e:
      self.logger_object.log_private_data(bind_variables_list)
      raise self.convert_gorpc_exception(e, sql_list)
    except Exception:
      logging.exception('gorpc low-level error')
      raise
    return rowsets

  def _stream_execute(
      self, sql, bind_variables, tablet_type, not_in_transaction=False):
    req = _create_req(sql, bind_variables, tablet_type, not_in_transaction)
    self._add_session(req)

    rpc_client = self._get_client_for_streaming()
    stream_fields = []
    stream_conversions = []

    try:
      rpc_client.stream_call('VTGate.StreamExecute', req)
      first_response = rpc_client.stream_next()
      if first_response:
        reply = first_response.reply['Result']
        for field in reply['Fields']:
          stream_fields.append((field['Name'], field['Type']))
          stream_conversions.append(
              field_types.conversions.get(field['Type']))
    except gorpc.GoRpcError as e:
      self.logger_object.log_private_data(bind_variables)
      raise self.convert_gorpc_exception(e, sql)
    except Exception:
      logging.exception('gorpc low-level error')
      raise

    def row_generator():
      try:
        while True:
          try:
            stream_result = rpc_client.stream_next()
            if stream_result is None:
              break
            if stream_result.reply.get('Result'):
              for result_item in stream_result.reply['Result']['Rows']:
                yield tuple(self._make_row(result_item, stream_conversions))
          except gorpc.GoRpcError as e:
            raise self.convert_gorpc_exception(e)
          except Exception:
            logging.exception('gorpc low-level error')
            raise
      finally:
        rpc_client.close()

    return row_generator(), stream_fields


def connect(*pargs, **kwargs):
  conn = VTGateConnection(*pargs, **kwargs)
  conn.dial()
  return conn
