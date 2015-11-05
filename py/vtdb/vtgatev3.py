# Copyright 2013 Google Inc. All Rights Reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

"""A vtgate v3 client."""

from itertools import izip
import logging

from vtproto import topodata_pb2

from net import bsonrpc
from net import gorpc

from vtdb import cursorv3
from vtdb import dbexceptions
from vtdb import field_types
from vtdb import vtdb_logger
from vtdb import vtgate_utils

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


@log_exception
def convert_exception(exc, *args):
  """Return an exception object expected by callers."""
  new_args = exc.args + args
  if isinstance(exc, gorpc.TimeoutError):
    return dbexceptions.TimeoutError(new_args)
  elif isinstance(exc, vtgate_utils.VitessError):
    return exc.convert_to_dbexception(new_args)
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
      'TabletType': topodata_pb2.TabletType.Value(tablet_type.upper()),
      'NotInTransaction': not_in_transaction,
  }
  return req


class VTGateConnection(object):
  """This utilizes the V3 API of VTGate."""

  def __init__(self, addr, timeout, user=None, password=None,
               keyfile=None, certfile=None):
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
    return bsonrpc.BsonRpcClient(
        self.addr, self.timeout, self.user, self.password,
        keyfile=self.keyfile, certfile=self.certfile)

  def _get_client(self):
    """Get current client or create a new one and connect."""
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
    try:
      if not self.is_closed():
        self.close()
      self._get_client().dial()
    except gorpc.GoRpcError as e:
      raise convert_exception(e, str(self))

  def close(self):
    if self.session:
      self.rollback()
    if self.client:
      self.client.close()

  def is_closed(self):
    return not self.client or self.client.is_closed()

  def cursor(self, *pargs, **kwargs):
    cursorclass = kwargs.pop('cursorclass', None) or cursorv3.Cursor
    return cursorclass(self, *pargs, **kwargs)

  def begin(self, effective_caller_id=None):
    try:
      req = {}
      self._add_caller_id(req, effective_caller_id)
      response = self._get_client().call('VTGate.Begin2', req)
      vtgate_utils.extract_rpc_error('VTGate.Begin2', response)
      self.effective_caller_id = effective_caller_id
      self.session = None
      self._update_session(response)
    except (gorpc.GoRpcError, vtgate_utils.VitessError) as e:
      raise convert_exception(e, str(self))

  def commit(self):
    try:
      req = {}
      self._add_caller_id(req, self.effective_caller_id)
      self._add_session(req)
      response = self._get_client().call('VTGate.Commit2', req)
      vtgate_utils.extract_rpc_error('VTGate.Commit2', response)
    except (gorpc.GoRpcError, vtgate_utils.VitessError) as e:
      raise convert_exception(e, str(self))
    finally:
      self.session = None
      self.effective_caller_id = None

  def rollback(self):
    try:
      req = {}
      self._add_caller_id(req, self.effective_caller_id)
      self._add_session(req)
      response = self._get_client().call('VTGate.Rollback2', req)
      vtgate_utils.extract_rpc_error('VTGate.Rollback2', response)
    except (gorpc.GoRpcError, vtgate_utils.VitessError) as e:
      raise convert_exception(e, str(self))
    finally:
      self.session = None
      self.effective_caller_id = None

  def _add_caller_id(self, req, caller_id):
    if caller_id:
      caller_id_dict = {}
      if caller_id.principal:
        caller_id_dict['Principal'] = caller_id.principal
      if caller_id.component:
        caller_id_dict['Component'] = caller_id.component
      if caller_id.subcomponent:
        caller_id_dict['Subcomponent'] = caller_id.subcomponent
      req['CallerID'] = caller_id_dict

  def _add_session(self, req):
    if self.session:
      req['Session'] = self.session

  def _update_session(self, response):
    if response.reply.get('Session'):
      self.session = response.reply['Session']

  def _execute(
      self, sql, bind_variables, tablet_type, not_in_transaction=False):
    """Send query and variables to VTGate.Execute."""
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
      vtgate_utils.extract_rpc_error('VTGate.Execute', response)
      reply = response.reply

      if reply.get('Result'):
        res = reply['Result']
        for field in res['Fields']:
          fields.append((field['Name'], field['Type']))
          conversions.append(field_types.conversions.get(field['Type']))

        for row in res['Rows']:
          results.append(tuple(_make_row(row, conversions)))

        rowcount = res['RowsAffected']
        lastrowid = res['InsertId']
    except (gorpc.GoRpcError, vtgate_utils.VitessError) as e:
      self.logger_object.log_private_data(bind_variables)
      raise convert_exception(e, str(self), sql)
    except Exception:
      logging.exception('gorpc low-level error')
      raise
    return results, rowcount, lastrowid, fields

  def _execute_batch(
      self, sql_list, bind_variables_list, tablet_type, as_transaction):
    """Send a list of queries and variables to VTGate.ExecuteBatch."""
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
          'TabletType': topodata_pb2.TabletType.Value(tablet_type.upper()),
          'AsTransaction': as_transaction,
      }
      self._add_session(req)
      response = self._get_client().call('VTGate.ExecuteBatch', req)
      self._update_session(response)
      vtgate_utils.extract_rpc_error('VTGate.ExecuteBatch', response)
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
    except (gorpc.GoRpcError, vtgate_utils.VitessError) as e:
      self.logger_object.log_private_data(bind_variables_list)
      raise convert_exception(e, str(self), sql_list)
    except Exception:
      logging.exception('gorpc low-level error')
      raise
    return rowsets

  def _stream_execute(
      self, sql, bind_variables, tablet_type, not_in_transaction=False):
    """Start a streaming query via VTGate.StreamExecute2."""
    req = _create_req(sql, bind_variables, tablet_type, not_in_transaction)
    self._add_session(req)

    rpc_client = self._get_client()

    stream_fields = []
    stream_conversions = []

    def drain_conn_after_streaming_app_error():
      """Drain connection of all incoming streaming packets (ignoring them).

      This is necessary for streaming calls which return application
      errors inside the RPC response (instead of through the usual GoRPC
      error return).  This is because GoRPC always expects the last
      packet to be an error; either the usual GoRPC application error
      return, or a special "end-of-stream" error.

      If an application error is returned with the RPC response, there
      will still be at least one more packet coming, as GoRPC has not
      seen anything that it considers to be an error. If the connection
      is not drained of this last packet, future reads from the wire
      will be off by one and will return errors.
      """
      next_result = rpc_client.stream_next()
      if next_result is not None:
        rpc_client.close()
        raise gorpc.GoRpcError(
            'Connection should only have one packet remaining'
            ' after streaming app error in RPC response.')

    try:
      method_name = 'VTGate.StreamExecute2'
      rpc_client.stream_call(method_name, req)
      first_response = rpc_client.stream_next()
      if first_response.reply.get('Err'):
        drain_conn_after_streaming_app_error()
        raise vtgate_utils.VitessError(
            method_name, first_response.reply['Err'])
      reply = first_response.reply['Result']

      for field in reply['Fields']:
        stream_fields.append((field['Name'], field['Type']))
        stream_conversions.append(
            field_types.conversions.get(field['Type']))
    except (gorpc.GoRpcError, vtgate_utils.VitessError) as e:
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
  """Return a list by calling conversion(cell) for each cell in row."""
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
