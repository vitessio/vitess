# Copyright 2013 Google Inc. All Rights Reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

"""A simple, direct connection to the vttablet query server."""


import logging
import random
import re

from net import gorpc
from vtdb import bson_vtgate_client
from vtdb import dbapi
from vtdb import dbexceptions
from vtdb import field_types
from vtdb import keyspace
from vtdb import keyrange_constants
from vtdb import vtdb_logger
from vtdb import vtgate_client
from vtdb import vtgate_cursor
from vtdb import vtgate_utils

_errno_pattern = re.compile(r'\(errno (\d+)\)')


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


def _create_req_with_keyspace_ids(
    sql, new_binds, keyspace, tablet_type, keyspace_ids, not_in_transaction):
  # keyspace_ids are Keyspace Ids packed to byte[]
  sql, new_binds = dbapi.prepare_query_bind_vars(sql, new_binds)
  new_binds = field_types.convert_bind_vars(new_binds)
  req = {
      'Sql': sql,
      'BindVariables': new_binds,
      'Keyspace': keyspace,
      'TabletType': tablet_type,
      'KeyspaceIds': keyspace_ids,
      'NotInTransaction': not_in_transaction,
  }
  return req


def _create_req_with_keyranges(
    sql, new_binds, keyspace, tablet_type, keyranges, not_in_transaction):
  # keyranges are keyspace.KeyRange objects with start/end packed to byte[]
  sql, new_binds = dbapi.prepare_query_bind_vars(sql, new_binds)
  new_binds = field_types.convert_bind_vars(new_binds)
  req = {
      'Sql': sql,
      'BindVariables': new_binds,
      'Keyspace': keyspace,
      'TabletType': tablet_type,
      'KeyRanges': keyranges,
      'NotInTransaction': not_in_transaction,
  }
  return req


class VTGateConnection(
    bson_vtgate_client.BsonVtgateClient, vtgate_client.VTGateClient):
  """A simple, direct connection to the vttablet query server.

  This is shard-unaware and only handles the most basic communication.
  If something goes wrong, this object should be thrown away and a new
  one instantiated.
  """

  cursor_cls = vtgate_cursor.VTGateCursor

  def __init__(self, addr, timeout, user=None, password=None,
               keyfile=None, certfile=None):
    super(VTGateConnection, self).__init__(
        addr, timeout, user, password, keyfile, certfile)
    self.session = None
    self.logger_object = vtdb_logger.get_logger()

  def convert_gorpc_exception(self, exc, *args, **kwargs):
    """This parses the protocol exceptions to the api interface exceptions.

    This also logs the exception and increments the appropriate error counters.

    Args:
      exc: raw protocol exception.
      args: additional args from the raising site.
      kwargs: additional keyword args from the raising site.

    Returns:
      Api interface exceptions - dbexceptions with new args.
    """
    new_args = exc.args + (str(self),) + args
    if kwargs:
      new_args += tuple(sorted(kwargs.itervalues()))
    new_exc = exc

    if isinstance(exc, gorpc.TimeoutError):
      new_exc = dbexceptions.TimeoutError(new_args)
    elif isinstance(exc, gorpc.AppError):
      new_exc = handle_app_error(new_args)
    elif isinstance(exc, gorpc.ProgrammingError):
      new_exc = dbexceptions.ProgrammingError(new_args)
    elif isinstance(exc, gorpc.GoRpcError):
      new_exc = dbexceptions.FatalError(new_args)

    keyspace_name = kwargs.get('keyspace', None)
    tablet_type = kwargs.get('tablet_type', None)

    vtgate_utils.log_exception(new_exc, keyspace=keyspace_name,
                               tablet_type=tablet_type)
    return new_exc

  def __str__(self):
    return '<VTGateConnection %s >' % self.addr

  def in_transaction(self):
    return bool(self.session)

  def begin(self, effective_caller_id=None):
    try:
      req = {}
      self._add_caller_id(req, effective_caller_id)
      response = self._get_client().call('VTGate.Begin2', req)
      self.effective_caller_id = effective_caller_id
      self.session = None
      self._update_session(response)
    except gorpc.GoRpcError as e:
      raise self.convert_gorpc_exception(e)

  def commit(self):
    try:
      req = {}
      self._add_caller_id(req, self.effective_caller_id)
      self._add_session(req)
      self._get_client().call('VTGate.Commit2', req)
    except gorpc.GoRpcError as e:
      raise self.convert_gorpc_exception(e)
    finally:
      self.session = None
      self.effective_caller_id = None

  def rollback(self):
    try:
      req = {}
      self._add_caller_id(req, self.effective_caller_id)
      self._add_session(req)
      self._get_client().call('VTGate.Rollback2', req)
    except gorpc.GoRpcError as e:
      raise self.convert_gorpc_exception(e)
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

  @vtgate_utils.exponential_backoff_retry((dbexceptions.RequestBacklog))
  def _execute(
      self, sql, bind_variables, keyspace, tablet_type, keyspace_ids=None,
      keyranges=None, not_in_transaction=False, effective_caller_id=None):
    exec_method = None
    req = None
    if keyspace_ids is not None:
      req = _create_req_with_keyspace_ids(
          sql, bind_variables, keyspace, tablet_type, keyspace_ids,
          not_in_transaction)
      exec_method = 'VTGate.ExecuteKeyspaceIds'
    elif keyranges is not None:
      req = _create_req_with_keyranges(
          sql, bind_variables, keyspace, tablet_type, keyranges,
          not_in_transaction)
      exec_method = 'VTGate.ExecuteKeyRanges'
    else:
      raise dbexceptions.ProgrammingError(
          '_execute called without specifying keyspace_ids or keyranges')

    self._add_caller_id(req, effective_caller_id)
    self._add_session(req)

    fields = []
    conversions = []
    results = []
    rowcount = 0
    lastrowid = 0
    try:
      response = self._get_client().call(exec_method, req)
      self._update_session(response)
      reply = response.reply
      if response.reply.get('Error'):
        raise gorpc.AppError(response.reply['Error'], exec_method)

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
      raise self.convert_gorpc_exception(e, sql, keyspace_ids, keyranges,
                                   keyspace=keyspace, tablet_type=tablet_type)
    except Exception:
      logging.exception('gorpc low-level error')
      raise
    return results, rowcount, lastrowid, fields

  @vtgate_utils.exponential_backoff_retry((dbexceptions.RequestBacklog))
  def _execute_entity_ids(
      self, sql, bind_variables, keyspace, tablet_type,
      entity_keyspace_id_map, entity_column_name, not_in_transaction=False,
      effective_caller_id=None):
    sql, new_binds = dbapi.prepare_query_bind_vars(sql, bind_variables)
    new_binds = field_types.convert_bind_vars(new_binds)
    req = {
        'Sql': sql,
        'BindVariables': new_binds,
        'Keyspace': keyspace,
        'TabletType': tablet_type,
        'EntityKeyspaceIDs': [
            {'ExternalID': xid, 'KeyspaceID': kid}
            for xid, kid in entity_keyspace_id_map.iteritems()],
        'EntityColumnName': entity_column_name,
        'NotInTransaction': not_in_transaction,
        }

    self._add_caller_id(req, effective_caller_id)
    self._add_session(req)

    fields = []
    conversions = []
    results = []
    rowcount = 0
    lastrowid = 0
    try:
      response = self._get_client().call('VTGate.ExecuteEntityIds', req)
      self._update_session(response)
      reply = response.reply
      if response.reply.get('Error'):
        raise gorpc.AppError(response.reply['Error'], 'VTGate.ExecuteEntityIds')

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
      raise self.convert_gorpc_exception(e, sql, entity_keyspace_id_map,
                                   keyspace=keyspace, tablet_type=tablet_type)
    except Exception:
      logging.exception('gorpc low-level error')
      raise
    return results, rowcount, lastrowid, fields

  @vtgate_utils.exponential_backoff_retry((dbexceptions.RequestBacklog))
  def _execute_batch(
      self, sql_list, bind_variables_list, keyspace_list, keyspace_ids_list,
      tablet_type, as_transaction, effective_caller_id=None):
    query_list = []
    for sql, bind_vars, keyspace, keyspace_ids in zip(
        sql_list, bind_variables_list, keyspace_list, keyspace_ids_list):
      sql, bind_vars = dbapi.prepare_query_bind_vars(sql, bind_vars)
      query = {}
      query['Sql'] = sql
      query['BindVariables'] = field_types.convert_bind_vars(bind_vars)
      query['Keyspace'] = keyspace
      query['KeyspaceIds'] = keyspace_ids
      query_list.append(query)

    rowsets = []

    try:
      req = {
          'Queries': query_list,
          'TabletType': tablet_type,
          'AsTransaction': as_transaction,
      }
      self._add_caller_id(req, effective_caller_id)
      self._add_session(req)
      response = self._get_client().call('VTGate.ExecuteBatchKeyspaceIds', req)
      self._update_session(response)
      if response.reply.get('Error'):
        raise gorpc.AppError(
            response.reply['Error'], 'VTGate.ExecuteBatchKeyspaceIds')
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
      raise self.convert_gorpc_exception(e, sql_list, keyspace_ids_list,
                                   keyspace='', tablet_type=tablet_type)
    except Exception:
      logging.exception('gorpc low-level error')
      raise
    return rowsets

  @vtgate_utils.exponential_backoff_retry((dbexceptions.RequestBacklog))
  def _stream_execute(
      self, sql, bind_variables, keyspace, tablet_type, keyspace_ids=None,
      keyranges=None, not_in_transaction=False, effective_caller_id=None):
    """Return a generator and the fields for the response.

    This method takes ownership of self.client, since multiple
    stream_executes can be active at once.

    Args:
      sql: Str sql.
      bind_variables: A (str: value) dict.
      keyspace: Str keyspace.
      tablet_type: Str tablet_type.
      keyspace_ids: List of uint64 or bytes keyspace_ids.
      keyranges: KeyRange objects.
      not_in_transaction: bool.
      effective_caller_id: CallerID.

    Returns:
      Generator, fields pair.

    Raises:
      dbexceptions.ProgrammingError: On bad input.
    """
    exec_method = None
    req = None
    if keyspace_ids is not None:
      req = _create_req_with_keyspace_ids(
          sql, bind_variables, keyspace, tablet_type, keyspace_ids,
          not_in_transaction)
      exec_method = 'VTGate.StreamExecuteKeyspaceIds'
    elif keyranges is not None:
      req = _create_req_with_keyranges(
          sql, bind_variables, keyspace, tablet_type, keyranges,
          not_in_transaction)
      exec_method = 'VTGate.StreamExecuteKeyRanges'
    else:
      raise dbexceptions.ProgrammingError(
          '_stream_execute called without specifying keyspace_ids or keyranges')

    self._add_caller_id(req, effective_caller_id)

    rpc_client = self._get_client_for_streaming()
    stream_fields = []
    stream_conversions = []

    try:
      rpc_client.stream_call(exec_method, req)
      first_response = rpc_client.stream_next()
      if first_response:
        reply = first_response.reply['Result']
        for field in reply['Fields']:
          stream_fields.append((field['Name'], field['Type']))
          stream_conversions.append(
              field_types.conversions.get(field['Type']))
    except gorpc.GoRpcError as e:
      self.logger_object.log_private_data(bind_variables)
      raise self.convert_gorpc_exception(e, sql, keyspace_ids, keyranges,
                                   keyspace=keyspace, tablet_type=tablet_type)
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

  def get_srv_keyspace(self, name):
    try:
      response = self._get_client().call('VTGate.GetSrvKeyspace', {
          'Keyspace': name,
          })
      # response.reply is a proto3 encoded in bson RPC.
      # we need to make it back to what keyspace.Keyspace expects
      return keyspace.Keyspace(
          name,
          keyrange_constants.srv_keyspace_proto3_to_old(response.reply))
    except gorpc.GoRpcError as e:
      raise self.convert_gorpc_exception(e, keyspace=name)
    except:
      logging.exception('gorpc low-level error')
      raise


def get_params_for_vtgate_conn(vtgate_addrs, timeout, user=None, password=None):
  db_params_list = []
  addrs = []
  if isinstance(vtgate_addrs, dict):
    if 'vt' not in vtgate_addrs:
      raise Exception("required vtgate service addrs 'vt' does not exist")
    addrs = vtgate_addrs['vt']
    random.shuffle(addrs)
  elif isinstance(vtgate_addrs, list):
    random.shuffle(vtgate_addrs)
    addrs = vtgate_addrs
  else:
    raise dbexceptions.Error('Wrong type for vtgate addrs %s' % vtgate_addrs)

  for addr in addrs:
    vt_params = dict()
    vt_params['addr'] = addr
    vt_params['timeout'] = timeout
    vt_params['user'] = user
    vt_params['password'] = password
    db_params_list.append(vt_params)
  return db_params_list


def connect(vtgate_addrs, timeout, user=None, password=None):
  db_params_list = get_params_for_vtgate_conn(vtgate_addrs, timeout,
                                              user=user, password=password)

  if not db_params_list:
    raise dbexceptions.OperationalError(
        'empty db params list - no db instance available for vtgate_addrs %s' %
        vtgate_addrs)

  db_exception = None
  host_addr = None
  for params in db_params_list:
    try:
      db_params = params.copy()
      host_addr = db_params['addr']
      conn = VTGateConnection(**db_params)
      conn.dial()
      return conn
    except Exception as e:
      db_exception = e
      logging.warning('db connection failed: %s, %s', host_addr, e)

  raise dbexceptions.OperationalError(
      'unable to create vt connection', host_addr, db_exception)

vtgate_client.register_conn_class('gorpc', VTGateConnection)
