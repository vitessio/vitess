# Copyright 2013 Google Inc. All Rights Reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

"""A simple, direct connection to the vttablet query server.

This currently supports both vtgatev2 and vtgatev3.
"""

# TODO(dumbunny): Rename module, class, and tests to vtgate_gorpc_client.

from itertools import izip
import logging
import random

from vtproto import topodata_pb2

from net import bsonrpc
from net import gorpc

from vtdb import dbapi
from vtdb import dbexceptions
from vtdb import field_types
from vtdb import keyrange_constants
from vtdb import keyspace
from vtdb import vtdb_logger
from vtdb import vtgate_client
from vtdb import vtgate_cursor
from vtdb import vtgate_utils


def _create_v2_request_with_keyspace_ids(
    sql, new_binds, keyspace_name, tablet_type, keyspace_ids,
    not_in_transaction):
  """Make a request dict from arguments.

  Args:
    sql: Str sql with format tokens.
    new_binds: Dict of bind variables.
    keyspace_name: Str keyspace name.
    tablet_type: Str tablet_type.
    keyspace_ids: Bytes list of keyspace IDs.
    not_in_transaction: Bool True if a transaction should not be started
      (generally used when sql is not a write).

  Returns:
    A (str: value) dict.
  """
  # keyspace_ids are Keyspace Ids packed to byte[]
  sql, new_binds = dbapi.prepare_query_bind_vars(sql, new_binds)
  new_binds = field_types.convert_bind_vars(new_binds)
  req = {
      'Sql': sql,
      'BindVariables': new_binds,
      'Keyspace': keyspace_name,
      'TabletType': topodata_pb2.TabletType.Value(tablet_type.upper()),
      'KeyspaceIds': keyspace_ids,
      'NotInTransaction': not_in_transaction,
  }
  return req


def _create_v3_request(
    sql, new_binds, tablet_type, not_in_transaction):
  """Make a request where routing info is derived from sql and bind vars.

  Args:
    sql: Str sql with format tokens.
    new_binds: Dict of bind variables.
    tablet_type: Str tablet_type.
    not_in_transaction: Bool True if a transaction should not be started
      (generally used when sql is not a write).

  Returns:
    A (str: value) dict.
  """
  # sql, new_binds = dbapi.prepare_query_bind_vars(sql, new_binds)
  new_binds = field_types.convert_bind_vars(new_binds)
  req = {
      'Sql': sql,
      'BindVariables': new_binds,
      'TabletType': topodata_pb2.TabletType.Value(tablet_type.upper()),
      'NotInTransaction': not_in_transaction,
  }
  return req


def _create_v2_request_with_keyranges(
    sql, new_binds, keyspace_name, tablet_type, keyranges, not_in_transaction):
  """Make a request dict from arguments.

  Args:
    sql: Str sql with format tokens.
    new_binds: Dict of bind variables.
    keyspace_name: Str keyspace name.
    tablet_type: Str tablet_type.
    keyranges: A list of keyrange.KeyRange objects.
    not_in_transaction: Bool True if a transaction should not be started
      (generally used when sql is not a write).

  Returns:
    A (str: value) dict.
  """
  sql, new_binds = dbapi.prepare_query_bind_vars(sql, new_binds)
  new_binds = field_types.convert_bind_vars(new_binds)
  req = {
      'Sql': sql,
      'BindVariables': new_binds,
      'Keyspace': keyspace_name,
      'TabletType': topodata_pb2.TabletType.Value(tablet_type.upper()),
      'KeyRanges': keyranges,
      'NotInTransaction': not_in_transaction,
  }
  return req


class VTGateConnection(vtgate_client.VTGateClient):
  """A simple, direct connection to the vttablet query server.

  This is shard-unaware and only handles the most basic communication.
  If something goes wrong, this object should be thrown away and a new
  one instantiated.
  """

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
        raise self._convert_exception(e)
    return self.client

  def __str__(self):
    return '<VTGateConnection %s >' % self.addr

  def dial(self):
    try:
      if not self.is_closed():
        self.close()
      self._get_client().dial()
    except gorpc.GoRpcError as e:
      raise self._convert_exception(e)

  def close(self):
    if self.session:
      self.rollback()
    if self.client:
      self.client.close()

  def is_closed(self):
    return not self.client or self.client.is_closed()

  def cursor(self, *pargs, **kwargs):
    cursorclass = kwargs.pop('cursorclass', None) or vtgate_cursor.VTGateCursor
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
      raise self._convert_exception(e)

  def commit(self):
    try:
      req = {}
      self._add_caller_id(req, self.effective_caller_id)
      self._add_session(req)
      response = self._get_client().call('VTGate.Commit2', req)
      vtgate_utils.extract_rpc_error('VTGate.Commit2', response)
    except (gorpc.GoRpcError, vtgate_utils.VitessError) as e:
      raise self._convert_exception(e)
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
      raise self._convert_exception(e)
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

  def _get_rowset_from_query_result(self, query_result):
    if not query_result:
      return [], 0, 0, []
    fields = []
    conversions = []
    results = []
    for field in query_result['Fields']:
      fields.append((field['Name'], field['Type']))
      conversions.append(field_types.conversions.get(field['Type']))
    for row in query_result['Rows']:
      results.append(tuple(_make_row(row, conversions)))
    rowcount = query_result['RowsAffected']
    lastrowid = query_result['InsertId']
    return results, rowcount, lastrowid, fields

  @vtgate_utils.exponential_backoff_retry((dbexceptions.TransientError))
  def _execute(
      self, sql, bind_variables, keyspace_name, tablet_type,
      keyspace_ids=None, keyranges=None,
      entity_keyspace_id_map=None, entity_column_name=None,
      not_in_transaction=False, effective_caller_id=None):
    """Execute query.

    Args:
      sql: The sql text, with %(format)s-style tokens.
      bind_variables: (str: value) dict of bind variables corresponding
        to sql %(format)s tokens.
      keyspace_name: Str name of keyspace.
      tablet_type: Str tablet type (e.g. master, rdonly, replica).
      keyspace_ids: bytes list of keyspace ID lists.
      keyranges: KeyRange objects.
      entity_keyspace_id_map: (column value: bytes) map from a column
        to a keyspace id. If defined, vtgate adds a per-shard expression
        to the WHERE clause, and ignores keyspace_ids and keyranges
        parameters.
      entity_column_name: Str name of entity column used by
        entity_keyspace_id_map.
      not_in_transaction: bool.
      effective_caller_id: CallerID object.

    Returns:
      The (results, rowcount, lastrowid, fields) tuple.
    """

    routing_kwargs = {}
    exec_method = None
    req = None
    if entity_keyspace_id_map is not None:
      # This supercedes keyspace_ids and keyranges.
      routing_kwargs['entity_keyspace_id_map'] = entity_keyspace_id_map
      routing_kwargs['entity_column_name'] = entity_column_name
      if entity_column_name is None:
        raise dbexceptions.ProgrammingError(
            '_execute called with entity_keyspace_id_map and no '
            'entity_column_name')
      sql, new_binds = dbapi.prepare_query_bind_vars(sql, bind_variables)
      new_binds = field_types.convert_bind_vars(new_binds)
      req = {
          'Sql': sql,
          'BindVariables': new_binds,
          'Keyspace': keyspace_name,
          'TabletType': topodata_pb2.TabletType.Value(tablet_type.upper()),
          'EntityKeyspaceIDs': [
              {'ExternalID': xid, 'KeyspaceID': kid}
              for xid, kid in entity_keyspace_id_map.iteritems()],
          'EntityColumnName': entity_column_name,
          'NotInTransaction': not_in_transaction,
      }
      exec_method = 'VTGate.ExecuteEntityIds'
    elif keyspace_ids is not None:
      if keyranges is not None:
        raise dbexceptions.ProgrammingError(
            '_execute called with keyspace_ids and keyranges both defined')
      routing_kwargs['keyspace_ids'] = keyspace_ids
      req = _create_v2_request_with_keyspace_ids(
          sql, bind_variables, keyspace_name, tablet_type, keyspace_ids,
          not_in_transaction)
      exec_method = 'VTGate.ExecuteKeyspaceIds'
    elif keyranges is not None:
      routing_kwargs['keyranges'] = keyranges
      req = _create_v2_request_with_keyranges(
          sql, bind_variables, keyspace_name, tablet_type, keyranges,
          not_in_transaction)
      exec_method = 'VTGate.ExecuteKeyRanges'
    else:
      req = _create_v3_request(
          sql, bind_variables, tablet_type, not_in_transaction)
      if keyspace_name is not None:
        raise dbexceptions.ProgrammingError(
            '_execute called with keyspace_name but no routing args')
      exec_method = 'VTGate.Execute'

    self._add_caller_id(req, effective_caller_id)
    if not not_in_transaction:
      self._add_session(req)
    try:
      response = self._get_client().call(exec_method, req)
      self._update_session(response)
      vtgate_utils.extract_rpc_error(exec_method, response)
      reply = response.reply
      return self._get_rowset_from_query_result(reply.get('Result'))
    except (gorpc.GoRpcError, vtgate_utils.VitessError) as e:
      self.logger_object.log_private_data(bind_variables)
      raise self._convert_exception(
          e, sql, keyspace=keyspace_name, tablet_type=tablet_type,
          **routing_kwargs)
    except Exception:
      logging.exception('gorpc low-level error')
      raise

  def _execute_batch(
      self, sql_list, bind_variables_list, keyspace_list, keyspace_ids_list,
      shards_list, tablet_type, as_transaction, effective_caller_id=None):
    """Send multiple items in a batch.

    All lists must be the same length. This may make two calls if
    some params define keyspace_ids and some params define shards.

    Args:
      sql_list: Str list of SQL with %(format)s tokens.
      bind_variables_list: (str: value) list of bind variables corresponding
        to sql %(format)s tokens.
      keyspace_list: Str list of keyspaces.
      keyspace_ids_list: (bytes list) list of keyspace ID lists.
      shards_list: (str list) list of shard lists. For a given query,
        either keyspace_ids or shards can be defined, not both.
      tablet_type: Str tablet type (e.g. master, rdonly replica).
      as_transaction: Bool True if in transaction.
      effective_caller_id: CallerID.

    Returns:
      Rowset list.

    Raises:
      gorpc.AppError: Error returned from vtgate server.
      dbexceptions.ProgrammingError: On bad input.
    """

    def build_query_list():
      """Create a query dict list from parameters."""
      query_list = []
      for sql, bind_vars, keyspace_name, keyspace_ids, shards in zip(
          sql_list, bind_variables_list, keyspace_list, keyspace_ids_list,
          shards_list):
        sql, bind_vars = dbapi.prepare_query_bind_vars(sql, bind_vars)
        query = {}
        query['Sql'] = sql
        query['BindVariables'] = field_types.convert_bind_vars(bind_vars)
        query['Keyspace'] = keyspace_name
        if keyspace_ids:
          if shards:
            raise dbexceptions.ProgrammingError(
                'Keyspace_ids and shards cannot both be defined '
                'for the same executemany query.')
          query['KeyspaceIds'] = keyspace_ids
        else:
          query['Shards'] = shards
        query_list.append(query)
      return query_list

    def query_uses_keyspace_ids(query):
      return bool(query.get('KeyspaceIds'))

    @vtgate_utils.exponential_backoff_retry((dbexceptions.TransientError))
    def make_execute_batch_call(self, query_list, uses_keyspace_ids):
      """Make an ExecuteBatch call for KeyspaceIds or Shards queries."""
      filtered_query_list = [
          query for query in query_list
          if query_uses_keyspace_ids(query) == uses_keyspace_ids]
      rowsets = []
      if not filtered_query_list:
        return rowsets
      try:
        req = {
            'Queries': filtered_query_list,
            'TabletType': topodata_pb2.TabletType.Value(tablet_type.upper()),
            'AsTransaction': as_transaction,
        }
        self._add_caller_id(req, effective_caller_id)
        self._add_session(req)
        if uses_keyspace_ids:
          exec_method = 'VTGate.ExecuteBatchKeyspaceIds'
        else:
          exec_method = 'VTGate.ExecuteBatchShard'
        response = self._get_client().call(exec_method, req)
        self._update_session(response)
        vtgate_utils.extract_rpc_error(exec_method, response)
        for query_result in response.reply['List']:
          rowsets.append(self._get_rowset_from_query_result(query_result))
      except (gorpc.GoRpcError, vtgate_utils.VitessError) as e:
        self.logger_object.log_private_data(bind_variables_list)
        raise self._convert_exception(
            e, sql_list, exec_method,
            keyspace='', tablet_type=tablet_type)
      except Exception:
        logging.exception('gorpc low-level error')
        raise
      return rowsets

    def merge_rowsets(query_list, keyspace_ids_rowsets, shards_rowsets):
      rowsets = []
      keyspace_ids_iter = iter(keyspace_ids_rowsets)
      shards_iter = iter(shards_rowsets)
      for query in query_list:
        if query_uses_keyspace_ids(query):
          rowsets.append(keyspace_ids_iter.next())
        else:
          rowsets.append(shards_iter.next())
      return rowsets

    query_list = build_query_list()
    keyspace_ids_rowsets = make_execute_batch_call(self, query_list, True)
    shards_rowsets = make_execute_batch_call(self, query_list, False)
    return merge_rowsets(query_list, keyspace_ids_rowsets, shards_rowsets)

  @vtgate_utils.exponential_backoff_retry((dbexceptions.TransientError))
  def _stream_execute(
      self, sql, bind_variables, keyspace_name, tablet_type, keyspace_ids=None,
      keyranges=None, not_in_transaction=False, effective_caller_id=None):
    """Return a generator and the fields for the response.

    This method takes ownership of self.client, since multiple
    stream_executes can be active at once.

    Args:
      sql: Str sql.
      bind_variables: A (str: value) dict.
      keyspace_name: Str keyspace name.
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
      req = _create_v2_request_with_keyspace_ids(
          sql, bind_variables, keyspace_name, tablet_type, keyspace_ids,
          not_in_transaction)
      exec_method = 'VTGate.StreamExecuteKeyspaceIds2'
    elif keyranges is not None:
      req = _create_v2_request_with_keyranges(
          sql, bind_variables, keyspace_name, tablet_type, keyranges,
          not_in_transaction)
      exec_method = 'VTGate.StreamExecuteKeyRanges2'
    else:
      if keyspace_name:
        raise dbexceptions.ProgrammingError(
            'keyspace_name should only be provided if keyspace_ids or '
            'keyranges is provided.')
      req = _create_v3_request(
          sql, bind_variables, tablet_type, not_in_transaction)
      exec_method = 'VTGate.StreamExecute2'

    self._add_caller_id(req, effective_caller_id)

    stream_fields = []
    stream_conversions = []
    rpc_client = self._create_client()
    try:
      rpc_client.dial()
    except gorpc.GoRpcError as e:
      raise self._convert_exception(e)

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
      rpc_client.stream_call(exec_method, req)
      first_response = rpc_client.stream_next()
      if first_response:
        if first_response.reply.get('Err'):
          drain_conn_after_streaming_app_error()
          raise vtgate_utils.VitessError(
              exec_method, first_response.reply['Err'])
        reply = first_response.reply['Result']
        for field in reply['Fields']:
          stream_fields.append((field['Name'], field['Type']))
          stream_conversions.append(
              field_types.conversions.get(field['Type']))
    except (gorpc.GoRpcError, vtgate_utils.VitessError) as e:
      self.logger_object.log_private_data(bind_variables)
      raise self._convert_exception(
          e, sql, keyspace_ids, keyranges,
          keyspace=keyspace_name, tablet_type=tablet_type)
    except Exception:
      logging.exception('gorpc low-level error')
      raise

    def row_generator():
      try:
        while True:
          stream_result = rpc_client.stream_next()
          if stream_result is None:
            break
          if stream_result.reply.get('Result'):
            for result_item in stream_result.reply['Result']['Rows']:
              yield tuple(_make_row(result_item, stream_conversions))
      except gorpc.GoRpcError as e:
        raise self._convert_exception(e)
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
      vtgate_utils.extract_rpc_error('VTGate.GetSrvKeyspace', response)
      # response.reply is a proto3 encoded in bson RPC.
      # we need to make it back to what keyspace.Keyspace expects
      return keyspace.Keyspace(
          name,
          keyrange_constants.srv_keyspace_proto3_to_old(response.reply))
    except gorpc.GoRpcError as e:
      raise self._convert_exception(e, keyspace=name)
    except:
      logging.exception('gorpc low-level error')
      raise

  def _convert_exception(self, exc, *args, **kwargs):
    """This parses the protocol exceptions to the api interface exceptions.

    This also logs the exception and increments the appropriate error counters.

    Args:
      exc: raw protocol exception.
      *args: additional args from the raising site.
      **kwargs: additional keyword args from the raising site.

    Returns:
      Api interface exceptions - dbexceptions with new args.
    """
    kwargs_as_str = vtgate_utils.convert_exception_kwargs(kwargs)
    exc.args += args
    if kwargs_as_str:
      exc.args += kwargs_as_str,
    new_args = (type(exc).__name__,) + exc.args
    if isinstance(exc, gorpc.TimeoutError):
      new_exc = dbexceptions.TimeoutError(new_args)
    elif isinstance(exc, vtgate_utils.VitessError):
      new_exc = exc.convert_to_dbexception(new_args)
    elif isinstance(exc, gorpc.ProgrammingError):
      new_exc = dbexceptions.ProgrammingError(new_args)
    elif isinstance(exc, gorpc.GoRpcError):
      new_exc = dbexceptions.FatalError(new_args)
    else:
      new_exc = exc
    vtgate_utils.log_exception(
        new_exc,
        keyspace=kwargs.get('keyspace'), tablet_type=kwargs.get('tablet_type'))
    return new_exc


def _make_row(row, conversions):
  """Return row with optional conversions applied to each cell."""
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


def get_params_for_vtgate_conn(vtgate_addrs, timeout, user=None, password=None):
  """Return a one-element (addr, timeout, user, password) params dict list."""
  db_params_list = []
  addrs = []
  if isinstance(vtgate_addrs, dict):
    if 'vt' not in vtgate_addrs:
      raise ValueError("required vtgate service addrs 'vt' does not exist")
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
  """Return opened connection to vtgate."""
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
