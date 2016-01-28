# Copyright 2013 Google Inc. All Rights Reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

"""A simple, direct connection to the vttablet query server, using gRPC.
"""

import datetime
import logging
import re
from urlparse import urlparse

from grpc.beta import implementations
from grpc.beta import interfaces
from grpc.framework.interfaces.face import face

from vtproto import query_pb2
from vtproto import topodata_pb2
from vtproto import vtgate_pb2
from vtproto import vtgateservice_pb2

from vtdb import dbapi
from vtdb import dbexceptions
from vtdb import field_types
from vtdb import field_types_proto3
from vtdb import keyrange_constants
from vtdb import keyspace
from vtdb import times
from vtdb import vtdb_logger
from vtdb import vtgate_client
from vtdb import vtgate_cursor
from vtdb import vtgate_utils


INT_UPPERBOUND_PLUS_ONE = 1<<63
_errno_pattern = re.compile(r'\(errno (\d+)\)', re.IGNORECASE)
_throttler_err_pattern = re.compile(
    r'exceeded (.*) quota, rate limiting', re.IGNORECASE)


class GRPCVTGateConnection(vtgate_client.VTGateClient):
  """A simple, direct connection to the vtgate query service.
  """

  def __init__(self, addr, timeout):
    self.addr = addr
    self.timeout = timeout
    self.stub = None
    self.session = None
    self.logger_object = vtdb_logger.get_logger()

  def dial(self):
    if self.stub:
      self.stub.close()

    p = urlparse('http://' + self.addr)
    channel = implementations.insecure_channel(p.hostname, p.port)
    self.stub = vtgateservice_pb2.beta_create_Vitess_stub(channel)

  def close(self):
    self.stub = None

  def is_closed(self):
    return self.stub is None

  def cursor(self, *pargs, **kwargs):
    cursorclass = kwargs.pop('cursorclass', None) or vtgate_cursor.VTGateCursor
    return cursorclass(self, *pargs, **kwargs)

  def begin(self, effective_caller_id=None):
    try:
      request = vtgate_pb2.BeginRequest()
      self._add_caller_id(request, effective_caller_id)
      response = self.stub.Begin(request, self.timeout)
      self.effective_caller_id = effective_caller_id
      self.session = response.session
    except (face.AbortionError, vtgate_utils.VitessError) as e:
      raise self._convert_exception(e)

  def commit(self):
    try:
      request = vtgate_pb2.CommitRequest()
      self._add_caller_id(request, self.effective_caller_id)
      self._add_session(request)
      self.stub.Commit(request, self.timeout)
    except (face.AbortionError, vtgate_utils.VitessError) as e:
      raise self._convert_exception(e)
    finally:
      self.session = None
      self.effective_caller_id = None

  def rollback(self):
    try:
      request = vtgate_pb2.RollbackRequest()
      self._add_caller_id(request, self.effective_caller_id)
      self._add_session(request)
      self.stub.Rollback(request, self.timeout)
    except (face.AbortionError, vtgate_utils.VitessError) as e:
      raise self._convert_exception(e)
    finally:
      self.session = None
      self.effective_caller_id = None

  def _execute(self, sql, bind_variables, keyspace_name, tablet_type,
               shards=None,
               keyspace_ids=None,
               keyranges=None,
               entity_keyspace_id_map=None, entity_column_name=None,
               not_in_transaction=False, effective_caller_id=None, **kwargs):

    # FIXME(alainjobart): keyspace should be in routing_kwargs,
    # as it's not used for v3.

    # FIXME(alainjobart): the v3 part doesn't take the ptyhon-style queries
    # for bind variables (the %(xxx)s), but our style (the :xxx).
    # this is not consistent with the rest.

    try:
      routing_kwargs = {}
      exec_method = None

      if entity_keyspace_id_map is not None:
        routing_kwargs['entity_keyspace_id_map'] = entity_keyspace_id_map
        routing_kwargs['entity_column_name'] = entity_column_name
        exec_method = 'ExecuteEntityIds'
        sql, bind_variables = dbapi.prepare_query_bind_vars(sql, bind_variables)

        request = vtgate_pb2.ExecuteEntityIdsRequest(
            query=query_pb2.BoundQuery(sql=sql),
            tablet_type=topodata_pb2.TabletType.Value(tablet_type.upper()),
            keyspace=keyspace_name,
            entity_column_name=entity_column_name,
            not_in_transaction=not_in_transaction,
        )
        self._add_caller_id(request, effective_caller_id)
        self._add_session(request)
        _convert_bind_vars(bind_variables, request.query.bind_variables)

        # TODO(alainjobart) add entity_keyspace_id_map

        response = self.stub.ExecuteEntityIds(request, self.timeout)

      elif keyspace_ids is not None:
        routing_kwargs['keyspace_ids'] = keyspace_ids
        exec_method = 'ExecuteKeyspaceIds'
        sql, bind_variables = dbapi.prepare_query_bind_vars(sql, bind_variables)

        request = vtgate_pb2.ExecuteKeyspaceIdsRequest(
            query=query_pb2.BoundQuery(sql=sql),
            tablet_type=topodata_pb2.TabletType.Value(tablet_type.upper()),
            keyspace=keyspace_name,
            not_in_transaction=not_in_transaction,
        )
        self._add_caller_id(request, effective_caller_id)
        self._add_session(request)
        self._add_keyspace_ids(request, keyspace_ids)
        _convert_bind_vars(bind_variables, request.query.bind_variables)

        response = self.stub.ExecuteKeyspaceIds(request, self.timeout)

      elif keyranges is not None:
        routing_kwargs['keyranges'] = keyranges
        exec_method = 'ExecuteKeyRanges'
        sql, bind_variables = dbapi.prepare_query_bind_vars(sql, bind_variables)

        request = vtgate_pb2.ExecuteKeyRangesRequest(
            query=query_pb2.BoundQuery(sql=sql),
            tablet_type=topodata_pb2.TabletType.Value(tablet_type.upper()),
            keyspace=keyspace_name,
            not_in_transaction=not_in_transaction,
        )
        self._add_caller_id(request, effective_caller_id)
        self._add_session(request)
        self._add_key_ranges(request, keyranges)
        _convert_bind_vars(bind_variables, request.query.bind_variables)

        response = self.stub.ExecuteKeyRanges(request, self.timeout)

      else:
        exec_method = 'Execute'

        request = vtgate_pb2.ExecuteRequest(
            query=query_pb2.BoundQuery(sql=sql),
            tablet_type=topodata_pb2.TabletType.Value(tablet_type.upper()),
            not_in_transaction=not_in_transaction,
        )
        self._add_caller_id(request, effective_caller_id)
        self._add_session(request)
        _convert_bind_vars(bind_variables, request.query.bind_variables)

        response = self.stub.Execute(request, self.timeout)

      self._extract_rpc_error(exec_method, response.error)
      self.session = response.session
      return self._get_rowset_from_query_result(response.result)

    except (face.AbortionError, vtgate_utils.VitessError) as e:
      self.logger_object.log_private_data(bind_variables)
      raise self._convert_exception(
          e, sql, keyspace=keyspace_name, tablet_type=tablet_type,
          **routing_kwargs)

  def _execute_batch(
      self, sql_list, bind_variables_list, keyspace_list, keyspace_ids_list,
      shards_list, tablet_type, as_transaction, effective_caller_id=None,
      **kwargs):

    try:
      if keyspace_ids_list:
        exec_method = 'ExecuteBatchKeyspaceIds'
        request = vtgate_pb2.ExecuteBatchKeyspaceIdsRequest(
            tablet_type=topodata_pb2.TabletType.Value(tablet_type.upper()),
            as_transaction=as_transaction,
        )
        self._add_caller_id(request, effective_caller_id)
        self._add_session(request)

        for sql, bind_variables, keyspace_name, keyspace_ids in zip(
            sql_list, bind_variables_list, keyspace_list, keyspace_ids_list):
          sql, bind_variables = dbapi.prepare_query_bind_vars(sql,
                                                              bind_variables)
          query = request.queries.add()
          query.query.sql = sql
          query.keyspace = keyspace_name
          self._add_keyspace_ids(query, keyspace_ids)
          _convert_bind_vars(bind_variables, query.query.bind_variables)

        response = self.stub.ExecuteBatchKeyspaceIds(request, self.timeout)

      else:
        exec_method = 'ExecuteBatchShards'
        request = vtgate_pb2.ExecuteBatchShardsRequest(
            tablet_type=topodata_pb2.TabletType.Value(tablet_type.upper()),
            as_transaction=as_transaction,
        )
        self._add_caller_id(request, effective_caller_id)
        self._add_session(request)

        for sql, bind_variables, keyspace_name, shards in zip(
            sql_list, bind_variables_list, keyspace_list, shards_list):
          sql, bind_variables = dbapi.prepare_query_bind_vars(sql,
                                                              bind_variables)
          query = request.queries.add()
          query.query.sql = sql
          query.keyspace = keyspace_name
          query.shards = shards
          _convert_bind_vars(bind_variables, query.query.bind_variables)

        response = self.stub.ExecuteBatchShards(request, self.timeout)

      self._extract_rpc_error(exec_method, response.error)
      self.session = response.session

      rowsets = []
      for result in response.results:
        rowset = self._get_rowset_from_query_result(result)
        rowsets.append(rowset)
      return rowsets

    except (face.AbortionError, vtgate_utils.VitessError) as e:
      self.logger_object.log_private_data(bind_variables_list)
      raise self._convert_exception(
          e, sql_list, exec_method, keyspace='', tablet_type=tablet_type)

  def _stream_execute(
      self, sql, bind_variables, keyspace_name, tablet_type, keyspace_ids=None,
      keyranges=None, not_in_transaction=False, effective_caller_id=None,
      **kwargs):

    try:
      sql, bind_variables = dbapi.prepare_query_bind_vars(sql, bind_variables)

      if keyspace_ids is not None:
        request = vtgate_pb2.StreamExecuteKeyspaceIdsRequest(
            query=query_pb2.BoundQuery(sql=sql),
            tablet_type=topodata_pb2.TabletType.Value(tablet_type.upper()),
            keyspace=keyspace_name,
        )
        self._add_caller_id(request, effective_caller_id)
        self._add_keyspace_ids(request, keyspace_ids)
        _convert_bind_vars(bind_variables, request.query.bind_variables)
        it = self.stub.StreamExecuteKeyspaceIds(request, self.timeout)

      elif keyranges is not None:
        request = vtgate_pb2.StreamExecuteKeyRangesRequest(
            query=query_pb2.BoundQuery(sql=sql),
            tablet_type=topodata_pb2.TabletType.Value(tablet_type.upper()),
            keyspace=keyspace_name,
        )
        self._add_caller_id(request, effective_caller_id)
        self._add_key_ranges(request, keyranges)
        _convert_bind_vars(bind_variables, request.query.bind_variables)
        it = self.stub.StreamExecuteKeyRanges(request, self.timeout)

      else:
        request = vtgate_pb2.StreamExecuteRequest(
            query=query_pb2.BoundQuery(sql=sql),
            tablet_type=topodata_pb2.TabletType.Value(tablet_type.upper()),
        )
        self._add_caller_id(request, effective_caller_id)
        _convert_bind_vars(bind_variables, request.query.bind_variables)
        it = self.stub.StreamExecute(request, self.timeout)

      first_response = it.next()
    except (face.AbortionError, vtgate_utils.VitessError) as e:
      self.logger_object.log_private_data(bind_variables)
      raise self._convert_exception(
          e, sql, keyspace_ids, keyranges,
          keyspace=keyspace_name, tablet_type=tablet_type)

    fields = []
    conversions = []
    for field in first_response.result.fields:
      fields.append((field.name, field.type))
      conversions.append(field_types_proto3.conversions.get(field.type))

    def row_generator():
      try:
        for response in it:
          for row in response.result.rows:
            yield tuple(_make_row(row, conversions))
      except Exception:
        logging.exception('gRPC low-level error')
        raise

    return row_generator(), fields

  def get_srv_keyspace(self, name):
    try:
      request = vtgate_pb2.GetSrvKeyspaceRequest(
          keyspace=name,
      )
      response = self.stub.GetSrvKeyspace(request, self.timeout)
      return keyspace.Keyspace(
          name,
          keyrange_constants.srv_keyspace_proto3_to_old(response.srv_keyspace))

    except (face.AbortionError, vtgate_utils.VitessError) as e:
      raise self._convert_exception(e, keyspace=name)

  def _add_caller_id(self, request, caller_id):
    if caller_id:
      if caller_id.principal:
        request.caller_id.principal = caller_id.principal
      if caller_id.component:
        request.caller_id.component = caller_id.component
      if caller_id.subcomponent:
        request.caller_id.subcomponent = caller_id.subcomponent

  def _add_session(self, request):
    if self.session:
      request.session.CopyFrom(self.session)

  def _add_keyspace_ids(self, request, keyspace_ids):
    request.keyspace_ids.extend(keyspace_ids)

  def _add_key_ranges(self, request, keyranges):
    for kr in keyranges:
      encoded_kr = request.key_ranges.add()
      encoded_kr.start = kr.Start
      encoded_kr.end = kr.End

  def _extract_rpc_error(self, exec_method, error):
    if error.code:
      raise vtgate_utils.VitessError(exec_method, {
          'Code': error.code,
          'Message': error.message,
      })

  def _get_rowset_from_query_result(self, query_result):
    if not query_result:
      return [], 0, 0, []
    fields = []
    conversions = []
    results = []
    for field in query_result.fields:
      fields.append((field.name, field.type))
      conversions.append(field_types_proto3.conversions.get(field.type))
    for row in query_result.rows:
      results.append(tuple(_make_row(row, conversions)))
    rowcount = query_result.rows_affected
    lastrowid = query_result.insert_id
    return results, rowcount, lastrowid, fields

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
    if isinstance(exc, vtgate_utils.VitessError):
      new_exc = exc.convert_to_dbexception(new_args)
    elif isinstance(exc, face.AbortionError):
      msg = exc.details
      if exc.code == interfaces.StatusCode.UNAVAILABLE:
        if _throttler_err_pattern.search(msg):
          return dbexceptions.ThrottledError(new_args)
        else:
          return dbexceptions.TransientError(new_args)
      elif exc.code == interfaces.StatusCode.ALREADY_EXISTS:
        new_exc = _prune_integrity_error(msg, new_args)
      else:
        # Unhandled RPC application error
        new_exc = dbexceptions.DatabaseError(new_args + (msg,))
    else:
      new_exc = exc
    vtgate_utils.log_exception(
        new_exc,
        keyspace=kwargs.get('keyspace'), tablet_type=kwargs.get('tablet_type'))
    return new_exc


def _convert_bind_vars(bind_variables, request_bind_variables):
  """Convert binding variables to ProtoBuffer."""
  for key, val in bind_variables.iteritems():
    if isinstance(val, int):
      request_bind_variables[key].type = query_pb2.INT64
      request_bind_variables[key].value = str(val)
    elif isinstance(val, long):
      if val < INT_UPPERBOUND_PLUS_ONE:
        request_bind_variables[key].type = query_pb2.INT64
        request_bind_variables[key].value = str(val)
      else:
        request_bind_variables[key].type = query_pb2.UINT64
        request_bind_variables[key].value = str(val)
    elif isinstance(val, float):
      request_bind_variables[key].type = query_pb2.FLOAT64
      request_bind_variables[key].value = str(val)
    elif hasattr(val, '__sql_literal__'):
      request_bind_variables[key].type = query_pb2.VARCHAR
      request_bind_variables[key].value = str(val.__sql_literal__())
    elif isinstance(val, datetime.datetime):
      request_bind_variables[key].type = query_pb2.VARCHAR
      request_bind_variables[key].value = times.DateTimeToString(val)
    elif isinstance(val, datetime.date):
      request_bind_variables[key].type = query_pb2.VARCHAR
      request_bind_variables[key].value = times.DateToString(val)
    elif isinstance(val, str):
      request_bind_variables[key].type = query_pb2.VARCHAR
      request_bind_variables[key].value = val
    elif isinstance(val, field_types.NoneType):
      request_bind_variables[key].type = query_pb2.NULL_TYPE
    elif isinstance(val, (set, tuple, list)):
      list_val = list(val)
      # FIXME(alainjobart) implement this
      var = _create_list_type(list_val)
    else:
      request_bind_variables[key].type = query_pb2.BindVariable.BINARY
      request_bind_variables[key].value = str(val)


def _prune_integrity_error(msg, exc_args):
  """Prunes an integrity error message and returns an IntegrityError."""
  parts = _errno_pattern.split(msg)
  pruned_msg = msg[:msg.find(parts[2])]
  exc_args = (pruned_msg,) + tuple(exc_args[1:])
  return dbexceptions.IntegrityError(exc_args)


def _make_row(row, conversions):
  """Builds a python native row from proto3 row."""
  converted_row = []
  offset = 0
  for i, l in enumerate(row.lengths):
    if l == -1:
      converted_row.append(None)
    elif conversions[i]:
      converted_row.append(conversions[i](row.values[offset:offset+l]))
      offset += l
    else:
      converted_row.append(row.values[offset:offset+l])
      offset += l
  return converted_row


vtgate_client.register_conn_class('grpc', GRPCVTGateConnection)
