# Copyright 2019 The Vitess Authors.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Utility module for proto3-python conversions.

This module defines the conversion functions from proto3 to python,
and utility methods / classes to convert requests / responses for any
python connector using the proto3 requests / responses.
"""

import datetime
from decimal import Decimal

from vtproto import query_pb2
from vtproto import topodata_pb2
from vtproto import vtgate_pb2
from vtproto import vtrpc_pb2

from vtdb import field_types
from vtdb import keyrange_constants
from vtdb import keyspace
from vtdb import times
from vtdb import vtgate_utils

# conversions is a map of type to the conversion function that needs
# to be used to convert the incoming array of bytes to the
# corresponding native python type.
# If a type doesn't need conversion, it's not in the map.
conversions = {
    query_pb2.INT8: int,
    query_pb2.UINT8: int,
    query_pb2.INT16: int,
    query_pb2.UINT16: int,
    query_pb2.INT24: int,
    query_pb2.UINT24: int,
    query_pb2.INT32: int,
    query_pb2.UINT32: int,
    query_pb2.INT64: int,
    query_pb2.UINT64: long,
    query_pb2.FLOAT32: float,
    query_pb2.FLOAT64: float,
    query_pb2.TIMESTAMP: times.DateTimeOrNone,
    query_pb2.DATE: times.DateOrNone,
    query_pb2.TIME: times.TimeDeltaOrNone,
    query_pb2.DATETIME: times.DateTimeOrNone,
    query_pb2.YEAR: int,
    query_pb2.DECIMAL: Decimal,
    # query_pb2.TEXT: no conversion
    # query_pb2.BLOB: no conversion
    # query_pb2.VARCHAR: no conversion
    # query_pb2.VARBINARY: no conversion
    # query_pb2.CHAR: no conversion
    # query_pb2.BINARY: no conversion
    # query_pb2.BIT: no conversion
    # query_pb2.ENUM: no conversion
    # query_pb2.SET: no conversion
    # query_pb2.TUPLE: no conversion
}

# legacy_code_to_code_map maps legacy error codes
# to the new code that matches grpc's canonical error codes.
legacy_code_to_code_map = {
    vtrpc_pb2.SUCCESS_LEGACY: vtrpc_pb2.OK,
    vtrpc_pb2.CANCELLED_LEGACY: vtrpc_pb2.CANCELED,
    vtrpc_pb2.UNKNOWN_ERROR_LEGACY: vtrpc_pb2.UNKNOWN,
    vtrpc_pb2.BAD_INPUT_LEGACY: vtrpc_pb2.INVALID_ARGUMENT,
    vtrpc_pb2.DEADLINE_EXCEEDED_LEGACY: vtrpc_pb2.DEADLINE_EXCEEDED,
    vtrpc_pb2.INTEGRITY_ERROR_LEGACY: vtrpc_pb2.ALREADY_EXISTS,
    vtrpc_pb2.PERMISSION_DENIED_LEGACY: vtrpc_pb2.PERMISSION_DENIED,
    vtrpc_pb2.RESOURCE_EXHAUSTED_LEGACY: vtrpc_pb2.RESOURCE_EXHAUSTED,
    vtrpc_pb2.QUERY_NOT_SERVED_LEGACY: vtrpc_pb2.FAILED_PRECONDITION,
    vtrpc_pb2.NOT_IN_TX_LEGACY: vtrpc_pb2.ABORTED,
    vtrpc_pb2.INTERNAL_ERROR_LEGACY: vtrpc_pb2.INTERNAL,
    vtrpc_pb2.TRANSIENT_ERROR_LEGACY: vtrpc_pb2.UNAVAILABLE,
    vtrpc_pb2.UNAUTHENTICATED_LEGACY: vtrpc_pb2.UNAUTHENTICATED,
}


INT_UPPERBOUND_PLUS_ONE = 1<<63


def make_row(row, convs):
  """Builds a python native row from proto3 row, and conversion array.

  Args:
    row: proto3 query.Row object
    convs: conversion function array

  Returns:
    an array of converted rows.
  """
  converted_row = []
  offset = 0
  for i, l in enumerate(row.lengths):
    if l == -1:
      converted_row.append(None)
    elif convs[i]:
      converted_row.append(convs[i](row.values[offset:offset+l]))
      offset += l
    else:
      converted_row.append(row.values[offset:offset+l])
      offset += l
  return converted_row


def build_value(v):
  """Build a proto value from any valid input."""
  val = query_pb2.Value()
  convert_value(v, val)
  return val


def convert_value(value, proto_value, allow_lists=False):
  """Convert a variable from python type to proto type+value.

  Args:
    value: the python value.
    proto_value: the proto3 object, needs a type and value field.
    allow_lists: allows the use of python lists.
  """
  if isinstance(value, bool):
    proto_value.type = query_pb2.INT64
    proto_value.value = str(int(value))
  elif isinstance(value, int):
    proto_value.type = query_pb2.INT64
    proto_value.value = str(value)
  elif isinstance(value, long):
    if value < INT_UPPERBOUND_PLUS_ONE:
      proto_value.type = query_pb2.INT64
    else:
      proto_value.type = query_pb2.UINT64
    proto_value.value = str(value)
  elif isinstance(value, float):
    proto_value.type = query_pb2.FLOAT64
    proto_value.value = str(value)
  elif hasattr(value, '__sql_literal__'):
    proto_value.type = query_pb2.VARBINARY
    proto_value.value = str(value.__sql_literal__())
  elif isinstance(value, datetime.datetime):
    proto_value.type = query_pb2.VARBINARY
    proto_value.value = times.DateTimeToString(value)
  elif isinstance(value, datetime.date):
    proto_value.type = query_pb2.VARBINARY
    proto_value.value = times.DateToString(value)
  elif isinstance(value, str):
    proto_value.type = query_pb2.VARBINARY
    proto_value.value = value
  elif isinstance(value, field_types.NoneType):
    proto_value.type = query_pb2.NULL_TYPE
  elif allow_lists and isinstance(value, (set, tuple, list)):
    # this only works for bind variables, not for entities.
    proto_value.type = query_pb2.TUPLE
    for v in list(value):
      proto_v = proto_value.values.add()
      convert_value(v, proto_v)
  else:
    proto_value.type = query_pb2.VARBINARY
    proto_value.value = str(value)


def convert_bind_vars(bind_variables, request_bind_variables):
  """Convert binding variables to proto3.

  Args:
    bind_variables: a map of strings to python native types.
    request_bind_variables: the proto3 object to add bind variables to.
  """
  if not bind_variables:
    return
  for key, val in bind_variables.iteritems():
    convert_value(val, request_bind_variables[key], allow_lists=True)


def convert_stream_event_statement(statement):
  """Converts encoded rows inside a StreamEvent.Statement to native types.

  Args:
    statement: the StreamEvent.Statement object.

  Returns:
    fields: array of names for the primary key columns.
    rows: array of tuples for each primary key value.
  """
  fields = []
  rows = []
  if statement.primary_key_fields:
    convs = []
    for field in statement.primary_key_fields:
      fields.append(field.name)
      convs.append(conversions.get(field.type))

    for r in statement.primary_key_values:
      row = tuple(make_row(r, convs))
      rows.append(row)

  return fields, rows


class Proto3Connection(object):
  """A base class for proto3-based python connectors.

  It assumes the derived object will contain a proto3 self.session object.
  """

  def __init__(self):
    self._effective_caller_id = None
    self.event_token = None
    self.fresher = None

  def _add_caller_id(self, request, caller_id):
    """Adds the vtgate_client.CallerID to the proto3 request, if any.

    Args:
      request: proto3 request (any of the {,stream,batch} execute queries).
      caller_id: vtgate_client.CallerID object.
    """
    if caller_id:
      if caller_id.principal:
        request.caller_id.principal = caller_id.principal
      if caller_id.component:
        request.caller_id.component = caller_id.component
      if caller_id.subcomponent:
        request.caller_id.subcomponent = caller_id.subcomponent

  def _add_session(self, request):
    """Adds self.session to the request, if any.

    Args:
      request: the proto3 request to add session to.
    """
    if self.session:
      request.session.CopyFrom(self.session)

  def update_session(self, response):
    """Updates the current session from the response, if it has one.

    Args:
      response: a proto3 response that may contain a session object.
    """
    if response.HasField('session') and response.session:
      self.session = response.session

  def _convert_entity_ids(self, entity_keyspace_ids, request_eki):
    """Convert external entity id map to ProtoBuffer.

    Args:
      entity_keyspace_ids: map of entity_keyspace_id.
      request_eki: destination proto3 list.
    """
    for xid, kid in entity_keyspace_ids.iteritems():
      eid = request_eki.add()
      eid.keyspace_id = kid
      convert_value(xid, eid, allow_lists=False)

  def _add_key_ranges(self, request, key_ranges):
    """Adds the provided keyrange.KeyRange objects to the proto3 request.

    Args:
      request: proto3 request.
      key_ranges: list of keyrange.KeyRange objects.
    """
    for kr in key_ranges:
      encoded_kr = request.key_ranges.add()
      encoded_kr.start = kr.Start
      encoded_kr.end = kr.End

  def _extract_rpc_error(self, exec_method, error):
    """Raises a VitessError for a proto3 vtrpc.RPCError structure, if set.

    Args:
      exec_method: name of the method to use in VitessError.
      error: vtrpc.RPCError structure.

    Raises:
      vtgate_utils.VitessError: if an error was set.
    """
    if error.code:
      raise vtgate_utils.VitessError(exec_method, error.code, error.message)
    elif error.legacy_code:
      raise vtgate_utils.VitessError(
          exec_method,
          legacy_code_to_code_map[error.legacy_code],
          error.message)

  def build_conversions(self, qr_fields):
    """Builds an array of fields and conversions from a result fields.

    Args:
      qr_fields: query result fields

    Returns:
      fields: array of fields
      convs: conversions to use.
    """
    fields = []
    convs = []
    for field in qr_fields:
      fields.append((field.name, field.type))
      convs.append(conversions.get(field.type))
    return fields, convs

  def _get_rowset_from_query_result(self, query_result):
    """Builds a python rowset from proto3 response.

    Args:
      query_result: proto3 query.QueryResult object.

    Returns:
      Array of rows
      Number of modified rows
      Last insert ID
      Fields array of (name, type) tuples.
    """
    if not query_result:
      return [], 0, 0, []
    fields, convs = self.build_conversions(query_result.fields)
    results = []
    for row in query_result.rows:
      results.append(tuple(make_row(row, convs)))
    rowcount = query_result.rows_affected
    lastrowid = query_result.insert_id
    return results, rowcount, lastrowid, fields

  def begin_request(self, effective_caller_id, single_db):
    """Builds a vtgate_pb2.BeginRequest object.

    Also remembers the effective caller id for next call to
    commit_request or rollback_request.

    Args:
      effective_caller_id: optional vtgate_client.CallerID.
      single_db: True if single db transaction is needed.

    Returns:
      A vtgate_pb2.BeginRequest object.
    """
    request = vtgate_pb2.BeginRequest()
    request.single_db = single_db
    self._add_caller_id(request, effective_caller_id)
    self._effective_caller_id = effective_caller_id
    return request

  def commit_request(self, twopc):
    """Builds a vtgate_pb2.CommitRequest object.

    Uses the effective_caller_id saved from begin_request().
    It will also clear the saved effective_caller_id.

    Args:
      twopc: perform 2-phase commit.

    Returns:
      A vtgate_pb2.CommitRequest object.
    """
    request = vtgate_pb2.CommitRequest()
    request.atomic = twopc
    self._add_caller_id(request, self._effective_caller_id)
    self._add_session(request)
    self._effective_caller_id = None
    return request

  def rollback_request(self):
    """Builds a vtgate_pb2.RollbackRequest object.

    Uses the effective_caller_id saved from begin_request().
    It will also clear the saved effective_caller_id.

    Returns:
      A vtgate_pb2.RollbackRequest object.
    """
    request = vtgate_pb2.RollbackRequest()
    self._add_caller_id(request, self._effective_caller_id)
    self._add_session(request)
    self._effective_caller_id = None
    return request

  def execute_request_and_name(self, sql, bind_variables, tablet_type,
                               keyspace_name,
                               shards,
                               keyspace_ids,
                               key_ranges,
                               entity_column_name, entity_keyspace_id_map,
                               not_in_transaction, effective_caller_id,
                               include_event_token, compare_event_token):
    """Builds the right vtgate_pb2 Request and method for an _execute call.

    Args:
      sql: the query to run. Bind Variables in there should be in python format.
      bind_variables: python map of bind variables.
      tablet_type: string tablet type.
      keyspace_name: keyspace to apply the query to.
      shards: array of strings representing the shards.
      keyspace_ids: array of keyspace ids.
      key_ranges: array of keyrange.KeyRange objects.
      entity_column_name: the column name to vary.
      entity_keyspace_id_map: map of external id to keyspace id.
      not_in_transaction: do not create a transaction to a new shard.
      effective_caller_id: optional vtgate_client.CallerID.
      include_event_token: boolean on whether to ask for event token.
      compare_event_token: set the result extras fresher based on this token.

    Returns:
      A vtgate_pb2.XXXRequest object.
      A dict that contains the routing parameters.
      The name of the remote method called.
    """

    if shards is not None:
      request = vtgate_pb2.ExecuteShardsRequest(keyspace=keyspace_name)
      request.shards.extend(shards)
      routing_kwargs = {'shards': shards}
      method_name = 'ExecuteShards'

    elif keyspace_ids is not None:
      request = vtgate_pb2.ExecuteKeyspaceIdsRequest(keyspace=keyspace_name)
      request.keyspace_ids.extend(keyspace_ids)
      routing_kwargs = {'keyspace_ids': keyspace_ids}
      method_name = 'ExecuteKeyspaceIds'

    elif key_ranges is not None:
      request = vtgate_pb2.ExecuteKeyRangesRequest(keyspace=keyspace_name)
      self._add_key_ranges(request, key_ranges)
      routing_kwargs = {'keyranges': key_ranges}
      method_name = 'ExecuteKeyRanges'

    elif entity_keyspace_id_map is not None:
      request = vtgate_pb2.ExecuteEntityIdsRequest(
          keyspace=keyspace_name,
          entity_column_name=entity_column_name)
      self._convert_entity_ids(entity_keyspace_id_map,
                               request.entity_keyspace_ids)
      routing_kwargs = {'entity_keyspace_id_map': entity_keyspace_id_map,
                        'entity_column_name': entity_column_name}
      method_name = 'ExecuteEntityIds'

    else:
      request = vtgate_pb2.ExecuteRequest()
      if keyspace_name:
        request.keyspace_shard = keyspace_name
      routing_kwargs = {}
      method_name = 'Execute'

    request.query.sql = sql
    convert_bind_vars(bind_variables, request.query.bind_variables)
    request.tablet_type = topodata_pb2.TabletType.Value(tablet_type.upper())
    request.not_in_transaction = not_in_transaction
    self._add_caller_id(request, effective_caller_id)
    self._add_session(request)
    if include_event_token:
      request.options.include_event_token = True
    if compare_event_token:
      request.options.compare_event_token.CopyFrom(compare_event_token)
    self.event_token = None
    self.fresher = None
    return request, routing_kwargs, method_name

  def process_execute_response(self, exec_method, response):
    """Processes an Execute* response, and returns the rowset.

    Args:
      exec_method: name of the method called.
      response: proto3 response returned.
    Returns:
      results: list of rows.
      rowcount: how many rows were affected.
      lastrowid: auto-increment value for the last row inserted.
      fields: describes the field names and types.
    """
    self.update_session(response)
    self._extract_rpc_error(exec_method, response.error)
    if response.result.extras:
      self.event_token = response.result.extras.event_token
      self.fresher = response.result.extras.fresher
    return self._get_rowset_from_query_result(response.result)

  def execute_batch_request_and_name(self, sql_list, bind_variables_list,
                                     keyspace_list,
                                     keyspace_ids_list, shards_list,
                                     tablet_type, as_transaction,
                                     effective_caller_id):
    """Builds the right vtgate_pb2 ExecuteBatch query.

    Args:
      sql_list: list os SQL statements.
      bind_variables_list: list of bind variables.
      keyspace_list: list of keyspaces.
      keyspace_ids_list: list of list of keyspace_ids.
      shards_list: list of shards.
      tablet_type: target tablet type.
      as_transaction: execute all statements in a single transaction.
      effective_caller_id: optional vtgate_client.CallerID.

    Returns:
      A proper vtgate_pb2.ExecuteBatchXXX object.
      The name of the remote method to call.
    """
    if keyspace_ids_list and keyspace_ids_list[0]:
      request = vtgate_pb2.ExecuteBatchKeyspaceIdsRequest()
      for sql, bind_variables, keyspace_name, keyspace_ids in zip(
          sql_list, bind_variables_list, keyspace_list, keyspace_ids_list):
        query = request.queries.add(keyspace=keyspace_name)
        query.query.sql = sql
        convert_bind_vars(bind_variables, query.query.bind_variables)
        query.keyspace_ids.extend(keyspace_ids)
      method_name = 'ExecuteBatchKeyspaceIds'
    else:
      request = vtgate_pb2.ExecuteBatchShardsRequest()
      for sql, bind_variables, keyspace_name, shards in zip(
          sql_list, bind_variables_list, keyspace_list, shards_list):
        query = request.queries.add(keyspace=keyspace_name)
        query.query.sql = sql
        convert_bind_vars(bind_variables, query.query.bind_variables)
        query.shards.extend(shards)
      method_name = 'ExecuteBatchShards'

    request.tablet_type = topodata_pb2.TabletType.Value(tablet_type.upper())
    request.as_transaction = as_transaction
    self._add_caller_id(request, effective_caller_id)
    self._add_session(request)
    return request, method_name

  def process_execute_batch_response(self, exec_method, response):
    """Processes an ExecuteBatch* response, and returns the rowsets.

    Args:
      exec_method: name of the method called.
      response: proto3 response returned.

    Returns:
      rowsets: array of tuples as would be returned by an execute method.
    """
    self.update_session(response)
    self._extract_rpc_error(exec_method, response.error)

    rowsets = []
    for result in response.results:
      rowset = self._get_rowset_from_query_result(result)
      rowsets.append(rowset)
    return rowsets

  def update_stream_request(self,
                            keyspace_name,
                            shard,
                            key_range,
                            tablet_type,
                            timestamp,
                            event,
                            effective_caller_id):
    """Builds the right vtgate_pb2 UpdateStreamRequest.

    Args:
      keyspace_name: keyspace to apply the query to.
      shard: shard to ask for.
      key_range: keyrange.KeyRange object.
      tablet_type: string tablet type.
      timestamp: when to start the stream from.
      event: alternate way to describe where to start the stream from.
      effective_caller_id: optional vtgate_client.CallerID.

    Returns:
      A vtgate_pb2.UpdateStreamRequest object.
    """
    request = vtgate_pb2.UpdateStreamRequest(keyspace=keyspace_name,
                                             tablet_type=tablet_type,
                                             shard=shard)
    if timestamp:
      request.timestamp = timestamp
    if event:
      if event.timestamp:
        request.event.timestamp = event.timestamp
      if event.shard:
        request.event.shard = event.shard
      if event.position:
        request.event.position = event.position
    if key_range:
      request.key_range.start = key_range.Start
      request.key_range.end = key_range.End
    self._add_caller_id(request, effective_caller_id)
    return request

  def message_stream_request(self,
                             keyspace_name,
                             shard,
                             key_range,
                             name,
                             effective_caller_id):
    """Builds the right vtgate_pb2 MessageStreamRequest.

    Args:
      keyspace_name: keyspace to apply the query to.
      shard: shard to ask for.
      key_range: keyrange.KeyRange object.
      name: message table name.
      effective_caller_id: optional vtgate_client.CallerID.

    Returns:
      A vtgate_pb2.MessageStreamRequest object.
    """
    request = vtgate_pb2.MessageStreamRequest(keyspace=keyspace_name,
                                              name=name,
                                              shard=shard)
    if key_range:
      request.key_range.start = key_range.Start
      request.key_range.end = key_range.End
    self._add_caller_id(request, effective_caller_id)
    return request

  def message_ack_request(self,
                          keyspace_name,
                          name,
                          ids,
                          effective_caller_id):
    """Builds the right vtgate_pb2 MessageAckRequest.

    Args:
      keyspace_name: keyspace to apply the query to.
      name: message table name.
      ids: list of message ids.
      effective_caller_id: optional vtgate_client.CallerID.

    Returns:
      A vtgate_pb2.MessageAckRequest object.
    """
    vals = []
    for v in ids:
      vals.append(build_value(v))
    request = vtgate_pb2.MessageAckRequest(keyspace=keyspace_name,
                                           name=name,
                                           ids=vals)
    self._add_caller_id(request, effective_caller_id)
    return request

  def stream_execute_request_and_name(self, sql, bind_variables, tablet_type,
                                      keyspace_name,
                                      shards,
                                      keyspace_ids,
                                      key_ranges,
                                      effective_caller_id):
    """Builds the right vtgate_pb2 Request and method for a _stream_execute.

    Args:
      sql: the query to run. Bind Variables in there should be in python format.
      bind_variables: python map of bind variables.
      tablet_type: string tablet type.
      keyspace_name: keyspace to apply the query to.
      shards: array of strings representing the shards.
      keyspace_ids: array of keyspace ids.
      key_ranges: array of keyrange.KeyRange objects.
      effective_caller_id: optional vtgate_client.CallerID.

    Returns:
      A vtgate_pb2.StreamExecuteXXXXRequest object.
      A dict that contains the routing parameters.
      The name of the remote method called.
    """

    if shards is not None:
      request = vtgate_pb2.StreamExecuteShardsRequest(keyspace=keyspace_name)
      request.shards.extend(shards)
      routing_kwargs = {'shards': shards}
      method_name = 'StreamExecuteShards'

    elif keyspace_ids is not None:
      request = vtgate_pb2.StreamExecuteKeyspaceIdsRequest(
          keyspace=keyspace_name)
      request.keyspace_ids.extend(keyspace_ids)
      routing_kwargs = {'keyspace_ids': keyspace_ids}
      method_name = 'StreamExecuteKeyspaceIds'

    elif key_ranges is not None:
      request = vtgate_pb2.StreamExecuteKeyRangesRequest(keyspace=keyspace_name)
      self._add_key_ranges(request, key_ranges)
      routing_kwargs = {'keyranges': key_ranges}
      method_name = 'StreamExecuteKeyRanges'

    else:
      request = vtgate_pb2.StreamExecuteRequest()
      if keyspace_name:
        request.keyspace_shard = keyspace_name
      routing_kwargs = {}
      method_name = 'StreamExecute'

    request.query.sql = sql
    convert_bind_vars(bind_variables, request.query.bind_variables)
    request.tablet_type = topodata_pb2.TabletType.Value(tablet_type.upper())
    self._add_caller_id(request, effective_caller_id)
    return request, routing_kwargs, method_name

  def srv_keyspace_proto3_to_old(self, sk):
    """Converts a proto3 SrvKeyspace.

    Args:
      sk: proto3 SrvKeyspace.

    Returns:
      dict with converted values.
    """
    result = {}

    if sk.sharding_column_name:
      result['ShardingColumnName'] = sk.sharding_column_name

    if sk.sharding_column_type == 1:
      result['ShardingColumnType'] = keyrange_constants.KIT_UINT64
    elif sk.sharding_column_type == 2:
      result['ShardingColumnType'] = keyrange_constants.KIT_BYTES

    sfmap = {}
    for sf in sk.served_from:
      tt = keyrange_constants.PROTO3_TABLET_TYPE_TO_STRING[sf.tablet_type]
      sfmap[tt] = sf.keyspace
    result['ServedFrom'] = sfmap

    if sk.partitions:
      pmap = {}
      for p in sk.partitions:
        tt = keyrange_constants.PROTO3_TABLET_TYPE_TO_STRING[p.served_type]
        srs = []
        for sr in p.shard_references:
          result_sr = {
              'Name': sr.name,
          }
          if sr.key_range:
            result_sr['KeyRange'] = {
                'Start': sr.key_range.start,
                'End': sr.key_range.end,
            }
          srs.append(result_sr)
        pmap[tt] = {
            'ShardReferences': srs,
        }
      result['Partitions'] = pmap

    return result

  def keyspace_from_response(self, name, response):
    """Builds a Keyspace object from the response of a GetSrvKeyspace call.

    Args:
      name: keyspace name.
      response: a GetSrvKeyspaceResponse object.

    Returns:
      A keyspace.Keyspace object.
    """
    return keyspace.Keyspace(
        name,
        self.srv_keyspace_proto3_to_old(response.srv_keyspace))
