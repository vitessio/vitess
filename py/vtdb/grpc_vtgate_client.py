# Copyright 2017 Google Inc.
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

"""A simple, direct connection to the vtgate proxy server, using gRPC.
"""

import logging
import re
from urlparse import urlparse

from vtdb import prefer_vtroot_imports  # pylint: disable=unused-import

import grpc

from vtproto import vtgate_pb2
from vtproto import vtgateservice_pb2_grpc

from vtdb import dbexceptions
from vtdb import proto3_encoding
from vtdb import vtdb_logger
from vtdb import vtgate_client
from vtdb import vtgate_cursor
from vtdb import vtgate_utils
from util import static_auth_client
from util import grpc_with_metadata


_errno_pattern = re.compile(r'\(errno (\d+)\)', re.IGNORECASE)


class GRPCVTGateConnection(vtgate_client.VTGateClient,
                           proto3_encoding.Proto3Connection):
  """A direct gRPC connection to the vtgate query service, using proto3.
  """

  def __init__(self, addr, timeout,
               root_certificates=None, private_key=None, certificate_chain=None,
               auth_static_client_creds=None,
               **kwargs):
    """Creates a new GRPCVTGateConnection.

    Args:
      addr: address to connect to.
      timeout: connection time out.
      root_certificates: PEM_encoded root certificates.
      private_key: PEM-encoded private key.
      certificate_chain: PEM-encoded certificate chain.
      auth_static_client_creds: basic auth credentials file path.
      **kwargs: passed up.
    """
    super(GRPCVTGateConnection, self).__init__(addr, timeout, **kwargs)
    self.stub = None
    self.root_certificates = root_certificates
    self.private_key = private_key
    self.certificate_chain = certificate_chain
    self.auth_static_client_creds = auth_static_client_creds
    self.logger_object = vtdb_logger.get_logger()

  def dial(self):
    if self.stub:
      self.stub.close()

    p = urlparse('http://' + self.addr)
    target = '%s:%s' % (p.hostname, p.port)

    if self.root_certificates or self.private_key or self.certificate_chain:
      creds = grpc.ssl_channel_credentials(
          self.root_certificates, self.private_key, self.certificate_chain)
      channel = grpc.secure_channel(target, creds)
    else:
      channel = grpc.insecure_channel(target)
    if self.auth_static_client_creds is not None:
      channel = grpc_with_metadata.GRPCWithMetadataChannel(
          channel,
          self.get_auth_static_client_creds)
    self.stub = vtgateservice_pb2_grpc.VitessStub(channel)

  def close(self):
    """close closes the server connection and frees up associated resources.

    The stub object is managed by the gRPC library, removing references
    to it will just close the channel.
    """
    if self.session and self.session.in_transaction:
      # If the endpoint is not responding, this would exception out,
      # just when we want to not connect to the endpoint any more.
      # Let's swallow that exception.
      try:
        self.rollback()
      except dbexceptions.DatabaseError:
        pass
    self.stub = None

  def is_closed(self):
    return self.stub is None

  def get_auth_static_client_creds(self):
    return static_auth_client.StaticAuthClientCreds(
        self.auth_static_client_creds).metadata()

  def cursor(self, *pargs, **kwargs):
    cursorclass = kwargs.pop('cursorclass', None) or vtgate_cursor.VTGateCursor
    return cursorclass(self, *pargs, **kwargs)

  def begin(self, effective_caller_id=None, single_db=False):
    try:
      request = self.begin_request(effective_caller_id, single_db)
      response = self.stub.Begin(request, self.timeout)
      self.update_session(response)
    except (grpc.RpcError, vtgate_utils.VitessError) as e:
      raise _convert_exception(e, 'Begin')

  def commit(self, twopc=False):
    try:
      request = self.commit_request(twopc)
      self.stub.Commit(request, self.timeout)
    except (grpc.RpcError, vtgate_utils.VitessError) as e:
      raise _convert_exception(e, 'Commit')
    finally:
      self.session = None

  def rollback(self):
    try:
      request = self.rollback_request()
      self.stub.Rollback(request, self.timeout)
    except (grpc.RpcError, vtgate_utils.VitessError) as e:
      raise _convert_exception(e, 'Rollback')
    finally:
      self.session = None

  @vtgate_utils.exponential_backoff_retry((dbexceptions.ThrottledError,
                                           dbexceptions.TransientError))
  def _execute(
      self, sql, bind_variables, tablet_type, keyspace_name=None,
      shards=None, keyspace_ids=None, keyranges=None,
      entity_keyspace_id_map=None, entity_column_name=None,
      not_in_transaction=False, effective_caller_id=None,
      include_event_token=False, compare_event_token=None, **kwargs):

    # FIXME(alainjobart): keyspace should be in routing_kwargs,
    # as it's not used for v3.

    try:
      request, routing_kwargs, method_name = self.execute_request_and_name(
          sql, bind_variables, tablet_type,
          keyspace_name, shards, keyspace_ids, keyranges,
          entity_column_name, entity_keyspace_id_map,
          not_in_transaction, effective_caller_id, include_event_token,
          compare_event_token)
      method = getattr(self.stub, method_name)
      response = method(request, self.timeout)
      return self.process_execute_response(method_name, response)

    except (grpc.RpcError, vtgate_utils.VitessError) as e:
      self.logger_object.log_private_data(bind_variables)
      raise _convert_exception(
          e, method_name,
          sql=sql, keyspace=keyspace_name, tablet_type=tablet_type,
          not_in_transaction=not_in_transaction,
          **routing_kwargs)

  @vtgate_utils.exponential_backoff_retry((dbexceptions.ThrottledError,
                                           dbexceptions.TransientError))
  def _execute_batch(
      self, sql_list, bind_variables_list, keyspace_list, keyspace_ids_list,
      shards_list, tablet_type, as_transaction, effective_caller_id=None,
      **kwargs):

    try:
      request, method_name = self.execute_batch_request_and_name(
          sql_list, bind_variables_list, keyspace_list,
          keyspace_ids_list, shards_list,
          tablet_type, as_transaction, effective_caller_id)
      method = getattr(self.stub, method_name)
      response = method(request, self.timeout)
      return self.process_execute_batch_response(method_name, response)

    except (grpc.RpcError, vtgate_utils.VitessError) as e:
      self.logger_object.log_private_data(bind_variables_list)
      raise _convert_exception(
          e, method_name,
          sqls=sql_list, tablet_type=tablet_type,
          as_transaction=as_transaction)

  @vtgate_utils.exponential_backoff_retry((dbexceptions.ThrottledError,
                                           dbexceptions.TransientError))
  def _stream_execute(
      self, sql, bind_variables, tablet_type, keyspace_name=None,
      shards=None, keyspace_ids=None, keyranges=None,
      effective_caller_id=None,
      **kwargs):

    try:
      request, routing_kwargs, method_name = (
          self.stream_execute_request_and_name(
              sql, bind_variables, tablet_type,
              keyspace_name,
              shards,
              keyspace_ids,
              keyranges,
              effective_caller_id))
      method = getattr(self.stub, method_name)
      it = method(request, self.timeout)
      first_response = it.next()
    except (grpc.RpcError, vtgate_utils.VitessError) as e:
      self.logger_object.log_private_data(bind_variables)
      raise _convert_exception(
          e, method_name,
          sql=sql, keyspace=keyspace_name, tablet_type=tablet_type,
          **routing_kwargs)

    fields, convs = self.build_conversions(first_response.result.fields)

    def row_generator():
      try:
        for response in it:
          for row in response.result.rows:
            yield tuple(proto3_encoding.make_row(row, convs))
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
      return self.keyspace_from_response(name, response)

    except (grpc.RpcError, vtgate_utils.VitessError) as e:
      raise _convert_exception(e, keyspace=name)

  @vtgate_utils.exponential_backoff_retry((dbexceptions.ThrottledError,
                                           dbexceptions.TransientError))
  def update_stream(
      self, keyspace_name, tablet_type,
      timestamp=None, event=None,
      shard=None, key_range=None,
      effective_caller_id=None,
      **kwargs):

    try:
      request = self.update_stream_request(
          keyspace_name, shard, key_range, tablet_type,
          timestamp, event, effective_caller_id)
      it = self.stub.UpdateStream(request, self.timeout)
    except (grpc.RpcError, vtgate_utils.VitessError) as e:
      raise _convert_exception(
          e, 'UpdateStream',
          keyspace=keyspace_name, tablet_type=tablet_type)

    def row_generator():
      try:
        for response in it:
          yield (response.event, response.resume_timestamp)
      except Exception as e:
        raise _convert_exception(e)

    return row_generator()

  @vtgate_utils.exponential_backoff_retry((dbexceptions.ThrottledError,
                                           dbexceptions.TransientError))
  def message_stream(
      self, keyspace, name,
      shard=None, key_range=None,
      effective_caller_id=None,
      **kwargs):

    try:
      request = self.message_stream_request(
          keyspace, shard, key_range,
          name, effective_caller_id)
      it = self.stub.MessageStream(request, self.timeout)
      first_response = it.next()
    except (grpc.RpcError, vtgate_utils.VitessError) as e:
      raise _convert_exception(
          e, 'MessageStream', name=name,
          keyspace=keyspace)

    fields, convs = self.build_conversions(first_response.result.fields)

    def row_generator():
      try:
        for response in it:
          for row in response.result.rows:
            yield tuple(proto3_encoding.make_row(row, convs))
      except Exception:
        logging.exception('gRPC low-level error')
        raise

    return row_generator(), fields

  @vtgate_utils.exponential_backoff_retry((dbexceptions.ThrottledError,
                                           dbexceptions.TransientError))
  def message_ack(
      self,
      name, ids,
      keyspace=None, effective_caller_id=None,
      **kwargs):

    try:
      request = self.message_ack_request(
          keyspace, name, ids, effective_caller_id)
      response = self.stub.MessageAck(request, self.timeout)
    except (grpc.RpcError, vtgate_utils.VitessError) as e:
      raise _convert_exception(
          e, 'MessageAck', name=name, ids=ids,
          keyspace=keyspace)

    return response.result.rows_affected

  def get_warnings(self):
    if self.session:
      return self.session.warnings
    return []

def _convert_exception(exc, *args, **kwargs):
  """This parses the protocol exceptions to the api interface exceptions.

  This also logs the exception and increments the appropriate error counters.

  Args:
    exc: raw protocol exception.
    *args: additional args from the raising site.
    **kwargs: additional keyword args from the raising site.
              They will be converted into a single string, and added as an extra
              arg to the exception.

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
  elif isinstance(exc, grpc.RpcError):
    # Most RpcErrors should also implement Call so we can get details.
    if isinstance(exc, grpc.Call):
      code = exc.code()
      details = exc.details()
      if code == grpc.StatusCode.DEADLINE_EXCEEDED:
        new_exc = dbexceptions.TimeoutError(new_args)
      elif code == grpc.StatusCode.UNAVAILABLE:
        if vtgate_utils.throttler_err_re.search(details):
          return dbexceptions.ThrottledError(new_args)
        else:
          return dbexceptions.TransientError(details, new_args)
      elif code == grpc.StatusCode.ALREADY_EXISTS:
        new_exc = _prune_integrity_error(details, new_args)
      elif code == grpc.StatusCode.FAILED_PRECONDITION:
        return dbexceptions.QueryNotServed(details, new_args)
      elif code == grpc.StatusCode.INVALID_ARGUMENT:
        return dbexceptions.ProgrammingError(details, new_args)
      else:
        # Other RPC error that we don't specifically handle.
        new_exc = dbexceptions.DatabaseError(new_args + (code, details))
    else:
      # RPC error that doesn't provide code and details.
      # Don't let gRPC-specific errors leak beyond this package.
      new_exc = dbexceptions.DatabaseError(new_args + (exc,))
  else:
    new_exc = exc
  vtgate_utils.log_exception(
      new_exc,
      keyspace=kwargs.get('keyspace'), tablet_type=kwargs.get('tablet_type'))
  return new_exc


def _prune_integrity_error(msg, exc_args):
  """Prunes an integrity error message and returns an IntegrityError."""
  parts = _errno_pattern.split(msg)
  pruned_msg = msg[:msg.find(parts[2])]
  exc_args = (pruned_msg,) + tuple(exc_args[1:])
  return dbexceptions.IntegrityError(exc_args)

vtgate_client.register_conn_class('grpc', GRPCVTGateConnection)
