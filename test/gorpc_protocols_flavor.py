#!/usr/bin/env python
"""Defines which protocols to use for the Go (BSON) RPC flavor."""

from net import gorpc

import protocols_flavor


class GoRpcProtocolsFlavor(protocols_flavor.ProtocolsFlavor):
  """Overrides to use go rpc everywhere."""

  def binlog_player_protocol(self):
    return 'gorpc'

  def binlog_player_python_protocol(self):
    return 'gorpc'

  def vtctl_client_protocol(self):
    return 'gorpc'

  def vtctl_python_client_protocol(self):
    return 'gorpc'

  def vtworker_client_protocol(self):
    # There is no GoRPC implementation for the vtworker RPC interface,
    # so we use gRPC as well.
    return 'grpc'

  def tablet_manager_protocol(self):
    # we do not support bson rpc for tablet manager any more.
    return 'grpc'

  def tabletconn_protocol(self):
    # GoRPC tabletconn doesn't work for the vtgate->vttablet interface,
    # since the go/bson package no longer encodes the non-standard
    # uint64 type.
    return 'grpc'

  def vtgate_protocol(self):
    return 'gorpc'

  def vtgate_python_protocol(self):
    return 'gorpc'

  def client_error_exception_type(self):
    return gorpc.AppError

  def rpc_timeout_message(self):
    return 'context deadline exceeded'

  def service_map(self):
    return [
        'bsonrpc-vt-updatestream',
        'bsonrpc-vt-vtctl',
        'bsonrpc-vt-vtgateservice',
        'grpc-vtworker',
        'grpc-queryservice',
        'grpc-tabletmanager',
        ]

  def vttest_protocol(self):
    return 'gorpc'


protocols_flavor.register_flavor('gorpc', GoRpcProtocolsFlavor)
