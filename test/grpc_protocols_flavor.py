#!/usr/bin/env python
"""Defines which protocols to use for the gRPC flavor."""

from grpc.framework.interfaces.face import face

import protocols_flavor


class GRpcProtocolsFlavor(protocols_flavor.ProtocolsFlavor):
  """Overrides to use gRPC everywhere where it is supported.

  If not supported yet, use GoRPC.
  """

  def binlog_player_protocol(self):
    return 'grpc'

  def binlog_player_python_protocol(self):
    return 'grpc'

  def vtctl_client_protocol(self):
    return 'grpc'

  def vtctl_python_client_protocol(self):
    return 'grpc'

  def vtworker_client_protocol(self):
    return 'grpc'

  def tablet_manager_protocol(self):
    return 'grpc'

  def tabletconn_protocol(self):
    return 'grpc'

  def vtgate_protocol(self):
    return 'grpc'

  def vtgate_python_protocol(self):
    return 'gorpc'

  def client_error_exception_type(self):
    return face.RemoteError

  def rpc_timeout_message(self):
    return 'context deadline exceeded'

  def service_map(self):
    return [
        'bsonrpc-vt-vtgateservice',
        'grpc-queryservice',
        'grpc-updatestream',
        'grpc-vtctl',
        'grpc-vtworker',
        'grpc-tabletmanager',
        'grpc-vtgateservice',
        ]

  def vttest_protocol(self):
    return 'gorpc'


protocols_flavor.register_flavor('grpc', GRpcProtocolsFlavor)
