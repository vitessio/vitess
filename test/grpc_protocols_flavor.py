#!/usr/bin/env python

import protocols_flavor

class GRpcProtocolsFlavor(protocols_flavor.ProtocolsFlavor):
  """Overrides to use gRPC everywhere where it is supported.
  If not supported yet, use GoRPC."""

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

  def rpc_timeout_message(self):
    return 'context deadline exceeded'

  def service_map(self):
    return [
        'grpc-queryservice',
        'grpc-updatestream',
        'grpc-vtctl',
        'grpc-vtworker',
        'grpc-tabletmanager',
        'grpc-vtgateservice',
        # enabled for vtgate_python_protocol
        'bsonrpc-vt-vtgateservice',
        ]

protocols_flavor.__knows_protocols_flavor_map['grpc'] = GRpcProtocolsFlavor
