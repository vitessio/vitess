#!/usr/bin/env python

import protocols_flavor

class GRpcProtocolsFlavor(protocols_flavor.ProtocolsFlavor):
  """Overrides to use gRPC everywhere where it is supported.
  If not supported yet, use GoRPC."""

  def binlog_player_protocol(self):
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

  def vtgate_protocol_flags(self):
    return ['-vtgate_protocol', 'gorpc']

  def rpc_timeout_message(self):
    return 'context deadline exceeded'

  def service_map(self):
    return [
        'grpc-queryservice',
        'grpc-updatestream',
        'grpc-vtctl',
        'grpc-vtworker',
        'grpc-tabletmanager',
        ]

protocols_flavor.__knows_protocols_flavor_map['grpc'] = GRpcProtocolsFlavor
