#!/usr/bin/env python

import protocols_flavor

class GoRpcProtocolsFlavor(protocols_flavor.ProtocolsFlavor):
  """Overrides to use go rpc everywhere"""

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
    return 'bson'

  def tabletconn_protocol(self):
    return 'gorpc'

  def vtgate_protocol(self):
    return 'gorpc'

  def vtgate_python_protocol(self):
    return 'gorpc'

  def rpc_timeout_message(self):
    return 'timeout waiting for'

  def service_map(self):
    return [
        'grpc-vtworker',
        # enabled for vtgate_python_protocol
        'bsonrpc-vt-vtgateservice',
        ]

protocols_flavor.__knows_protocols_flavor_map['gorpc'] = GoRpcProtocolsFlavor
