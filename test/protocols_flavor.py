#!/usr/bin/env python

import logging

class ProtocolsFlavor(object):
  """Base class for protocols"""

  def binlog_player_protocol(self):
    """Returns the name of the binlog player protocol to use."""
    raise NotImplementedError('Not implemented in the base class')

  def vtctl_client_protocol(self):
    """Returns the protocol to use for vtctl connections.
    Needs to be supported both in python and go."""
    raise NotImplementedError('Not implemented in the base class')

  def vtworker_client_protocol(self):
    """Returns the protocol to use for vtworker connections."""
    raise NotImplementedError('Not implemented in the base class')

  def tablet_manager_protocol(self):
    """Returns the protocol to use for the tablet manager protocol."""
    raise NotImplementedError('Not implemented in the base class')

  def tabletconn_protocol(self):
    """Returns the protocol to use for connections from vtctl/vtgate to
    vttablet."""
    raise NotImplementedError('Not implemented in the base class')

  def vtgate_protocol_flags(self):
    """Returns the flags to use for specifying the vtgate protocol."""
    raise NotImplementedError('Not implemented in the base class')

  def rpc_timeout_message(self):
    """Returns the error message used by the protocol to indicate a timeout."""
    raise NotImplementedError('Not implemented in the base class')

  def service_map(self):
    """Returns a list of entries for the service map to enable all
    relevant protocols in all servers."""
    raise NotImplementedError('Not implemented in the base class')


class GoRpcProtocolsFlavor(ProtocolsFlavor):
  """Overrides to use go rpc everywhere"""

  def binlog_player_protocol(self):
    return 'gorpc'

  def vtctl_client_protocol(self):
    return 'gorpc'

  def vtworker_client_protocol(self):
    # There is no GoRPC implementation for the vtworker RPC interface,
    # so we use gRPC as well.
    return 'grpc'

  def tablet_manager_protocol(self):
    return 'bson'

  def tabletconn_protocol(self):
    return 'gorpc'

  def vtgate_protocol_flags(self):
    return ['-vtgate_protocol', 'gorpc']

  def rpc_timeout_message(self):
    return 'timeout waiting for'

  def service_map(self):
    return ['grpc-vtworker']


class GRpcProtocolsFlavor(ProtocolsFlavor):
  """Overrides to use gRPC everywhere where it is supported.
  If not supported yet, use GoRPC."""

  def binlog_player_protocol(self):
    return 'grpc'

  def vtctl_client_protocol(self):
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


__knows_protocols_flavor_map = {
  'gorpc': GoRpcProtocolsFlavor,
  'grpc': GRpcProtocolsFlavor,
}
__protocols_flavor = None

def protocols_flavor():
  return __protocols_flavor

def set_protocols_flavor(flavor):
  global __protocols_flavor

  if not flavor:
    flavor = 'gorpc'

  klass = __knows_protocols_flavor_map.get(flavor, None)
  if not klass:
    logging.error('Unknown protocols flavor %s', flavor)
    exit(1)
  __protocols_flavor = klass()

  logging.debug('Using protocols flavor %s', flavor)
