#!/usr/bin/env python

import logging
import os

class ProtocolsFlavor(object):
  """Base class for protocols"""

  def binlog_player_protocol_flags(self):
    """Returns the flags to pass to process to set the binlog player protocol."""
    return []

  def vtctl_client_protocol(self):
    """Returns the protocol to use for vtctl connections. Needs to be supported both in python and go."""
    return ""

  def tablet_manager_protocol_flags(self):
    """Returns the flags to use for specifying the tablet manager protocol."""
    return ['-tablet_manager_protocol', 'bson']

  def tabletconn_protocol_flags(self):
    """Returns the flags to use for specifying the query service protocol."""
    return ['-tablet_protocol', 'gorpc']

  def rpc_timeout_message(self):
    """Returns the error message used by the protocol to indicate a timeout."""
    raise NotImplementedError('Implementations need to overwrite this')

class GoRpcProtocolsFlavor(ProtocolsFlavor):
  """Overrides to use go rpc everywhere"""

  def binlog_player_protocol_flags(self):
    return ['-binlog_player_protocol', 'gorpc']

  def vtctl_client_protocol(self):
    return 'gorpc'

  def tablet_manager_protocol_flags(self):
    return ['-tablet_manager_protocol', 'bson']

  def tabletconn_protocol_flags(self):
    return ['-tablet_protocol', 'gorpc']

  def rpc_timeout_message(self):
    return 'timeout waiting for'

__knows_protocols_flavor_map = {
  'gorpc': GoRpcProtocolsFlavor,
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
