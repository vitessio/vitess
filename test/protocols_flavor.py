#!/usr/bin/env python

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

"""Base class for protocols flavor.

Each set of protocols has a flavor object. The current one is dynamically
imported once.
"""


class ProtocolsFlavor(object):
  """Base class for protocols flavor."""

  def binlog_player_protocol(self):
    """The binlog player protocol between vttablets, in go."""
    raise NotImplementedError('Not implemented in the base class')

  def vtctl_client_protocol(self):
    """The protocol to use for vtctl connections.

    This is just for the go client.
    """
    raise NotImplementedError('Not implemented in the base class')

  def vtctl_python_client_protocol(self):
    """The protocol to use for vtctl connections.

    This is just for the python client.
    """
    raise NotImplementedError('Not implemented in the base class')

  def vtworker_client_protocol(self):
    """The protocol to use for vtworker connections."""
    raise NotImplementedError('Not implemented in the base class')

  def tablet_manager_protocol(self):
    """The protocol to use for the tablet manager protocol."""
    raise NotImplementedError('Not implemented in the base class')

  def tabletconn_protocol(self):
    """The protocol to use for connections from vtctl/vtgate to vttablet."""
    raise NotImplementedError('Not implemented in the base class')

  def throttler_client_protocol(self):
    """Client protocol for the resharding throttler.

    This RPC interface is enabled in vtworker and vttablet.
    """
    raise NotImplementedError('Not implemented in the base class')

  def vtgate_protocol(self):
    """The protocol to use to talk to vtgate, in go."""
    raise NotImplementedError('Not implemented in the base class')

  def vtgate_python_protocol(self):
    """The protocol to use to talk to vtgate with python clients."""
    raise NotImplementedError('Not implemented in the base class')

  def client_error_exception_type(self):
    """The exception type the RPC client implementation returns for errors."""
    raise NotImplementedError('Not implemented in the base class')

  def rpc_timeout_message(self):
    """The error message used by the protocol to indicate a timeout."""
    raise NotImplementedError('Not implemented in the base class')

  def service_map(self):
    """A list of entries to enable all relevant protocols in all servers."""
    raise NotImplementedError('Not implemented in the base class')

  def vttest_protocol(self):
    """Python protocol to use to talk to a vttest client."""
    raise NotImplementedError('Not implemented in the base class')


_protocols_flavor = None


def protocols_flavor():
  """Returns the current ProtocolsFlavor object."""
  return _protocols_flavor


def set_protocols_flavor(flavor):
  """Set the protocols flavor implementation."""
  global _protocols_flavor
  _protocols_flavor = flavor
