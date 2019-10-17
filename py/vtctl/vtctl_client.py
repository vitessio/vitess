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
"""This module defines the vtctl client interface.
"""

import logging

# mapping from protocol to python class. The protocol matches the string
# used by vtctlclient as a -vtctl_client_protocol parameter.
vtctl_client_conn_classes = dict()


def register_conn_class(protocol, c):
  """Used by implementations to register themselves.

  Args:
    protocol: short string to document the protocol.
    c: class to register.
  """
  vtctl_client_conn_classes[protocol] = c


def connect(protocol, *pargs, **kargs):
  """connect will return a dialed VtctlClient connection to a vtctl server.

  Args:
    protocol: the registered protocol to use.
    *pargs: passed to the registered protocol __init__ method.
    **kargs: passed to the registered protocol __init__ method.

  Returns:
    A dialed VtctlClient.

  Raises:
    ValueError: if the protocol is unknown.
  """
  if protocol not in vtctl_client_conn_classes:
    raise ValueError('Unknown vtctl protocol', protocol)
  conn = vtctl_client_conn_classes[protocol](*pargs, **kargs)
  conn.dial()
  return conn


class Event(object):
  """Event is streamed by VtctlClient.

  Eventually, we will just use the proto3 definition for logutil.proto/Event.
  """

  INFO = 0
  WARNING = 1
  ERROR = 2
  CONSOLE = 3

  def __init__(self, time, level, file, line, value):
    self.time = time
    self.level = level
    self.file = file
    self.line = line
    self.value = value


class VtctlClient(object):
  """VtctlClient is the interface for the vtctl client implementations.

  All implementations must implement all these methods.
  If something goes wrong with the connection, this object will be thrown out.
  """

  def __init__(self, addr, timeout):
    """Initialize a vtctl connection.

    Args:
      addr: server address. Can be protocol dependent.
      timeout: connection timeout (float, in seconds).
    """
    pass

  def dial(self):
    """Dial to the server. If successful, call close() to close the connection.
    """
    pass

  def close(self):
    """Close the connection. This object may be re-used again by calling dial().
    """
    pass

  def is_closed(self):
    """Checks the connection status.

    Returns:
      True if this connection is closed.
    """
    pass

  def execute_vtctl_command(self, args, action_timeout=30.0):
    """Executes a remote command on the vtctl server.

    Args:
      args: Command line to run.
      action_timeout: total timeout for the action (float, in seconds).

    Returns:
      This is a generator method that yields Event objects.
    """
    pass


def execute_vtctl_command(client, args, action_timeout=30.0,
                          info_to_debug=False):
  """This is a helper method that executes a remote vtctl command.

  It logs the output to the logging module, and returns the console output.

  Args:
    client: VtctlClient object to use.
    args: Command line to run.
    action_timeout: total timeout for the action (float, in seconds).
    info_to_debug: if set, changes the info messages to debug.

  Returns:
    The console output of the action.
  """

  console_result = ''
  for e in client.execute_vtctl_command(args, action_timeout=action_timeout):
    if e.level == Event.INFO:
      if info_to_debug:
        logging.debug('%s', e.value)
      else:
        logging.info('%s', e.value)
    elif e.level == Event.WARNING:
      logging.warning('%s', e.value)
    elif e.level == Event.ERROR:
      logging.error('%s', e.value)
    elif e.level == Event.CONSOLE:
      console_result += e.value

  return console_result
