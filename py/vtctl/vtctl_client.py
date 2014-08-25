# Copyright 2014, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

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
  """connect will return a dialed VctlClient connection to a vtctl server.

  Args:
    protocol: the registered protocol to use.
    arsg: passed to the registered protocol __init__ method.

  Returns:
    A dialed VctlClient.
  """
  if not protocol in vtctl_client_conn_classes:
    raise Exception('Unknown vtclient protocol', protocol)
  conn = vtctl_client_conn_classes[protocol](*pargs, **kargs)
  conn.dial()
  return conn


class VctlClient(object):
  """VctlClient is the interface for the vtctl client implementations.
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

  def execute_vtctl_command(self, args, action_timeout=30.0,
                            lock_timeout=5.0, info_to_debug=False):
    """Executes a remote command on the vtctl server.

    Args:
      args: Command line to run.
      action_timeout: total timeout for the action (float, in seconds).
      lock_timeout: timeout for locking topology (float, in seconds).
      info_to_debug: if set, changes the info messages to debug.

    Returns:
      The console output of the action.
    """
    pass
