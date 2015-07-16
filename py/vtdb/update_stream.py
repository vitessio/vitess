# Copyright 2015, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# mapping from protocol to python class. The protocol matches the string
# used by vttablet as a -binlog_player_protocol parameter.
update_stream_conn_classes = dict()


def register_conn_class(protocol, c):
  """Used by implementations to register themselves.

  Args:
    protocol: short string to document the protocol.
    c: class to register.
  """
  update_stream_conn_classes[protocol] = c


def connect(protocol, *pargs, **kargs):
  """connect will return a dialed UpdateStreamConnection connection to
  an update stream server.

  Args:
    protocol: the registered protocol to use.
    arsg: passed to the registered protocol __init__ method.

  Returns:
    A dialed UpdateStreamConnection.

  """
  if not protocol in update_stream_conn_classes:
    raise Exception('Unknown update stream protocol', protocol)
  conn = update_stream_conn_classes[protocol](*pargs, **kargs)
  conn.dial()
  return conn


class StreamEvent(object):
  """StreamEvent describes a single event in the update stream.

  Eventually we will use the proto3 definition object.
  """

  ERR = 0
  DML = 1
  DDL = 2
  POS = 3

  def __init__(self, category, table_name, fields, rows, sql, timestamp,
               position):
    self.category = category
    self.table_name = table_name
    self.fields = fields
    self.rows = rows
    self.sql = sql
    self.timestamp = timestamp
    self.position = position


class UpdateStreamConnection(object):
  """UpdateStreamConnection is the interface for the update stream
  client implementations.
  All implementations must implement all these methods.
  If something goes wrong with the connection, this object will be thrown out.

  """

  def __init__(self, addr, timeout):
    """Initialize an update stream connection.

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

  def stream_update(self, position):
    """Generator method to stream the updates from a given replication point.

    Args:
      position: Starting position to stream from.

    Returns:
      This is a generator method that yields StreamEvent objects.
    """
    pass
