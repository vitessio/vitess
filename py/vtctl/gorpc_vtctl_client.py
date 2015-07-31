# Copyright 2014 Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.
"""This is the go rpc client implementation of the vtctl client interface.
"""

from net import bsonrpc
from vtctl import vtctl_client


class GoRpcVtctlClient(vtctl_client.VtctlClient):
  """GoRpcVtctlClient is the go rpc implementation of VtctlClient.

  It is registered as 'gorpc' protocol.
  """

  def __init__(self, addr, timeout, user=None, password=None,
               keyfile=None, certfile=None):
    self.addr = addr
    self.timeout = timeout
    self.client = bsonrpc.BsonRpcClient(addr, timeout, user, password,
                                        keyfile=keyfile, certfile=certfile)
    self.connected = False

  def __str__(self):
    return '<GoRpcVtctlClient %s>' % self.addr

  def dial(self):
    if self.connected:
      self.client.close()

    self.client.dial()
    self.connected = True

  def close(self):
    self.connected = False
    self.client.close()

  def is_closed(self):
    return self.client.is_closed()

  def execute_vtctl_command(self, args, action_timeout=30.0, lock_timeout=5.0):
    req = {
        'Args': args,
        'ActionTimeout': long(action_timeout * 1e9),
        'LockTimeout': long(lock_timeout * 1e9),
    }
    self.client.stream_call('VtctlServer.ExecuteVtctlCommand', req)
    while True:
      e = self.client.stream_next()
      if e is None:
        break
      yield vtctl_client.Event(e.reply['Time'], e.reply['Level'],
                               e.reply['File'], e.reply['Line'],
                               e.reply['Value'])
