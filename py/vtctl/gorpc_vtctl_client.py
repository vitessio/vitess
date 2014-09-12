# Copyright 2014, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import logging
import re

from net import bsonrpc
from vtctl import vtctl_client


class GoRpcVtctlClient(vtctl_client.VctlClient):
  """GoRpcVtctlClient is the go rpc implementation of VctlClient.
  It is registered as 'gorpc' protocol.
  """

  def __init__(self, addr, timeout, user=None, password=None, encrypted=False,
               keyfile=None, certfile=None):
    self.addr = addr
    self.timeout = timeout
    self.client = bsonrpc.BsonRpcClient(addr, timeout, user, password, encrypted=encrypted, keyfile=keyfile, certfile=certfile)
    self.connected = False

  def __str__(self):
    return '<VtctlClient %s>' % self.addr

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
    req = {
      'Args':          args,
      'ActionTimeout': long(action_timeout * 1000000000),
      'LockTimeout':   long(lock_timeout * 1000000000),
    }
    self.client.stream_call('VtctlServer.ExecuteVtctlCommand', req)
    console_result = ''
    while True:
      e = self.client.stream_next()
      if e is None:
        break
      if e.reply['Level'] == 0:
        if info_to_debug:
          logging.debug('%s', e.reply['Value'])
        else:
          logging.info('%s', e.reply['Value'])
      elif e.reply['Level'] == 1:
        logging.warning('%s', e.reply['Value'])
      elif e.reply['Level'] == 2:
        logging.error('%s', e.reply['Value'])
      elif e.reply['Level'] == 3:
        console_result += e.reply['Value']

    return console_result
