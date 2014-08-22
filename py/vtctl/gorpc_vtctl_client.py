# Copyright 2014, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import logging
import re

from net import bsonrpc


# A simple, direct connection to the vtctl server, using go rpc.
# If something goes wrong, this object should be thrown away and a new one instantiated.
class GoRpcVtctlClient(object):

  def __init__(self, addr, timeout, user=None, password=None, encrypted=False, keyfile=None, certfile=None):
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
    """execute_vtctl_command executes a remote command on the vtctl server.

    action_timeout and lock_timeout are in seconds, floats.
    info_to_debug will change the info messages into the debug level.
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


def connect(*pargs, **kargs):
  conn = VtctlClient(*pargs, **kargs)
  conn.dial()
  return conn
