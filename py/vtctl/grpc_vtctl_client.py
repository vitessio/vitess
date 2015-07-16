# Copyright 2015, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# This file contains the grpc implementation of the vtctl client.
# It is untested and doesn't work just yet: ExecuteVtctlCommand
# just seems to time out.

import datetime
from urlparse import urlparse

import vtctl_client
from vtproto import vtctldata_pb2
from vtproto import vtctlservice_pb2

class GRPCVtctlClient(vtctl_client.VctlClient):
  """GoRpcVtctlClient is the gRPC implementation of VctlClient.
  It is registered as 'grpc' protocol.
  """

  def __init__(self, addr, timeout, user=None, password=None, encrypted=False,
               keyfile=None, certfile=None):
    self.addr = addr
    self.timeout = timeout
    self.stub = None

  def __str__(self):
    return '<VtctlClient %s>' % self.addr

  def dial(self):
    if self.stub:
      self.stub.close()

    p = urlparse("http://" + self.addr)
    self.stub = vtctlservice_pb2.early_adopter_create_Vtctl_stub(p.hostname,
                                                                 p.port)

  def close(self):
    self.stub.close()
    self.stub = None

  def is_closed(self):
    return self.stub == None

  def execute_vtctl_command(self, args, action_timeout=30.0, lock_timeout=5.0):
    req = vtctldata_pb2.ExecuteVtctlCommandRequest(
        args=args,
        action_timeout=long(action_timeout * 1000000000),
        lock_timeout=long(lock_timeout * 1000000000))
    with self.stub as stub:
      for response in stub.ExecuteVtctlCommand(req, action_timeout):
        t = datetime.datetime.utcfromtimestamp(response.event.time.seconds)
        yield vtctl_client.Event(t, response.event.level, response.event.file,
                                 response.event.line, response.event.value)


vtctl_client.register_conn_class("grpc", GRPCVtctlClient)
