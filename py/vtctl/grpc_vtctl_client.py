# Copyright 2015 Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.
"""This file contains the grpc implementation of the vtctl client.
"""

import datetime
from urlparse import urlparse

import vtctl_client
from vtproto import vtctldata_pb2
from vtproto import vtctlservice_pb2


class GRPCVtctlClient(vtctl_client.VtctlClient):
  """GoRpcVtctlClient is the gRPC implementation of VtctlClient.

  It is registered as 'grpc' protocol.
  """

  def __init__(self, addr, timeout):
    self.addr = addr
    self.timeout = timeout
    self.stub = None

  def __str__(self):
    return '<GRPCVtctlClient %s>' % self.addr

  def dial(self):
    if self.stub:
      self.stub.close()

    p = urlparse('http://' + self.addr)
    self.stub = vtctlservice_pb2.early_adopter_create_Vtctl_stub(p.hostname,
                                                                 p.port)

  def close(self):
    self.stub.close()
    self.stub = None

  def is_closed(self):
    return self.stub is None

  def execute_vtctl_command(self, args, action_timeout=30.0, lock_timeout=5.0):
    req = vtctldata_pb2.ExecuteVtctlCommandRequest(
        args=args,
        action_timeout=long(action_timeout * 1e9),
        lock_timeout=long(lock_timeout * 1e9))
    with self.stub as stub:
      it = stub.ExecuteVtctlCommand(req, action_timeout)
      for response in it:
        t = datetime.datetime.utcfromtimestamp(response.event.time.seconds)
        try:
          yield vtctl_client.Event(t, response.event.level, response.event.file,
                                   response.event.line, response.event.value)
        except GeneratorExit:
          # if the loop is interrupted for any reason, we need to
          # cancel the iterator, so we close the RPC connection,
          # and the with __exit__ statement is executed.

          # FIXME(alainjobart) this is flaky. It sometimes doesn't stop
          # the iterator, and we don't get out of the 'with'.
          # Sending a Ctrl-C to the process then works for some reason.
          it.cancel()
          break


vtctl_client.register_conn_class('grpc', GRPCVtctlClient)
