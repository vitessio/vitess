# Copyright 2015 Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.
"""This file contains the grpc implementation of the vtctl client.
"""

import datetime
from urlparse import urlparse

# Import main protobuf library first
# to work around import order issues.
import google.protobuf  # pylint: disable=unused-import

from grpc.beta import implementations

import vtctl_client

from vtproto import vtctldata_pb2
from vtproto import vtctlservice_pb2


class GRPCVtctlClient(vtctl_client.VtctlClient):
  """GRPCVtctlClient is the gRPC implementation of VtctlClient.

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
    channel = implementations.insecure_channel(p.hostname, p.port)
    self.stub = vtctlservice_pb2.beta_create_Vtctl_stub(channel)

  def close(self):
    self.stub = None

  def is_closed(self):
    return self.stub is None

  def execute_vtctl_command(self, args, action_timeout=30.0):
    req = vtctldata_pb2.ExecuteVtctlCommandRequest(
        args=args,
        action_timeout=long(action_timeout * 1e9))
    it = self.stub.ExecuteVtctlCommand(req, action_timeout)
    for response in it:
      t = datetime.datetime.utcfromtimestamp(response.event.time.seconds)
      yield vtctl_client.Event(t, response.event.level, response.event.file,
                               response.event.line, response.event.value)


vtctl_client.register_conn_class('grpc', GRPCVtctlClient)
