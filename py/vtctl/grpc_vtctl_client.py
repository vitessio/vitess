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
"""This file contains the grpc implementation of the vtctl client.
"""

import datetime
from urlparse import urlparse

from vtdb import prefer_vtroot_imports  # pylint: disable=unused-import

import grpc

import vtctl_client

from vtproto import vtctldata_pb2
from vtproto import vtctlservice_pb2
from util import static_auth_client
from util import grpc_with_metadata

class GRPCVtctlClient(vtctl_client.VtctlClient):
  """GRPCVtctlClient is the gRPC implementation of VtctlClient.

  It is registered as 'grpc' protocol.
  """

  def __init__(self, addr, timeout, auth_static_client_creds = None):
    self.addr = addr
    self.timeout = timeout
    self.stub = None
    self.auth_static_client_creds = auth_static_client_creds

  def __str__(self):
    return '<GRPCVtctlClient %s>' % self.addr

  def dial(self):
    if self.stub:
      self.stub.close()

    p = urlparse('http://' + self.addr)
    channel = grpc.insecure_channel('%s:%s' % (p.hostname, p.port))
    if self.auth_static_client_creds is not None:
      channel = grpc_with_metadata.GRPCWithMetadataChannel(channel, self.get_auth_static_client_creds)
    self.stub = vtctlservice_pb2.VtctlStub(channel)

  def close(self):
    self.stub = None

  def is_closed(self):
    return self.stub is None

  def get_auth_static_client_creds(self):
    return static_auth_client.StaticAuthClientCreds(self.auth_static_client_creds).metadata()

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
