#!/usr/bin/env python

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

"""Defines which protocols to use for the gRPC flavor."""

from grpc.framework.interfaces.face import face

import protocols_flavor

# Now imports all the implementations we need.
# We will change this to explicit registration soon.
from vtctl import grpc_vtctl_client  # pylint: disable=unused-import
from vtdb import grpc_vtgate_client  # pylint: disable=unused-import


class GRpcProtocolsFlavor(protocols_flavor.ProtocolsFlavor):
  """Definitons to use gRPC everywhere.
  """

  def binlog_player_protocol(self):
    return 'grpc'

  def vtctl_client_protocol(self):
    return 'grpc'

  def vtctl_python_client_protocol(self):
    return 'grpc'

  def vtworker_client_protocol(self):
    return 'grpc'

  def tablet_manager_protocol(self):
    return 'grpc'

  def tabletconn_protocol(self):
    return 'grpc'

  def throttler_client_protocol(self):
    return 'grpc'

  def vtgate_protocol(self):
    return 'grpc'

  def vtgate_python_protocol(self):
    return 'grpc'

  def client_error_exception_type(self):
    return face.AbortionError

  def rpc_timeout_message(self):
    return 'context deadline exceeded'

  def service_map(self):
    return [
        'grpc-tabletmanager',
        'grpc-throttler',
        'grpc-queryservice',
        'grpc-updatestream',
        'grpc-vtctl',
        'grpc-vtworker',
        'grpc-vtgateservice',
        ]

  def vttest_protocol(self):
    return 'grpc'
