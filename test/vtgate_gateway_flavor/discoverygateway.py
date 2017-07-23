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
"""Contain VTGate discovery gateway flavor."""

import gateway


class DiscoveryGateway(gateway.VTGateGateway):
  """Overrides to use discovery gateway."""

  def flags(self, cell=None, tablets=None):
    """Return a list of args that tell a VTGate process to start with."""
    return ['-cells_to_watch', cell]

  def connection_count_vars(self):
    """Return the vars name containing the number of serving connections."""
    return 'HealthcheckConnections'

  def no_tablet_found_message(self):
    """Return the text message that appears in the gateway."""
    return 'no valid tablet'


gateway.register_flavor('discoverygateway', DiscoveryGateway)
