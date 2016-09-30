#!/usr/bin/env python

# Copyright 2015 Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.
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
