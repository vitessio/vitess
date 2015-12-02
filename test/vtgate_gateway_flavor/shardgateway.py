#!/usr/bin/env python

# Copyright 2015, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.
"""Contain VTGate shard gateway flavor."""

import gateway


class ShardGateway(gateway.VTGateGateway):
  """Overrides to use shard gateway."""

  def setup(self):
    """Initialize dependent service."""
    pass

  def teardown(self):
    """Teardown dependent service."""
    pass

  def flags(self, cell=None):
    """Return a list of args that tell a VTGate process to start with."""
    return []

  def wipe(self):
    """Wipe the Vitess paths in the topo server."""
    pass


gateway.register_flavor('shardgateway', ShardGateway)
