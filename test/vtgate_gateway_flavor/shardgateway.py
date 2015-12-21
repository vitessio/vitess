#!/usr/bin/env python

# Copyright 2015 Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.
"""Contain VTGate shard gateway flavor."""

import gateway


class ShardGateway(gateway.VTGateGateway):
  """Overrides to use shard gateway."""

  def flags(self, cell=None, tablets=None):
    """Return a list of args that tell a VTGate process to start with."""
    return []


gateway.register_flavor('shardgateway', ShardGateway)
