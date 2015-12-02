#!/usr/bin/env python

# Copyright 2015, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.
"""Contain abstract VTGate gateway flavor."""

import logging


class VTGateGateway(object):
  """Base class that defines the required interface."""

  def setup(self):
    """Initialize dependent service."""
    raise NotImplementedError()

  def teardown(self):
    """Teardown dependent service."""
    raise NotImplementedError()

  def flags(self, cell=None):
    """Return a list of args that tell a VTGate process to start with."""
    raise NotImplementedError()

  def wipe(self):
    """Wipe the Vitess paths in the topo server."""
    raise NotImplementedError()

  def flavor(self):
    """Return the name of this topo server flavor."""
    return self.flavor_name

flavor_map = {}

_gateway = None


def vtgate_gateway_flavor():
  """Return the VTGate gateway flavor instance."""
  return _gateway


def set_vtgate_gateway_flavor(flavor):
  """Set the VTGate gateway flavor to be used."""
  global _gateway

  if not flavor:
    flavor = "shardgateway"

  cls = flavor_map.get(flavor, None)
  if not cls:
    logging.error("Unknown VTGate gateway flavor %s", flavor)
    exit(1)

  _gateway = cls()
  _gateway.flavor_name = flavor
  logging.debug("Using VTGate gateway flavor '%s'", flavor)


def register_flavor(key, cls):
  """Register the available VTGate gateway flavors."""
  flavor_map[key] = cls
