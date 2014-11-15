#!/usr/bin/env python

# Copyright 2014, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import logging


class TopoServer(object):
  """Base class that defines the required interface."""

  def setup(self):
    """Initialize the topo server."""
    raise NotImplementedError()

  def teardown(self):
    """Teardown the topo server."""
    raise NotImplementedError()

  def flags(self):
    """Return a list of args that tell a Vitess process to use this topo server."""
    raise NotImplementedError()

  def wipe(self):
    """Wipe the Vitess paths in the topo server."""
    raise NotImplementedError()

  def flavor(self):
    """Return the name of this topo server flavor."""
    return self.flavor_name

flavor_map = {}

__server = None


def topo_server():
  return __server


def set_topo_server_flavor(flavor):
  global __server

  if flavor in flavor_map:
    __server = flavor_map[flavor]
    logging.debug("Using topo server flavor '%s'", flavor)
  elif not flavor:
    if len(flavor_map) == 1:
      (flavor, __server) = flavor_map.iteritems().next()
      logging.debug("Using default topo server flavor '%s'", flavor)
    else:
      logging.error(
          "No --topo-server-flavor specified. Registered flavors: [%s]",
          ",".join(flavor_map.keys()))
      return
  else:
    logging.error(
        "Unknown topo server flavor '%s'. Registered flavors: [%s]", flavor,
        ",".join(flavor_map.keys()))
    return

  __server.flavor_name = flavor
