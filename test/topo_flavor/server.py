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

"""The abstract class for choosing topo server."""

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
    """Return a list of args that tell a Vitess process to use this topo server.
    """
    raise NotImplementedError()

  def wipe(self):
    """Wipe the Vitess paths in the topo server."""
    raise NotImplementedError()

  def update_addr(self, cell, keyspace, shard, tablet_index, port):
    """Update topo server with additional information."""
    raise NotImplementedError()

  def flavor(self):
    """Return the name of this topo server flavor."""
    return self.flavor_name


flavor_map = {}

_server = None


def topo_server():
  return _server


def set_topo_server_flavor(flavor):
  """Set which topo server to use."""
  global _server

  if flavor in flavor_map:
    _server = flavor_map[flavor]
    logging.debug("Using topo server flavor '%s'", flavor)
  elif not flavor:
    if len(flavor_map) == 1:
      (flavor, _server) = flavor_map.iteritems().next()
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

  _server.flavor_name = flavor
