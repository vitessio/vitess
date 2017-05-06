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
"""Contain abstract VTGate gateway flavor."""

import logging


class VTGateGateway(object):
  """Base class that defines the required interface."""

  def flags(self, cell=None, tablets=None):
    """Return a list of args that tell a VTGate process to start with."""
    raise NotImplementedError()

  def connection_count_vars(self):
    """Return the vars name containing the number of serving connections."""
    raise NotImplementedError()

  def no_tablet_found_message(self):
    """Return the text message that appears in the gateway.

    When we ask a gateway implementation to perform an operation and
    there is no available tablet for it, this string will appear in
    the error message.
    """
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
    flavor = "discoverygateway"

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
