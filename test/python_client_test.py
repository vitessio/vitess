#!/usr/bin/env python
#
# Copyright 2015 Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.
"""This test uses vtgateclienttest to test the vtdb python vtgate client.
"""

import logging
import unittest

import environment
from protocols_flavor import protocols_flavor
import utils

from vtdb import vtgate_client_testsuite
from vtdb import vtgate_client

vtgateclienttest_process = None
vtgateclienttest_port = None
vtgateclienttest_grpc_port = None


def setUpModule():
  global vtgateclienttest_process
  global vtgateclienttest_port
  global vtgateclienttest_grpc_port

  try:
    environment.topo_server().setup()

    vtgateclienttest_port = environment.reserve_ports(1)
    args = environment.binary_args('vtgateclienttest') + [
        '-log_dir', environment.vtlogroot,
        '-port', str(vtgateclienttest_port),
        ]

    if protocols_flavor().vtgate_python_protocol() == 'grpc':
      vtgateclienttest_grpc_port = environment.reserve_ports(1)
      args.extend(['-grpc_port', str(vtgateclienttest_grpc_port)])
    if protocols_flavor().service_map():
      args.extend(['-service_map', ','.join(protocols_flavor().service_map())])

    vtgateclienttest_process = utils.run_bg(args)
    utils.wait_for_vars('vtgateclienttest', vtgateclienttest_port)
  except:
    tearDownModule()
    raise


def tearDownModule():
  utils.kill_sub_process(vtgateclienttest_process, soft=True)
  if vtgateclienttest_process:
    vtgateclienttest_process.wait()

  environment.topo_server().teardown()


class TestPythonClientBase(unittest.TestCase):

  def setUp(self):
    super(TestPythonClientBase, self).setUp()
    protocol = protocols_flavor().vtgate_python_protocol()
    if protocol == 'grpc':
      addr = 'localhost:%d' % vtgateclienttest_grpc_port
    else:
      addr = 'localhost:%d' % vtgateclienttest_port
    self.conn = vtgate_client.connect(protocol, addr, 30.0)
    logging.info(
        'Start: %s, protocol %s.',
        '.'.join(self.id().split('.')[-2:]), protocol)

  def tearDown(self):
    self.conn.close()


class TestErrors(TestPythonClientBase,
                 vtgate_client_testsuite.TestErrors):
  """Test cases to verify that the Python client can handle errors correctly."""


class TestTransactionFlags(TestPythonClientBase,
                           vtgate_client_testsuite.TestTransactionFlags):
  """Success test cases for the Python client."""


class TestSuccess(TestPythonClientBase,
                  vtgate_client_testsuite.TestSuccess):
  """Success test cases for the Python client."""


class TestCallerId(TestPythonClientBase,
                   vtgate_client_testsuite.TestCallerId):
  """Caller ID test cases for the Python client."""


class TestEcho(TestPythonClientBase,
               vtgate_client_testsuite.TestEcho):
  """Send queries to the server, check the returned result matches."""


if __name__ == '__main__':
  utils.main()
