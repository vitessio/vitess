#!/usr/bin/env python
#
# Copyright 2015 Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.
"""This test uses vtgateclienttest to test the vtdb python vtgate client.
"""

import struct
import unittest

import environment
from protocols_flavor import protocols_flavor
import utils

from vtdb import dbexceptions
from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import vtgatev2


vtgateclienttest_process = None
vtgateclienttest_port = None
vtgateclienttest_grpc_port = None


def setUpModule():
  global vtgateclienttest_process
  global vtgateclienttest_port
  global vtgateclienttest_grpc_port

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


def tearDownModule():
  utils.kill_sub_process(vtgateclienttest_process, soft=True)
  vtgateclienttest_process.wait()


class TestPythonClient(unittest.TestCase):
  CONNECT_TIMEOUT = 10.0

  def setUp(self):
    # FIXME(alainjobart, dumbunny): use the factory
    addrs = {'vt': ['localhost:%u' % vtgateclienttest_port,]}
    self.conn = vtgatev2.connect(addrs, self.CONNECT_TIMEOUT)

  def tearDown(self):
    self.conn.close()

  def test_integrity_error(self):
    """Test we correctly raise dbexceptions.IntegrityError.
    """

    # FIXME(alainjobart) add test for Execute once factory supports it

    # FIXME(alainjobart) add test for ExecuteShards once factory supports it

    # ExecuteKeyspaceIds test
    cursor = self.conn.cursor('keyspace', 'master',
                              keyspace_ids=[struct.Struct('!Q').pack(0x80)])
    with self.assertRaises(dbexceptions.IntegrityError):
      cursor.execute('return integrity error', {})
    cursor.close()

    # ExecuteKeyRanges test
    kr = keyrange.KeyRange(keyrange_constants.NON_PARTIAL_KEYRANGE)
    cursor = self.conn.cursor('keyspace', 'master',
                              keyranges=[kr])
    with self.assertRaises(dbexceptions.IntegrityError):
      cursor.execute('return integrity error', {})
    cursor.close()

    # FIXME(alainjobart) add test for ExecuteEntityIds once factory supports it

    # FIXME(alainjobart) add test for ExecuteBatchShard

    # FIXME(alainjobart) add test for ExecuteBatchKeyspaceIds


if __name__ == '__main__':
  utils.main()
