#!/usr/bin/env python
#
# Copyright 2016, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# "unittest" is used indirectly by importing "vertical_split", but pylint does
# not grasp this.
# Import it explicitly to make pylint happy and stop it complaining about
# setUpModule, tearDownModule and the missing module docstring.
import unittest  # pylint: disable=unused-import

import environment
import utils
import vertical_split


def setUpModule():
  vertical_split.setUpModule()


def tearDownModule():
  vertical_split.tearDownModule()


class TestAutomationVerticalSplit(vertical_split.TestVerticalSplit):
  """End-to-end test for running a vertical split via the automation framework.

  This test is a subset of vertical_split.py. The "VerticalSplitTask" automation
  operation runs the major commands for a vertical split instead of calling them
  "manually" from the test.
  """

  def test_vertical_split(self):
    # Use a dedicated worker to run all vtworker commands.
    worker_proc, _, worker_rpc_port = utils.run_vtworker_bg(
        ['--cell', 'test_nj'],
        auto_log=True)
    vtworker_endpoint = 'localhost:' + str(worker_rpc_port)

    automation_server_proc, automation_server_port = (
        utils.run_automation_server())

    _, vtctld_endpoint = utils.vtctld.rpc_endpoint()
    params = {'source_keyspace': 'source_keyspace',
              'dest_keyspace': 'destination_keyspace',
              'shard_list': '0',
              'tables': '/moving/,view1',
              'vtctld_endpoint': vtctld_endpoint,
              'vtworker_endpoint': vtworker_endpoint,
             }
    args = ['--server', 'localhost:' + str(automation_server_port),
            '--task', 'VerticalSplitTask']
    args.extend(['--param=' + k + '=' + v for k, v in params.items()])
    utils.run(environment.binary_args('automation_client') + args)

    self._check_srv_keyspace('')
    self._check_blacklisted_tables(vertical_split.source_master,
                                   ['/moving/', 'view1'])
    self._check_blacklisted_tables(vertical_split.source_replica,
                                   ['/moving/', 'view1'])
    self._check_blacklisted_tables(vertical_split.source_rdonly1,
                                   ['/moving/', 'view1'])
    self._check_blacklisted_tables(vertical_split.source_rdonly2,
                                   ['/moving/', 'view1'])

    # check the binlog player is gone now
    vertical_split.destination_master.wait_for_binlog_player_count(0)

    utils.kill_sub_process(automation_server_proc, soft=True)
    utils.kill_sub_process(worker_proc, soft=True)


if __name__ == '__main__':
  utils.main()
