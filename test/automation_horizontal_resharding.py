#!/usr/bin/env python
#
# Copyright 2015, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.
"""End-to-end test for horizontal resharding automation."""

import environment
import utils
import worker


def setUpModule():
  worker.setUpModule()
  utils.Vtctld().start()

def tearDownModule():
  worker.tearDownModule()


class TestAutomationHorizontalResharding(worker.TestBaseSplitClone):
  """This test reuses worker.py because worker.py also covers the happy path
  of the horizontal resharding code. Instead of running the different resharding
  steps "manually" as part of the test, they will be run by the automation
  cluster operation.
  """

  def test_regular_operation(self):
    # Use a dedicated worker to run all vtworker commands.
    worker_proc, _, worker_rpc_port = utils.run_vtworker_bg(
        ['--cell', 'test_nj'],
        auto_log=True)
    vtworker_endpoint = "localhost:" + str(worker_rpc_port)

    automation_server_proc, automation_server_port = utils.run_automation_server()

    keyspace = 'test_keyspace'
    source_shard_list = '0'
    dest_shard_list = '-80,80-'
    _, vtctld_endpoint = utils.vtctld.rpc_endpoint()
    utils.run(environment.binary_argstr('automation_client') +
              ' --server localhost:' + str(automation_server_port) +
              ' --task HorizontalReshardingTask' +
              ' --param keyspace=' + keyspace +
              ' --param source_shard_list=' + source_shard_list +
              ' --param dest_shard_list=' + dest_shard_list +
              ' --param source_shard_rdonly_list=' +
              worker.shard_rdonly1.tablet_alias +
              ' --param dest_shard_rdonly_list=' +
              worker.shard_0_rdonly1.tablet_alias + ',' +
              worker.shard_1_rdonly1.tablet_alias +
              ' --param vtctld_endpoint=' + vtctld_endpoint +
              ' --param vtworker_endpoint=' + vtworker_endpoint)

    self.assert_shard_data_equal(0, worker.shard_master,
                                 worker.shard_0_tablets.replica)
    self.assert_shard_data_equal(1, worker.shard_master,
                                 worker.shard_1_tablets.replica)

    utils.kill_sub_process(automation_server_proc, soft=True)
    utils.kill_sub_process(worker_proc, soft=True)

if __name__ == '__main__':
  utils.main(test_options=worker.add_test_options)
