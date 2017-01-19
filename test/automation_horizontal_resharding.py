#!/usr/bin/env python
#
# Copyright 2015, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.
"""End-to-end test for horizontal resharding automation."""

# "unittest" is used indirectly by importing "worker", but pylint does
# not grasp this.
# Import it explicitly to make pylint happy and stop it complaining about
# setUpModule, tearDownModule and the missing module docstring.
import unittest  # pylint: disable=unused-import

import environment
import utils
import worker


def setUpModule():
  worker.setUpModule()
  utils.Vtctld().start()


def tearDownModule():
  worker.tearDownModule()


class TestAutomationHorizontalResharding(worker.TestBaseSplitClone):
  """End-to-end test for horizontal resharding automation.

  This test reuses worker.py because worker.py also covers the happy path
  of the horizontal resharding code. Instead of running the different resharding
  steps "manually" as part of the test, they will be run by the automation
  cluster operation.
  """

  KEYSPACE = 'test_keyspace'

  def test_regular_operation(self):
    # Use a dedicated worker to run all vtworker commands.
    worker_proc, _, worker_rpc_port = utils.run_vtworker_bg(
        ['--cell', 'test_nj'],
        auto_log=True)
    vtworker_endpoint = 'localhost:' + str(worker_rpc_port)

    automation_server_proc, automation_server_port = (
        utils.run_automation_server())

    source_shard_list = '0'
    dest_shard_list = '-80,80-'
    _, vtctld_endpoint = utils.vtctld.rpc_endpoint()
    utils.run(
        environment.binary_argstr('automation_client') +
        ' --server localhost:' + str(automation_server_port) +
        ' --task HorizontalReshardingTask' +
        ' --param keyspace=' + self.KEYSPACE +
        ' --param source_shard_list=' + source_shard_list +
        ' --param dest_shard_list=' + dest_shard_list +
        ' --param vtctld_endpoint=' + vtctld_endpoint +
        ' --param vtworker_endpoint=' + vtworker_endpoint +
        ' --param min_healthy_rdonly_tablets=1')

    self.verify()

    utils.kill_sub_process(automation_server_proc, soft=True)
    utils.kill_sub_process(worker_proc, soft=True)

  def verify(self):
    self.assert_shard_data_equal(0, worker.shard_master,
                                 worker.shard_0_tablets.replica)
    self.assert_shard_data_equal(1, worker.shard_master,
                                 worker.shard_1_tablets.replica)

    # Verify effect of MigrateServedTypes. Dest shards are serving now.
    utils.check_srv_keyspace(
        'test_nj', self.KEYSPACE,
        'Partitions(master): -80 80-\n'
        'Partitions(rdonly): -80 80-\n'
        'Partitions(replica): -80 80-\n')

    # Check that query service is disabled (source shard) or enabled (dest).

    # source shard: query service must be disabled after MigrateServedTypes.
    utils.check_tablet_query_service(
        self, worker.shard_rdonly1,
        serving=False, tablet_control_disabled=True)
    utils.check_tablet_query_service(
        self, worker.shard_replica,
        serving=False, tablet_control_disabled=True)
    utils.check_tablet_query_service(
        self, worker.shard_master,
        serving=False, tablet_control_disabled=True)

    # dest shard -80: query service must be enabled after MigrateServedTypes.
    utils.check_tablet_query_service(
        self, worker.shard_0_rdonly1,
        serving=True, tablet_control_disabled=False)
    utils.check_tablet_query_service(
        self, worker.shard_0_replica,
        serving=True, tablet_control_disabled=False)
    utils.check_tablet_query_service(
        self, worker.shard_0_master,
        serving=True, tablet_control_disabled=False)

    # dest shard 80-: query service must be enabled after MigrateServedTypes.
    utils.check_tablet_query_service(
        self, worker.shard_1_rdonly1,
        serving=True, tablet_control_disabled=False)
    utils.check_tablet_query_service(
        self, worker.shard_1_replica,
        serving=True, tablet_control_disabled=False)
    utils.check_tablet_query_service(
        self, worker.shard_1_master,
        serving=True, tablet_control_disabled=False)

if __name__ == '__main__':
  utils.main(test_options=worker.add_test_options)
