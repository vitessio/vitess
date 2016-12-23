# Copyright 2016 Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.
"""End-to-end test for horizontal resharding workflow."""

# pylint: disable=unused-import
import re
import unittest

import utils
import worker


def setUpModule():
  try:
    worker.setUpModule()
    utils.Vtctld().start()
  except:
    tearDownModule()
    raise


def tearDownModule():
  worker.tearDownModule()


class TestWorkflowHorizontalResharding(worker.TestBaseSplitClone):
  """End-to-end test for horizontal resharding workflow.

  This test reuses worker.py, which covers the happy path
  of the horizontal resharding code.
  """
  KEYSPACE = 'test_keyspace'

  def test_successful_resharding(self):
    """Normal horizontal resharding, "happy path test"."""
    worker_proc, _, worker_rpc_port = utils.run_vtworker_bg(
        ['--cell', 'test_nj'], auto_log=True)
    vtworker_endpoint = 'localhost:' + str(worker_rpc_port)

    self._error = None
    # source_shard_list = '0'
    # dest_shard_list = '-80,80-'

    # uncomment this can test the workflow UI
    # utils.pause('Now is a good time to look at vtctld UI at: localhost:%s' % (utils.vtctld.port))

    vtctl_res = utils.run_vtctl(
        [
            'WorkflowCreate', 'horizontal_resharding',
            '-keyspace=test_keyspace',
            '-vtworkers=%s' % vtworker_endpoint
        ],
        auto_log=True)
    hw_uuid = re.match(r'^uuid: (.*)$', vtctl_res[0]).group(1)

    utils.pause('Now is a good time to look at vtctld UI at: '
                '%s, workflow uuid=%s' % (utils.vtctld.port, hw_uuid))

    vtctl_res = utils.run_vtctl(['WorkflowWait', hw_uuid])

    self.verify()
    utils.kill_sub_process(worker_proc, soft=True)

  def verify(self):
    self.assert_shard_data_equal(0, worker.shard_master,
                                 worker.shard_0_tablets.replica)
    self.assert_shard_data_equal(1, worker.shard_master,
                                 worker.shard_1_tablets.replica)

    # Verify effect of MigrateServedTypes. Dest shards are serving now.
    utils.check_srv_keyspace('test_nj', self.KEYSPACE,
                             'Partitions(master): -80 80-\n'
                             'Partitions(rdonly): -80 80-\n'
                             'Partitions(replica): -80 80-\n')

    utils.run_vtctl(
        ['RunHealthCheck', worker.shard_rdonly1.tablet_alias], auto_log=True)

    # source shard: query service must be disabled after MigrateServedTypes.
    utils.check_tablet_query_service(
        self, worker.shard_rdonly1, serving=False, tablet_control_disabled=True)
    utils.check_tablet_query_service(
        self, worker.shard_replica, serving=False, tablet_control_disabled=True)
    utils.check_tablet_query_service(
        self, worker.shard_master, serving=False, tablet_control_disabled=True)

    # dest shard -80: query service must be disabled after MigrateServedTypes.
    # Run explicit healthcheck because 'rdonly' tablet may still be 'spare'.
    utils.run_vtctl(
        ['RunHealthCheck', worker.shard_0_rdonly1.tablet_alias], auto_log=True)
    utils.check_tablet_query_service(
        self,
        worker.shard_0_rdonly1,
        serving=True,
        tablet_control_disabled=False)
    utils.check_tablet_query_service(
        self,
        worker.shard_0_replica,
        serving=True,
        tablet_control_disabled=False)
    utils.check_tablet_query_service(
        self,
        worker.shard_0_master,
        serving=True,
        tablet_control_disabled=False)

    # dest shard 80-: query service must be disabled after MigrateServedTypes.
    # Run explicit healthcheck because 'rdonly' tablet is still 'spare'.
    utils.run_vtctl(
        ['RunHealthCheck', worker.shard_1_rdonly1.tablet_alias], auto_log=True)
    utils.check_tablet_query_service(
        self,
        worker.shard_1_rdonly1,
        serving=True,
        tablet_control_disabled=False)
    utils.check_tablet_query_service(
        self,
        worker.shard_1_replica,
        serving=True,
        tablet_control_disabled=False)
    utils.check_tablet_query_service(
        self,
        worker.shard_1_master,
        serving=True,
        tablet_control_disabled=False)


if __name__ == '__main__':
  utils.main(test_options=worker.add_test_options)
