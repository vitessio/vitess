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
"""End-to-end test for horizontal resharding workflow."""

import re
# "unittest" is used indirectly by importing "worker", but pylint does
# not grasp this.
# Import it explicitly to make pylint happy and stop it complaining about
# setUpModule, tearDownModule and the missing module docstring.
import unittest  # pylint: disable=unused-import


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


class TestHorizontalReshardingWorkflow(worker.TestBaseSplitClone):
  """End-to-end test for horizontal resharding workflow.

  This test reuses worker.py, which sets up the environment.
  """
  KEYSPACE = 'test_keyspace'

  def test_successful_resharding(self):
    """Reshard from 1 to 2 shards by running the workflow."""
    worker_proc, _, worker_rpc_port = utils.run_vtworker_bg(
        ['--cell', 'test_nj'], auto_log=True)
    vtworker_endpoint = 'localhost:%d' % worker_rpc_port

    stdout = utils.run_vtctl(['WorkflowCreate', 'horizontal_resharding',
                              '-keyspace=test_keyspace',
                              '-vtworkers=%s' % vtworker_endpoint,
                              '-enable_approvals=false'],
                             auto_log=True)
    workflow_uuid = re.match(r'^uuid: (.*)$', stdout[0]).group(1)

    utils.pause('Now is a good time to look at vtctld UI at: '
                '%s, workflow uuid=%s' % (utils.vtctld.port, workflow_uuid))

    utils.run_vtctl(['WorkflowWait', workflow_uuid])

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

    # source shard: query service must be disabled after MigrateServedTypes.
    source_shards = [worker.shard_rdonly1,
                     worker.shard_replica,
                     worker.shard_master]
    for shard in source_shards:
      utils.check_tablet_query_service(
          self, shard, serving=False, tablet_control_disabled=True)

    # dest shard -80, 80-: query service must be enabled
    # after MigrateServedTypes.
    dest_shards = [worker.shard_0_rdonly1,
                   worker.shard_0_replica,
                   worker.shard_0_master,
                   worker.shard_1_rdonly1,
                   worker.shard_1_replica,
                   worker.shard_1_master]
    for shard in dest_shards:
      utils.check_tablet_query_service(
          self,
          shard,
          serving=True,
          tablet_control_disabled=False)

if __name__ == '__main__':
  utils.main(test_options=worker.add_test_options)
