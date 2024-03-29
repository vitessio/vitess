/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package reshard

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/tabletgateway/buffer"
	"vitess.io/vitess/go/vt/log"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

const (
	maxWait              = 10 * time.Second
	acceptableLagSeconds = 5
)

func waitForLowLag(t *testing.T, clusterInstance *cluster.LocalProcessCluster, keyspace, workflow string) {
	var lagSeconds int64
	waitDuration := 500 * time.Millisecond
	duration := maxWait
	for duration > 0 {
		output, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("Workflow", "--keyspace", keyspace, "show", "--workflow", workflow)
		require.NoError(t, err)

		var resp vtctldatapb.GetWorkflowsResponse
		err = json2.Unmarshal([]byte(output), &resp)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(resp.Workflows), 1, "responce should have at least one workflow")
		lagSeconds := resp.Workflows[0].MaxVReplicationTransactionLag

		require.NoError(t, err, output)
		if lagSeconds <= acceptableLagSeconds {
			log.Infof("waitForLowLag acceptable for workflow %s, keyspace %s, current lag is %d", workflow, keyspace, lagSeconds)
			break
		} else {
			log.Infof("waitForLowLag too high for workflow %s, keyspace %s, current lag is %d", workflow, keyspace, lagSeconds)
		}
		time.Sleep(waitDuration)
		duration -= waitDuration
	}

	if duration <= 0 {
		t.Fatalf("waitForLowLag timed out for workflow %s, keyspace %s, current lag is %d", workflow, keyspace, lagSeconds)
	}
}

func reshard02(t *testing.T, clusterInstance *cluster.LocalProcessCluster, keyspaceName string, reads, writes buffer.QueryEngine) {
	keyspace := &cluster.Keyspace{Name: keyspaceName}
	err := clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 1, false)
	require.NoError(t, err)
	workflowName := "buf2buf"

	err = clusterInstance.VtctldClientProcess.ExecuteCommand("Reshard", "Create", "--target-keyspace", keyspaceName, "--workflow", workflowName, "--source-shards", "0", "--target-shards", "-80,80-")
	require.NoError(t, err)

	// Execute the resharding operation
	reads.ExpectQueries(25)
	writes.ExpectQueries(25)

	waitForLowLag(t, clusterInstance, keyspaceName, workflowName)
	err = clusterInstance.VtctldClientProcess.ExecuteCommand("Reshard", "SwitchTraffic", "--target-keyspace", keyspaceName, "--workflow", workflowName, "--tablet-types=rdonly,replica")
	require.NoError(t, err)

	err = clusterInstance.VtctldClientProcess.ExecuteCommand("Reshard", "SwitchTraffic", "--target-keyspace", keyspaceName, "--workflow", workflowName, "--tablet-types=primary")
	require.NoError(t, err)

	err = clusterInstance.VtctldClientProcess.ExecuteCommand("Reshard", "--target-keyspace", keyspaceName, "--workflow", workflowName, "Complete")
	require.NoError(t, err)
}

const vschema = `{
   "sharded": true,
   "vindexes": {
     "hash_index": {
       "type": "hash"
     }
   },
   "tables": {
     "buffer": {
        "column_vindexes": [
         {
           "column": "id",
           "name": "hash_index"
         }
       ]
     }
   }
 }`

func assertResharding(t *testing.T, shard string, stats *buffer.VTGateBufferingStats) {
	stopLabel := fmt.Sprintf("%s.%s", shard, "ReshardingComplete")

	assert.Greater(t, stats.BufferFailoverDurationSumMs[shard], 0)
	assert.Greater(t, stats.BufferRequestsBuffered[shard], 0)
	assert.Greater(t, stats.BufferStops[stopLabel], 0)
}

func TestBufferResharding(t *testing.T) {
	t.Run("slow queries", func(t *testing.T) {
		bt := &buffer.BufferingTest{
			Assert:      assertResharding,
			Failover:    reshard02,
			SlowQueries: true,
			VSchema:     vschema,
		}
		bt.Test(t)
	})

	t.Run("fast queries", func(t *testing.T) {
		bt := &buffer.BufferingTest{
			Assert:      assertResharding,
			Failover:    reshard02,
			SlowQueries: false,
			VSchema:     vschema,
		}
		bt.Test(t)
	})
}
