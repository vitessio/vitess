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

package api

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/vtorc/utils"
)

// make an api call to /api/problems endpoint
// and verify the output
func TestProblemsAPI(t *testing.T) {
	defer cluster.PanicHandler(t)
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 2, 0, nil, cluster.VTOrcConfiguration{
		PreventCrossDataCenterPrimaryFailover: true,
		RecoveryPeriodBlockSeconds:            5,
	}, 1, "")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	vtorc := clusterInfo.ClusterInstance.VTOrcProcesses[0]

	// find primary from topo
	primary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, primary, "should have elected a primary")

	// find the replica tablet
	var replica *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two tablets, so the one not the primary must be the replica
		if tablet.Alias != primary.Alias {
			replica = tablet
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")

	// check that the replication is setup correctly before we set read-only
	utils.CheckReplication(t, clusterInfo, primary, []*cluster.Vttablet{replica}, 10*time.Second)

	// Check that initially there are no problems and the api endpoint returns null
	status, resp := utils.MakeAPICall(t, vtorc, "/api/problems")
	assert.Equal(t, 200, status)
	assert.Equal(t, "null", resp)

	// insert an errant GTID in the replica
	_, err := utils.RunSQL(t, "insert into vt_insert_test(id, msg) values (10173, 'test 178342')", replica, "vt_ks")
	require.NoError(t, err)

	// Wait until VTOrc picks up on this errant GTID and verify
	// that we see a not null result on the api/problems page
	// and the replica instance is marked as one of the problems
	status, resp = utils.MakeAPICallRetry(t, vtorc, "/api/problems", func(_ int, response string) bool {
		return response == "null"
	})
	assert.Equal(t, 200, status, resp)
	assert.Contains(t, resp, fmt.Sprintf(`"InstanceAlias": "%v"`, replica.Alias))

	// Check that filtering using keyspace and shard works
	status, resp = utils.MakeAPICall(t, vtorc, "/api/problems?keyspace=ks&shard=0")
	assert.Equal(t, 200, status, resp)
	assert.Contains(t, resp, fmt.Sprintf(`"InstanceAlias": "%v"`, replica.Alias))

	// Check that filtering using keyspace and shard works
	status, resp = utils.MakeAPICall(t, vtorc, "/api/problems?keyspace=ks&shard=80-")
	assert.Equal(t, 200, status, resp)
	assert.Equal(t, "null", resp)
}
