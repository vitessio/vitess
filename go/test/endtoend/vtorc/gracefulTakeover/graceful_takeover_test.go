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

package gracefulTakeover

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/vtorc/utils"
)

// make an api call to graceful primary takeover and let vtorc fix it
// covers the test case graceful-master-takeover from orchestrator
func TestGracefulTakeover(t *testing.T) {
	defer cluster.PanicHandler(t)
	utils.SetupVttabletsAndVtorc(t, clusterInfo, 2, 0, nil, "test_config.json")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	// find primary from topo
	curPrimary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// find the replica tablet
	var replica *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two tablets, so the one not the primary must be the replica
		if tablet.Alias != curPrimary.Alias {
			replica = tablet
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")

	// check that the replication is setup correctly before we failover
	utils.CheckReplication(t, clusterInfo, curPrimary, []*cluster.Vttablet{replica}, 10*time.Second)

	response, err := http.Get(fmt.Sprintf("http://localhost:3000/api/graceful-primary-takeover/localhost/%d/localhost/%d", curPrimary.MySQLPort, replica.MySQLPort))
	require.NoError(t, err)
	assert.Equal(t, 200, response.StatusCode)

	// check that the replica gets promoted
	utils.CheckPrimaryTablet(t, clusterInfo, replica, true)
	utils.VerifyWritesSucceed(t, clusterInfo, replica, []*cluster.Vttablet{curPrimary}, 10*time.Second)
}
