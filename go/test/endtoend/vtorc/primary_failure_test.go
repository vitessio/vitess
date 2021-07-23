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

package vtorc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

// 2. bring down primary, let orc promote replica
func TestDownPrimary(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupVttabletsAndVtorc(t, 2, 0, nil)
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	// find primary from topo
	curPrimary := shardPrimaryTablet(t, clusterInstance, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// Make the current primary database unavailable.
	err := curPrimary.MysqlctlProcess.Stop()
	require.NoError(t, err)
	defer func() {
		// we remove the tablet from our global list since its mysqlctl process has stopped and cannot be reused for other tests
		for i, tablet := range replicaTablets {
			if tablet == curPrimary {
				// remove this tablet since its mysql has stopped
				replicaTablets = append(replicaTablets[:i], replicaTablets[i+1:]...)
				killTablets([]*cluster.Vttablet{curPrimary})
				return
			}
		}
	}()

	for _, tablet := range shard0.Vttablets {
		// we know we have only two tablets, so the "other" one must be the new primary
		if tablet.Alias != curPrimary.Alias {
			checkPrimaryTablet(t, clusterInstance, tablet)
			break
		}
	}
}
