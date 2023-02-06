/*
Copyright 2022 The Vitess Authors.

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

package logic

import (
	"testing"

	"github.com/openark/golib/sqlutils"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/inst"
)

// TestTopologyRecovery tests various operations related to topology recovery like reading from and writing it to the database.
func TestTopologyRecovery(t *testing.T) {
	// Open the vtorc
	// After the test completes delete everything from the vitess_tablet table
	orcDb, err := db.OpenVTOrc()
	require.NoError(t, err)
	defer func() {
		_, err = orcDb.Exec("delete from topology_recovery")
		require.NoError(t, err)
	}()

	replicationAnalysis := inst.ReplicationAnalysis{
		AnalyzedInstanceKey: inst.InstanceKey{
			Hostname: hostname,
			Port:     101,
		},
		TabletType: tab101.Type,
		ClusterDetails: inst.ClusterInfo{
			Keyspace: keyspace,
			Shard:    shard,
		},
		AnalyzedKeyspace: keyspace,
		AnalyzedShard:    shard,
		Analysis:         inst.ReplicaIsWritable,
		IsReadOnly:       false,
	}
	topologyRecovery := NewTopologyRecovery(replicationAnalysis)

	t.Run("writing to topology recovery", func(t *testing.T) {
		topologyRecovery, err = writeTopologyRecovery(topologyRecovery)
		require.NoError(t, err)
		// The ID field should be populated after the insert
		require.Greater(t, topologyRecovery.ID, int64(0))
	})

	t.Run("read recoveries", func(t *testing.T) {
		recoveries, err := ReadRecentRecoveries(false, 0)
		require.NoError(t, err)
		require.Len(t, recoveries, 1)
		// Assert that the ID field matches the one that we just wrote
		require.EqualValues(t, topologyRecovery.ID, recoveries[0].ID)
	})
}

// TestBlockedRecoveryInsertion tests that we are able to insert into the blocked_recovery table.
func TestBlockedRecoveryInsertion(t *testing.T) {
	orcDb, err := db.OpenVTOrc()
	require.NoError(t, err)
	defer func() {
		_, err = orcDb.Exec("delete from blocked_topology_recovery")
		require.NoError(t, err)
	}()

	analysisEntry := &inst.ReplicationAnalysis{
		AnalyzedInstanceKey: inst.InstanceKey{
			Hostname: "localhost",
			Port:     100,
		},
		ClusterDetails: inst.ClusterInfo{
			Keyspace: "ks",
			Shard:    "0",
		},
		Analysis: inst.DeadPrimaryAndSomeReplicas,
	}
	blockedRecovery := &TopologyRecovery{
		ID: 1,
	}
	err = RegisterBlockedRecoveries(analysisEntry, []*TopologyRecovery{blockedRecovery})
	require.NoError(t, err)

	totalBlockedRecoveries := 0
	err = db.QueryVTOrc("select count(*) as blocked_recoveries from blocked_topology_recovery", nil, func(rowMap sqlutils.RowMap) error {
		totalBlockedRecoveries = rowMap.GetInt("blocked_recoveries")
		return nil
	})
	require.NoError(t, err)
	// There should be 1 blocked recovery after insertion
	require.Equal(t, 1, totalBlockedRecoveries)
}
