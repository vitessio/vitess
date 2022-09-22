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

package reparent

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/tabletgateway/buffer"
	"vitess.io/vitess/go/vt/log"
)

const (
	demoteQuery                = "SET GLOBAL read_only = ON;FLUSH TABLES WITH READ LOCK;UNLOCK TABLES;"
	disableSemiSyncSourceQuery = "SET GLOBAL rpl_semi_sync_master_enabled = 0"
	enableSemiSyncSourceQuery  = "SET GLOBAL rpl_semi_sync_master_enabled = 1"
	promoteQuery               = "STOP SLAVE;RESET SLAVE ALL;SET GLOBAL read_only = OFF;"

	hostname = "localhost"
)

func failoverExternalReparenting(t *testing.T, clusterInstance *cluster.LocalProcessCluster, keyspaceUnshardedName string, reads, writes buffer.QueryEngine) {
	// Execute the failover.
	reads.ExpectQueries(10)
	writes.ExpectQueries(10)

	start := time.Now()

	// Demote Query
	primary := clusterInstance.Keyspaces[0].Shards[0].Vttablets[0]
	replica := clusterInstance.Keyspaces[0].Shards[0].Vttablets[1]
	oldPrimary := primary
	newPrimary := replica
	primary.VttabletProcess.QueryTablet(demoteQuery, keyspaceUnshardedName, true)
	if primary.VttabletProcess.EnableSemiSync {
		primary.VttabletProcess.QueryTablet(disableSemiSyncSourceQuery, keyspaceUnshardedName, true)
	}

	// Wait for replica to catch up to primary.
	cluster.WaitForReplicationPos(t, primary, replica, "localhost", 60.0)

	duration := time.Since(start)
	minUnavailabilityInS := 1.0
	if duration.Seconds() < minUnavailabilityInS {
		w := minUnavailabilityInS - duration.Seconds()
		log.Infof("Waiting for %.1f seconds because the failover was too fast (took only %.3f seconds)", w, duration.Seconds())
		time.Sleep(time.Duration(w) * time.Second)
	}

	// Promote replica to new primary.
	replica.VttabletProcess.QueryTablet(promoteQuery, keyspaceUnshardedName, true)

	if replica.VttabletProcess.EnableSemiSync {
		replica.VttabletProcess.QueryTablet(enableSemiSyncSourceQuery, keyspaceUnshardedName, true)
	}

	// Configure old primary to replicate from new primary.

	_, gtID := cluster.GetPrimaryPosition(t, *newPrimary, hostname)

	// Use 'localhost' as hostname because Travis CI worker hostnames
	// are too long for MySQL replication.
	changeSourceCommands := fmt.Sprintf("RESET SLAVE;SET GLOBAL gtid_slave_pos = '%s';CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d ,MASTER_USER='vt_repl', MASTER_USE_GTID = slave_pos;START SLAVE;", gtID, "localhost", newPrimary.MySQLPort)
	oldPrimary.VttabletProcess.QueryTablet(changeSourceCommands, keyspaceUnshardedName, true)

	// Notify the new vttablet primary about the reparent.
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("TabletExternallyReparented", newPrimary.Alias)
	require.NoError(t, err)
}

func failoverPlannedReparenting(t *testing.T, clusterInstance *cluster.LocalProcessCluster, keyspaceUnshardedName string, reads, writes buffer.QueryEngine) {
	// Execute the failover.
	reads.ExpectQueries(10)
	writes.ExpectQueries(10)

	err := clusterInstance.VtctlclientProcess.ExecuteCommand("PlannedReparentShard", "--", "--keyspace_shard",
		fmt.Sprintf("%s/%s", keyspaceUnshardedName, "0"),
		"--new_primary", clusterInstance.Keyspaces[0].Shards[0].Vttablets[1].Alias)
	require.NoError(t, err)
}

func assertFailover(t *testing.T, shard string, stats *buffer.VTGateBufferingStats) {
	stopLabel := fmt.Sprintf("%s.%s", shard, "NewPrimarySeen")

	assert.Greater(t, stats.BufferFailoverDurationSumMs[shard], 0)
	assert.Greater(t, stats.BufferRequestsBuffered[shard], 0)
	assert.Greater(t, stats.BufferStops[stopLabel], 0)

	// Number of buffering stops must be equal to the number of seen failovers.
	assert.Equal(t, stats.HealthcheckPrimaryPromoted[shard], stats.BufferStops[stopLabel])
}

func TestBufferReparenting(t *testing.T) {
	t.Run("TER without reserved connection", func(t *testing.T) {
		bt := &buffer.BufferingTest{
			Assert:      assertFailover,
			Failover:    failoverExternalReparenting,
			ReserveConn: false,
		}
		bt.Test(t)
	})
	t.Run("TER with reserved connection", func(t *testing.T) {
		bt := &buffer.BufferingTest{
			Assert:      assertFailover,
			Failover:    failoverExternalReparenting,
			ReserveConn: true,
		}
		bt.Test(t)
	})
	t.Run("PRS without reserved connections", func(t *testing.T) {
		bt := &buffer.BufferingTest{
			Assert:      assertFailover,
			Failover:    failoverPlannedReparenting,
			ReserveConn: false,
		}
		bt.Test(t)
	})
	t.Run("PRS with reserved connections", func(t *testing.T) {
		bt := &buffer.BufferingTest{
			Assert:      assertFailover,
			Failover:    failoverPlannedReparenting,
			ReserveConn: true,
		}
		bt.Test(t)
	})
}
