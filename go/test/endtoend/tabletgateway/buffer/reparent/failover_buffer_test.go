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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/log"
)

var (
	demoteQueries  = []string{"SET GLOBAL read_only = ON", "FLUSH TABLES WITH READ LOCK", "UNLOCK TABLES"}
	promoteQueries = []string{"STOP REPLICA", "RESET REPLICA ALL", "SET GLOBAL read_only = OFF"}
)

// queryTabletMultiple runs a sequence of statements on a single connection to
// the tablet's mysqld, against the tablet's vt_<keyspace> database.
func queryTabletMultiple(ctx context.Context, tablet *vitesst.Tablet, keyspace string, queries []string) error {
	conn, err := vitesst.GetMySQLConn(ctx, tablet, "vt_"+keyspace)
	if err != nil {
		return err
	}
	defer conn.Close()

	for _, query := range queries {
		log.Info(fmt.Sprintf("Executing query %s (on %s)", query, tablet.Alias()))
		if _, err := conn.ExecuteFetch(query, 1000, true); err != nil {
			return err
		}
	}
	return nil
}

// primaryPosition returns the tablet's current executed replication position.
func primaryPosition(t *testing.T, tablet *vitesst.Tablet) replication.Position {
	conn, err := vitesst.GetMySQLConn(t.Context(), tablet, "")
	require.NoError(t, err)
	defer conn.Close()

	pos, err := conn.PrimaryPosition()
	require.NoError(t, err)
	return pos
}

// resetBinaryLogsCommand returns the version-appropriate command to reset the
// tablet's binary logs.
func resetBinaryLogsCommand(t *testing.T, tablet *vitesst.Tablet) string {
	conn, err := vitesst.GetMySQLConn(t.Context(), tablet, "")
	require.NoError(t, err)
	defer conn.Close()

	return conn.ResetBinaryLogsCommand()
}

// waitForReplicationPos waits for tabletB's replication position to catch up to
// where tabletA is now.
func waitForReplicationPos(t *testing.T, tabletA, tabletB *vitesst.Tablet, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	replicationPosA := primaryPosition(t, tabletA)
	for {
		replicationPosB := primaryPosition(t, tabletB)
		if replicationPosB.AtLeast(replicationPosA) {
			return
		}
		msg := fmt.Sprintf("%s's replication position to catch up to %s's;currently at: %s, waiting to catch up to: %s", tabletB.Alias(), tabletA.Alias(), replicationPosB, replicationPosA)
		select {
		case <-ctx.Done():
			assert.FailNowf(t, "Timeout waiting for condition '%s'", msg)
			return
		case <-ticker.C:
		}
	}
}

func failoverExternalReparenting(t *testing.T, clusterInstance *vitesst.Cluster, keyspaceUnshardedName string, reads, writes QueryEngine) {
	ctx := t.Context()

	// Execute the failover.
	reads.ExpectQueries(10)
	writes.ExpectQueries(10)

	start := time.Now()

	// Demote Query
	shard := clusterInstance.Keyspace(keyspaceUnshardedName).Shard("0")
	primary := shard.Primary()
	replica := shard.Replicas()[0]
	oldPrimary := primary
	newPrimary := replica
	err := queryTabletMultiple(ctx, primary, keyspaceUnshardedName, demoteQueries)
	require.NoError(t, err)

	// Wait for replica to catch up to primary.
	waitForReplicationPos(t, primary, replica, time.Minute)

	duration := time.Since(start)
	minUnavailabilityInS := 1.0
	if duration.Seconds() < minUnavailabilityInS {
		w := minUnavailabilityInS - duration.Seconds()
		log.Info(fmt.Sprintf("Waiting for %.1f seconds because the failover was too fast (took only %.3f seconds)", w, duration.Seconds()))
		time.Sleep(time.Duration(w) * time.Second)
	}

	// Promote replica to new primary.
	err = queryTabletMultiple(ctx, replica, keyspaceUnshardedName, promoteQueries)
	require.NoError(t, err)

	// Configure old primary to replicate from new primary.

	gtID := primaryPosition(t, newPrimary).String()

	resetCmd := resetBinaryLogsCommand(t, oldPrimary)
	changeSourceCommands := []string{
		"STOP REPLICA",
		resetCmd,
		fmt.Sprintf("SET GLOBAL gtid_purged = '%s'", gtID),
		fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_HOST='%s', SOURCE_PORT=%d, SOURCE_USER='vt_repl', GET_SOURCE_PUBLIC_KEY = 1, SOURCE_AUTO_POSITION = 1", newPrimary.Name(), tabletMySQLPort),
		"START REPLICA",
	}
	err = queryTabletMultiple(ctx, oldPrimary, keyspaceUnshardedName, changeSourceCommands)
	require.NoError(t, err)

	// Notify the new vttablet primary about the reparent.
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "TabletExternallyReparented", newPrimary.Alias())
	require.NoError(t, err)
}

func failoverPlannedReparenting(t *testing.T, clusterInstance *vitesst.Cluster, keyspaceUnshardedName string, reads, writes QueryEngine) {
	// Execute the failover.
	reads.ExpectQueries(10)
	writes.ExpectQueries(10)

	err := clusterInstance.Vtctld().ExecuteCommand(t.Context(), "PlannedReparentShard",
		fmt.Sprintf("%s/%s", keyspaceUnshardedName, "0"),
		"--new-primary", clusterInstance.Keyspace(keyspaceUnshardedName).Shard("0").Replicas()[0].Alias())
	require.NoError(t, err)
}

func assertFailover(t *testing.T, shard string, stats *VTGateBufferingStats) {
	stopLabel := fmt.Sprintf("%s.%s", shard, "NewPrimarySeen")

	assert.Greater(t, stats.BufferFailoverDurationSumMs[shard], 0)
	assert.Greater(t, stats.BufferRequestsBuffered[shard], 0)
	assert.Greater(t, stats.BufferStops[stopLabel], 0)

	// Number of buffering stops must be equal to the number of seen failovers.
	assert.Equal(t, stats.HealthcheckPrimaryPromoted[shard], stats.BufferStops[stopLabel])
}

func TestBufferReparenting(t *testing.T) {
	t.Run("TER without reserved connection", func(t *testing.T) {
		bt := &BufferingTest{
			Assert:      assertFailover,
			Failover:    failoverExternalReparenting,
			ReserveConn: false,
		}
		bt.Test(t)
	})
	t.Run("TER with reserved connection", func(t *testing.T) {
		bt := &BufferingTest{
			Assert:      assertFailover,
			Failover:    failoverExternalReparenting,
			ReserveConn: true,
		}
		bt.Test(t)
	})
	t.Run("PRS without reserved connections", func(t *testing.T) {
		bt := &BufferingTest{
			Assert:      assertFailover,
			Failover:    failoverPlannedReparenting,
			ReserveConn: false,
		}
		bt.Test(t)
	})
	t.Run("PRS with reserved connections", func(t *testing.T) {
		bt := &BufferingTest{
			Assert:      assertFailover,
			Failover:    failoverPlannedReparenting,
			ReserveConn: true,
		}
		bt.Test(t)
	})
}
