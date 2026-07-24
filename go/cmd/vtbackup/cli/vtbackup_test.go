/*
Copyright 2026 The Vitess Authors.

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

package cli

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func TestFindErrantGTIDs(t *testing.T) {
	const (
		primaryUUID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
		errantUUID  = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	)

	// Decode each candidate and primary replication position.
	mysqlPosition := func(value string) replication.Position {
		t.Helper()

		position, err := replication.DecodePosition("MySQL56/" + value)
		require.NoError(t, err)
		return position
	}

	tests := []struct {
		name            string
		candidate       replication.Position
		primary         replication.Position
		wantErrantGTIDs string
	}{
		{
			// A backup behind the primary contains no extra transactions.
			name:      "candidate is not errant",
			candidate: mysqlPosition(primaryUUID + ":1-10"),
			primary:   mysqlPosition(primaryUUID + ":1-20"),
		},
		{
			// An empty position contains no extra transactions.
			name:      "empty candidate",
			candidate: replication.Position{},
			primary:   mysqlPosition(primaryUUID + ":1-20"),
		},
		{
			// Transactions ahead of the primary are errant even when they use the primary's UUID.
			name:            "candidate is ahead under the primary UUID",
			candidate:       mysqlPosition(primaryUUID + ":1-20"),
			primary:         mysqlPosition(primaryUUID + ":1-10"),
			wantErrantGTIDs: primaryUUID + ":11-20",
		},
		{
			// Unsupported replication position types are not compared.
			name:      "non MySQL position",
			candidate: testCatchupPosition(10),
			primary:   testCatchupPosition(20),
		},
		{
			// Transactions from another server are returned as the errant difference.
			name:            "candidate has errant GTIDs",
			candidate:       mysqlPosition(primaryUUID + ":1-10," + errantUUID + ":1"),
			primary:         mysqlPosition(primaryUUID + ":1-20"),
			wantErrantGTIDs: errantUUID + ":1",
		},
	}

	// Compare each candidate with the primary and verify the exact difference.
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errantGTIDs := findErrantGTIDs(tt.candidate, tt.primary)
			assert.Equal(t, tt.wantErrantGTIDs, errantGTIDs.String())
		})
	}
}

func TestVerifyNoErrantGTIDsInBaseBackup(t *testing.T) {
	const (
		primaryUUID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
		errantUUID  = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
		backupName  = "2026-07-23.120000.zone1-0000000101"
	)

	mysqlPosition := func(value string) replication.Position {
		t.Helper()

		position, err := replication.DecodePosition("MySQL56/" + value)
		require.NoError(t, err)
		return position
	}

	// Give the restored backup one transaction that is absent from the primary.
	restoredPosition := mysqlPosition(primaryUUID + ":1-10," + errantUUID + ":1")
	primaryPosition := mysqlPosition(primaryUUID + ":1-20")

	// Validate that vtbackup returns a correct error that the base backup has errant GTIDs.
	err := verifyNoErrantGTIDsInBaseBackup(restoreInfo{position: restoredPosition, backupName: backupName}, primaryPosition)
	require.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
	require.EqualError(t, err, `base backup "`+backupName+`" has errant GTIDs "`+errantUUID+`:1" relative to current primary`)
}

func TestCatchUpReplicationForBackupClearsLastErrWhenReplicationBecomesHealthy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()

		oldInitKeyspace := initKeyspace
		oldInitShard := initShard
		initKeyspace = "test_keyspace"
		initShard = "0"
		t.Cleanup(func() {
			initKeyspace = oldInitKeyspace
			initShard = oldInitShard
		})

		ts := memorytopo.NewServer(ctx, "zone1")
		t.Cleanup(ts.Close)
		require.NoError(t, ts.CreateKeyspace(ctx, initKeyspace, &topodatapb.Keyspace{}))
		require.NoError(t, ts.CreateShard(ctx, initKeyspace, initShard))
		primaryAlias := &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}
		require.NoError(t, ts.CreateTablet(ctx, &topodatapb.Tablet{
			Alias:         primaryAlias,
			Keyspace:      initKeyspace,
			Shard:         initShard,
			Hostname:      "primary.test",
			MysqlHostname: "primary-mysql.test",
			MysqlPort:     3306,
			Type:          topodatapb.TabletType_PRIMARY,
		}))
		_, err := ts.UpdateShardFields(ctx, initKeyspace, initShard, func(si *topo.ShardInfo) error {
			si.PrimaryAlias = primaryAlias
			return nil
		})
		require.NoError(t, err)

		restorePos := testCatchupPosition(1)
		primaryPos := testCatchupPosition(3)
		statuses := []replication.ReplicationStatus{
			{
				Position:    restorePos,
				IOState:     replication.ReplicationStateConnecting,
				LastIOError: "Replica I/O for channel '': Error reconnecting to source 'vt_test@192.0.2.10:3306'. This was attempt 1/300, with a delay of 10 seconds between attempts. Message: Can't connect to MySQL server on '192.0.2.10:3306' (111), Error_code: MY-002003",
				SQLState:    replication.ReplicationStateRunning,
			},
		}
		for range int(timeoutWaitingForReplicationStatus.Seconds()) + 1 {
			statuses = append(statuses, replication.ReplicationStatus{
				Position: restorePos,
				IOState:  replication.ReplicationStateRunning,
				SQLState: replication.ReplicationStateRunning,
			})
		}
		statuses = append(
			statuses,
			replication.ReplicationStatus{
				Position: primaryPos,
				IOState:  replication.ReplicationStateRunning,
				SQLState: replication.ReplicationStateRunning,
			},
			replication.ReplicationStatus{
				Position: primaryPos,
				IOState:  replication.ReplicationStateRunning,
				SQLState: replication.ReplicationStateRunning,
			},
		)
		mysqld := &catchupReplicationMysqlDaemon{
			statuses: statuses,
		}

		status, err := catchUpReplicationForBackup(ctx, ts, mysqld, restorePos, primaryPos)

		require.NoError(t, err)
		assert.True(t, status.Position.Equal(primaryPos))
		assert.Equal(t, 1, mysqld.setReplicationSourceCalls)
		assert.Equal(t, 1, mysqld.stopReplicationCalls)
	})
}

type catchupReplicationMysqlDaemon struct {
	mysqlctl.MysqlDaemon

	statuses                  []replication.ReplicationStatus
	statusCalls               int
	setReplicationSourceCalls int
	stopReplicationCalls      int
}

func (m *catchupReplicationMysqlDaemon) ReplicationStatus(ctx context.Context) (replication.ReplicationStatus, error) {
	status := m.statuses[m.statusCalls]
	m.statusCalls++
	return status, nil
}

func (m *catchupReplicationMysqlDaemon) SetReplicationSource(ctx context.Context, host string, port int32, heartbeatInterval float64, stopReplicationBefore bool, startReplicationAfter bool) error {
	m.setReplicationSourceCalls++
	return nil
}

func (m *catchupReplicationMysqlDaemon) StopReplication(ctx context.Context, hookExtraEnv map[string]string) error {
	m.stopReplicationCalls++
	return nil
}

func testCatchupPosition(pos uint64) replication.Position {
	return replication.Position{GTIDSet: replication.FilePosGTID{File: "source-bin.000001", Pos: pos}}
}
