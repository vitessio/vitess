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

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

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
