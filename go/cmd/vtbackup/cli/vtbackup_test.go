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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/mysqlctl"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func TestCatchUpReplicationForBackupRetargetsCycledSourceIP(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	topoServer := newVtbackupTestTopo(t, []*topodatapb.Tablet{
		newVtbackupTestTablet(100, topodatapb.TabletType_PRIMARY, "10.0.189.249", 3306),
	}, 100)

	mysqld := newStalledVtbackupTestMysqlDaemon(t, "10.0.189.245", 3306, "10.0.189.249", 3306)

	status, err := catchUpReplicationForBackup(ctx, topoServer, mysqld, vtbackupTestPosition(1), vtbackupTestPosition(2))
	require.NoError(t, err)
	require.True(t, status.Position.AtLeast(vtbackupTestPosition(2)))
}

func TestCatchUpReplicationForBackupRetargetsMissingSourceTablet(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	topoServer := newVtbackupTestTopo(t, []*topodatapb.Tablet{
		newVtbackupTestTablet(101, topodatapb.TabletType_PRIMARY, "new-primary", 3306),
	}, 101)

	mysqld := newStalledVtbackupTestMysqlDaemon(t, "old-primary", 3306, "new-primary", 3306)

	status, err := catchUpReplicationForBackup(ctx, topoServer, mysqld, vtbackupTestPosition(1), vtbackupTestPosition(2))
	require.NoError(t, err)
	require.True(t, status.Position.AtLeast(vtbackupTestPosition(2)))
}

func newVtbackupTestTopo(t *testing.T, tablets []*topodatapb.Tablet, primaryUID uint32) *topo.Server {
	oldKeyspace, oldShard := initKeyspace, initShard
	initKeyspace, initShard = "ks", "0"
	t.Cleanup(func() {
		initKeyspace, initShard = oldKeyspace, oldShard
	})

	ctx := t.Context()
	topoServer := memorytopo.NewServer(ctx, "zone1")
	require.NoError(t, topoServer.CreateKeyspace(ctx, initKeyspace, &topodatapb.Keyspace{}))
	require.NoError(t, topoServer.CreateShard(ctx, initKeyspace, initShard))
	for _, tablet := range tablets {
		require.NoError(t, topoServer.CreateTablet(ctx, tablet))
	}

	_, err := topoServer.UpdateShardFields(ctx, initKeyspace, initShard, func(si *topo.ShardInfo) error {
		si.PrimaryAlias = &topodatapb.TabletAlias{Cell: "zone1", Uid: primaryUID}
		return nil
	})
	require.NoError(t, err)

	return topoServer
}

func newVtbackupTestTablet(uid uint32, tabletType topodatapb.TabletType, mysqlHostname string, mysqlPort int32) *topodatapb.Tablet {
	return &topodatapb.Tablet{
		Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: uid},
		Keyspace:      "ks",
		Shard:         "0",
		Type:          tabletType,
		MysqlHostname: mysqlHostname,
		MysqlPort:     mysqlPort,
	}
}

func newStalledVtbackupTestMysqlDaemon(t *testing.T, currentHost string, currentPort int32, wantHost string, wantPort int32) *mysqlctl.FakeMysqlDaemon {
	restorePos := vtbackupTestPosition(1)
	primaryPos := vtbackupTestPosition(2)

	mysqld := &mysqlctl.FakeMysqlDaemon{
		Replicating:                   true,
		IOThreadRunning:               true,
		CurrentPrimaryPosition:        restorePos,
		CurrentSourceHost:             currentHost,
		CurrentSourcePort:             currentPort,
		ExpectedExecuteSuperQueryList: []string{"STOP REPLICA"},
	}
	mysqld.SetReplicationSourceFunc = func(_ context.Context, host string, port int32, heartbeatInterval float64, stopReplicationBefore bool, startReplicationAfter bool) error {
		require.Equal(t, wantHost, host)
		require.Equal(t, wantPort, port)
		require.True(t, stopReplicationBefore)
		require.True(t, startReplicationAfter)

		mysqld.CurrentSourceHost = host
		mysqld.CurrentSourcePort = port
		mysqld.CurrentPrimaryPosition = primaryPos
		return nil
	}

	return mysqld
}

func vtbackupTestPosition(sequence int) replication.Position {
	if sequence == 1 {
		return replication.MustParsePosition(replication.Mysql56FlavorID, "16b1039f-22b6-11ed-b765-0a43f95f28a3:1")
	}
	return replication.MustParsePosition(replication.Mysql56FlavorID, fmt.Sprintf("16b1039f-22b6-11ed-b765-0a43f95f28a3:1-%d", sequence))
}
