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

package grpcvtctldserver

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vtenv"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
	"vitess.io/vitess/go/vt/proto/vttime"
)

// TestDeleteTabletsPrimaryMismatchLogs checks the warning logged when a
// deleted primary isn't the shard's primary: the shard's primary is logged
// only when the shard has one.
func TestDeleteTabletsPrimaryMismatchLogs(t *testing.T) {
	const msg = "Deleting primary from shard but primary in Shard object was different"
	tests := []struct {
		name        string
		shardHas    *topodatapb.TabletAlias
		wantPrimary any
	}{
		{name: "no shard primary"},
		{
			name:        "other shard primary",
			shardHas:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
			wantPrimary: "zone1-0000000101",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			ts := memorytopo.NewServer(ctx, "zone1")
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(vtenv.NewTestEnv(), ts)
			})
			alias := &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}
			testutil.AddTablets(ctx, t, ts, nil, &topodatapb.Tablet{
				Alias:    alias,
				Type:     topodatapb.TabletType_PRIMARY,
				Keyspace: "testkeyspace",
				Shard:    "-",
				// Later than the shard's, so the tablet counts as the primary.
				PrimaryTermStartTime: &vttime.Time{Seconds: 100},
			})
			if tt.shardHas != nil {
				_, err := ts.UpdateShardFields(ctx, "testkeyspace", "-", func(si *topo.ShardInfo) error {
					si.PrimaryAlias = tt.shardHas
					return nil
				})
				require.NoError(t, err)
			}
			logs := logtest.Capture(t, slog.LevelInfo)

			_, err := vtctld.DeleteTablets(ctx, &vtctldatapb.DeleteTabletsRequest{
				TabletAliases: []*topodatapb.TabletAlias{alias},
				AllowPrimary:  true,
			})
			require.NoError(t, err)

			record := logs.Record(t, msg)
			assert.Equal(t, "WARN", record["level"])
			assert.Equal(t, "zone1-0000000100", record["tablet"])
			assert.Equal(t, "testkeyspace", record["keyspace"])
			assert.Equal(t, "-", record["shard"])
			assert.Equal(t, tt.wantPrimary, record["primary_tablet"])
			assert.NotContains(t, logs.String(), "<nil>")
		})
	}
}

// TestRefreshStateByShardLogsEvents checks that the refresh's progress
// messages are logged under message, at their own level, with the shard.
func TestRefreshStateByShardLogsEvents(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "zone1")
	tmc := &testutil.TabletManagerClient{
		RefreshStateResults: map[string]error{
			"zone1-0000000100": nil,
			"zone1-0000000101": errors.New("refresh failed"),
		},
	}
	testutil.AddTablets(ctx, t, ts, nil,
		&topodatapb.Tablet{Hostname: "zone1-100", Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}, Keyspace: "ks", Shard: "-"},
		&topodatapb.Tablet{Hostname: "zone1-101", Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101}, Keyspace: "ks", Shard: "-"},
	)
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(vtenv.NewTestEnv(), ts)
	})
	logs := logtest.Capture(t, slog.LevelInfo)

	_, err := vtctld.RefreshStateByShard(ctx, &vtctldatapb.RefreshStateByShardRequest{Keyspace: "ks", Shard: "-"})
	require.NoError(t, err)

	levels := make(map[string]any)
	for _, record := range logs.Records(t) {
		if record["msg"] != "RefreshTabletsByShard event" {
			continue
		}
		assert.Equal(t, "ks", record["keyspace"])
		assert.Equal(t, "-", record["shard"])
		message, _ := record["message"].(string)
		levels[message] = record["level"]
	}
	assert.Equal(t, "INFO", levels["RefreshTabletsByShard called on shard ks/-"], levels)
	assert.Equal(t, "WARN", levels["RefreshTabletsByShard: failed to refresh zone1-0000000101: refresh failed"], levels)
}

// TestShardReplicationPositionsLogsTablets checks that the tablets whose
// replication status is gathered are logged as a list of aliases.
func TestShardReplicationPositionsLogsTablets(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "zone1")
	tmc := &testutil.TabletManagerClient{
		PrimaryPositionResults: map[string]struct {
			Position string
			Error    error
		}{
			"zone1-0000000100": {Position: "primary_tablet_position"},
		},
		ReplicationStatusResults: map[string]struct {
			Position *replicationdatapb.Status
			Error    error
		}{
			"zone1-0000000101": {Position: &replicationdatapb.Status{Position: "replica_tablet_position"}},
			"zone1-0000000102": {Position: &replicationdatapb.Status{Position: "replica_tablet_position"}},
			"zone1-0000000103": {Position: &replicationdatapb.Status{Position: "replica_tablet_position"}},
		},
	}
	testutil.AddTablets(ctx, t, ts, &testutil.AddTabletOptions{AlsoSetShardPrimary: true},
		&topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 103}, Keyspace: "testkeyspace", Shard: "-", Type: topodatapb.TabletType_REPLICA},
		&topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101}, Keyspace: "testkeyspace", Shard: "-", Type: topodatapb.TabletType_REPLICA},
		&topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 102}, Keyspace: "testkeyspace", Shard: "-", Type: topodatapb.TabletType_REPLICA},
		&topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}, Keyspace: "testkeyspace", Shard: "-", Type: topodatapb.TabletType_PRIMARY},
	)
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(vtenv.NewTestEnv(), ts)
	})
	logs := logtest.Capture(t, slog.LevelInfo)

	_, err := vtctld.ShardReplicationPositions(ctx, &vtctldatapb.ShardReplicationPositionsRequest{Keyspace: "testkeyspace", Shard: "-"})
	require.NoError(t, err)

	record := logs.Record(t, "Gathering tablet replication status")
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, []any{"zone1-0000000100", "zone1-0000000101", "zone1-0000000102", "zone1-0000000103"}, record["tablets"])

	// A shard with no tablets logs an empty list, not null.
	require.NoError(t, ts.CreateKeyspace(ctx, "emptyks", &topodatapb.Keyspace{}))
	require.NoError(t, ts.CreateShard(ctx, "emptyks", "-"))
	logs.Reset()
	_, err = vtctld.ShardReplicationPositions(ctx, &vtctldatapb.ShardReplicationPositionsRequest{Keyspace: "emptyks", Shard: "-"})
	require.NoError(t, err)

	record = logs.Record(t, "Gathering tablet replication status")
	assert.Equal(t, []any{}, record["tablets"])
}
