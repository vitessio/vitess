/*
Copyright 2020 The Vitess Authors.

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
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"testing"
	"time"

	_flag "vitess.io/vitess/go/internal/flag"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	hk "vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vtctl/localvtctldclient"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/vttablet/tmclienttest"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	mysqlctlpb "vitess.io/vitess/go/vt/proto/mysqlctl"
	querypb "vitess.io/vitess/go/vt/proto/query"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
	"vitess.io/vitess/go/vt/proto/vttime"
)

func init() {
	backupstorage.BackupStorageImplementation = testutil.BackupStorageImplementation

	// For tests that don't actually care about mocking the tmclient (i.e. they
	// call NewVtctldServer to initialize the unit under test), this needs to be
	// set.
	//
	// Tests that do care about the tmclient should use
	// testutil.NewVtctldServerWithTabletManagerClient to initialize their
	// VtctldServer.
	tmclienttest.SetProtocol("go.vt.vtctl.grpcvtctldserver", "grpcvtctldserver.test")
	tmclient.RegisterTabletManagerClientFactory("grpcvtctldserver.test", func() tmclient.TabletManagerClient {
		return nil
	})
}

func TestPanicHandler(t *testing.T) {
	t.Parallel()

	defer func() {
		err := recover()
		assert.Nil(t, err, "bad request should catch panic")
	}()

	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, nil, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	_, err := vtctld.AddCellInfo(context.Background(), nil)
	assert.Error(t, err)
}

func TestAddCellInfo(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		name      string
		ts        *topo.Server
		req       *vtctldatapb.AddCellInfoRequest
		shouldErr bool
	}{
		{
			ts: memorytopo.NewServer("zone1"),
			req: &vtctldatapb.AddCellInfoRequest{
				Name: "zone2",
				CellInfo: &topodatapb.CellInfo{
					ServerAddress: ":2222",
					Root:          "/zone2",
				},
			},
		},
		{
			name: "cell already exists",
			ts:   memorytopo.NewServer("zone1"),
			req: &vtctldatapb.AddCellInfoRequest{
				Name: "zone1",
				CellInfo: &topodatapb.CellInfo{
					ServerAddress: ":1111",
					Root:          "/zone1",
				},
			},
			shouldErr: true,
		},
		{
			name: "no cell root",
			ts:   memorytopo.NewServer("zone1"),
			req: &vtctldatapb.AddCellInfoRequest{
				Name: "zone2",
				CellInfo: &topodatapb.CellInfo{
					ServerAddress: ":2222",
				},
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, tt.ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			_, err := vtctld.AddCellInfo(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			ci, err := tt.ts.GetCellInfo(ctx, tt.req.Name, true)
			require.NoError(t, err, "failed to read new cell %s from topo", tt.req.Name)
			utils.MustMatch(t, tt.req.CellInfo, ci)
		})
	}
}

func TestAddCellsAlias(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		name      string
		ts        *topo.Server
		setup     func(ts *topo.Server) error
		req       *vtctldatapb.AddCellsAliasRequest
		shouldErr bool
	}{
		{
			ts: memorytopo.NewServer("zone1", "zone2", "zone3"),
			req: &vtctldatapb.AddCellsAliasRequest{
				Name:  "zone",
				Cells: []string{"zone1", "zone2", "zone3"},
			},
		},
		{
			name: "alias exists",
			ts:   memorytopo.NewServer("zone1", "zone2", "zone3"),
			setup: func(ts *topo.Server) error {
				return ts.CreateCellsAlias(ctx, "zone", &topodatapb.CellsAlias{
					Cells: []string{"zone1", "zone2"},
				})
			},
			req: &vtctldatapb.AddCellsAliasRequest{
				Name:  "zone",
				Cells: []string{"zone1", "zone2", "zone3"},
			},
			shouldErr: true,
		},
		{
			name: "alias overlaps",
			ts:   memorytopo.NewServer("zone1", "zone2", "zone3"),
			setup: func(ts *topo.Server) error {
				return ts.CreateCellsAlias(context.Background(), "zone_a", &topodatapb.CellsAlias{
					Cells: []string{"zone1", "zone3"},
				})
			},
			req: &vtctldatapb.AddCellsAliasRequest{
				Name:  "zone_b",
				Cells: []string{"zone1", "zone2", "zone3"},
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.setup != nil {
				err := tt.setup(tt.ts)
				require.NoError(t, err, "test setup failed")
			}

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, tt.ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			_, err := vtctld.AddCellsAlias(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			ca, err := tt.ts.GetCellsAlias(ctx, tt.req.Name, true)
			require.NoError(t, err, "failed to read new cells alias %s from topo", tt.req.Name)
			utils.MustMatch(t, &topodatapb.CellsAlias{Cells: tt.req.Cells}, ca)
		})
	}
}

func TestApplyRoutingRules(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		name          string
		cells         []string
		req           *vtctldatapb.ApplyRoutingRulesRequest
		expectedRules *vschemapb.RoutingRules
		topoDown      bool
		shouldErr     bool
	}{
		{
			name:  "success",
			cells: []string{"zone1"},
			req: &vtctldatapb.ApplyRoutingRulesRequest{
				RoutingRules: &vschemapb.RoutingRules{
					Rules: []*vschemapb.RoutingRule{
						{
							FromTable: "t1",
							ToTables:  []string{"t1", "t2"},
						},
					},
				},
			},
			expectedRules: &vschemapb.RoutingRules{
				Rules: []*vschemapb.RoutingRule{
					{
						FromTable: "t1",
						ToTables:  []string{"t1", "t2"},
					},
				},
			},
		},
		{
			name:  "rebuild failed (bad cell)",
			cells: []string{"zone1"},
			req: &vtctldatapb.ApplyRoutingRulesRequest{
				RoutingRules: &vschemapb.RoutingRules{
					Rules: []*vschemapb.RoutingRule{
						{
							FromTable: "t1",
							ToTables:  []string{"t1", "t2"},
						},
					},
				},
				RebuildCells: []string{"zone1", "zone2"},
			},
			shouldErr: true,
		},
		{
			// this test case is exactly like the previous, but we don't fail
			// because we don't rebuild the vschema graph (which would fail the
			// way we've set up the test case with the bogus cell).
			name:  "rebuild skipped",
			cells: []string{"zone1"},
			req: &vtctldatapb.ApplyRoutingRulesRequest{
				RoutingRules: &vschemapb.RoutingRules{
					Rules: []*vschemapb.RoutingRule{
						{
							FromTable: "t1",
							ToTables:  []string{"t1", "t2"},
						},
					},
				},
				SkipRebuild:  true,
				RebuildCells: []string{"zone1", "zone2"},
			},
			expectedRules: &vschemapb.RoutingRules{
				Rules: []*vschemapb.RoutingRule{
					{
						FromTable: "t1",
						ToTables:  []string{"t1", "t2"},
					},
				},
			},
			shouldErr: false,
		},
		{
			name:      "topo down",
			cells:     []string{"zone1"},
			req:       &vtctldatapb.ApplyRoutingRulesRequest{},
			topoDown:  true,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts, factory := memorytopo.NewServerAndFactory(tt.cells...)
			if tt.topoDown {
				factory.SetError(errors.New("topo down for testing"))
			}

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			_, err := vtctld.ApplyRoutingRules(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err, "ApplyRoutingRules(%+v) failed", tt.req)

			rr, err := ts.GetRoutingRules(ctx)
			require.NoError(t, err, "failed to get routing rules from topo to compare")
			utils.MustMatch(t, tt.expectedRules, rr)
		})
	}
}

func TestApplyVSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		req       *vtctldatapb.ApplyVSchemaRequest
		exp       *vtctldatapb.ApplyVSchemaResponse
		shouldErr bool
	}{
		{
			name: "normal",
			req: &vtctldatapb.ApplyVSchemaRequest{
				Keyspace: "testkeyspace",
				VSchema: &vschemapb.Keyspace{
					Sharded: false,
				},
			},
			exp: &vtctldatapb.ApplyVSchemaResponse{
				VSchema: &vschemapb.Keyspace{
					Sharded: false,
				},
			},
			shouldErr: false,
		}, {
			name: "skip rebuild",
			req: &vtctldatapb.ApplyVSchemaRequest{
				Keyspace: "testkeyspace",
				VSchema: &vschemapb.Keyspace{
					Sharded: false,
				},
				SkipRebuild: true,
			},
			exp: &vtctldatapb.ApplyVSchemaResponse{
				VSchema: &vschemapb.Keyspace{
					Sharded: false,
				},
			},
			shouldErr: false,
		}, {
			name: "both",
			req: &vtctldatapb.ApplyVSchemaRequest{
				Keyspace: "testkeyspace",
				VSchema: &vschemapb.Keyspace{
					Sharded: false,
				},
				Sql: "some vschema ddl here",
			},
			shouldErr: true,
		}, {
			name: "neither",
			req: &vtctldatapb.ApplyVSchemaRequest{
				Keyspace: "testkeyspace",
			},
			shouldErr: true,
		}, {
			name: "dry run",
			req: &vtctldatapb.ApplyVSchemaRequest{
				Keyspace: "testkeyspace",
				VSchema: &vschemapb.Keyspace{
					Sharded: false,
				},
				DryRun: true,
			},
			exp: &vtctldatapb.ApplyVSchemaResponse{
				VSchema: &vschemapb.Keyspace{
					Sharded: false,
				},
			},
			shouldErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			ts := memorytopo.NewServer("zone1")
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			testutil.AddKeyspace(ctx, t, ts, &vtctldatapb.Keyspace{
				Name: tt.req.Keyspace,
				Keyspace: &topodatapb.Keyspace{
					KeyspaceType: topodatapb.KeyspaceType_NORMAL,
				},
			})

			origVSchema := &vschemapb.Keyspace{
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"v1": {
						Type: "hash",
					},
				},
			}
			err := ts.SaveVSchema(ctx, tt.req.Keyspace, origVSchema)
			require.NoError(t, err)

			origSrvVSchema := &vschemapb.SrvVSchema{
				Keyspaces: map[string]*vschemapb.Keyspace{
					"testkeyspace": {
						Sharded: true,
						Vindexes: map[string]*vschemapb.Vindex{
							"v1": {
								Type: "hash",
							},
						},
					},
				},
				RoutingRules: &vschemapb.RoutingRules{
					Rules: []*vschemapb.RoutingRule{},
				},
			}
			err = ts.UpdateSrvVSchema(ctx, "zone1", origSrvVSchema)
			require.NoError(t, err)

			res, err := vtctld.ApplyVSchema(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			utils.MustMatch(t, tt.exp, res)

			if tt.req.DryRun {
				actual, err := ts.GetVSchema(ctx, tt.req.Keyspace)
				require.NoError(t, err)
				utils.MustMatch(t, origVSchema, actual)
			}

			finalSrvVSchema, err := ts.GetSrvVSchema(ctx, "zone1")
			require.NoError(t, err)

			if tt.req.SkipRebuild || tt.req.DryRun {
				utils.MustMatch(t, origSrvVSchema, finalSrvVSchema)
			} else {
				changedSrvVSchema := &vschemapb.SrvVSchema{
					Keyspaces: map[string]*vschemapb.Keyspace{
						"testkeyspace": {
							Sharded: false,
						},
					},
					RoutingRules: &vschemapb.RoutingRules{
						Rules: []*vschemapb.RoutingRule{},
					},
					ShardRoutingRules: &vschemapb.ShardRoutingRules{
						Rules: []*vschemapb.ShardRoutingRule{},
					},
				}
				utils.MustMatch(t, changedSrvVSchema, finalSrvVSchema)
			}
		})
	}
}

func TestBackup(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		ts        *topo.Server
		tmc       tmclient.TabletManagerClient
		tablet    *topodatapb.Tablet
		req       *vtctldatapb.BackupRequest
		shouldErr bool
		assertion func(t *testing.T, responses []*vtctldatapb.BackupResponse, err error)
	}{
		{
			name: "ok",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				Backups: map[string]struct {
					Events        []*logutilpb.Event
					EventInterval time.Duration
					EventJitter   time.Duration
					ErrorAfter    time.Duration
				}{
					"zone1-0000000100": {
						Events: []*logutilpb.Event{{}, {}, {}},
					},
				},
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				Type:     topodatapb.TabletType_REPLICA,
				Keyspace: "ks",
				Shard:    "-",
			},
			req: &vtctldatapb.BackupRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			assertion: func(t *testing.T, responses []*vtctldatapb.BackupResponse, err error) {
				assert.ErrorIs(t, err, io.EOF, "expected Recv loop to end with io.EOF")
				assert.Equal(t, 3, len(responses), "expected 3 messages from backupclient stream")
			},
		},
		{
			name: "cannot backup primary",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				Backups: map[string]struct {
					Events        []*logutilpb.Event
					EventInterval time.Duration
					EventJitter   time.Duration
					ErrorAfter    time.Duration
				}{
					"zone1-0000000100": {
						Events: []*logutilpb.Event{{}, {}, {}},
					},
				},
			},
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				Type:     topodatapb.TabletType_PRIMARY,
				Keyspace: "ks",
				Shard:    "-",
			},
			req: &vtctldatapb.BackupRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			assertion: func(t *testing.T, responses []*vtctldatapb.BackupResponse, err error) {
				assert.NotErrorIs(t, err, io.EOF, "expected backupclient stream to close with non-EOF")
				assert.Zero(t, len(responses), "expected no backupclient messages")
			},
		},
		{
			name: "allow-primary",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				Backups: map[string]struct {
					Events        []*logutilpb.Event
					EventInterval time.Duration
					EventJitter   time.Duration
					ErrorAfter    time.Duration
				}{
					"zone1-0000000100": {
						Events: []*logutilpb.Event{{}, {}, {}},
					},
				},
			},
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				Type:     topodatapb.TabletType_PRIMARY,
				Keyspace: "ks",
				Shard:    "-",
			},
			req: &vtctldatapb.BackupRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				AllowPrimary: true,
			},
			assertion: func(t *testing.T, responses []*vtctldatapb.BackupResponse, err error) {
				assert.ErrorIs(t, err, io.EOF, "expected Recv loop to end with io.EOF")
				assert.Equal(t, 3, len(responses), "expected 3 messages from backupclient stream")
			},
		},
		{
			name: "no tablet",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				Backups: map[string]struct {
					Events        []*logutilpb.Event
					EventInterval time.Duration
					EventJitter   time.Duration
					ErrorAfter    time.Duration
				}{
					"zone1-0000000100": {
						Events: []*logutilpb.Event{{}, {}, {}},
					},
				},
			},
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				Type:     topodatapb.TabletType_REPLICA,
				Keyspace: "ks",
				Shard:    "-",
			},
			req: &vtctldatapb.BackupRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  404,
				},
			},
			assertion: func(t *testing.T, responses []*vtctldatapb.BackupResponse, err error) {
				assert.NotErrorIs(t, err, io.EOF, "expected backupclient stream to close with non-EOF")
				assert.Zero(t, len(responses), "expected no backupclient messages")
			},
		},
		{
			name: "midstream error",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				Backups: map[string]struct {
					Events        []*logutilpb.Event
					EventInterval time.Duration
					EventJitter   time.Duration
					ErrorAfter    time.Duration
				}{
					"zone1-0000000100": {
						Events:        []*logutilpb.Event{{}, {}, {}},
						EventInterval: 100 * time.Millisecond,
						ErrorAfter:    20 * time.Millisecond,
					},
				},
			},
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				Type:     topodatapb.TabletType_REPLICA,
				Keyspace: "ks",
				Shard:    "-",
			},
			req: &vtctldatapb.BackupRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			assertion: func(t *testing.T, responses []*vtctldatapb.BackupResponse, err error) {
				assert.NotErrorIs(t, err, io.EOF, "expected Recv loop to end with error other than io.EOF")
				assert.Less(t, len(responses), 3, "expected fewer than 3 messages from backupclient stream")
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if tt.tablet != nil {
				testutil.AddTablet(ctx, t, tt.ts, tt.tablet, nil)
			}
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, tt.ts, tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			client := localvtctldclient.New(vtctld)
			stream, err := client.Backup(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			responses, err := func() (responses []*vtctldatapb.BackupResponse, err error) {
				for {
					resp, err := stream.Recv()
					if err != nil {
						return responses, err
					}

					responses = append(responses, resp)
				}
			}()

			if tt.assertion != nil {
				func() {
					t.Helper()
					tt.assertion(t, responses, err)
				}()
			}
		})
	}
}

func TestBackupShard(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		ts        *topo.Server
		tmc       tmclient.TabletManagerClient
		tablets   []*topodatapb.Tablet
		req       *vtctldatapb.BackupShardRequest
		shouldErr bool
		assertion func(t *testing.T, responses []*vtctldatapb.BackupResponse, err error)
	}{
		{
			name: "ok",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				Backups: map[string]struct {
					Events        []*logutilpb.Event
					EventInterval time.Duration
					EventJitter   time.Duration
					ErrorAfter    time.Duration
				}{
					"zone1-0000000100": {
						Events: []*logutilpb.Event{{}, {}, {}},
					},
				},
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000200": {
						Position: "some-position",
					},
				},
				ReplicationStatusResults: map[string]struct {
					Position *replicationdatapb.Status
					Error    error
				}{
					"zone1-0000000100": {
						Position: &replicationdatapb.Status{
							ReplicationLagSeconds: 0,
						},
					},
				},
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "ks",
					Shard:    "-",
					Type:     topodatapb.TabletType_REPLICA,
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  200,
					},
					Keyspace: "ks",
					Shard:    "-",
					Type:     topodatapb.TabletType_PRIMARY,
				},
			},
			req: &vtctldatapb.BackupShardRequest{
				Keyspace: "ks",
				Shard:    "-",
			},
			assertion: func(t *testing.T, responses []*vtctldatapb.BackupResponse, err error) {
				assert.ErrorIs(t, err, io.EOF, "expected Recv loop to end with io.EOF")
				assert.Equal(t, 3, len(responses), "expected 3 messages from backupclient stream")
			},
		},
		{
			name: "cannot backup primary",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				Backups: map[string]struct {
					Events        []*logutilpb.Event
					EventInterval time.Duration
					EventJitter   time.Duration
					ErrorAfter    time.Duration
				}{
					"zone1-0000000100": {
						Events: []*logutilpb.Event{{}, {}, {}},
					},
				},
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "some-position",
					},
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "ks",
					Shard:    "-",
				},
			},
			req: &vtctldatapb.BackupShardRequest{
				Keyspace: "ks",
				Shard:    "-",
			},
			assertion: func(t *testing.T, responses []*vtctldatapb.BackupResponse, err error) {
				assert.NotErrorIs(t, err, io.EOF, "expected backupclient stream to close with non-EOF")
				assert.Zero(t, len(responses), "expected no backupclient messages")
			},
		},
		{
			name: "allow-primary",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				Backups: map[string]struct {
					Events        []*logutilpb.Event
					EventInterval time.Duration
					EventJitter   time.Duration
					ErrorAfter    time.Duration
				}{
					"zone1-0000000100": {
						Events: []*logutilpb.Event{{}, {}, {}},
					},
				},
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "some-position",
					},
				},
				ReplicationStatusResults: map[string]struct {
					Position *replicationdatapb.Status
					Error    error
				}{
					"zone1-0000000101": {
						Position: &replicationdatapb.Status{},
					},
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "ks",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Type:     topodatapb.TabletType_BACKUP,
					Keyspace: "ks",
					Shard:    "-",
				},
			},
			req: &vtctldatapb.BackupShardRequest{
				Keyspace:     "ks",
				Shard:        "-",
				AllowPrimary: true,
			},
			assertion: func(t *testing.T, responses []*vtctldatapb.BackupResponse, err error) {
				assert.ErrorIs(t, err, io.EOF, "expected Recv loop to end with io.EOF")
				assert.Equal(t, 3, len(responses), "expected 3 messages from backupclient stream")
			},
		},
		{
			name: "no available tablet",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				Backups: map[string]struct {
					Events        []*logutilpb.Event
					EventInterval time.Duration
					EventJitter   time.Duration
					ErrorAfter    time.Duration
				}{
					"zone1-0000000100": {
						Events: []*logutilpb.Event{{}, {}, {}},
					},
				},
				ReplicationStatusResults: map[string]struct {
					Position *replicationdatapb.Status
					Error    error
				}{
					"zone1-0000000100": {
						Error: assert.AnError,
					},
					"zone1-0000000101": {
						Error: assert.AnError,
					},
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "ks",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "ks",
					Shard:    "-",
				},
			},
			req: &vtctldatapb.BackupShardRequest{},
			assertion: func(t *testing.T, responses []*vtctldatapb.BackupResponse, err error) {
				assert.NotErrorIs(t, err, io.EOF, "expected backupclient stream to close with non-EOF")
				assert.Zero(t, len(responses), "expected no backupclient messages")
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			testutil.AddTablets(ctx, t, tt.ts,
				&testutil.AddTabletOptions{
					AlsoSetShardPrimary: true,
				}, tt.tablets...,
			)
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, tt.ts, tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			client := localvtctldclient.New(vtctld)
			stream, err := client.BackupShard(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			responses, err := func() (responses []*vtctldatapb.BackupResponse, err error) {
				for {
					resp, err := stream.Recv()
					if err != nil {
						return responses, err
					}

					responses = append(responses, resp)
				}
			}()

			if tt.assertion != nil {
				func() {
					t.Helper()
					tt.assertion(t, responses, err)
				}()
			}
		})
	}
}

func TestChangeTabletType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cells     []string
		tablets   []*topodatapb.Tablet
		req       *vtctldatapb.ChangeTabletTypeRequest
		expected  *vtctldatapb.ChangeTabletTypeResponse
		shouldErr bool
	}{
		{
			name:  "success",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "ks",
					Shard:    "0",
					Type:     topodatapb.TabletType_REPLICA,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "ks",
					Shard:    "0",
					Type:     topodatapb.TabletType_PRIMARY,
				},
			},
			req: &vtctldatapb.ChangeTabletTypeRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				DbType: topodatapb.TabletType_RDONLY,
			},
			expected: &vtctldatapb.ChangeTabletTypeResponse{
				BeforeTablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "ks",
					Shard:    "0",
					Type:     topodatapb.TabletType_REPLICA,
				},
				AfterTablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "ks",
					Shard:    "0",
					Type:     topodatapb.TabletType_RDONLY,
				},
				WasDryRun: false,
			},
			shouldErr: false,
		},
		{
			name:  "dry run",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "ks",
					Shard:    "0",
					Type:     topodatapb.TabletType_REPLICA,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "ks",
					Shard:    "0",
					Type:     topodatapb.TabletType_PRIMARY,
				},
			},
			req: &vtctldatapb.ChangeTabletTypeRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				DbType: topodatapb.TabletType_RDONLY,
				DryRun: true,
			},
			expected: &vtctldatapb.ChangeTabletTypeResponse{
				BeforeTablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "ks",
					Shard:    "0",
					Type:     topodatapb.TabletType_REPLICA,
				},
				AfterTablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "ks",
					Shard:    "0",
					Type:     topodatapb.TabletType_RDONLY,
				},
				WasDryRun: true,
			},
			shouldErr: false,
		},
		{
			name:  "tablet not found",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  200,
					},
					Keyspace: "ks",
					Shard:    "0",
					Type:     topodatapb.TabletType_REPLICA,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "ks",
					Shard:    "0",
					Type:     topodatapb.TabletType_PRIMARY,
				},
			},
			req: &vtctldatapb.ChangeTabletTypeRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				DbType: topodatapb.TabletType_RDONLY,
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name:  "primary promotions not allowed",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "ks",
					Shard:    "0",
					Type:     topodatapb.TabletType_REPLICA,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "ks",
					Shard:    "0",
					Type:     topodatapb.TabletType_PRIMARY,
				},
			},
			req: &vtctldatapb.ChangeTabletTypeRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				DbType: topodatapb.TabletType_PRIMARY,
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name:  "primary demotions not allowed",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "ks",
					Shard:    "0",
					Type:     topodatapb.TabletType_PRIMARY,
				},
			},
			req: &vtctldatapb.ChangeTabletTypeRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				DbType: topodatapb.TabletType_REPLICA,
			},
			expected:  nil,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			ts := memorytopo.NewServer(tt.cells...)
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, &testutil.TabletManagerClient{
				TopoServer: ts,
			}, func(ts *topo.Server) vtctlservicepb.VtctldServer { return NewVtctldServer(ts) })

			testutil.AddTablets(ctx, t, ts, &testutil.AddTabletOptions{
				AlsoSetShardPrimary: true,
			}, tt.tablets...)

			resp, err := vtctld.ChangeTabletType(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)

			// If we are testing a dry-run, then the tablet in the actual
			// topo should match the BeforeTablet in the response. Otherwise,
			// the tablet in the actual topo should match the AfterTablet in
			// the response.
			expectedRealType := resp.AfterTablet.Type
			msg := "ChangeTabletType did not cause topo update"
			if tt.req.DryRun {
				expectedRealType = resp.BeforeTablet.Type
				msg = "dryrun type change resulted in real type change"
			}

			tablet, err := ts.GetTablet(ctx, tt.req.TabletAlias)
			assert.NoError(t, err,
				"could not load tablet %s from topo after type change %v -> %v [dryrun=%t]",
				topoproto.TabletAliasString(tt.req.TabletAlias),
				resp.BeforeTablet.Type,
				resp.AfterTablet.Type,
				resp.WasDryRun,
			)
			utils.MustMatch(t, expectedRealType, tablet.Type, msg)
		})
	}

	t.Run("tabletmanager failure", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		ts := memorytopo.NewServer("zone1")
		vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, &testutil.TabletManagerClient{
			TopoServer: nil,
		}, func(ts *topo.Server) vtctlservicepb.VtctldServer { return NewVtctldServer(ts) })

		testutil.AddTablet(ctx, t, ts, &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  100,
			},
			Keyspace: "ks",
			Shard:    "0",
			Type:     topodatapb.TabletType_REPLICA,
		}, nil)
		testutil.AddTablet(ctx, t, ts, &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  101,
			},
			Keyspace: "ks",
			Shard:    "0",
			Type:     topodatapb.TabletType_PRIMARY,
		}, &testutil.AddTabletOptions{
			AlsoSetShardPrimary: true,
		})

		_, err := vtctld.ChangeTabletType(ctx, &vtctldatapb.ChangeTabletTypeRequest{
			TabletAlias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  100,
			},
			DbType: topodatapb.TabletType_RDONLY,
		})
		assert.Error(t, err)
	})
}

func TestCreateKeyspace(t *testing.T) {
	t.Parallel()

	cells := []string{"zone1", "zone2", "zone3"}
	tests := []struct {
		name               string
		topo               map[string]*topodatapb.Keyspace
		vschemas           map[string]*vschemapb.Keyspace
		req                *vtctldatapb.CreateKeyspaceRequest
		expected           *vtctldatapb.CreateKeyspaceResponse
		shouldErr          bool
		vschemaShouldExist bool
		expectedVSchema    *vschemapb.Keyspace
	}{
		{
			name: "normal keyspace",
			topo: nil,
			req: &vtctldatapb.CreateKeyspaceRequest{
				Name: "testkeyspace",
				Type: topodatapb.KeyspaceType_NORMAL,
			},
			expected: &vtctldatapb.CreateKeyspaceResponse{
				Keyspace: &vtctldatapb.Keyspace{
					Name: "testkeyspace",
					Keyspace: &topodatapb.Keyspace{
						KeyspaceType: topodatapb.KeyspaceType_NORMAL,
					},
				},
			},
			vschemaShouldExist: true,
			expectedVSchema: &vschemapb.Keyspace{
				Sharded: false,
			},
			shouldErr: false,
		},
		{
			name: "snapshot keyspace",
			topo: map[string]*topodatapb.Keyspace{
				"testkeyspace": {
					KeyspaceType: topodatapb.KeyspaceType_NORMAL,
				},
			},
			vschemas: map[string]*vschemapb.Keyspace{
				"testkeyspace": {
					Sharded: true,
					Vindexes: map[string]*vschemapb.Vindex{
						"h1": {
							Type: "hash",
						},
					},
				},
			},
			req: &vtctldatapb.CreateKeyspaceRequest{
				Name:         "testsnapshot",
				Type:         topodatapb.KeyspaceType_SNAPSHOT,
				BaseKeyspace: "testkeyspace",
				SnapshotTime: &vttime.Time{
					Seconds: 1,
				},
			},
			expected: &vtctldatapb.CreateKeyspaceResponse{
				Keyspace: &vtctldatapb.Keyspace{
					Name: "testsnapshot",
					Keyspace: &topodatapb.Keyspace{
						KeyspaceType: topodatapb.KeyspaceType_SNAPSHOT,
						BaseKeyspace: "testkeyspace",
						SnapshotTime: &vttime.Time{
							Seconds: 1,
						},
					},
				},
			},
			vschemaShouldExist: true,
			expectedVSchema: &vschemapb.Keyspace{
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"h1": {
						Type: "hash",
					},
				},
				RequireExplicitRouting: true,
			},
			shouldErr: false,
		},
		{
			name: "snapshot keyspace with no base keyspace specified",
			topo: nil,
			req: &vtctldatapb.CreateKeyspaceRequest{
				Name:         "testsnapshot",
				Type:         topodatapb.KeyspaceType_SNAPSHOT,
				SnapshotTime: &vttime.Time{},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "snapshot keyspace with no snapshot time",
			topo: nil,
			req: &vtctldatapb.CreateKeyspaceRequest{
				Name:         "testsnapshot",
				Type:         topodatapb.KeyspaceType_SNAPSHOT,
				BaseKeyspace: "testkeyspace",
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "snapshot keyspace with nonexistent base keyspace",
			topo: nil,
			req: &vtctldatapb.CreateKeyspaceRequest{
				Name:         "testsnapshot",
				Type:         topodatapb.KeyspaceType_SNAPSHOT,
				BaseKeyspace: "testkeyspace",
				SnapshotTime: &vttime.Time{Seconds: 100},
			},
			expected: &vtctldatapb.CreateKeyspaceResponse{
				Keyspace: &vtctldatapb.Keyspace{
					Name: "testsnapshot",
					Keyspace: &topodatapb.Keyspace{
						KeyspaceType: topodatapb.KeyspaceType_SNAPSHOT,
						BaseKeyspace: "testkeyspace",
						SnapshotTime: &vttime.Time{Seconds: 100},
					},
				},
			},
			vschemaShouldExist: true,
			expectedVSchema: &vschemapb.Keyspace{
				Sharded:                false,
				RequireExplicitRouting: true,
			},
			shouldErr: false,
		},
		{
			name: "invalid keyspace type",
			topo: nil,
			req: &vtctldatapb.CreateKeyspaceRequest{
				Name: "badkeyspacetype",
				Type: 10000000,
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "keyspace exists/no force",
			topo: map[string]*topodatapb.Keyspace{
				"testkeyspace": {
					KeyspaceType: topodatapb.KeyspaceType_NORMAL,
				},
			},
			req: &vtctldatapb.CreateKeyspaceRequest{
				Name:  "testkeyspace",
				Type:  topodatapb.KeyspaceType_NORMAL,
				Force: false,
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "keyspace exists/force",
			topo: map[string]*topodatapb.Keyspace{
				"testkeyspace": {
					KeyspaceType: topodatapb.KeyspaceType_NORMAL,
				},
			},
			req: &vtctldatapb.CreateKeyspaceRequest{
				Name:  "testkeyspace",
				Type:  topodatapb.KeyspaceType_NORMAL,
				Force: true,
			},
			expected: &vtctldatapb.CreateKeyspaceResponse{
				Keyspace: &vtctldatapb.Keyspace{
					Name: "testkeyspace",
					Keyspace: &topodatapb.Keyspace{
						KeyspaceType: topodatapb.KeyspaceType_NORMAL,
					},
				},
			},
			vschemaShouldExist: true,
			expectedVSchema: &vschemapb.Keyspace{
				Sharded: false,
			},
			shouldErr: false,
		},
		{
			name: "allow empty vschema",
			topo: nil,
			req: &vtctldatapb.CreateKeyspaceRequest{
				Name:              "testkeyspace",
				Type:              topodatapb.KeyspaceType_NORMAL,
				AllowEmptyVSchema: true,
			},
			expected: &vtctldatapb.CreateKeyspaceResponse{
				Keyspace: &vtctldatapb.Keyspace{
					Name: "testkeyspace",
					Keyspace: &topodatapb.Keyspace{
						KeyspaceType: topodatapb.KeyspaceType_NORMAL,
					},
				},
			},
			vschemaShouldExist: false,
			expectedVSchema:    nil,
			shouldErr:          false,
		}, {
			name: "keyspace with durability policy specified",
			topo: nil,
			req: &vtctldatapb.CreateKeyspaceRequest{
				Name:             "testkeyspace",
				Type:             topodatapb.KeyspaceType_NORMAL,
				DurabilityPolicy: "semi_sync",
			},
			expected: &vtctldatapb.CreateKeyspaceResponse{
				Keyspace: &vtctldatapb.Keyspace{
					Name: "testkeyspace",
					Keyspace: &topodatapb.Keyspace{
						KeyspaceType:     topodatapb.KeyspaceType_NORMAL,
						DurabilityPolicy: "semi_sync",
					},
				},
			},
			vschemaShouldExist: true,
			expectedVSchema: &vschemapb.Keyspace{
				Sharded: false,
			},
			shouldErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.req == nil {
				t.Skip("test not yet implemented")
			}

			ctx := context.Background()
			ts := memorytopo.NewServer(cells...)
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			for name, ks := range tt.topo {
				testutil.AddKeyspace(ctx, t, ts, &vtctldatapb.Keyspace{
					Name:     name,
					Keyspace: ks,
				})
			}

			for name, vs := range tt.vschemas {
				require.NoError(t, ts.SaveVSchema(ctx, name, vs), "error in SaveVSchema(%v, %+v)", name, vs)
			}

			// Create the keyspace and make some assertions
			resp, err := vtctld.CreateKeyspace(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			testutil.AssertKeyspacesEqual(t, tt.expected.Keyspace, resp.Keyspace, "%+v\n%+v\n", tt.expected.Keyspace, resp.Keyspace)

			// Fetch the newly-created keyspace out of the topo and assert on it
			ks, err := ts.GetKeyspace(ctx, tt.req.Name)
			assert.NoError(t, err, "cannot get keyspace %v after creating", tt.req.Name)
			require.NotNil(t, ks.Keyspace)

			actualKs := &vtctldatapb.Keyspace{
				Name:     tt.req.Name,
				Keyspace: ks.Keyspace,
			}
			testutil.AssertKeyspacesEqual(
				t,
				tt.expected.Keyspace,
				actualKs,
				"created keyspace %v does not match requested keyspace (name = %v) %v",
				actualKs,
				tt.expected.Keyspace,
			)

			// Finally, check the VSchema
			vs, err := ts.GetVSchema(ctx, tt.req.Name)
			if !tt.vschemaShouldExist {
				assert.True(t, topo.IsErrType(err, topo.NoNode), "vschema should not exist, but got other error = %v", err)
				return
			}
			assert.NoError(t, err)
			utils.MustMatch(t, tt.expectedVSchema, vs)
		})
	}
}

func TestCreateShard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		keyspaces []*vtctldatapb.Keyspace
		shards    []*vtctldatapb.Shard
		topoErr   error
		req       *vtctldatapb.CreateShardRequest
		expected  *vtctldatapb.CreateShardResponse
		shouldErr bool
	}{
		{
			name: "success",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			shards:  nil,
			topoErr: nil,
			req: &vtctldatapb.CreateShardRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
			},
			expected: &vtctldatapb.CreateShardResponse{
				Keyspace: &vtctldatapb.Keyspace{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
				Shard: &vtctldatapb.Shard{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						KeyRange:         &topodatapb.KeyRange{},
						IsPrimaryServing: true,
					},
				},
				ShardAlreadyExists: false,
			},
			shouldErr: false,
		},
		{
			name:      "include parent",
			keyspaces: nil,
			shards:    nil,
			topoErr:   nil,
			req: &vtctldatapb.CreateShardRequest{
				Keyspace:      "testkeyspace",
				ShardName:     "-",
				IncludeParent: true,
			},
			expected: &vtctldatapb.CreateShardResponse{
				Keyspace: &vtctldatapb.Keyspace{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
				Shard: &vtctldatapb.Shard{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						KeyRange:         &topodatapb.KeyRange{},
						IsPrimaryServing: true,
					},
				},
				ShardAlreadyExists: false,
			},
			shouldErr: false,
		},
		{
			name:      "keyspace does not exist",
			keyspaces: nil,
			shards:    nil,
			topoErr:   nil,
			req: &vtctldatapb.CreateShardRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "include parent/keyspace exists/no force",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			shards:  nil,
			topoErr: nil,
			req: &vtctldatapb.CreateShardRequest{
				Keyspace:      "testkeyspace",
				ShardName:     "-",
				IncludeParent: true,
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "include parent/keyspace exists/force",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			shards:  nil,
			topoErr: nil,
			req: &vtctldatapb.CreateShardRequest{
				Keyspace:      "testkeyspace",
				ShardName:     "-",
				IncludeParent: true,
				Force:         true,
			},
			expected: &vtctldatapb.CreateShardResponse{
				Keyspace: &vtctldatapb.Keyspace{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
				Shard: &vtctldatapb.Shard{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						KeyRange:         &topodatapb.KeyRange{},
						IsPrimaryServing: true,
					},
				},
				ShardAlreadyExists: false,
			},
			shouldErr: false,
		},
		{
			name: "shard exists/no force",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			topoErr: nil,
			req: &vtctldatapb.CreateShardRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "shard exists/force",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			topoErr: nil,
			req: &vtctldatapb.CreateShardRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
				Force:     true,
			},
			expected: &vtctldatapb.CreateShardResponse{
				Keyspace: &vtctldatapb.Keyspace{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
				Shard: &vtctldatapb.Shard{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						KeyRange:         &topodatapb.KeyRange{},
						IsPrimaryServing: true,
					},
				},
				ShardAlreadyExists: true,
			},
			shouldErr: false,
		},
		{
			name: "topo is down",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			shards:  nil,
			topoErr: assert.AnError,
			req: &vtctldatapb.CreateShardRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
			},
			expected:  nil,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.req == nil {
				t.Skip("focusing on other tests")
			}

			ctx := context.Background()
			ts, topofactory := memorytopo.NewServerAndFactory("zone1")
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			for _, ks := range tt.keyspaces {
				testutil.AddKeyspace(ctx, t, ts, ks)
			}

			testutil.AddShards(ctx, t, ts, tt.shards...)

			if tt.topoErr != nil {
				topofactory.SetError(tt.topoErr)
			}

			resp, err := vtctld.CreateShard(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestDeleteCellInfo(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		name      string
		ts        *topo.Server
		req       *vtctldatapb.DeleteCellInfoRequest
		shouldErr bool
	}{
		{
			ts: memorytopo.NewServer("zone1", "zone2"),
			req: &vtctldatapb.DeleteCellInfoRequest{
				Name: "zone2",
			},
		},
		{
			name: "cell does not exist",
			ts:   memorytopo.NewServer("zone1"),
			req: &vtctldatapb.DeleteCellInfoRequest{
				Name: "zone2",
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, tt.ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			_, err := vtctld.DeleteCellInfo(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			ci, err := tt.ts.GetCellInfo(ctx, tt.req.Name, true)
			assert.True(t, topo.IsErrType(err, topo.NoNode), "expected cell %s to no longer exist; found %+v", tt.req.Name, ci)
		})
	}
}

func TestDeleteCellsAlias(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		name      string
		ts        *topo.Server
		setup     func(ts *topo.Server) error
		req       *vtctldatapb.DeleteCellsAliasRequest
		shouldErr bool
	}{
		{
			ts: memorytopo.NewServer("zone1", "zone2"),
			setup: func(ts *topo.Server) error {
				return ts.CreateCellsAlias(ctx, "zone", &topodatapb.CellsAlias{
					Cells: []string{"zone1", "zone2"},
				})
			},
			req: &vtctldatapb.DeleteCellsAliasRequest{
				Name: "zone",
			},
		},
		{
			name: "alias does not exist",
			ts:   memorytopo.NewServer("zone1", "zone2"),
			setup: func(ts *topo.Server) error {
				return ts.CreateCellsAlias(ctx, "zone_a", &topodatapb.CellsAlias{
					Cells: []string{"zone1", "zone2"},
				})
			},
			req: &vtctldatapb.DeleteCellsAliasRequest{
				Name: "zone_b",
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.setup != nil {
				err := tt.setup(tt.ts)
				require.NoError(t, err, "test setup failed")
			}

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, tt.ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			_, err := vtctld.DeleteCellsAlias(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			ca, err := tt.ts.GetCellsAlias(ctx, tt.req.Name, true)
			assert.True(t, topo.IsErrType(err, topo.NoNode), "expected cell alias %s to no longer exist; found %+v", tt.req.Name, ca)
		})
	}
}

func TestDeleteKeyspace(t *testing.T) {
	t.Parallel()

	type testcase struct {
		name                       string
		keyspaces                  []*vtctldatapb.Keyspace
		shards                     []*vtctldatapb.Shard
		srvKeyspaces               map[string]map[string]*topodatapb.SrvKeyspace
		before                     func(t *testing.T, ts *topo.Server, tt testcase) func()
		topoErr                    error
		req                        *vtctldatapb.DeleteKeyspaceRequest
		expected                   *vtctldatapb.DeleteKeyspaceResponse
		expectedRemainingKeyspaces []string
		expectedRemainingShards    map[string][]string
		shouldErr                  bool
	}
	tests := []testcase{
		{
			name: "success",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			shards:       nil,
			srvKeyspaces: nil,
			topoErr:      nil,
			req: &vtctldatapb.DeleteKeyspaceRequest{
				Keyspace: "testkeyspace",
			},
			expected:                   &vtctldatapb.DeleteKeyspaceResponse{},
			expectedRemainingKeyspaces: []string{},
			expectedRemainingShards:    map[string][]string{},
			shouldErr:                  false,
		},
		{
			name: "keyspace does not exist",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "otherkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			shards:       nil,
			srvKeyspaces: nil,
			topoErr:      nil,
			req: &vtctldatapb.DeleteKeyspaceRequest{
				Keyspace: "testkeyspace",
			},
			expected:                   nil,
			expectedRemainingKeyspaces: []string{"otherkeyspace"},
			expectedRemainingShards: map[string][]string{
				"otherkeyspace": nil,
			},
			shouldErr: true,
		},
		{
			name: "keyspace has shards/Recursive=false",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-80",
				},
				{
					Keyspace: "testkeyspace",
					Name:     "80-",
				},
			},
			srvKeyspaces: nil,
			topoErr:      nil,
			req: &vtctldatapb.DeleteKeyspaceRequest{
				Keyspace: "testkeyspace",
			},
			expected:                   nil,
			expectedRemainingKeyspaces: []string{"testkeyspace"},
			expectedRemainingShards: map[string][]string{
				"testkeyspace": {"-80", "80-"},
			},
			shouldErr: true,
		},
		{
			name: "keyspace has shards/Recursive=true",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-80",
				},
				{
					Keyspace: "testkeyspace",
					Name:     "80-",
				},
			},
			srvKeyspaces: nil,
			topoErr:      nil,
			req: &vtctldatapb.DeleteKeyspaceRequest{
				Keyspace:  "testkeyspace",
				Recursive: true,
			},
			expected:                   &vtctldatapb.DeleteKeyspaceResponse{},
			expectedRemainingKeyspaces: []string{},
			expectedRemainingShards:    map[string][]string{},
			shouldErr:                  false,
		},
		// Not sure how to force this case because we always pass
		// (Recursive=true, EvenIfServing=true) so anything short of "topo
		// server is down" won't fail, and "topo server is down" will cause us
		// to error before we even reach this point in the code, so, ¯\_(ツ)_/¯.
		// {
		// 	name: "recursive/cannot delete shard",
		// },
		{
			name: "topo error",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			shards:       nil,
			srvKeyspaces: nil,
			topoErr:      assert.AnError,
			req: &vtctldatapb.DeleteKeyspaceRequest{
				Keyspace: "testkeyspace",
			},
			expected:                   nil,
			expectedRemainingKeyspaces: []string{"testkeyspace"},
			expectedRemainingShards: map[string][]string{
				"testkeyspace": nil,
			},
			shouldErr: true,
		},
		{
			name: "keyspace is locked",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			shards:       nil,
			srvKeyspaces: nil,
			topoErr:      nil,
			before: func(t *testing.T, ts *topo.Server, tt testcase) func() {
				_, unlock, err := ts.LockKeyspace(context.Background(), tt.req.Keyspace, "test.DeleteKeyspace")
				require.NoError(t, err, "failed to lock keyspace %s before test", tt.req.Keyspace)
				return func() {
					unlock(&err)
					if !topo.IsErrType(err, topo.NoNode) {
						assert.NoError(t, err, "error while unlocking keyspace %s after test", tt.req.Keyspace)
					}
				}
			},
			req: &vtctldatapb.DeleteKeyspaceRequest{
				Keyspace: "testkeyspace",
			},
			expected:                   nil,
			expectedRemainingKeyspaces: []string{"testkeyspace"},
			expectedRemainingShards: map[string][]string{
				"testkeyspace": nil,
			},
			shouldErr: true,
		},
		{
			name: "keyspace is locked with force",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			shards:       nil,
			srvKeyspaces: nil,
			topoErr:      nil,
			before: func(t *testing.T, ts *topo.Server, tt testcase) func() {
				_, unlock, err := ts.LockKeyspace(context.Background(), tt.req.Keyspace, "test.DeleteKeyspace")
				require.NoError(t, err, "failed to lock keyspace %s before test", tt.req.Keyspace)
				return func() {
					unlock(&err)
					if !topo.IsErrType(err, topo.NoNode) {
						assert.NoError(t, err, "error while unlocking keyspace %s after test", tt.req.Keyspace)
					}
				}
			},
			req: &vtctldatapb.DeleteKeyspaceRequest{
				Keyspace: "testkeyspace",
				Force:    true,
			},
			expected:                   &vtctldatapb.DeleteKeyspaceResponse{},
			expectedRemainingKeyspaces: nil,
			expectedRemainingShards:    nil,
			shouldErr:                  false,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cells := []string{"zone1", "zone2", "zone3"}

			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
			defer cancel()

			ts, topofactory := memorytopo.NewServerAndFactory(cells...)
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			testutil.AddKeyspaces(ctx, t, ts, tt.keyspaces...)
			testutil.AddShards(ctx, t, ts, tt.shards...)
			testutil.UpdateSrvKeyspaces(ctx, t, ts, tt.srvKeyspaces)

			if tt.topoErr != nil {
				topofactory.SetError(tt.topoErr)
			}

			defer func() {
				if tt.expectedRemainingKeyspaces == nil {
					return
				}

				topofactory.SetError(nil)

				keyspaces, err := ts.GetKeyspaces(ctx)
				require.NoError(t, err, "cannot get keyspaces names after DeleteKeyspace call")
				assert.ElementsMatch(t, tt.expectedRemainingKeyspaces, keyspaces)

				if tt.expectedRemainingShards == nil {
					return
				}

				remainingShards := make(map[string][]string, len(keyspaces))

				for _, ks := range keyspaces {
					shards, err := ts.GetShardNames(ctx, ks)
					require.NoError(t, err, "cannot get shard names for keyspace %s", ks)

					remainingShards[ks] = shards
				}

				utils.MustMatch(t, tt.expectedRemainingShards, remainingShards)
			}()

			if tt.before != nil {
				if after := tt.before(t, ts, tt); after != nil {
					defer after()
				}
			}

			resp, err := vtctld.DeleteKeyspace(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestDeleteShards(t *testing.T) {
	t.Parallel()

	type testcase struct {
		name                    string
		shards                  []*vtctldatapb.Shard
		tablets                 []*topodatapb.Tablet
		replicationGraphs       []*topo.ShardReplicationInfo
		srvKeyspaces            map[string]map[string]*topodatapb.SrvKeyspace
		topoErr                 error
		before                  func(t *testing.T, ts *topo.Server, tt testcase) func()
		req                     *vtctldatapb.DeleteShardsRequest
		expected                *vtctldatapb.DeleteShardsResponse
		expectedRemainingShards []*vtctldatapb.Shard
		shouldErr               bool
	}
	tests := []testcase{
		{
			name: "success",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			tablets: nil,
			topoErr: nil,
			req: &vtctldatapb.DeleteShardsRequest{
				Shards: []*vtctldatapb.Shard{
					{
						Keyspace: "testkeyspace",
						Name:     "-",
					},
				},
			},
			expected:                &vtctldatapb.DeleteShardsResponse{},
			expectedRemainingShards: []*vtctldatapb.Shard{},
			shouldErr:               false,
		},
		{
			name:    "shard not found",
			shards:  nil,
			tablets: nil,
			topoErr: nil,
			req: &vtctldatapb.DeleteShardsRequest{
				Shards: []*vtctldatapb.Shard{
					{
						Keyspace: "testkeyspace",
						Name:     "-",
					},
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "multiple shards",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
				{
					Keyspace: "otherkeyspace",
					Name:     "-80",
				},
				{
					Keyspace: "otherkeyspace",
					Name:     "80-",
				},
			},
			tablets: nil,
			topoErr: nil,
			req: &vtctldatapb.DeleteShardsRequest{
				Shards: []*vtctldatapb.Shard{
					{
						Keyspace: "testkeyspace",
						Name:     "-",
					},
					{
						Keyspace: "otherkeyspace",
						Name:     "-80",
					},
				},
			},
			expected: &vtctldatapb.DeleteShardsResponse{},
			expectedRemainingShards: []*vtctldatapb.Shard{
				{
					Keyspace: "otherkeyspace",
					Name:     "80-",
				},
			},
			shouldErr: false,
		},
		{
			name: "topo is down",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			tablets: nil,
			topoErr: assert.AnError,
			req: &vtctldatapb.DeleteShardsRequest{
				Shards: []*vtctldatapb.Shard{
					{
						Keyspace: "testkeyspace",
						Name:     "-",
					},
				},
			},
			expected: nil,
			expectedRemainingShards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			shouldErr: true,
		},
		{
			name: "shard is serving/EvenIfServing=false",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			tablets: nil,
			srvKeyspaces: map[string]map[string]*topodatapb.SrvKeyspace{
				"zone1": {
					"testkeyspace": &topodatapb.SrvKeyspace{
						Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
							{
								ServedType: topodatapb.TabletType_PRIMARY,
								ShardReferences: []*topodatapb.ShardReference{
									{
										Name:     "-",
										KeyRange: &topodatapb.KeyRange{},
									},
								},
							},
						},
					},
				},
			},
			topoErr: nil,
			req: &vtctldatapb.DeleteShardsRequest{
				Shards: []*vtctldatapb.Shard{
					{
						Keyspace: "testkeyspace",
						Name:     "-",
					},
				},
			},
			expected: nil,
			expectedRemainingShards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			shouldErr: true,
		},
		{
			name: "shard is serving/EvenIfServing=true",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			tablets: nil,
			srvKeyspaces: map[string]map[string]*topodatapb.SrvKeyspace{
				"zone1": {
					"testkeyspace": &topodatapb.SrvKeyspace{
						Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
							{
								ServedType: topodatapb.TabletType_PRIMARY,
								ShardReferences: []*topodatapb.ShardReference{
									{
										Name:     "-",
										KeyRange: &topodatapb.KeyRange{},
									},
								},
							},
						},
					},
				},
			},
			topoErr: nil,
			req: &vtctldatapb.DeleteShardsRequest{
				Shards: []*vtctldatapb.Shard{
					{
						Keyspace: "testkeyspace",
						Name:     "-",
					},
				},
				EvenIfServing: true,
			},
			expected:                &vtctldatapb.DeleteShardsResponse{},
			expectedRemainingShards: []*vtctldatapb.Shard{},
			shouldErr:               false,
		},
		{
			name: "ShardReplication in topo",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			tablets: nil,
			replicationGraphs: []*topo.ShardReplicationInfo{
				topo.NewShardReplicationInfo(&topodatapb.ShardReplication{
					Nodes: []*topodatapb.ShardReplication_Node{
						{
							TabletAlias: &topodatapb.TabletAlias{
								Cell: "zone1",
								Uid:  100,
							},
						},
					},
				}, "zone1", "testkeyspace", "-"),
				topo.NewShardReplicationInfo(&topodatapb.ShardReplication{
					Nodes: []*topodatapb.ShardReplication_Node{
						{
							TabletAlias: &topodatapb.TabletAlias{
								Cell: "zone2",
								Uid:  200,
							},
						},
					},
				}, "zone2", "testkeyspace", "-"),
				topo.NewShardReplicationInfo(&topodatapb.ShardReplication{
					Nodes: []*topodatapb.ShardReplication_Node{
						{
							TabletAlias: &topodatapb.TabletAlias{
								Cell: "zone3",
								Uid:  300,
							},
						},
					},
				}, "zone3", "testkeyspace", "-"),
			},
			topoErr: nil,
			req: &vtctldatapb.DeleteShardsRequest{
				Shards: []*vtctldatapb.Shard{
					{
						Keyspace: "testkeyspace",
						Name:     "-",
					},
				},
			},
			expected:                &vtctldatapb.DeleteShardsResponse{},
			expectedRemainingShards: []*vtctldatapb.Shard{},
			shouldErr:               false,
		},
		{
			name: "shard has tablets/Recursive=false",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			topoErr: nil,
			req: &vtctldatapb.DeleteShardsRequest{
				Shards: []*vtctldatapb.Shard{
					{
						Keyspace: "testkeyspace",
						Name:     "-",
					},
				},
			},
			expected: nil,
			expectedRemainingShards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			shouldErr: true,
		},
		{
			name: "shard has tablets/Recursive=true",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			topoErr: nil,
			req: &vtctldatapb.DeleteShardsRequest{
				Shards: []*vtctldatapb.Shard{
					{
						Keyspace: "testkeyspace",
						Name:     "-",
					},
				},
				Recursive: true,
			},
			expected:                &vtctldatapb.DeleteShardsResponse{},
			expectedRemainingShards: []*vtctldatapb.Shard{},
			shouldErr:               false,
		},
		{
			name: "tablets in topo belonging to other shard",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-80",
				},
				{
					Keyspace: "testkeyspace",
					Name:     "80-",
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "80-",
				},
			},
			topoErr: nil,
			req: &vtctldatapb.DeleteShardsRequest{
				Shards: []*vtctldatapb.Shard{
					{
						Keyspace: "testkeyspace",
						Name:     "-80",
					},
				},
			},
			expected: &vtctldatapb.DeleteShardsResponse{},
			expectedRemainingShards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "80-",
				},
			},
			shouldErr: false,
		},
		{
			name: "shard is locked",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			tablets: nil,
			topoErr: nil,
			before: func(t *testing.T, ts *topo.Server, tt testcase) func() {
				shard := tt.req.Shards[0]
				_, unlock, err := ts.LockShard(context.Background(), shard.Keyspace, shard.Name, "test.DeleteShard")
				require.NoError(t, err, "failed to lock shard %s/%s before test", shard.Keyspace, shard.Name)
				return func() {
					unlock(&err)
					if !topo.IsErrType(err, topo.NoNode) {
						assert.NoError(t, err, "error while unlocking shard %s/%s after test", shard.Keyspace, shard.Name)
					}
				}
			},
			req: &vtctldatapb.DeleteShardsRequest{
				Shards: []*vtctldatapb.Shard{
					{
						Keyspace: "testkeyspace",
						Name:     "-",
					},
				},
			},
			expected: nil,
			expectedRemainingShards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			shouldErr: true,
		},
		{
			name: "shard is locked with force",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			tablets: nil,
			topoErr: nil,
			before: func(t *testing.T, ts *topo.Server, tt testcase) func() {
				shard := tt.req.Shards[0]
				_, unlock, err := ts.LockShard(context.Background(), shard.Keyspace, shard.Name, "test.DeleteShard")
				require.NoError(t, err, "failed to lock shard %s/%s before test", shard.Keyspace, shard.Name)
				return func() {
					unlock(&err)
					if !topo.IsErrType(err, topo.NoNode) {
						assert.NoError(t, err, "error while unlocking shard %s/%s after test", shard.Keyspace, shard.Name)
					}
				}
			},
			req: &vtctldatapb.DeleteShardsRequest{
				Shards: []*vtctldatapb.Shard{
					{
						Keyspace: "testkeyspace",
						Name:     "-",
					},
				},
				Force: true,
			},
			expected:                &vtctldatapb.DeleteShardsResponse{},
			expectedRemainingShards: []*vtctldatapb.Shard{},
			shouldErr:               false,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cells := []string{"zone1", "zone2", "zone3"}

			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
			defer cancel()

			ts, topofactory := memorytopo.NewServerAndFactory(cells...)
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			testutil.AddShards(ctx, t, ts, tt.shards...)
			testutil.AddTablets(ctx, t, ts, nil, tt.tablets...)
			testutil.SetupReplicationGraphs(ctx, t, ts, tt.replicationGraphs...)
			testutil.UpdateSrvKeyspaces(ctx, t, ts, tt.srvKeyspaces)

			if tt.topoErr != nil {
				topofactory.SetError(tt.topoErr)
			}

			if tt.expectedRemainingShards != nil {
				defer func() {
					topofactory.SetError(nil)

					actualShards := []*vtctldatapb.Shard{}

					keyspaces, err := ts.GetKeyspaces(ctx)
					require.NoError(t, err, "cannot get keyspace names to check remaining shards")

					for _, ks := range keyspaces {
						shards, err := ts.GetShardNames(ctx, ks)
						require.NoError(t, err, "cannot get shard names for keyspace %s", ks)

						for _, shard := range shards {
							actualShards = append(actualShards, &vtctldatapb.Shard{
								Keyspace: ks,
								Name:     shard,
							})
						}
					}

					assert.ElementsMatch(t, tt.expectedRemainingShards, actualShards)
				}()
			}

			if tt.before != nil {
				if after := tt.before(t, ts, tt); after != nil {
					defer after()
				}
			}

			resp, err := vtctld.DeleteShards(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestDeleteSrvKeyspace(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		name      string
		vschemas  map[string]*vschemapb.SrvVSchema
		req       *vtctldatapb.DeleteSrvVSchemaRequest
		shouldErr bool
	}{
		{
			name: "success",
			vschemas: map[string]*vschemapb.SrvVSchema{
				"zone1": {
					Keyspaces: map[string]*vschemapb.Keyspace{
						"ks1": {},
						"ks2": {},
					},
					RoutingRules: &vschemapb.RoutingRules{Rules: []*vschemapb.RoutingRule{}},
				},
				"zone2": {
					Keyspaces: map[string]*vschemapb.Keyspace{
						"ks3": {},
					},
					RoutingRules: &vschemapb.RoutingRules{Rules: []*vschemapb.RoutingRule{}},
				},
			},
			req: &vtctldatapb.DeleteSrvVSchemaRequest{
				Cell: "zone2",
			},
		},
		{
			name: "cell not found",
			vschemas: map[string]*vschemapb.SrvVSchema{
				"zone1": {
					Keyspaces: map[string]*vschemapb.Keyspace{
						"ks1": {},
						"ks2": {},
					},
					RoutingRules: &vschemapb.RoutingRules{Rules: []*vschemapb.RoutingRule{}},
				},
				"zone2": {
					Keyspaces: map[string]*vschemapb.Keyspace{
						"ks3": {},
					},
					RoutingRules: &vschemapb.RoutingRules{Rules: []*vschemapb.RoutingRule{}},
				},
			},
			req: &vtctldatapb.DeleteSrvVSchemaRequest{
				Cell: "zone404",
			},
			shouldErr: true,
		},
		{
			name:      "empty cell argument",
			req:       &vtctldatapb.DeleteSrvVSchemaRequest{},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cells := make([]string, 0, len(tt.vschemas))
			finalVSchemas := make(map[string]*vschemapb.SrvVSchema, len(tt.vschemas)) // the set of vschemas that should be left after the Delete
			for cell, vschema := range tt.vschemas {
				cells = append(cells, cell)

				if cell == tt.req.Cell {
					vschema = nil
				}

				finalVSchemas[cell] = vschema
			}

			ts := memorytopo.NewServer(cells...)
			for cell, vschema := range tt.vschemas {
				err := ts.UpdateSrvVSchema(ctx, cell, vschema)
				require.NoError(t, err, "failed to update SrvVSchema in cell = %v, vschema = %+v", cell, vschema)
			}

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			_, err := vtctld.DeleteSrvVSchema(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			resp, err := vtctld.GetSrvVSchemas(ctx, &vtctldatapb.GetSrvVSchemasRequest{})
			require.NoError(t, err, "GetSrvVSchemas error")
			utils.MustMatch(t, resp.SrvVSchemas, finalVSchemas)
		})
	}
}

func TestDeleteTablets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                     string
		tablets                  []*topodatapb.Tablet
		shardFieldUpdates        map[string]func(*topo.ShardInfo) error
		lockedShards             []*vtctldatapb.Shard
		topoError                error
		req                      *vtctldatapb.DeleteTabletsRequest
		expected                 *vtctldatapb.DeleteTabletsResponse
		expectedRemainingTablets []*topodatapb.Tablet
		shouldErr                bool
	}{
		{
			name: "single replica",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			lockedShards: nil,
			topoError:    nil,
			req: &vtctldatapb.DeleteTabletsRequest{
				TabletAliases: []*topodatapb.TabletAlias{
					{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			expected:                 &vtctldatapb.DeleteTabletsResponse{},
			expectedRemainingTablets: []*topodatapb.Tablet{},
			shouldErr:                false,
		},
		{
			name: "single primary/no AllowPrimary",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "-",
					PrimaryTermStartTime: &vttime.Time{
						Seconds:     100,
						Nanoseconds: 10,
					},
				},
			},
			lockedShards: nil,
			topoError:    nil,
			req: &vtctldatapb.DeleteTabletsRequest{
				TabletAliases: []*topodatapb.TabletAlias{
					{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "single primary/with AllowPrimary",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "-",
					PrimaryTermStartTime: &vttime.Time{
						Seconds:     100,
						Nanoseconds: 10,
					},
				},
			},
			lockedShards: nil,
			topoError:    nil,
			req: &vtctldatapb.DeleteTabletsRequest{
				TabletAliases: []*topodatapb.TabletAlias{
					{
						Cell: "zone1",
						Uid:  100,
					},
				},
				AllowPrimary: true,
			},
			expected:                 &vtctldatapb.DeleteTabletsResponse{},
			expectedRemainingTablets: []*topodatapb.Tablet{},
			shouldErr:                false,
		},
		{
			name: "multiple tablets",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			lockedShards: nil,
			topoError:    nil,
			req: &vtctldatapb.DeleteTabletsRequest{
				TabletAliases: []*topodatapb.TabletAlias{
					{
						Cell: "zone1",
						Uid:  100,
					},
					{
						Cell: "zone1",
						Uid:  102,
					},
				},
			},
			expected: &vtctldatapb.DeleteTabletsResponse{},
			expectedRemainingTablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			shouldErr: false,
		},
		{
			name: "stale primary record",
			tablets: []*topodatapb.Tablet{
				{
					// The stale primary we're going to delete.
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "-",
					PrimaryTermStartTime: &vttime.Time{
						Seconds:     100,
						Nanoseconds: 10,
					},
				},
				{
					// The real shard primary, which we'll update in the shard
					// record below.
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "-",
					PrimaryTermStartTime: &vttime.Time{
						Seconds:     1001,
						Nanoseconds: 101,
					},
				},
			},
			shardFieldUpdates: map[string]func(*topo.ShardInfo) error{
				"testkeyspace/-": func(si *topo.ShardInfo) error {
					si.PrimaryAlias = &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					}
					si.PrimaryTermStartTime = &vttime.Time{
						Seconds:     1001,
						Nanoseconds: 101,
					}

					return nil
				},
			},
			lockedShards: nil,
			topoError:    nil,
			req: &vtctldatapb.DeleteTabletsRequest{
				TabletAliases: []*topodatapb.TabletAlias{
					{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			expected: &vtctldatapb.DeleteTabletsResponse{},
			expectedRemainingTablets: []*topodatapb.Tablet{
				{
					// The true shard primary still exists (phew!)
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "-",
					PrimaryTermStartTime: &vttime.Time{
						Seconds:     1001,
						Nanoseconds: 101,
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "tablet not found",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			lockedShards: nil,
			topoError:    nil,
			req: &vtctldatapb.DeleteTabletsRequest{
				TabletAliases: []*topodatapb.TabletAlias{
					{
						Cell: "zone1",
						Uid:  200,
					},
				},
			},
			expected: nil,
			expectedRemainingTablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			shouldErr: true,
		},
		{
			name: "shard is locked",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "-",
					PrimaryTermStartTime: &vttime.Time{
						Seconds:     100,
						Nanoseconds: 10,
					},
				},
			},
			lockedShards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			topoError: nil,
			req: &vtctldatapb.DeleteTabletsRequest{
				TabletAliases: []*topodatapb.TabletAlias{
					{
						Cell: "zone1",
						Uid:  100,
					},
				},
				AllowPrimary: true,
			},
			expected: nil,
			expectedRemainingTablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "-",
					PrimaryTermStartTime: &vttime.Time{
						Seconds:     100,
						Nanoseconds: 10,
					},
				},
			},
			shouldErr: true,
		},
		{
			name: "another shard is locked",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "-80",
					PrimaryTermStartTime: &vttime.Time{
						Seconds:     100,
						Nanoseconds: 10,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "80-",
					PrimaryTermStartTime: &vttime.Time{
						Seconds:     200,
						Nanoseconds: 20,
					},
				},
			},
			lockedShards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "80-",
				},
			},
			topoError: nil,
			req: &vtctldatapb.DeleteTabletsRequest{
				TabletAliases: []*topodatapb.TabletAlias{
					{
						// testkeyspace/-80
						Cell: "zone1",
						Uid:  100,
					},
				},
				AllowPrimary: true,
			},
			expected: &vtctldatapb.DeleteTabletsResponse{},
			expectedRemainingTablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "80-",
					PrimaryTermStartTime: &vttime.Time{
						Seconds:     200,
						Nanoseconds: 20,
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "topo server is down",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			lockedShards: nil,
			topoError:    assert.AnError,
			req: &vtctldatapb.DeleteTabletsRequest{
				TabletAliases: []*topodatapb.TabletAlias{
					{
						Cell: "zone1",
						Uid:  200,
					},
				},
			},
			expected: nil,
			expectedRemainingTablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.req == nil {
				t.Skip("focusing on other tests")
			}

			ctx := context.Background()
			ts, topofactory := memorytopo.NewServerAndFactory("zone1")
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			// Setup tablets and shards
			testutil.AddTablets(ctx, t, ts, nil, tt.tablets...)

			for key, updateFn := range tt.shardFieldUpdates {
				ks, shard, err := topoproto.ParseKeyspaceShard(key)
				require.NoError(t, err, "bad keyspace/shard provided in shardFieldUpdates: %s", key)

				_, err = ts.UpdateShardFields(ctx, ks, shard, updateFn)
				require.NoError(t, err, "failed to update shard fields for %s", key)
			}

			// Set locks
			for _, shard := range tt.lockedShards {
				lctx, unlock, lerr := ts.LockShard(ctx, shard.Keyspace, shard.Name, "testing locked shard")
				require.NoError(t, lerr, "cannot lock shard %s/%s", shard.Keyspace, shard.Name)
				// unlock at the end of the test, we don't care about this error
				// value anymore
				defer unlock(&lerr)

				// we do, however, care that the lock context gets propogated
				// both to additional calls to lock, and to the actual RPC call.
				ctx = lctx
			}

			// Set errors
			if tt.topoError != nil {
				topofactory.SetError(tt.topoError)
			}

			checkRemainingTablets := func() {
				topofactory.SetError(nil)

				resp, err := vtctld.GetTablets(ctx, &vtctldatapb.GetTabletsRequest{})
				assert.NoError(t, err, "cannot look up tablets from topo after issuing DeleteTablets request")
				testutil.AssertSameTablets(t, tt.expectedRemainingTablets, resp.Tablets)
			}

			// Run the test
			resp, err := vtctld.DeleteTablets(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)

				if tt.expectedRemainingTablets != nil {
					checkRemainingTablets()
				}

				return
			}

			assert.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
			checkRemainingTablets()
		})
	}
}

func TestEmergencyReparentShard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ts      *topo.Server
		tmc     tmclient.TabletManagerClient
		tablets []*topodatapb.Tablet

		req                 *vtctldatapb.EmergencyReparentShardRequest
		expected            *vtctldatapb.EmergencyReparentShardResponse
		expectEventsToOccur bool
		shouldErr           bool
	}{
		{
			name: "successful reparent",
			ts:   memorytopo.NewServer("zone1"),
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_PRIMARY,
					PrimaryTermStartTime: &vttime.Time{
						Seconds: 100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Type:     topodatapb.TabletType_RDONLY,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			tmc: &testutil.TabletManagerClient{
				DemotePrimaryResults: map[string]struct {
					Status *replicationdatapb.PrimaryStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.PrimaryStatus{
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
						},
					},
				},
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000200": nil,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000200": {},
				},
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000200": {},
				},
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000100": nil,
					"zone1-0000000101": nil,
				},
				StopReplicationAndGetStatusResults: map[string]struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						Error: mysql.ErrNotReplica,
					},
					"zone1-0000000101": {
						Error: assert.AnError,
					},
					"zone1-0000000200": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{IoState: int32(mysql.ReplicationStateRunning), SqlState: int32(mysql.ReplicationStateRunning)},
							After: &replicationdatapb.Status{
								SourceUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
								Position:         "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
							},
						},
					},
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5": nil,
					},
					"zone1-0000000200": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5": nil,
					},
				},
			},
			req: &vtctldatapb.EmergencyReparentShardRequest{
				Keyspace: "testkeyspace",
				Shard:    "-",
				NewPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
				WaitReplicasTimeout: protoutil.DurationToProto(time.Millisecond * 10),
			},
			expected: &vtctldatapb.EmergencyReparentShardResponse{
				Keyspace: "testkeyspace",
				Shard:    "-",
				PromotedPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},
			expectEventsToOccur: true,
			shouldErr:           false,
		},
		{
			// Note: this is testing the error-handling done in
			// (*VtctldServer).EmergencyReparentShard, not the logic of an ERS.
			// That logic is tested in reparentutil, and not here. Therefore,
			// the simplest way to trigger a failure is to attempt an ERS on a
			// shard that does not exist.
			name:    "failed reparent",
			ts:      memorytopo.NewServer("zone1"),
			tablets: nil,

			req: &vtctldatapb.EmergencyReparentShardRequest{
				Keyspace: "testkeyspace",
				Shard:    "-",
			},
			expectEventsToOccur: false,
			shouldErr:           true,
		},
		{
			name: "invalid WaitReplicasTimeout",
			req: &vtctldatapb.EmergencyReparentShardRequest{
				WaitReplicasTimeout: &vttime.Duration{
					Seconds: -1,
					Nanos:   1,
				},
			},
			shouldErr: true,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			testutil.AddTablets(ctx, t, tt.ts, &testutil.AddTabletOptions{
				AlsoSetShardPrimary:  true,
				ForceSetShardPrimary: true,
				SkipShardCreation:    false,
			}, tt.tablets...)

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, tt.ts, tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			resp, err := vtctld.EmergencyReparentShard(ctx, tt.req)

			// We defer this because we want to check in both error and non-
			// error cases, but after the main set of assertions for those
			// cases.
			defer func() {
				if !tt.expectEventsToOccur {
					testutil.AssertNoLogutilEventsOccurred(t, resp, "expected no events to occur during ERS")

					return
				}

				testutil.AssertLogutilEventsOccurred(t, resp, "expected events to occur during ERS")
			}()

			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			testutil.AssertEmergencyReparentShardResponsesEqual(t, tt.expected, resp)
		})
	}
}

func TestExecuteFetchAsApp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tablet    *topodatapb.Tablet
		tmc       *testutil.TabletManagerClient
		req       *vtctldatapb.ExecuteFetchAsAppRequest
		expected  *vtctldatapb.ExecuteFetchAsAppResponse
		shouldErr bool
	}{
		{
			name: "ok",
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			tmc: &testutil.TabletManagerClient{
				ExecuteFetchAsAppResults: map[string]struct {
					Response *querypb.QueryResult
					Error    error
				}{
					"zone1-0000000100": {
						Response: &querypb.QueryResult{
							InsertId: 100,
						},
					},
				},
			},
			req: &vtctldatapb.ExecuteFetchAsAppRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				Query: "select 1;",
			},
			expected: &vtctldatapb.ExecuteFetchAsAppResponse{
				Result: &querypb.QueryResult{
					InsertId: 100,
				},
			},
		},
		{
			name: "tablet not found",
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			tmc: &testutil.TabletManagerClient{
				ExecuteFetchAsAppResults: map[string]struct {
					Response *querypb.QueryResult
					Error    error
				}{
					"zone1-0000000100": {
						Response: &querypb.QueryResult{
							InsertId: 100,
						},
					},
				},
			},
			req: &vtctldatapb.ExecuteFetchAsAppRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  404,
				},
				Query: "select 1;",
			},
			shouldErr: true,
		},
		{
			name: "query error",
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			tmc: &testutil.TabletManagerClient{
				ExecuteFetchAsAppResults: map[string]struct {
					Response *querypb.QueryResult
					Error    error
				}{
					"zone1-0000000100": {
						Error: assert.AnError,
					},
				},
			},
			req: &vtctldatapb.ExecuteFetchAsAppRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				Query: "select 1;",
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			ts := memorytopo.NewServer("zone1")
			testutil.AddTablet(ctx, t, ts, tt.tablet, nil)

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			resp, err := vtctld.ExecuteFetchAsApp(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestExecuteFetchAsDBA(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tablet    *topodatapb.Tablet
		tmc       *testutil.TabletManagerClient
		req       *vtctldatapb.ExecuteFetchAsDBARequest
		expected  *vtctldatapb.ExecuteFetchAsDBAResponse
		shouldErr bool
	}{
		{
			name: "ok",
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			tmc: &testutil.TabletManagerClient{
				ExecuteFetchAsDbaResults: map[string]struct {
					Response *querypb.QueryResult
					Error    error
				}{
					"zone1-0000000100": {
						Response: &querypb.QueryResult{
							InsertId: 100,
						},
					},
				},
			},
			req: &vtctldatapb.ExecuteFetchAsDBARequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				Query: "select 1;",
			},
			expected: &vtctldatapb.ExecuteFetchAsDBAResponse{
				Result: &querypb.QueryResult{
					InsertId: 100,
				},
			},
		},
		{
			name: "tablet not found",
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			tmc: &testutil.TabletManagerClient{
				ExecuteFetchAsDbaResults: map[string]struct {
					Response *querypb.QueryResult
					Error    error
				}{
					"zone1-0000000100": {
						Response: &querypb.QueryResult{
							InsertId: 100,
						},
					},
				},
			},
			req: &vtctldatapb.ExecuteFetchAsDBARequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  404,
				},
				Query: "select 1;",
			},
			shouldErr: true,
		},
		{
			name: "query error",
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			tmc: &testutil.TabletManagerClient{
				ExecuteFetchAsDbaResults: map[string]struct {
					Response *querypb.QueryResult
					Error    error
				}{
					"zone1-0000000100": {
						Error: assert.AnError,
					},
				},
			},
			req: &vtctldatapb.ExecuteFetchAsDBARequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				Query: "select 1;",
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			ts := memorytopo.NewServer("zone1")
			testutil.AddTablet(ctx, t, ts, tt.tablet, nil)

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			resp, err := vtctld.ExecuteFetchAsDBA(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestExecuteHook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		ts        *topo.Server
		tmc       tmclient.TabletManagerClient
		tablets   []*topodatapb.Tablet
		req       *vtctldatapb.ExecuteHookRequest
		shouldErr bool
	}{
		{
			name: "ok",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				ExecuteHookResults: map[string]struct {
					Response *hk.HookResult
					Error    error
				}{
					"zone1-0000000100": {
						Response: &hk.HookResult{},
					},
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			req: &vtctldatapb.ExecuteHookRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				TabletHookRequest: &tabletmanagerdatapb.ExecuteHookRequest{},
			},
		},
		{
			name: "nil hook request",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				ExecuteHookResults: map[string]struct {
					Response *hk.HookResult
					Error    error
				}{
					"zone1-0000000100": {
						Response: &hk.HookResult{},
					},
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			req: &vtctldatapb.ExecuteHookRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				TabletHookRequest: nil,
			},
			shouldErr: true,
		},
		{
			name: "hook with slash",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				ExecuteHookResults: map[string]struct {
					Response *hk.HookResult
					Error    error
				}{
					"zone1-0000000100": {
						Response: &hk.HookResult{},
					},
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			req: &vtctldatapb.ExecuteHookRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				TabletHookRequest: &tabletmanagerdatapb.ExecuteHookRequest{
					Name: "hooks/cannot/contain/slashes",
				},
			},
			shouldErr: true,
		},
		{
			name: "no such tablet",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				ExecuteHookResults: map[string]struct {
					Response *hk.HookResult
					Error    error
				}{
					"zone1-0000000100": {
						Response: &hk.HookResult{},
					},
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			req: &vtctldatapb.ExecuteHookRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  404,
				},
				TabletHookRequest: &tabletmanagerdatapb.ExecuteHookRequest{},
			},
			shouldErr: true,
		},
		{
			name: "tablet hook failure",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				ExecuteHookResults: map[string]struct {
					Response *hk.HookResult
					Error    error
				}{
					"zone1-0000000100": {
						Error: assert.AnError,
					},
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			req: &vtctldatapb.ExecuteHookRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				TabletHookRequest: &tabletmanagerdatapb.ExecuteHookRequest{},
			},
			shouldErr: true,
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			testutil.AddTablets(ctx, t, tt.ts, nil, tt.tablets...)
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, tt.ts, tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			_, err := vtctld.ExecuteHook(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestFindAllShardsInKeyspace(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	ks := &vtctldatapb.Keyspace{
		Name:     "testkeyspace",
		Keyspace: &topodatapb.Keyspace{},
	}
	testutil.AddKeyspace(ctx, t, ts, ks)

	si1, err := ts.GetOrCreateShard(ctx, ks.Name, "-80")
	require.NoError(t, err)
	si2, err := ts.GetOrCreateShard(ctx, ks.Name, "80-")
	require.NoError(t, err)

	resp, err := vtctld.FindAllShardsInKeyspace(ctx, &vtctldatapb.FindAllShardsInKeyspaceRequest{Keyspace: ks.Name})
	assert.NoError(t, err)
	assert.NotNil(t, resp)

	expected := map[string]*vtctldatapb.Shard{
		"-80": {
			Keyspace: ks.Name,
			Name:     "-80",
			Shard:    si1.Shard,
		},
		"80-": {
			Keyspace: ks.Name,
			Name:     "80-",
			Shard:    si2.Shard,
		},
	}

	utils.MustMatch(t, expected, resp.Shards)

	_, err = vtctld.FindAllShardsInKeyspace(ctx, &vtctldatapb.FindAllShardsInKeyspaceRequest{Keyspace: "nothing"})
	assert.Error(t, err)
}

func TestGetBackups(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer()
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	testutil.BackupStorage.Backups = map[string][]string{
		"testkeyspace/-": {"backup1", "backup2"},
	}

	expected := &vtctldatapb.GetBackupsResponse{
		Backups: []*mysqlctlpb.BackupInfo{
			{
				Directory: "testkeyspace/-",
				Name:      "backup1",
				Keyspace:  "testkeyspace",
				Shard:     "-",
			},
			{
				Directory: "testkeyspace/-",
				Name:      "backup2",
				Keyspace:  "testkeyspace",
				Shard:     "-",
			},
		},
	}

	resp, err := vtctld.GetBackups(ctx, &vtctldatapb.GetBackupsRequest{
		Keyspace: "testkeyspace",
		Shard:    "-",
	})
	assert.NoError(t, err)
	utils.MustMatch(t, expected, resp)

	t.Run("no backupstorage", func(t *testing.T) {
		backupstorage.BackupStorageImplementation = "doesnotexist"
		defer func() { backupstorage.BackupStorageImplementation = testutil.BackupStorageImplementation }()

		_, err := vtctld.GetBackups(ctx, &vtctldatapb.GetBackupsRequest{
			Keyspace: "testkeyspace",
			Shard:    "-",
		})
		assert.Error(t, err)
	})

	t.Run("listbackups error", func(t *testing.T) {
		testutil.BackupStorage.ListBackupsError = assert.AnError
		defer func() { testutil.BackupStorage.ListBackupsError = nil }()

		_, err := vtctld.GetBackups(ctx, &vtctldatapb.GetBackupsRequest{
			Keyspace: "testkeyspace",
			Shard:    "-",
		})
		assert.Error(t, err)
	})

	t.Run("parsing times and aliases", func(t *testing.T) {
		testutil.BackupStorage.Backups["ks2/-80"] = []string{
			"2021-06-11.123456.zone1-101",
		}

		resp, err := vtctld.GetBackups(ctx, &vtctldatapb.GetBackupsRequest{
			Keyspace: "ks2",
			Shard:    "-80",
		})
		require.NoError(t, err)
		expected := &vtctldatapb.GetBackupsResponse{
			Backups: []*mysqlctlpb.BackupInfo{
				{
					Directory: "ks2/-80",
					Name:      "2021-06-11.123456.zone1-101",
					Keyspace:  "ks2",
					Shard:     "-80",
					Time:      protoutil.TimeToProto(time.Date(2021, time.June, 11, 12, 34, 56, 0, time.UTC)),
					TabletAlias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
				},
			},
		}
		utils.MustMatch(t, expected, resp)
	})

	t.Run("limiting", func(t *testing.T) {
		unlimited, err := vtctld.GetBackups(ctx, &vtctldatapb.GetBackupsRequest{
			Keyspace: "testkeyspace",
			Shard:    "-",
		})
		require.NoError(t, err)

		limited, err := vtctld.GetBackups(ctx, &vtctldatapb.GetBackupsRequest{
			Keyspace: "testkeyspace",
			Shard:    "-",
			Limit:    1,
		})
		require.NoError(t, err)

		assert.Equal(t, len(limited.Backups), 1, "expected limited backups to have length 1")
		assert.Less(t, len(limited.Backups), len(unlimited.Backups), "expected limited backups to be less than unlimited")
		utils.MustMatch(t, limited.Backups[0], unlimited.Backups[len(unlimited.Backups)-1], "expected limiting to keep N most recent")
	})
}

func TestGetKeyspace(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	expected := &vtctldatapb.GetKeyspaceResponse{
		Keyspace: &vtctldatapb.Keyspace{
			Name:     "testkeyspace",
			Keyspace: &topodatapb.Keyspace{},
		},
	}
	testutil.AddKeyspace(ctx, t, ts, expected.Keyspace)

	ks, err := vtctld.GetKeyspace(ctx, &vtctldatapb.GetKeyspaceRequest{Keyspace: expected.Keyspace.Name})
	assert.NoError(t, err)
	utils.MustMatch(t, expected, ks)

	_, err = vtctld.GetKeyspace(ctx, &vtctldatapb.GetKeyspaceRequest{Keyspace: "notfound"})
	assert.Error(t, err)
}

func TestGetCellInfoNames(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2", "cell3")
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	resp, err := vtctld.GetCellInfoNames(ctx, &vtctldatapb.GetCellInfoNamesRequest{})
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"cell1", "cell2", "cell3"}, resp.Names)

	ts = memorytopo.NewServer()
	vtctld = testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	resp, err = vtctld.GetCellInfoNames(ctx, &vtctldatapb.GetCellInfoNamesRequest{})
	assert.NoError(t, err)
	assert.Empty(t, resp.Names)

	ts, topofactory := memorytopo.NewServerAndFactory("cell1")
	vtctld = testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	topofactory.SetError(assert.AnError)
	_, err = vtctld.GetCellInfoNames(ctx, &vtctldatapb.GetCellInfoNamesRequest{})
	assert.Error(t, err)
}

func TestGetCellInfo(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ts := memorytopo.NewServer()
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	expected := &topodatapb.CellInfo{
		ServerAddress: "example.com",
		Root:          "vitess",
	}
	input := proto.Clone(expected).(*topodatapb.CellInfo)
	require.NoError(t, ts.CreateCellInfo(ctx, "cell1", input))

	resp, err := vtctld.GetCellInfo(ctx, &vtctldatapb.GetCellInfoRequest{Cell: "cell1"})
	assert.NoError(t, err)
	utils.MustMatch(t, expected, resp.CellInfo)

	_, err = vtctld.GetCellInfo(ctx, &vtctldatapb.GetCellInfoRequest{Cell: "does_not_exist"})
	assert.Error(t, err)

	_, err = vtctld.GetCellInfo(ctx, &vtctldatapb.GetCellInfoRequest{})
	assert.Error(t, err)
}

func TestGetCellsAliases(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ts := memorytopo.NewServer("c11", "c12", "c13", "c21", "c22")
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	alias1 := &topodatapb.CellsAlias{
		Cells: []string{"c11", "c12", "c13"},
	}
	alias2 := &topodatapb.CellsAlias{
		Cells: []string{"c21", "c22"},
	}

	for i, alias := range []*topodatapb.CellsAlias{alias1, alias2} {
		input := proto.Clone(alias).(*topodatapb.CellsAlias)
		name := fmt.Sprintf("a%d", i+1)
		require.NoError(t, ts.CreateCellsAlias(ctx, name, input), "cannot create cells alias %d (idx = %d) = %+v", i+1, i, input)
	}

	expected := map[string]*topodatapb.CellsAlias{
		"a1": alias1,
		"a2": alias2,
	}

	resp, err := vtctld.GetCellsAliases(ctx, &vtctldatapb.GetCellsAliasesRequest{})
	assert.NoError(t, err)
	utils.MustMatch(t, expected, resp.Aliases)

	ts, topofactory := memorytopo.NewServerAndFactory()
	vtctld = testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	topofactory.SetError(assert.AnError)
	_, err = vtctld.GetCellsAliases(ctx, &vtctldatapb.GetCellsAliasesRequest{})
	assert.Error(t, err)
}

func TestGetFullStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cells      []string
		tablets    []*topodatapb.Tablet
		req        *vtctldatapb.GetFullStatusRequest
		serverUUID string
		shouldErr  bool
	}{
		{
			name:  "success",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "ks",
					Shard:    "0",
					Type:     topodatapb.TabletType_REPLICA,
				},
			},
			req: &vtctldatapb.GetFullStatusRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			serverUUID: "abcd",
			shouldErr:  false,
		},
		{
			name:  "tablet not found",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  200,
					},
					Keyspace: "ks",
					Shard:    "0",
					Type:     topodatapb.TabletType_REPLICA,
				},
			},
			req: &vtctldatapb.GetFullStatusRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			ts := memorytopo.NewServer(tt.cells...)
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, &testutil.TabletManagerClient{
				TopoServer: ts,
				FullStatusResult: &replicationdatapb.FullStatus{
					ServerUuid: tt.serverUUID,
				},
			}, func(ts *topo.Server) vtctlservicepb.VtctldServer { return NewVtctldServer(ts) })

			testutil.AddTablets(ctx, t, ts, &testutil.AddTabletOptions{
				AlsoSetShardPrimary: true,
			}, tt.tablets...)

			resp, err := vtctld.GetFullStatus(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			utils.MustMatch(t, tt.serverUUID, resp.Status.ServerUuid)
		})
	}
}

func TestGetKeyspaces(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ts, topofactory := memorytopo.NewServerAndFactory("cell1")
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	resp, err := vtctld.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
	assert.NoError(t, err)
	assert.Empty(t, resp.Keyspaces)

	expected := []*vtctldatapb.Keyspace{
		{
			Name:     "ks1",
			Keyspace: &topodatapb.Keyspace{},
		},
		{
			Name:     "ks2",
			Keyspace: &topodatapb.Keyspace{},
		},
		{
			Name:     "ks3",
			Keyspace: &topodatapb.Keyspace{},
		},
	}
	for _, ks := range expected {
		testutil.AddKeyspace(ctx, t, ts, ks)
	}

	resp, err = vtctld.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
	assert.NoError(t, err)
	utils.MustMatch(t, expected, resp.Keyspaces)

	topofactory.SetError(errors.New("error from toposerver"))

	_, err = vtctld.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
	assert.Error(t, err)
}

func TestGetPermissions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	var testGetPermissionsReply = &tabletmanagerdatapb.Permissions{
		UserPermissions: []*tabletmanagerdatapb.UserPermission{
			{
				Host: "host1",
				User: "user1",
				Privileges: map[string]string{
					"create": "yes",
					"delete": "no",
				},
			},
		},
		DbPermissions: []*tabletmanagerdatapb.DbPermission{
			{
				Host: "host2",
				Db:   "db1",
				User: "user2",
				Privileges: map[string]string{
					"create": "no",
					"delete": "yes",
				},
			},
		},
	}
	tests := []struct {
		name      string
		tablets   []*topodatapb.Tablet
		tmc       testutil.TabletManagerClient
		req       *vtctldatapb.GetPermissionsRequest
		shouldErr bool
	}{
		{
			name: "ok",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			tmc: testutil.TabletManagerClient{
				GetPermissionsResults: map[string]struct {
					Permissions *tabletmanagerdatapb.Permissions
					Error       error
				}{
					"zone1-0000000100": {
						Permissions: testGetPermissionsReply,
						Error:       nil,
					},
				},
			},
			req: &vtctldatapb.GetPermissionsRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
		},
		{
			name: "no tablet",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  404,
					},
				},
			},
			tmc: testutil.TabletManagerClient{
				GetPermissionsResults: map[string]struct {
					Permissions *tabletmanagerdatapb.Permissions
					Error       error
				}{
					"zone1-0000000100": {
						Permissions: testGetPermissionsReply,
						Error:       nil,
					},
				},
			},
			req: &vtctldatapb.GetPermissionsRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			shouldErr: true,
		},
		{
			name: "tmc call failed",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			tmc: testutil.TabletManagerClient{
				GetPermissionsResults: map[string]struct {
					Permissions *tabletmanagerdatapb.Permissions
					Error       error
				}{
					"zone1-0000000100": {
						Permissions: testGetPermissionsReply,
						Error:       assert.AnError,
					},
				},
			},
			req: &vtctldatapb.GetPermissionsRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := memorytopo.NewServer("zone1")
			testutil.AddTablets(ctx, t, ts, nil, tt.tablets...)

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, &tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			resp, err := vtctld.GetPermissions(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}
			// we should expect same user and DB permissions as assigned
			assert.Equal(t, resp.Permissions.DbPermissions[0].Host, "host2")
			assert.Equal(t, resp.Permissions.UserPermissions[0].Host, "host1")

			require.NoError(t, err)
		})
	}
}

func TestGetRoutingRules(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		name      string
		topoDown  bool
		rrIn      *vschemapb.RoutingRules
		expected  *vschemapb.RoutingRules
		shouldErr bool
	}{
		{
			name: "success",
			rrIn: &vschemapb.RoutingRules{
				Rules: []*vschemapb.RoutingRule{
					{
						FromTable: "t1",
						ToTables:  []string{"t2", "t3"},
					},
				},
			},
			expected: &vschemapb.RoutingRules{
				Rules: []*vschemapb.RoutingRule{
					{
						FromTable: "t1",
						ToTables:  []string{"t2", "t3"},
					},
				},
			},
		},
		{
			name:     "empty routing rules",
			rrIn:     nil,
			expected: &vschemapb.RoutingRules{},
		},
		{
			name:      "topo error",
			topoDown:  true,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts, factory := memorytopo.NewServerAndFactory()
			if tt.rrIn != nil {
				err := ts.SaveRoutingRules(ctx, tt.rrIn)
				require.NoError(t, err, "could not save routing rules: %+v", tt.rrIn)
			}

			if tt.topoDown {
				factory.SetError(errors.New("topo down for testing"))
			}

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			resp, err := vtctld.GetRoutingRules(ctx, &vtctldatapb.GetRoutingRulesRequest{})
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			utils.MustMatch(t, resp.RoutingRules, tt.expected)
		})
	}
}

func TestGetSchema(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("zone1")
	tmc := testutil.TabletManagerClient{
		GetSchemaResults: map[string]struct {
			Schema *tabletmanagerdatapb.SchemaDefinition
			Error  error
		}{},
	}
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, &tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	validAlias := &topodatapb.TabletAlias{
		Cell: "zone1",
		Uid:  100,
	}
	testutil.AddTablet(ctx, t, ts, &topodatapb.Tablet{
		Alias: validAlias,
	}, nil)
	otherAlias := &topodatapb.TabletAlias{
		Cell: "zone1",
		Uid:  101,
	}
	testutil.AddTablet(ctx, t, ts, &topodatapb.Tablet{
		Alias: otherAlias,
	}, nil)

	// we need to run this on each test case or they will pollute each other
	setupSchema := func() {
		tmc.GetSchemaResults[topoproto.TabletAliasString(validAlias)] = struct {
			Schema *tabletmanagerdatapb.SchemaDefinition
			Error  error
		}{
			Schema: &tabletmanagerdatapb.SchemaDefinition{
				DatabaseSchema: "CREATE DATABASE vt_testkeyspace",
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					{
						Name: "t1",
						Schema: `CREATE TABLE t1 (
	id int(11) not null,
	PRIMARY KEY (id)
);`,
						Type:       "BASE",
						Columns:    []string{"id"},
						DataLength: 100,
						RowCount:   50,
						Fields: []*querypb.Field{
							{
								Name: "id",
								Type: querypb.Type_INT32,
							},
						},
					},
				},
			},
			Error: nil,
		}
	}

	tests := []*struct {
		name      string
		req       *vtctldatapb.GetSchemaRequest
		expected  *vtctldatapb.GetSchemaResponse
		shouldErr bool
	}{
		{
			name: "normal path",
			req: &vtctldatapb.GetSchemaRequest{
				TabletAlias: validAlias,
			},
			expected: &vtctldatapb.GetSchemaResponse{
				Schema: &tabletmanagerdatapb.SchemaDefinition{
					DatabaseSchema: "CREATE DATABASE vt_testkeyspace",
					TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
						{
							Name: "t1",
							Schema: `CREATE TABLE t1 (
	id int(11) not null,
	PRIMARY KEY (id)
);`,
							Type:       "BASE",
							Columns:    []string{"id"},
							DataLength: 100,
							RowCount:   50,
							Fields: []*querypb.Field{
								{
									Name: "id",
									Type: querypb.Type_INT32,
								},
							},
						},
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "table names only",
			req: &vtctldatapb.GetSchemaRequest{
				TabletAlias:    validAlias,
				TableNamesOnly: true,
			},
			expected: &vtctldatapb.GetSchemaResponse{
				Schema: &tabletmanagerdatapb.SchemaDefinition{
					DatabaseSchema: "CREATE DATABASE vt_testkeyspace",
					TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
						{
							Name: "t1",
						},
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "table sizes only",
			req: &vtctldatapb.GetSchemaRequest{
				TabletAlias:    validAlias,
				TableSizesOnly: true,
			},
			expected: &vtctldatapb.GetSchemaResponse{
				Schema: &tabletmanagerdatapb.SchemaDefinition{
					DatabaseSchema: "CREATE DATABASE vt_testkeyspace",
					TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
						{
							Name:       "t1",
							Type:       "BASE",
							DataLength: 100,
							RowCount:   50,
						},
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "table names take precedence over table sizes",
			req: &vtctldatapb.GetSchemaRequest{
				TabletAlias:    validAlias,
				TableNamesOnly: true,
				TableSizesOnly: true,
			},
			expected: &vtctldatapb.GetSchemaResponse{
				Schema: &tabletmanagerdatapb.SchemaDefinition{
					DatabaseSchema: "CREATE DATABASE vt_testkeyspace",
					TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
						{
							Name: "t1",
						},
					},
				},
			},
			shouldErr: false,
		},
		// error cases
		{
			name: "no tablet",
			req: &vtctldatapb.GetSchemaRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "notfound",
					Uid:  100,
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "no schema",
			req: &vtctldatapb.GetSchemaRequest{
				TabletAlias: otherAlias,
			},
			expected:  nil,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setupSchema()

			resp, err := vtctld.GetSchema(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestGetShard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		topo      []*vtctldatapb.Shard
		topoError error
		req       *vtctldatapb.GetShardRequest
		expected  *vtctldatapb.GetShardResponse
		shouldErr bool
	}{
		{
			name: "success",
			topo: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			topoError: nil,
			req: &vtctldatapb.GetShardRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
			},
			expected: &vtctldatapb.GetShardResponse{
				Shard: &vtctldatapb.Shard{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						KeyRange:         &topodatapb.KeyRange{},
						IsPrimaryServing: true,
					},
				},
			},
			shouldErr: false,
		},
		{
			name:      "shard not found",
			topo:      nil,
			topoError: nil,
			req: &vtctldatapb.GetShardRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
			},
			shouldErr: true,
		},
		{
			name: "unavailable topo server",
			topo: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			topoError: assert.AnError,
			req:       &vtctldatapb.GetShardRequest{},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cells := []string{"zone1", "zone2", "zone3"}

			ctx := context.Background()
			ts, topofactory := memorytopo.NewServerAndFactory(cells...)
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			testutil.AddShards(ctx, t, ts, tt.topo...)

			if tt.topoError != nil {
				topofactory.SetError(tt.topoError)
			}

			resp, err := vtctld.GetShard(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestGetSrvKeyspaceNames(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		name               string
		srvKeyspacesByCell map[string]map[string]*topodatapb.SrvKeyspace
		topoError          error
		req                *vtctldatapb.GetSrvKeyspaceNamesRequest
		expected           *vtctldatapb.GetSrvKeyspaceNamesResponse
		shouldErr          bool
	}{
		{
			name: "success",
			srvKeyspacesByCell: map[string]map[string]*topodatapb.SrvKeyspace{
				"zone1": {
					"ks1": {},
					"ks2": {},
				},
				"zone2": {
					"ks1": {},
				},
			},
			req: &vtctldatapb.GetSrvKeyspaceNamesRequest{},
			expected: &vtctldatapb.GetSrvKeyspaceNamesResponse{
				Names: map[string]*vtctldatapb.GetSrvKeyspaceNamesResponse_NameList{
					"zone1": {
						Names: []string{"ks1", "ks2"},
					},
					"zone2": {
						Names: []string{"ks1"},
					},
				},
			},
		},
		{
			name: "cell filtering",
			srvKeyspacesByCell: map[string]map[string]*topodatapb.SrvKeyspace{
				"zone1": {
					"ks1": {},
					"ks2": {},
				},
				"zone2": {
					"ks1": {},
				},
			},
			req: &vtctldatapb.GetSrvKeyspaceNamesRequest{
				Cells: []string{"zone2"},
			},
			expected: &vtctldatapb.GetSrvKeyspaceNamesResponse{
				Names: map[string]*vtctldatapb.GetSrvKeyspaceNamesResponse_NameList{
					"zone2": {
						Names: []string{"ks1"},
					},
				},
			},
		},
		{
			name: "all cells topo down",
			srvKeyspacesByCell: map[string]map[string]*topodatapb.SrvKeyspace{
				"zone1": {
					"ks1": {},
					"ks2": {},
				},
				"zone2": {
					"ks1": {},
				},
			},
			req:       &vtctldatapb.GetSrvKeyspaceNamesRequest{},
			topoError: errors.New("topo down for testing"),
			shouldErr: true,
		},
		{
			name: "cell filtering topo down",
			srvKeyspacesByCell: map[string]map[string]*topodatapb.SrvKeyspace{
				"zone1": {
					"ks1": {},
					"ks2": {},
				},
				"zone2": {
					"ks1": {},
				},
			},
			req: &vtctldatapb.GetSrvKeyspaceNamesRequest{
				Cells: []string{"zone2"},
			},
			topoError: errors.New("topo down for testing"),
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cells := make([]string, 0, len(tt.srvKeyspacesByCell))
			for cell := range tt.srvKeyspacesByCell {
				cells = append(cells, cell)
			}

			ts, factory := memorytopo.NewServerAndFactory(cells...)

			for cell, srvKeyspaces := range tt.srvKeyspacesByCell {
				for ks, srvks := range srvKeyspaces {
					err := ts.UpdateSrvKeyspace(ctx, cell, ks, srvks)
					require.NoError(t, err, "UpdateSrvKeyspace(%s, %s, %+v) failed", cell, ks, srvks)
				}
			}

			if tt.topoError != nil {
				factory.SetError(tt.topoError)
			}

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			resp, err := vtctld.GetSrvKeyspaceNames(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			for _, names := range resp.Names {
				sort.Strings(names.Names)
			}
			for _, names := range tt.expected.Names {
				sort.Strings(names.Names)
			}
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestGetSrvKeyspaces(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		cells        []string
		srvKeyspaces []*testutil.SrvKeyspace
		topoErr      error
		req          *vtctldatapb.GetSrvKeyspacesRequest
		expected     *vtctldatapb.GetSrvKeyspacesResponse
		shouldErr    bool
	}{
		{
			name:  "success",
			cells: []string{"zone1", "zone2"},
			srvKeyspaces: []*testutil.SrvKeyspace{
				{
					Cell:        "zone1",
					Keyspace:    "testkeyspace",
					SrvKeyspace: &topodatapb.SrvKeyspace{},
				},
				{
					Cell:        "zone2",
					Keyspace:    "testkeyspace",
					SrvKeyspace: &topodatapb.SrvKeyspace{},
				},
			},
			req: &vtctldatapb.GetSrvKeyspacesRequest{
				Keyspace: "testkeyspace",
			},
			expected: &vtctldatapb.GetSrvKeyspacesResponse{
				SrvKeyspaces: map[string]*topodatapb.SrvKeyspace{
					"zone1": {},
					"zone2": {},
				},
			},
			shouldErr: false,
		},
		{
			name:  "filtering by cell",
			cells: []string{"zone1", "zone2"},
			srvKeyspaces: []*testutil.SrvKeyspace{
				{
					Cell:        "zone1",
					Keyspace:    "testkeyspace",
					SrvKeyspace: &topodatapb.SrvKeyspace{},
				},
				{
					Cell:        "zone2",
					Keyspace:    "testkeyspace",
					SrvKeyspace: &topodatapb.SrvKeyspace{},
				},
			},
			req: &vtctldatapb.GetSrvKeyspacesRequest{
				Keyspace: "testkeyspace",
				Cells:    []string{"zone1"},
			},
			expected: &vtctldatapb.GetSrvKeyspacesResponse{
				SrvKeyspaces: map[string]*topodatapb.SrvKeyspace{
					"zone1": {},
				},
			},
			shouldErr: false,
		},
		{
			name:  "no srvkeyspace for single cell",
			cells: []string{"zone1", "zone2"},
			srvKeyspaces: []*testutil.SrvKeyspace{
				{
					Cell:        "zone1",
					Keyspace:    "testkeyspace",
					SrvKeyspace: &topodatapb.SrvKeyspace{},
				},
			},
			req: &vtctldatapb.GetSrvKeyspacesRequest{
				Keyspace: "testkeyspace",
			},
			expected: &vtctldatapb.GetSrvKeyspacesResponse{
				SrvKeyspaces: map[string]*topodatapb.SrvKeyspace{
					"zone1": {},
					"zone2": nil,
				},
			},
			shouldErr: false,
		},
		{
			name:  "error getting cell names",
			cells: []string{"zone1"},
			srvKeyspaces: []*testutil.SrvKeyspace{
				{
					Cell:        "zone1",
					Keyspace:    "testkeyspace",
					SrvKeyspace: &topodatapb.SrvKeyspace{},
				},
			},
			topoErr: assert.AnError,
			req: &vtctldatapb.GetSrvKeyspacesRequest{
				Keyspace: "testkeyspace",
			},
			shouldErr: true,
		},
		{
			name:  "error getting srvkeyspace",
			cells: []string{"zone1"},
			srvKeyspaces: []*testutil.SrvKeyspace{
				{
					Cell:        "zone1",
					Keyspace:    "testkeyspace",
					SrvKeyspace: &topodatapb.SrvKeyspace{},
				},
			},
			topoErr: assert.AnError,
			req: &vtctldatapb.GetSrvKeyspacesRequest{
				Keyspace: "testkeyspace",
				Cells:    []string{"zone1"},
			},
			shouldErr: true,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.req == nil {
				t.SkipNow()
			}

			ts, topofactory := memorytopo.NewServerAndFactory(tt.cells...)

			testutil.AddSrvKeyspaces(t, ts, tt.srvKeyspaces...)
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			if tt.topoErr != nil {
				topofactory.SetError(tt.topoErr)
			}

			resp, err := vtctld.GetSrvKeyspaces(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestGetSrvVSchema(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ts, topofactory := memorytopo.NewServerAndFactory("zone1", "zone2")
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	zone1SrvVSchema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"testkeyspace": {
				Sharded:                true,
				RequireExplicitRouting: false,
			},
		},
		RoutingRules: &vschemapb.RoutingRules{
			Rules: []*vschemapb.RoutingRule{},
		},
	}
	zone2SrvVSchema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"testkeyspace": {
				Sharded:                true,
				RequireExplicitRouting: false,
			},
			"unsharded": {
				Sharded:                false,
				RequireExplicitRouting: false,
			},
		},
		RoutingRules: &vschemapb.RoutingRules{
			Rules: []*vschemapb.RoutingRule{},
		},
	}

	err := ts.UpdateSrvVSchema(ctx, "zone1", zone1SrvVSchema)
	require.NoError(t, err, "cannot add zone1 srv vschema")
	err = ts.UpdateSrvVSchema(ctx, "zone2", zone2SrvVSchema)
	require.NoError(t, err, "cannot add zone2 srv vschema")

	expected := &vschemapb.SrvVSchema{ // have to copy our structs because of proto marshal artifacts
		Keyspaces: map[string]*vschemapb.Keyspace{
			"testkeyspace": {
				Sharded:                true,
				RequireExplicitRouting: false,
			},
		},
		RoutingRules: &vschemapb.RoutingRules{
			Rules: []*vschemapb.RoutingRule{},
		},
	}
	resp, err := vtctld.GetSrvVSchema(ctx, &vtctldatapb.GetSrvVSchemaRequest{Cell: "zone1"})
	assert.NoError(t, err)
	utils.MustMatch(t, expected.Keyspaces, resp.SrvVSchema.Keyspaces, "GetSrvVSchema(zone1) mismatch")
	assert.ElementsMatch(t, expected.RoutingRules.Rules, resp.SrvVSchema.RoutingRules.Rules, "GetSrvVSchema(zone1) rules mismatch")

	expected = &vschemapb.SrvVSchema{ // have to copy our structs because of proto marshal artifacts
		Keyspaces: map[string]*vschemapb.Keyspace{
			"testkeyspace": {
				Sharded:                true,
				RequireExplicitRouting: false,
			},
			"unsharded": {
				Sharded:                false,
				RequireExplicitRouting: false,
			},
		},
		RoutingRules: &vschemapb.RoutingRules{
			Rules: []*vschemapb.RoutingRule{},
		},
	}
	resp, err = vtctld.GetSrvVSchema(ctx, &vtctldatapb.GetSrvVSchemaRequest{Cell: "zone2"})
	assert.NoError(t, err)
	utils.MustMatch(t, expected.Keyspaces, resp.SrvVSchema.Keyspaces, "GetSrvVSchema(zone2) mismatch")
	assert.ElementsMatch(t, expected.RoutingRules.Rules, resp.SrvVSchema.RoutingRules.Rules, "GetSrvVSchema(zone2) rules mismatch")

	resp, err = vtctld.GetSrvVSchema(ctx, &vtctldatapb.GetSrvVSchemaRequest{Cell: "dne"})
	assert.Error(t, err, "GetSrvVSchema(dne)")
	assert.Nil(t, resp, "GetSrvVSchema(dne)")

	topofactory.SetError(assert.AnError)
	_, err = vtctld.GetSrvVSchema(ctx, &vtctldatapb.GetSrvVSchemaRequest{Cell: "zone1"})
	assert.Error(t, err)
}

func TestGetSrvVSchemas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		req       *vtctldatapb.GetSrvVSchemasRequest
		expected  *vtctldatapb.GetSrvVSchemasResponse
		topoErr   error
		shouldErr bool
	}{
		{
			name: "success",
			req:  &vtctldatapb.GetSrvVSchemasRequest{},
			expected: &vtctldatapb.GetSrvVSchemasResponse{
				SrvVSchemas: map[string]*vschemapb.SrvVSchema{
					"zone1": {
						Keyspaces: map[string]*vschemapb.Keyspace{
							"testkeyspace": {
								Sharded:                true,
								RequireExplicitRouting: false,
							},
						},
						RoutingRules: &vschemapb.RoutingRules{
							Rules: []*vschemapb.RoutingRule{},
						},
					},
					"zone2": {
						Keyspaces: map[string]*vschemapb.Keyspace{
							"testkeyspace": {
								Sharded:                true,
								RequireExplicitRouting: false,
							},
							"unsharded": {
								Sharded:                false,
								RequireExplicitRouting: false,
							},
						},
						RoutingRules: &vschemapb.RoutingRules{
							Rules: []*vschemapb.RoutingRule{},
						},
					},
					"zone3": {},
				},
			},
		},
		{
			name: "filtering by cell",
			req: &vtctldatapb.GetSrvVSchemasRequest{
				Cells: []string{"zone2"},
			},
			expected: &vtctldatapb.GetSrvVSchemasResponse{
				SrvVSchemas: map[string]*vschemapb.SrvVSchema{
					"zone2": {
						Keyspaces: map[string]*vschemapb.Keyspace{
							"testkeyspace": {
								Sharded:                true,
								RequireExplicitRouting: false,
							},
							"unsharded": {
								Sharded:                false,
								RequireExplicitRouting: false,
							},
						},
						RoutingRules: &vschemapb.RoutingRules{
							Rules: []*vschemapb.RoutingRule{},
						},
					},
				},
			},
		},
		{
			name: "no SrvVSchema for single cell",
			req: &vtctldatapb.GetSrvVSchemasRequest{
				Cells: []string{"zone3"},
			},
			expected: &vtctldatapb.GetSrvVSchemasResponse{
				SrvVSchemas: map[string]*vschemapb.SrvVSchema{
					"zone3": {},
				},
			},
		},
		{
			name: "topology error",
			req: &vtctldatapb.GetSrvVSchemasRequest{
				Cells: []string{"zone2"},
			},
			topoErr:   assert.AnError,
			shouldErr: true,
		},
		{
			name: "cell doesn't exist",
			req: &vtctldatapb.GetSrvVSchemasRequest{
				Cells: []string{"doesnt-exist"},
			},
			expected: &vtctldatapb.GetSrvVSchemasResponse{
				SrvVSchemas: map[string]*vschemapb.SrvVSchema{},
			},
		},
		{
			name: "one of many cells doesn't exist",
			req: &vtctldatapb.GetSrvVSchemasRequest{
				Cells: []string{"zone1", "doesnt-exist"},
			},
			expected: &vtctldatapb.GetSrvVSchemasResponse{
				SrvVSchemas: map[string]*vschemapb.SrvVSchema{
					"zone1": {
						Keyspaces: map[string]*vschemapb.Keyspace{
							"testkeyspace": {
								Sharded:                true,
								RequireExplicitRouting: false,
							},
						},
						RoutingRules: &vschemapb.RoutingRules{
							Rules: []*vschemapb.RoutingRule{},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			ts, topofactory := memorytopo.NewServerAndFactory("zone1", "zone2", "zone3")
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			zone1SrvVSchema := &vschemapb.SrvVSchema{
				Keyspaces: map[string]*vschemapb.Keyspace{
					"testkeyspace": {
						Sharded:                true,
						RequireExplicitRouting: false,
					},
				},
				RoutingRules: &vschemapb.RoutingRules{
					Rules: []*vschemapb.RoutingRule{},
				},
			}

			zone2SrvVSchema := &vschemapb.SrvVSchema{
				Keyspaces: map[string]*vschemapb.Keyspace{
					"testkeyspace": {
						Sharded:                true,
						RequireExplicitRouting: false,
					},
					"unsharded": {
						Sharded:                false,
						RequireExplicitRouting: false,
					},
				},
				RoutingRules: &vschemapb.RoutingRules{
					Rules: []*vschemapb.RoutingRule{},
				},
			}

			err := ts.UpdateSrvVSchema(ctx, "zone1", zone1SrvVSchema)
			require.NoError(t, err, "cannot add zone1 srv vschema")
			err = ts.UpdateSrvVSchema(ctx, "zone2", zone2SrvVSchema)
			require.NoError(t, err, "cannot add zone2 srv vschema")

			if tt.topoErr != nil {
				topofactory.SetError(tt.topoErr)
			}

			resp, err := vtctld.GetSrvVSchemas(ctx, tt.req)

			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestGetTablet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  100,
		},
		Hostname: "localhost",
		Keyspace: "testkeyspace",
		Shard:    "-",
		Type:     topodatapb.TabletType_REPLICA,
	}

	testutil.AddTablet(ctx, t, ts, tablet, nil)

	resp, err := vtctld.GetTablet(ctx, &vtctldatapb.GetTabletRequest{
		TabletAlias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  100,
		},
	})
	assert.NoError(t, err)
	utils.MustMatch(t, resp.Tablet, tablet)

	// not found
	_, err = vtctld.GetTablet(ctx, &vtctldatapb.GetTabletRequest{
		TabletAlias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  101,
		},
	})
	assert.Error(t, err)
}

func TestGetTablets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cells     []string
		tablets   []*topodatapb.Tablet
		req       *vtctldatapb.GetTabletsRequest
		expected  []*topodatapb.Tablet
		shouldErr bool
	}{
		{
			name:      "no tablets",
			cells:     []string{"cell1"},
			tablets:   []*topodatapb.Tablet{},
			req:       &vtctldatapb.GetTabletsRequest{},
			expected:  []*topodatapb.Tablet{},
			shouldErr: false,
		},
		{
			name:  "keyspace and shard filter",
			cells: []string{"cell1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					Keyspace: "ks1",
					Shard:    "-80",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  101,
					},
					Keyspace: "ks1",
					Shard:    "80-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  102,
					},
					Keyspace: "ks2",
					Shard:    "-",
				},
			},
			req: &vtctldatapb.GetTabletsRequest{
				Keyspace: "ks1",
				Shard:    "80-",
			},
			expected: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  101,
					},
					Keyspace: "ks1",
					Shard:    "80-",
				},
			},
			shouldErr: false,
		},
		{
			name:  "keyspace filter",
			cells: []string{"cell1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					Keyspace: "ks1",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  101,
					},
					Keyspace: "ks1",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  102,
					},
					Keyspace: "otherkeyspace",
				},
			},
			req: &vtctldatapb.GetTabletsRequest{
				Keyspace: "ks1",
			},
			expected: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					Keyspace: "ks1",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  101,
					},
					Keyspace: "ks1",
				},
			},
			shouldErr: false,
		},
		{
			name:  "keyspace and shard filter - stale primary",
			cells: []string{"cell1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					Keyspace: "ks1",
					Shard:    "-80",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  101,
					},
					Keyspace: "ks1",
					Shard:    "80-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  102,
					},
					Keyspace:             "ks2",
					Shard:                "-",
					Type:                 topodatapb.TabletType_PRIMARY,
					PrimaryTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 15, 4, 5, 0, time.UTC)),
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  103,
					},
					Keyspace:             "ks2",
					Shard:                "-",
					Hostname:             "stale.primary",
					Type:                 topodatapb.TabletType_PRIMARY,
					PrimaryTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 14, 4, 5, 0, time.UTC)),
				},
			},
			req: &vtctldatapb.GetTabletsRequest{
				Keyspace: "ks2",
				Shard:    "-",
			},
			expected: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  102,
					},
					Keyspace:             "ks2",
					Shard:                "-",
					Type:                 topodatapb.TabletType_PRIMARY,
					PrimaryTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 15, 4, 5, 0, time.UTC)),
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  103,
					},
					Keyspace:             "ks2",
					Shard:                "-",
					Hostname:             "stale.primary",
					Type:                 topodatapb.TabletType_UNKNOWN,
					PrimaryTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 14, 4, 5, 0, time.UTC)),
				},
			},
			shouldErr: false,
		},
		{
			name:  "stale primary",
			cells: []string{"cell1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					Keyspace:             "ks1",
					Shard:                "-",
					Hostname:             "slightly less stale",
					Type:                 topodatapb.TabletType_PRIMARY,
					PrimaryTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 15, 4, 5, 0, time.UTC)),
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  101,
					},
					Hostname:             "stale primary",
					Keyspace:             "ks1",
					Shard:                "-",
					Type:                 topodatapb.TabletType_PRIMARY,
					PrimaryTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 14, 4, 5, 0, time.UTC)),
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  103,
					},
					Hostname:             "true primary",
					Keyspace:             "ks1",
					Shard:                "-",
					Type:                 topodatapb.TabletType_PRIMARY,
					PrimaryTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 16, 4, 5, 0, time.UTC)),
				},
			},
			req: &vtctldatapb.GetTabletsRequest{},
			expected: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					Keyspace:             "ks1",
					Shard:                "-",
					Hostname:             "slightly less stale",
					Type:                 topodatapb.TabletType_UNKNOWN,
					PrimaryTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 15, 4, 5, 0, time.UTC)),
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  101,
					},
					Hostname:             "stale primary",
					Keyspace:             "ks1",
					Shard:                "-",
					Type:                 topodatapb.TabletType_UNKNOWN,
					PrimaryTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 14, 4, 5, 0, time.UTC)),
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  103,
					},
					Hostname:             "true primary",
					Keyspace:             "ks1",
					Shard:                "-",
					Type:                 topodatapb.TabletType_PRIMARY,
					PrimaryTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 16, 4, 5, 0, time.UTC)),
				},
			},
			shouldErr: false,
		},
		{
			name:    "keyspace and shard filter - error",
			cells:   []string{"cell1"},
			tablets: []*topodatapb.Tablet{},
			req: &vtctldatapb.GetTabletsRequest{
				Keyspace: "ks1",
				Shard:    "-",
			},
			expected:  []*topodatapb.Tablet{},
			shouldErr: true,
		},
		{
			name:  "cells filter",
			cells: []string{"cell1", "cell2", "cell3"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell2",
						Uid:  200,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell3",
						Uid:  300,
					},
				},
			},
			req: &vtctldatapb.GetTabletsRequest{
				Cells: []string{"cell1", "cell3"},
			},
			expected: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell3",
						Uid:  300,
					},
				},
			},
			shouldErr: false,
		},
		{
			name:  "cells filter with single error is nonfatal",
			cells: []string{"cell1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					Keyspace: "ks1",
					Shard:    "-",
				},
			},
			req: &vtctldatapb.GetTabletsRequest{
				Cells: []string{"cell1", "doesnotexist"},
			},
			expected: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					Keyspace: "ks1",
					Shard:    "-",
				},
			},
			shouldErr: false,
		},
		{
			name:  "cells filter with single error is fatal in strict mode",
			cells: []string{"cell1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					Keyspace: "ks1",
					Shard:    "-",
				},
			},
			req: &vtctldatapb.GetTabletsRequest{
				Cells:  []string{"cell1", "doesnotexist"},
				Strict: true,
			},
			shouldErr: true,
		},
		{
			name:  "in nonstrict mode if all cells fail the request fails",
			cells: []string{"cell1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					Keyspace: "ks1",
					Shard:    "-",
				},
			},
			req: &vtctldatapb.GetTabletsRequest{
				Cells: []string{"doesnotexist", "alsodoesnotexist"},
			},
			shouldErr: true,
		},
		{
			name:  "tablet alias filtering",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			req: &vtctldatapb.GetTabletsRequest{
				TabletAliases: []*topodatapb.TabletAlias{
					{
						Cell: "zone1",
						Uid:  100,
					},
					{
						// This tablet doesn't exist, but doesn't cause a failure.
						Cell: "zone404",
						Uid:  404,
					},
				},
				// The below filters are ignored, because TabletAliases always
				// takes precedence.
				Keyspace: "another_keyspace",
				Shard:    "-80",
				Cells:    []string{"zone404"},
			},
			expected: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			shouldErr: false,
		},
		{
			name:    "tablet alias filter with none found",
			tablets: []*topodatapb.Tablet{},
			req: &vtctldatapb.GetTabletsRequest{
				TabletAliases: []*topodatapb.TabletAlias{
					{
						Cell: "zone1",
						Uid:  101,
					},
				},
			},
			expected:  []*topodatapb.Tablet{},
			shouldErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			ts := memorytopo.NewServer(tt.cells...)
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			testutil.AddTablets(ctx, t, ts, nil, tt.tablets...)

			resp, err := vtctld.GetTablets(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			testutil.AssertSameTablets(t, tt.expected, resp.Tablets)
		})
	}
}

func TestGetTopologyPath(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2", "cell3")
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	err := ts.CreateKeyspace(ctx, "keyspace1", &topodatapb.Keyspace{})
	require.NoError(t, err)

	testutil.AddTablets(ctx, t, ts, nil, &topodatapb.Tablet{
		Alias:         &topodatapb.TabletAlias{Cell: "cell1", Uid: 100},
		Hostname:      "localhost",
		Keyspace:      "keyspace1",
		MysqlHostname: "localhost",
		MysqlPort:     17100,
	})
	require.NoError(t, err)

	tests := []struct {
		name      string
		path      string
		shouldErr bool
		expected  *vtctldatapb.GetTopologyPathResponse
	}{
		{
			name: "root path",
			path: "/",
			expected: &vtctldatapb.GetTopologyPathResponse{
				Cell: &vtctldatapb.TopologyCell{
					Path:     "/",
					Children: []string{"global", "cell1", "cell2", "cell3"},
				},
			},
		},
		{
			name:      "invalid path",
			path:      "",
			shouldErr: true,
		},
		{
			name: "global path",
			path: "/global",
			expected: &vtctldatapb.GetTopologyPathResponse{
				Cell: &vtctldatapb.TopologyCell{
					Name:     "global",
					Path:     "/global",
					Children: []string{"cells", "keyspaces"},
				},
			},
		},
		{
			name: "terminal data path",
			path: "/cell1/tablets/cell1-0000000100/Tablet",
			expected: &vtctldatapb.GetTopologyPathResponse{
				Cell: &vtctldatapb.TopologyCell{
					Name: "Tablet",
					Path: "/cell1/tablets/cell1-0000000100/Tablet",
					Data: "alias:{cell:\"cell1\" uid:100} hostname:\"localhost\" keyspace:\"keyspace1\" mysql_hostname:\"localhost\" mysql_port:17100",
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			resp, err := vtctld.GetTopologyPath(ctx, &vtctldatapb.GetTopologyPathRequest{
				Path: tt.path,
			})

			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestGetVSchema(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ts := memorytopo.NewServer("zone1")
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	t.Run("found", func(t *testing.T) {
		t.Parallel()

		err := ts.SaveVSchema(ctx, "testkeyspace", &vschemapb.Keyspace{
			Sharded: true,
			Vindexes: map[string]*vschemapb.Vindex{
				"v1": {
					Type: "hash",
				},
			},
		})
		require.NoError(t, err)

		expected := &vtctldatapb.GetVSchemaResponse{
			VSchema: &vschemapb.Keyspace{
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"v1": {
						Type: "hash",
					},
				},
			},
		}

		resp, err := vtctld.GetVSchema(ctx, &vtctldatapb.GetVSchemaRequest{
			Keyspace: "testkeyspace",
		})
		assert.NoError(t, err)
		utils.MustMatch(t, expected, resp)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		_, err := vtctld.GetVSchema(ctx, &vtctldatapb.GetVSchemaRequest{
			Keyspace: "doesnotexist",
		})
		assert.Error(t, err)
	})
}

func TestPingTablet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ts := memorytopo.NewServer("zone1")
	testutil.AddTablet(ctx, t, ts, &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone1",
			Uid:  100,
		},
		Keyspace: "testkeyspace",
		Shard:    "-",
	}, nil)

	tests := []struct {
		name      string
		tmc       testutil.TabletManagerClient
		req       *vtctldatapb.PingTabletRequest
		expected  *vtctldatapb.PingTabletResponse
		shouldErr bool
	}{
		{
			name: "ok",
			tmc: testutil.TabletManagerClient{
				PingResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			req: &vtctldatapb.PingTabletRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected: &vtctldatapb.PingTabletResponse{},
		},
		{
			name: "tablet not found",
			tmc: testutil.TabletManagerClient{
				PingResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			req: &vtctldatapb.PingTabletRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone2",
					Uid:  404,
				},
			},
			shouldErr: true,
		},
		{
			name: "ping rpc error",
			tmc: testutil.TabletManagerClient{
				PingResults: map[string]error{
					"zone1-0000000100": assert.AnError,
				},
			},
			req: &vtctldatapb.PingTabletRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, &tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			resp, err := vtctld.PingTablet(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				assert.Nil(t, resp)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, resp)
		})
	}
}

func TestPlannedReparentShard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ts      *topo.Server
		tmc     tmclient.TabletManagerClient
		tablets []*topodatapb.Tablet

		req                 *vtctldatapb.PlannedReparentShardRequest
		expected            *vtctldatapb.PlannedReparentShardResponse
		expectEventsToOccur bool
		shouldErr           bool
	}{
		{
			name: "successful reparent",
			ts:   memorytopo.NewServer("zone1"),
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_PRIMARY,
					PrimaryTermStartTime: &vttime.Time{
						Seconds: 100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Type:     topodatapb.TabletType_RDONLY,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			tmc: &testutil.TabletManagerClient{
				DemotePrimaryResults: map[string]struct {
					Status *replicationdatapb.PrimaryStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.PrimaryStatus{
							Position: "primary-demotion position",
						},
						Error: nil,
					},
				},
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "doesn't matter",
						Error:    nil,
					},
				},
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000200": nil,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000200": {
						Result: "promotion position",
						Error:  nil,
					},
				},
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000200": nil, // waiting for primary-position during promotion
					// reparent SetReplicationSource calls
					"zone1-0000000100": nil,
					"zone1-0000000101": nil,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000200": {
						"primary-demotion position": nil,
					},
				},
			},
			req: &vtctldatapb.PlannedReparentShardRequest{
				Keyspace: "testkeyspace",
				Shard:    "-",
				NewPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
				WaitReplicasTimeout: protoutil.DurationToProto(time.Millisecond * 10),
			},
			expected: &vtctldatapb.PlannedReparentShardResponse{
				Keyspace: "testkeyspace",
				Shard:    "-",
				PromotedPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},
			expectEventsToOccur: true,
			shouldErr:           false,
		},
		{
			// Note: this is testing the error-handling done in
			// (*VtctldServer).PlannedReparentShard, not the logic of an PRS.
			// That logic is tested in reparentutil, and not here. Therefore,
			// the simplest way to trigger a failure is to attempt an PRS on a
			// shard that does not exist.
			name:    "failed reparent",
			ts:      memorytopo.NewServer("zone1"),
			tablets: nil,
			req: &vtctldatapb.PlannedReparentShardRequest{
				Keyspace: "testkeyspace",
				Shard:    "-",
			},
			expectEventsToOccur: false,
			shouldErr:           true,
		},
		{
			name: "invalid WaitReplicasTimeout",
			req: &vtctldatapb.PlannedReparentShardRequest{
				WaitReplicasTimeout: &vttime.Duration{
					Seconds: -1,
					Nanos:   1,
				},
			},
			shouldErr: true,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			testutil.AddTablets(ctx, t, tt.ts, &testutil.AddTabletOptions{
				AlsoSetShardPrimary:  true,
				ForceSetShardPrimary: true,
				SkipShardCreation:    false,
			}, tt.tablets...)

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, tt.ts, tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			resp, err := vtctld.PlannedReparentShard(ctx, tt.req)

			// We defer this because we want to check in both error and non-
			// error cases, but after the main set of assertions for those
			// cases.
			defer func() {
				if !tt.expectEventsToOccur {
					testutil.AssertNoLogutilEventsOccurred(t, resp, "expected no events to occur during ERS")

					return
				}

				testutil.AssertLogutilEventsOccurred(t, resp, "expected events to occur during ERS")
			}()

			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			testutil.AssertPlannedReparentShardResponsesEqual(t, tt.expected, resp)
		})
	}
}

func TestRebuildKeyspaceGraph(t *testing.T) {
	t.Parallel()

	t.Run("ok", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		ts := memorytopo.NewServer("zone1")
		testutil.AddKeyspace(ctx, t, ts, &vtctldatapb.Keyspace{
			Name: "testkeyspace",
		})
		vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
			return NewVtctldServer(ts)
		})

		_, err := vtctld.RebuildKeyspaceGraph(ctx, &vtctldatapb.RebuildKeyspaceGraphRequest{
			Keyspace: "testkeyspace",
		})
		assert.NoError(t, err)
	})

	t.Run("no such keyspace", func(t *testing.T) {
		t.Parallel()

		ts := memorytopo.NewServer("zone1")
		vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
			return NewVtctldServer(ts)
		})

		_, err := vtctld.RebuildKeyspaceGraph(context.Background(), &vtctldatapb.RebuildKeyspaceGraphRequest{
			Keyspace: "testkeyspace",
		})
		assert.Error(t, err)
	})

	t.Run("topo unavailable", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		ts, factory := memorytopo.NewServerAndFactory("zone1")
		testutil.AddKeyspace(ctx, t, ts, &vtctldatapb.Keyspace{
			Name: "testkeyspace",
		})
		vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
			return NewVtctldServer(ts)
		})
		factory.SetError(assert.AnError)

		_, err := vtctld.RebuildKeyspaceGraph(ctx, &vtctldatapb.RebuildKeyspaceGraphRequest{
			Keyspace: "testkeyspace",
		})
		assert.Error(t, err)
	})

	t.Run("lock error", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		ts := memorytopo.NewServer("zone1")
		testutil.AddKeyspace(ctx, t, ts, &vtctldatapb.Keyspace{
			Name: "testkeyspace",
		})
		vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
			return NewVtctldServer(ts)
		})

		_, unlock, lerr := ts.LockKeyspace(context.Background(), "testkeyspace", "test lock")
		require.NoError(t, lerr, "could not lock keyspace for testing")

		defer unlock(&lerr)
		defer func() { require.NoError(t, lerr, "could not unlock testkeyspace after test") }()

		ctx, cancel := context.WithTimeout(ctx, time.Millisecond*50)
		defer cancel()
		_, err := vtctld.RebuildKeyspaceGraph(ctx, &vtctldatapb.RebuildKeyspaceGraphRequest{
			Keyspace: "testkeyspace",
		})
		assert.Error(t, err)
	})
}

func TestRebuildVSchemaGraph(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	req := &vtctldatapb.RebuildVSchemaGraphRequest{}
	tests := []struct {
		name      string
		topoDown  bool
		shouldErr bool
	}{
		{
			name: "success",
		},
		{
			name:      "topo down",
			topoDown:  true,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts, factory := memorytopo.NewServerAndFactory("zone1")
			if tt.topoDown {
				factory.SetError(errors.New("topo down for testing"))
			}

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			_, err := vtctld.RebuildVSchemaGraph(ctx, req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestRefreshState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		name              string
		ts                *topo.Server
		tablet            *topodatapb.Tablet
		refreshStateError error
		req               *vtctldatapb.RefreshStateRequest
		shouldErr         bool
	}{
		{
			name: "success",
			ts:   memorytopo.NewServer("zone1"),
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			refreshStateError: nil,
			req: &vtctldatapb.RefreshStateRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
		},
		{
			name:      "tablet alias nil",
			ts:        memorytopo.NewServer(),
			req:       &vtctldatapb.RefreshStateRequest{},
			shouldErr: true,
		},
		{
			name: "tablet not found",
			ts:   memorytopo.NewServer("zone1"),
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			refreshStateError: nil,
			req: &vtctldatapb.RefreshStateRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  400,
				},
			},
			shouldErr: true,
		},
		{
			name: "RefreshState failed",
			ts:   memorytopo.NewServer("zone1"),
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			refreshStateError: fmt.Errorf("%w: RefreshState failed", assert.AnError),
			req: &vtctldatapb.RefreshStateRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var tmc testutil.TabletManagerClient
			if tt.tablet != nil {
				testutil.AddTablet(ctx, t, tt.ts, tt.tablet, nil)
				tmc.RefreshStateResults = map[string]error{
					topoproto.TabletAliasString(tt.tablet.Alias): tt.refreshStateError,
				}
			}

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, tt.ts, &tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			_, err := vtctld.RefreshState(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestRefreshStateByShard(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		name               string
		ts                 *topo.Server
		tablets            []*topodatapb.Tablet
		refreshStateErrors []error // must have len(tablets)
		req                *vtctldatapb.RefreshStateByShardRequest
		expected           *vtctldatapb.RefreshStateByShardResponse
		shouldErr          bool
	}{
		{
			name: "success",
			ts:   memorytopo.NewServer("zone1", "zone2"),
			tablets: []*topodatapb.Tablet{
				{
					Hostname: "zone1-100",
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "ks",
					Shard:    "-",
				},
				{
					Hostname: "zone2-100",
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  100,
					},
					Keyspace: "ks",
					Shard:    "-",
				},
			},
			refreshStateErrors: []error{
				nil, // zone1-100
				nil, // zone2-100
			},
			req: &vtctldatapb.RefreshStateByShardRequest{
				Keyspace: "ks",
				Shard:    "-",
			},
			expected: &vtctldatapb.RefreshStateByShardResponse{},
		},
		{
			name: "cell filtering",
			ts:   memorytopo.NewServer("zone1", "zone2"),
			tablets: []*topodatapb.Tablet{
				{
					Hostname: "zone1-100",
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "ks",
					Shard:    "-",
				},
				{
					Hostname: "zone2-100",
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  100,
					},
					Keyspace: "ks",
					Shard:    "-",
				},
			},
			refreshStateErrors: []error{
				nil,
				fmt.Errorf("%w: RefreshState failed on zone2-100", assert.AnError),
			},
			req: &vtctldatapb.RefreshStateByShardRequest{
				Keyspace: "ks",
				Shard:    "-",
				Cells:    []string{"zone1"}, // If we didn't filter, we would get IsPartialRefresh=true because of the failure in zone2.
			},
			expected: &vtctldatapb.RefreshStateByShardResponse{
				IsPartialRefresh:      false,
				PartialRefreshDetails: "",
			},
			shouldErr: false,
		},
		{
			name: "partial result",
			ts:   memorytopo.NewServer("zone1", "zone2"),
			tablets: []*topodatapb.Tablet{
				{
					Hostname: "zone1-100",
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "ks",
					Shard:    "-",
				},
				{
					Hostname: "zone2-100",
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  100,
					},
					Keyspace: "ks",
					Shard:    "-",
				},
			},
			refreshStateErrors: []error{
				nil,
				fmt.Errorf("%w: RefreshState failed on zone2-100", assert.AnError),
			},
			req: &vtctldatapb.RefreshStateByShardRequest{
				Keyspace: "ks",
				Shard:    "-",
			},
			expected: &vtctldatapb.RefreshStateByShardResponse{
				IsPartialRefresh:      true,
				PartialRefreshDetails: "failed to refresh tablet zone2-0000000100: assert.AnError general error for testing: RefreshState failed on zone2-100",
			},
			shouldErr: false,
		},
		{
			name:      "missing keyspace argument",
			ts:        memorytopo.NewServer(),
			req:       &vtctldatapb.RefreshStateByShardRequest{},
			shouldErr: true,
		},
		{
			name: "missing shard argument",
			ts:   memorytopo.NewServer(),
			req: &vtctldatapb.RefreshStateByShardRequest{
				Keyspace: "ks",
			},
			shouldErr: true,
		},
		{
			name: "shard not found",
			ts:   memorytopo.NewServer("zone1"),
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "ks",
					Shard:    "-80",
				},
			},
			refreshStateErrors: []error{nil},
			req: &vtctldatapb.RefreshStateByShardRequest{
				Keyspace: "ks2",
				Shard:    "-",
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, len(tt.tablets), len(tt.refreshStateErrors), "Invalid test case: must have one refreshStateError for each tablet")

			tmc := &testutil.TabletManagerClient{
				RefreshStateResults: make(map[string]error, len(tt.tablets)),
			}
			testutil.AddTablets(ctx, t, tt.ts, nil, tt.tablets...)
			for i, tablet := range tt.tablets {
				key := topoproto.TabletAliasString(tablet.Alias)
				tmc.RefreshStateResults[key] = tt.refreshStateErrors[i]
			}

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, tt.ts, tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			resp, err := vtctld.RefreshStateByShard(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestReloadSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tablets   []*topodatapb.Tablet
		tmc       testutil.TabletManagerClient
		req       *vtctldatapb.ReloadSchemaRequest
		shouldErr bool
	}{
		{
			name: "ok",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			tmc: testutil.TabletManagerClient{
				ReloadSchemaResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			req: &vtctldatapb.ReloadSchemaRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
		},
		{
			name: "tablet not found",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			tmc: testutil.TabletManagerClient{
				ReloadSchemaResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			req: &vtctldatapb.ReloadSchemaRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  404,
				},
			},
			shouldErr: true,
		},
		{
			name: "tmc failure",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			tmc: testutil.TabletManagerClient{
				ReloadSchemaResults: map[string]error{
					"zone1-0000000100": assert.AnError,
				},
			},
			req: &vtctldatapb.ReloadSchemaRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			shouldErr: true,
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			ts := memorytopo.NewServer("zone1")
			testutil.AddTablets(ctx, t, ts, nil, tt.tablets...)

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, &tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			_, err := vtctld.ReloadSchema(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestReloadSchemaKeyspace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tablets   []*topodatapb.Tablet
		tmc       testutil.TabletManagerClient
		req       *vtctldatapb.ReloadSchemaKeyspaceRequest
		expected  *vtctldatapb.ReloadSchemaKeyspaceResponse
		shouldErr bool
	}{
		{
			name: "ok",
			tablets: []*topodatapb.Tablet{
				{
					Keyspace: "ks1",
					Shard:    "-80",
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_PRIMARY,
				},
				{
					Keyspace: "ks1",
					Shard:    "-80",
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type: topodatapb.TabletType_REPLICA,
				},
				{
					Keyspace: "ks1",
					Shard:    "80-",
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Type: topodatapb.TabletType_PRIMARY,
				},
				{
					Keyspace: "ks1",
					Shard:    "80-",
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  201,
					},
					Type: topodatapb.TabletType_REPLICA,
				},
			},
			tmc: testutil.TabletManagerClient{
				ReloadSchemaResults: map[string]error{
					"zone2-0000000200": nil,
					"zone2-0000000201": nil,
				},
			},
			req: &vtctldatapb.ReloadSchemaKeyspaceRequest{
				Keyspace: "ks1",
			},
			expected: &vtctldatapb.ReloadSchemaKeyspaceResponse{},
		},
		{
			name: "keyspace not found",
			req: &vtctldatapb.ReloadSchemaKeyspaceRequest{
				Keyspace: "ks1",
			},
			shouldErr: true,
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := memorytopo.NewServer("zone1", "zone2", "zone3")
			testutil.AddTablets(ctx, t, ts, &testutil.AddTabletOptions{
				AlsoSetShardPrimary: true,
			}, tt.tablets...)

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, &tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			resp, err := vtctld.ReloadSchemaKeyspace(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			// ReloadSchemaKeyspace does each shard concurrently, so we sort
			// here to reduce flakes.
			sort.Sort(testutil.EventValueSorter(resp.Events))
			sort.Sort(testutil.EventValueSorter(tt.expected.Events))

			testutil.AssertLogutilEventsMatch(t, tt.expected.Events, resp.Events)
		})
	}
}

func TestReloadSchemaShard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tablets   []*topodatapb.Tablet
		tmc       testutil.TabletManagerClient
		req       *vtctldatapb.ReloadSchemaShardRequest
		expected  *vtctldatapb.ReloadSchemaShardResponse
		shouldErr bool
	}{
		{
			name: "ok",
			tablets: []*topodatapb.Tablet{
				{
					Keyspace: "ks1",
					Shard:    "-",
					Type:     topodatapb.TabletType_PRIMARY,
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
				{
					Keyspace: "ks1",
					Shard:    "-",
					Type:     topodatapb.TabletType_REPLICA,
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
				},
				{
					Keyspace: "ks1",
					Shard:    "-",
					Type:     topodatapb.TabletType_REPLICA,
					Alias: &topodatapb.TabletAlias{
						Cell: "zone3",
						Uid:  300,
					},
				},
			},
			tmc: testutil.TabletManagerClient{
				ReloadSchemaResults: map[string]error{
					"zone1-0000000100": assert.AnError, // without IncludePrimary this is fine.
					"zone2-0000000200": nil,
					"zone3-0000000300": nil,
				},
			},
			req: &vtctldatapb.ReloadSchemaShardRequest{
				Keyspace: "ks1",
				Shard:    "-",
			},
			expected:  &vtctldatapb.ReloadSchemaShardResponse{},
			shouldErr: false,
		},
		{
			name: "shard not found",
			req: &vtctldatapb.ReloadSchemaShardRequest{
				Keyspace: "ks1",
				Shard:    "-",
			},
			expected: &vtctldatapb.ReloadSchemaShardResponse{
				Events: []*logutilpb.Event{
					{
						Value: `ReloadSchemaShard\(ks1/-\) failed to load tablet list, will not reload schema \(use vtctl ReloadSchemaShard to try again\):.*$`,
					},
				},
			},
		},
		{
			name: "include primary, with failure",
			tablets: []*topodatapb.Tablet{
				{
					Keyspace: "ks1",
					Shard:    "-",
					Type:     topodatapb.TabletType_PRIMARY,
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
				{
					Keyspace: "ks1",
					Shard:    "-",
					Type:     topodatapb.TabletType_REPLICA,
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
				},
				{
					Keyspace: "ks1",
					Shard:    "-",
					Type:     topodatapb.TabletType_REPLICA,
					Alias: &topodatapb.TabletAlias{
						Cell: "zone3",
						Uid:  300,
					},
				},
			},
			tmc: testutil.TabletManagerClient{
				ReloadSchemaResults: map[string]error{
					"zone1-0000000100": assert.AnError, // with IncludePrimary this triggers an event
					"zone2-0000000200": nil,
					"zone3-0000000300": nil,
				},
			},
			req: &vtctldatapb.ReloadSchemaShardRequest{
				Keyspace:       "ks1",
				Shard:          "-",
				IncludePrimary: true,
			},
			expected: &vtctldatapb.ReloadSchemaShardResponse{
				Events: []*logutilpb.Event{
					{
						Value: "Failed to reload schema on replica tablet zone1-0000000100 in ks1/-.*$",
					},
				},
			},
			shouldErr: false,
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := memorytopo.NewServer("zone1", "zone2", "zone3")
			testutil.AddTablets(ctx, t, ts, &testutil.AddTabletOptions{
				AlsoSetShardPrimary: true,
			}, tt.tablets...)

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, &tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			resp, err := vtctld.ReloadSchemaShard(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			testutil.AssertLogutilEventsMatch(t, tt.expected.Events, resp.Events)
		})
	}
}

func TestRemoveBackup(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer()
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	setup := func() {
		testutil.BackupStorage.Backups = map[string][]string{
			"testkeyspace/-": {"backup1", "backup2", "backup3"},
		}
	}

	t.Run("ok", func(t *testing.T) {
		setup()
		_, err := vtctld.RemoveBackup(ctx, &vtctldatapb.RemoveBackupRequest{
			Keyspace: "testkeyspace",
			Shard:    "-",
			Name:     "backup2",
		})

		assert.NoError(t, err)

		resp, err := vtctld.GetBackups(ctx, &vtctldatapb.GetBackupsRequest{
			Keyspace: "testkeyspace",
			Shard:    "-",
		})
		require.NoError(t, err)

		var backupNames []string
		for _, bi := range resp.Backups {
			backupNames = append(backupNames, bi.Name)
		}
		utils.MustMatch(t, []string{"backup1", "backup3"}, backupNames, "expected \"backup2\" to be removed")
	})

	t.Run("no bucket found", func(t *testing.T) {
		setup()
		_, err := vtctld.RemoveBackup(ctx, &vtctldatapb.RemoveBackupRequest{
			Keyspace: "notfound",
			Shard:    "-",
			Name:     "somebackup",
		})
		assert.Error(t, err)
	})

	t.Run("no backup found in bucket", func(t *testing.T) {
		setup()
		_, err := vtctld.RemoveBackup(ctx, &vtctldatapb.RemoveBackupRequest{
			Keyspace: "testkeyspace",
			Shard:    "-",
			Name:     "notfound",
		})
		assert.Error(t, err)
	})
}

func TestRemoveKeyspaceCell(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                    string
		keyspace                *vtctldatapb.Keyspace
		shards                  []*vtctldatapb.Shard
		topoError               error
		topoIsLocked            bool
		srvKeyspaceDoesNotExist bool
		req                     *vtctldatapb.RemoveKeyspaceCellRequest
		expected                *vtctldatapb.RemoveKeyspaceCellResponse
		shouldErr               bool
	}{
		{
			name:     "success",
			keyspace: nil,
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			topoError:               nil,
			topoIsLocked:            false,
			srvKeyspaceDoesNotExist: false,
			req: &vtctldatapb.RemoveKeyspaceCellRequest{
				Keyspace: "testkeyspace",
				Cell:     "zone1",
			},
			expected:  &vtctldatapb.RemoveKeyspaceCellResponse{},
			shouldErr: false,
		},
		{
			name: "success/empty keyspace",
			keyspace: &vtctldatapb.Keyspace{
				Name:     "testkeyspace",
				Keyspace: &topodatapb.Keyspace{},
			},
			shards:                  nil,
			topoError:               nil,
			topoIsLocked:            false,
			srvKeyspaceDoesNotExist: false,
			req: &vtctldatapb.RemoveKeyspaceCellRequest{
				Keyspace: "testkeyspace",
				Cell:     "zone1",
			},
			expected:  &vtctldatapb.RemoveKeyspaceCellResponse{},
			shouldErr: false,
		},
		{
			name: "keyspace not found",
			keyspace: &vtctldatapb.Keyspace{
				Name:     "otherkeyspace",
				Keyspace: &topodatapb.Keyspace{},
			},
			shards:                  nil,
			topoError:               nil,
			topoIsLocked:            false,
			srvKeyspaceDoesNotExist: false,
			req: &vtctldatapb.RemoveKeyspaceCellRequest{
				Keyspace: "testkeyspace",
				Cell:     "zone1",
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name:     "topo is down",
			keyspace: nil,
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			topoError:               assert.AnError,
			topoIsLocked:            false,
			srvKeyspaceDoesNotExist: false,
			req: &vtctldatapb.RemoveKeyspaceCellRequest{
				Keyspace: "testkeyspace",
				Cell:     "zone1",
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name:     "topo is locked",
			keyspace: nil,
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			topoError:               nil,
			topoIsLocked:            true,
			srvKeyspaceDoesNotExist: false,
			req: &vtctldatapb.RemoveKeyspaceCellRequest{
				Keyspace: "testkeyspace",
				Cell:     "zone1",
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name:     "srvkeyspace already deleted",
			keyspace: nil,
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			topoError:               nil,
			topoIsLocked:            false,
			srvKeyspaceDoesNotExist: true,
			req: &vtctldatapb.RemoveKeyspaceCellRequest{
				Keyspace: "testkeyspace",
				Cell:     "zone1",
			},
			expected:  nil,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cells := []string{"zone1", "zone2", "zone3"}

			ctx := context.Background()
			ts, topofactory := memorytopo.NewServerAndFactory(cells...)
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			// Setup topo
			if tt.keyspace != nil {
				testutil.AddKeyspace(ctx, t, ts, tt.keyspace)
			}

			testutil.AddShards(ctx, t, ts, tt.shards...)

			// For certain tests, we don't actually create the SrvKeyspace
			// object.
			if !tt.srvKeyspaceDoesNotExist {
				updateSrvKeyspace := func(keyspace string) {
					for _, cell := range cells {
						err := ts.UpdateSrvKeyspace(ctx, cell, keyspace, &topodatapb.SrvKeyspace{})
						require.NoError(t, err, "could not create empty SrvKeyspace for keyspace %s in cell %s", tt.req.Keyspace, cell)
					}
				}

				updateSrvKeyspace(tt.req.Keyspace)
				if tt.keyspace != nil {
					updateSrvKeyspace(tt.keyspace.Name)
				}
			}

			// Set errors and locks
			if tt.topoError != nil {
				topofactory.SetError(tt.topoError)
			}

			if tt.topoIsLocked {
				lctx, unlock, err := ts.LockKeyspace(ctx, tt.req.Keyspace, "testing locked keyspace")
				require.NoError(t, err, "cannot lock keyspace %s", tt.req.Keyspace)
				defer unlock(&err)

				ctx = lctx
			}

			resp, err := vtctld.RemoveKeyspaceCell(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestRemoveShardCell(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		servingCells      []string
		shards            []*vtctldatapb.Shard
		replicationGraphs []*topo.ShardReplicationInfo
		topoError         error
		topoIsLocked      bool
		req               *vtctldatapb.RemoveShardCellRequest
		expected          *vtctldatapb.RemoveShardCellResponse
		shouldErr         bool
	}{
		{
			name: "success",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			replicationGraphs: []*topo.ShardReplicationInfo{
				topo.NewShardReplicationInfo(&topodatapb.ShardReplication{
					Nodes: []*topodatapb.ShardReplication_Node{
						{
							TabletAlias: &topodatapb.TabletAlias{
								Cell: "zone1",
								Uid:  100,
							},
						},
					},
				}, "zone1", "testkeyspace", "-"),
				topo.NewShardReplicationInfo(&topodatapb.ShardReplication{
					Nodes: []*topodatapb.ShardReplication_Node{
						{
							TabletAlias: &topodatapb.TabletAlias{
								Cell: "zone2",
								Uid:  200,
							},
						},
					},
				}, "zone2", "testkeyspace", "-"),
				topo.NewShardReplicationInfo(&topodatapb.ShardReplication{
					Nodes: []*topodatapb.ShardReplication_Node{
						{
							TabletAlias: &topodatapb.TabletAlias{
								Cell: "zone3",
								Uid:  300,
							},
						},
					},
				}, "zone3", "testkeyspace", "-"),
			},
			req: &vtctldatapb.RemoveShardCellRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
				Cell:      "zone2",
				Recursive: true,
			},
			expected:  &vtctldatapb.RemoveShardCellResponse{},
			shouldErr: false,
		},
		{
			name: "success/no tablets",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			req: &vtctldatapb.RemoveShardCellRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
				Cell:      "zone2",
			},
			expected:  &vtctldatapb.RemoveShardCellResponse{},
			shouldErr: false,
		},
		{
			name:              "nonexistent shard",
			shards:            nil,
			replicationGraphs: nil,
			req: &vtctldatapb.RemoveShardCellRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
				Cell:      "zone2",
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "cell does not exist",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			req: &vtctldatapb.RemoveShardCellRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
				Cell:      "fourthzone",
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "cell not in serving list",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			servingCells:      []string{"zone1"},
			replicationGraphs: nil,
			req: &vtctldatapb.RemoveShardCellRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
				Cell:      "zone2",
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "tablets/non-recursive",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			replicationGraphs: []*topo.ShardReplicationInfo{
				topo.NewShardReplicationInfo(&topodatapb.ShardReplication{
					Nodes: []*topodatapb.ShardReplication_Node{
						{
							TabletAlias: &topodatapb.TabletAlias{
								Cell: "zone1",
								Uid:  100,
							},
						},
					},
				}, "zone1", "testkeyspace", "-"),
				topo.NewShardReplicationInfo(&topodatapb.ShardReplication{
					Nodes: []*topodatapb.ShardReplication_Node{
						{
							TabletAlias: &topodatapb.TabletAlias{
								Cell: "zone2",
								Uid:  200,
							},
						},
					},
				}, "zone2", "testkeyspace", "-"),
				topo.NewShardReplicationInfo(&topodatapb.ShardReplication{
					Nodes: []*topodatapb.ShardReplication_Node{
						{
							TabletAlias: &topodatapb.TabletAlias{
								Cell: "zone3",
								Uid:  300,
							},
						},
					},
				}, "zone3", "testkeyspace", "-"),
			},
			req: &vtctldatapb.RemoveShardCellRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
				Cell:      "zone2",
				Recursive: false, // non-recursive + replication graph = failure
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "topo server down",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			replicationGraphs: nil,
			req: &vtctldatapb.RemoveShardCellRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
				Cell:      "zone2",
			},
			topoError:    assert.AnError,
			topoIsLocked: false,
			expected:     nil,
			shouldErr:    true,
		},
		// Not sure how to set up this test case.
		// {
		// 	name: "topo server down for replication check/no force",
		// },
		// Not sure how to set up this test case.
		// {
		// 	name: "topo server down for replication check/force",
		// },
		{
			name: "cannot lock keyspace",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			replicationGraphs: nil,
			req: &vtctldatapb.RemoveShardCellRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
				Cell:      "zone2",
			},
			topoError:    nil,
			topoIsLocked: true,
			expected:     nil,
			shouldErr:    true,
		},
		// Not sure how to set up this test case.
		// {
		// 	name: "cannot delete srvkeyspace partition",
		// },
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cells := []string{"zone1", "zone2", "zone3"}

			ctx := context.Background()
			ts, topofactory := memorytopo.NewServerAndFactory(cells...)
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			// Setup shard topos and replication graphs.
			testutil.AddShards(ctx, t, ts, tt.shards...)
			testutil.SetupReplicationGraphs(ctx, t, ts, tt.replicationGraphs...)

			// Set up srvkeyspace partitions; a little gross.
			servingCells := tt.servingCells
			if servingCells == nil { // we expect an explicit empty list to have a shard with no serving cells
				servingCells = cells
			}

			for _, shard := range tt.shards {
				lctx, unlock, lerr := ts.LockKeyspace(ctx, shard.Keyspace, "initializing serving graph for test")
				require.NoError(t, lerr, "cannot lock keyspace %s to initialize serving graph", shard.Keyspace)

				for _, cell := range servingCells {

					err := ts.UpdateSrvKeyspace(lctx, cell, shard.Keyspace, &topodatapb.SrvKeyspace{
						Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
							{
								ServedType: topodatapb.TabletType_REPLICA,
								ShardReferences: []*topodatapb.ShardReference{
									{
										Name: shard.Name,
									},
								},
							},
						},
					})
					require.NoError(t, err, "cannot update srvkeyspace for %s/%s in cell %v", shard.Keyspace, shard.Name, cell)
				}

				unlock(&lerr)
			}

			// Set errors and locks.
			if tt.topoError != nil {
				topofactory.SetError(tt.topoError)
			}

			if tt.topoIsLocked {
				lctx, unlock, err := ts.LockKeyspace(ctx, tt.req.Keyspace, "testing locked keyspace")
				require.NoError(t, err, "cannot lock keyspace %s", tt.req.Keyspace)
				defer unlock(&err)

				// Need to use the lock ctx in the RPC call so we fail when
				// attempting to lock the keyspace rather than waiting forever
				// for the lock. Explicitly setting a deadline would be another
				// way to achieve this.
				ctx = lctx
			}

			// Make the RPC and assert things about it.
			resp, err := vtctld.RemoveShardCell(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestReparentTablet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tmc       tmclient.TabletManagerClient
		tablets   []*topodatapb.Tablet
		shards    []*vtctldatapb.Shard
		topoErr   error
		req       *vtctldatapb.ReparentTabletRequest
		expected  *vtctldatapb.ReparentTabletResponse
		shouldErr bool
	}{
		{
			name: "success",
			tmc: &testutil.TabletManagerClient{
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "-",
					PrimaryTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						PrimaryAlias: &topodatapb.TabletAlias{
							Cell: "zone2",
							Uid:  200,
						},
						PrimaryTermStartTime: &vttime.Time{
							Seconds: 1000,
						},
						IsPrimaryServing: true,
					},
				},
			},
			req: &vtctldatapb.ReparentTabletRequest{
				Tablet: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected: &vtctldatapb.ReparentTabletResponse{
				Keyspace: "testkeyspace",
				Shard:    "-",
				Primary: &topodatapb.TabletAlias{
					Cell: "zone2",
					Uid:  200,
				},
			},
			shouldErr: false,
		},
		{
			name: "tablet is nil",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "-",
					PrimaryTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						PrimaryAlias: &topodatapb.TabletAlias{
							Cell: "zone2",
							Uid:  200,
						},
						PrimaryTermStartTime: &vttime.Time{
							Seconds: 1000,
						},
						IsPrimaryServing: true,
					},
				},
			},
			req: &vtctldatapb.ReparentTabletRequest{
				Tablet: nil,
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "tablet not in topo",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "-",
					PrimaryTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						PrimaryAlias: &topodatapb.TabletAlias{
							Cell: "zone2",
							Uid:  200,
						},
						PrimaryTermStartTime: &vttime.Time{
							Seconds: 1000,
						},
						IsPrimaryServing: true,
					},
				},
			},
			req: &vtctldatapb.ReparentTabletRequest{
				Tablet: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "shard not in topo",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "-",
					PrimaryTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
			},
			shards: nil,
			req: &vtctldatapb.ReparentTabletRequest{
				Tablet: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "shard has no primary",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						IsPrimaryServing: false,
					},
				},
			},
			req: &vtctldatapb.ReparentTabletRequest{
				Tablet: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "shard primary not in topo",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "-",
					PrimaryTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						PrimaryAlias: &topodatapb.TabletAlias{
							Cell: "zone3",
							Uid:  300,
						},
						PrimaryTermStartTime: &vttime.Time{
							Seconds: 1010,
						},
						IsPrimaryServing: true,
					},
				},
			},
			req: &vtctldatapb.ReparentTabletRequest{
				Tablet: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "shard primary is not type PRIMARY",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						PrimaryAlias: &topodatapb.TabletAlias{
							Cell: "zone2",
							Uid:  200,
						},
						PrimaryTermStartTime: &vttime.Time{
							Seconds: 1010,
						},
						IsPrimaryServing: true,
					},
				},
			},
			req: &vtctldatapb.ReparentTabletRequest{
				Tablet: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "shard primary is not actually in shard",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "otherkeyspace",
					Shard:    "-",
					PrimaryTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						PrimaryAlias: &topodatapb.TabletAlias{
							Cell: "zone2",
							Uid:  200,
						},
						PrimaryTermStartTime: &vttime.Time{
							Seconds: 1010,
						},
						IsPrimaryServing: true,
					},
				},
			},
			req: &vtctldatapb.ReparentTabletRequest{
				Tablet: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "requested tablet is shard primary",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						PrimaryAlias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						PrimaryTermStartTime: &vttime.Time{
							Seconds: 1010,
						},
						IsPrimaryServing: true,
					},
				},
			},
			req: &vtctldatapb.ReparentTabletRequest{
				Tablet: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "tmc.SetReplicationSource failure",
			tmc: &testutil.TabletManagerClient{
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000100": assert.AnError,
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "-",
					PrimaryTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						PrimaryAlias: &topodatapb.TabletAlias{
							Cell: "zone2",
							Uid:  200,
						},
						PrimaryTermStartTime: &vttime.Time{
							Seconds: 1000,
						},
						IsPrimaryServing: true,
					},
				},
			},
			req: &vtctldatapb.ReparentTabletRequest{
				Tablet: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "topo is down",
			tmc: &testutil.TabletManagerClient{
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "-",
					PrimaryTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						PrimaryAlias: &topodatapb.TabletAlias{
							Cell: "zone2",
							Uid:  200,
						},
						PrimaryTermStartTime: &vttime.Time{
							Seconds: 1000,
						},
						IsPrimaryServing: true,
					},
				},
			},
			topoErr: assert.AnError,
			req: &vtctldatapb.ReparentTabletRequest{
				Tablet: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected:  nil,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.req == nil {
				t.Skip("focused on other test cases right now")
			}

			cells := []string{"zone1", "zone2", "zone3"}

			ctx := context.Background()
			ts, topofactory := memorytopo.NewServerAndFactory(cells...)
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			testutil.AddTablets(ctx, t, ts, &testutil.AddTabletOptions{
				SkipShardCreation: true,
			}, tt.tablets...)
			testutil.AddShards(ctx, t, ts, tt.shards...)

			if tt.topoErr != nil {
				topofactory.SetError(tt.topoErr)
			}

			resp, err := vtctld.ReparentTablet(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestRestoreFromBackup(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		ts        *topo.Server
		tmc       tmclient.TabletManagerClient
		tablets   []*topodatapb.Tablet
		req       *vtctldatapb.RestoreFromBackupRequest
		shouldErr bool
		assertion func(t *testing.T, responses []*vtctldatapb.RestoreFromBackupResponse, err error)
	}{
		{
			name: "ok",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				RestoreFromBackupResults: map[string]struct {
					Events        []*logutilpb.Event
					EventInterval time.Duration
					EventJitter   time.Duration
					ErrorAfter    time.Duration
				}{
					"zone1-0000000100": {
						Events: []*logutilpb.Event{{}, {}, {}},
					},
				},
				SetReplicationSourceResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "ks",
					Shard:    "-",
					Type:     topodatapb.TabletType_REPLICA,
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  200,
					},
					Keyspace: "ks",
					Shard:    "-",
					Type:     topodatapb.TabletType_PRIMARY,
				},
			},
			req: &vtctldatapb.RestoreFromBackupRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			assertion: func(t *testing.T, responses []*vtctldatapb.RestoreFromBackupResponse, err error) {
				assert.ErrorIs(t, err, io.EOF, "expected Recv loop to end with io.EOF")
				assert.Equal(t, 3, len(responses), "expected 3 messages from restorefrombackupclient stream")
			},
		},
		{
			name: "no such tablet",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				Backups: map[string]struct {
					Events        []*logutilpb.Event
					EventInterval time.Duration
					EventJitter   time.Duration
					ErrorAfter    time.Duration
				}{
					"zone1-0000000100": {
						Events: []*logutilpb.Event{{}, {}, {}},
					},
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "ks",
					Shard:    "-",
				},
			},
			req: &vtctldatapb.RestoreFromBackupRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone404",
					Uid:  404,
				},
			},
			assertion: func(t *testing.T, responses []*vtctldatapb.RestoreFromBackupResponse, err error) {
				assert.NotErrorIs(t, err, io.EOF, "expected restorefrombackupclient stream to close with non-EOF")
				assert.Zero(t, len(responses), "expected no restorefrombackupclient messages")
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			testutil.AddTablets(ctx, t, tt.ts,
				&testutil.AddTabletOptions{
					AlsoSetShardPrimary: true,
				}, tt.tablets...,
			)
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, tt.ts, tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			client := localvtctldclient.New(vtctld)
			stream, err := client.RestoreFromBackup(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			responses, err := func() (responses []*vtctldatapb.RestoreFromBackupResponse, err error) {
				for {
					resp, err := stream.Recv()
					if err != nil {
						return responses, err
					}

					responses = append(responses, resp)
				}
			}()

			if tt.assertion != nil {
				func() {
					t.Helper()
					tt.assertion(t, responses, err)
				}()
			}
		})
	}
}

func TestRunHealthCheck(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		name      string
		tablets   []*topodatapb.Tablet
		tmc       testutil.TabletManagerClient
		req       *vtctldatapb.RunHealthCheckRequest
		shouldErr bool
	}{
		{
			name: "ok",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			tmc: testutil.TabletManagerClient{
				RunHealthCheckResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			req: &vtctldatapb.RunHealthCheckRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
		},
		{
			name: "no tablet",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  404,
					},
				},
			},
			tmc: testutil.TabletManagerClient{
				RunHealthCheckResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			req: &vtctldatapb.RunHealthCheckRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			shouldErr: true,
		},
		{
			name: "tmc call failed",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			tmc: testutil.TabletManagerClient{
				RunHealthCheckResults: map[string]error{
					"zone1-0000000100": assert.AnError,
				},
			},
			req: &vtctldatapb.RunHealthCheckRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := memorytopo.NewServer("zone1")
			testutil.AddTablets(ctx, t, ts, nil, tt.tablets...)

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, &tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			_, err := vtctld.RunHealthCheck(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestSetKeyspaceDurabilityPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		keyspaces   []*vtctldatapb.Keyspace
		req         *vtctldatapb.SetKeyspaceDurabilityPolicyRequest
		expected    *vtctldatapb.SetKeyspaceDurabilityPolicyResponse
		expectedErr string
	}{
		{
			name: "ok",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "ks1",
					Keyspace: &topodatapb.Keyspace{},
				},
				{
					Name:     "ks2",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			req: &vtctldatapb.SetKeyspaceDurabilityPolicyRequest{
				Keyspace:         "ks1",
				DurabilityPolicy: "none",
			},
			expected: &vtctldatapb.SetKeyspaceDurabilityPolicyResponse{
				Keyspace: &topodatapb.Keyspace{
					DurabilityPolicy: "none",
				},
			},
		},
		{
			name: "keyspace not found",
			req: &vtctldatapb.SetKeyspaceDurabilityPolicyRequest{
				Keyspace: "ks1",
			},
			expectedErr: "node doesn't exist: keyspaces/ks1",
		},
		{
			name: "fail to update durability policy",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "ks1",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			req: &vtctldatapb.SetKeyspaceDurabilityPolicyRequest{
				Keyspace:         "ks1",
				DurabilityPolicy: "non-existent",
			},
			expectedErr: "durability policy <non-existent> is not a valid policy. Please register it as a policy first",
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := memorytopo.NewServer("zone1")
			testutil.AddKeyspaces(ctx, t, ts, tt.keyspaces...)

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			resp, err := vtctld.SetKeyspaceDurabilityPolicy(ctx, tt.req)
			if tt.expectedErr != "" {
				assert.EqualError(t, err, tt.expectedErr)
				return
			}

			require.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestSetShardIsPrimaryServing(t *testing.T) {
	t.Parallel()

	type testcase struct {
		name      string
		ctx       context.Context
		ts        *topo.Server
		setup     func(*testing.T, *testcase)
		teardown  func(*testing.T, *testcase)
		req       *vtctldatapb.SetShardIsPrimaryServingRequest
		expected  *vtctldatapb.SetShardIsPrimaryServingResponse
		shouldErr bool
	}

	tests := []*testcase{
		{
			name: "ok",
			setup: func(t *testing.T, tt *testcase) {
				tt.ctx = context.Background()
				tt.ts = memorytopo.NewServer("zone1")
				testutil.AddShards(tt.ctx, t, tt.ts, &vtctldatapb.Shard{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard:    &topodatapb.Shard{},
				})
			},
			req: &vtctldatapb.SetShardIsPrimaryServingRequest{
				Keyspace:  "testkeyspace",
				Shard:     "-",
				IsServing: true,
			},
			expected: &vtctldatapb.SetShardIsPrimaryServingResponse{
				Shard: &topodatapb.Shard{
					IsPrimaryServing: true,
				},
			},
		},
		{
			name: "lock error",
			setup: func(t *testing.T, tt *testcase) {
				var cancel func()
				tt.ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*50)
				tt.ts = memorytopo.NewServer("zone1")
				testutil.AddShards(tt.ctx, t, tt.ts, &vtctldatapb.Shard{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard:    &topodatapb.Shard{},
				})

				_, unlock, err := tt.ts.LockKeyspace(tt.ctx, "testkeyspace", "test lock")
				require.NoError(t, err)
				tt.teardown = func(t *testing.T, tt *testcase) {
					var err error
					unlock(&err)
					assert.NoError(t, err)
					cancel()
				}
			},
			req: &vtctldatapb.SetShardIsPrimaryServingRequest{
				Keyspace:  "testkeyspace",
				Shard:     "-",
				IsServing: true,
			},
			expected:  nil,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.setup != nil {
				tt.setup(t, tt)
			}
			if tt.teardown != nil {
				defer tt.teardown(t, tt)
			}

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, tt.ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			resp, err := vtctld.SetShardIsPrimaryServing(tt.ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestSetShardTabletControl(t *testing.T) {
	t.Parallel()

	type testcase struct {
		name      string
		ctx       context.Context
		ts        *topo.Server
		setup     func(*testing.T, *testcase)
		teardown  func(*testing.T, *testcase)
		req       *vtctldatapb.SetShardTabletControlRequest
		expected  *vtctldatapb.SetShardTabletControlResponse
		shouldErr bool
	}

	tests := []*testcase{
		{
			name: "ok",
			setup: func(t *testing.T, tt *testcase) {
				tt.ctx = context.Background()
				tt.ts = memorytopo.NewServer("zone1", "zone2", "zone3")

				testutil.AddShards(tt.ctx, t, tt.ts, &vtctldatapb.Shard{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						TabletControls: []*topodatapb.Shard_TabletControl{
							{
								TabletType:   topodatapb.TabletType_REPLICA,
								Cells:        []string{"zone1"},
								DeniedTables: []string{"t1"},
							},
							{
								TabletType:   topodatapb.TabletType_REPLICA,
								Cells:        []string{"zone2", "zone3"},
								DeniedTables: []string{"t2"},
							},
						},
					},
				})
			},
			req: &vtctldatapb.SetShardTabletControlRequest{
				Keyspace:     "testkeyspace",
				Shard:        "-",
				DeniedTables: []string{"t1"},
				Cells:        []string{"zone2", "zone3"},
				TabletType:   topodatapb.TabletType_REPLICA,
			},
			expected: &vtctldatapb.SetShardTabletControlResponse{
				Shard: &topodatapb.Shard{
					TabletControls: []*topodatapb.Shard_TabletControl{
						{
							TabletType:   topodatapb.TabletType_REPLICA,
							Cells:        []string{"zone1", "zone2", "zone3"},
							DeniedTables: []string{"t1"},
						},
						{
							TabletType:   topodatapb.TabletType_REPLICA,
							Cells:        []string{"zone2", "zone3"},
							DeniedTables: []string{"t2"},
						},
					},
				},
			},
		},
		{
			name: "remove tabletcontrols",
			setup: func(t *testing.T, tt *testcase) {
				tt.ctx = context.Background()
				tt.ts = memorytopo.NewServer("zone1", "zone2", "zone3")

				testutil.AddShards(tt.ctx, t, tt.ts, &vtctldatapb.Shard{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						TabletControls: []*topodatapb.Shard_TabletControl{
							{
								TabletType:   topodatapb.TabletType_REPLICA,
								Cells:        []string{"zone1"},
								DeniedTables: []string{"t1"},
							},
							{
								TabletType:   topodatapb.TabletType_REPLICA,
								Cells:        []string{"zone2", "zone3"},
								DeniedTables: []string{"t2"},
							},
						},
					},
				})
			},
			req: &vtctldatapb.SetShardTabletControlRequest{
				Keyspace:   "testkeyspace",
				Shard:      "-",
				TabletType: topodatapb.TabletType_REPLICA,
				Remove:     true,
			},
			expected: &vtctldatapb.SetShardTabletControlResponse{
				Shard: &topodatapb.Shard{},
			},
		},
		{
			name: "disable queryservice",
			setup: func(t *testing.T, tt *testcase) {
				tt.ctx = context.Background()
				tt.ts = memorytopo.NewServer("zone1", "zone2", "zone3")

				testutil.AddShards(tt.ctx, t, tt.ts, &vtctldatapb.Shard{
					Keyspace: "testkeyspace",
					Name:     "-",
				})

				lctx, unlock, lerr := tt.ts.LockKeyspace(tt.ctx, "testkeyspace", "locking to create partitions for test")
				require.NoError(t, lerr, "could not lock keyspace to setup test partitions")
				var err error
				defer unlock(&err)
				defer func() { require.NoError(t, err) }()

				err = tt.ts.UpdateSrvKeyspace(lctx, "zone1", "testkeyspace", &topodatapb.SrvKeyspace{
					Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
						{
							ServedType: topodatapb.TabletType_REPLICA,
							ShardTabletControls: []*topodatapb.ShardTabletControl{
								{
									Name:                 "-",
									QueryServiceDisabled: false,
								},
							},
						},
					},
				})
				require.NoError(t, err)
				err = tt.ts.UpdateSrvKeyspace(lctx, "zone2", "testkeyspace", &topodatapb.SrvKeyspace{
					Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
						{
							ServedType: topodatapb.TabletType_REPLICA,
							ShardTabletControls: []*topodatapb.ShardTabletControl{
								{
									Name:                 "-",
									QueryServiceDisabled: true,
								},
							},
						},
					},
				})
				require.NoError(t, err)
			},
			teardown: func(t *testing.T, tt *testcase) {
				expected := map[string][]*topodatapb.ShardTabletControl{
					"zone1": {
						{
							Name:                 "-",
							QueryServiceDisabled: true,
						},
					},
					"zone2": {
						{
							Name:                 "-",
							QueryServiceDisabled: true,
						},
					},
				}
				for cell, expectedControls := range expected {
					partitions, err := tt.ts.GetSrvKeyspace(tt.ctx, cell, "testkeyspace")
					require.NoError(t, err, "could not get srvkeyspace for testkeyspace/%s", cell)

					for _, partition := range partitions.Partitions {
						if partition.ServedType != topodatapb.TabletType_REPLICA {
							continue
						}

						utils.MustMatch(t, expectedControls, partition.ShardTabletControls)
					}
				}
			},
			req: &vtctldatapb.SetShardTabletControlRequest{
				Keyspace:            "testkeyspace",
				Shard:               "-",
				Cells:               []string{"zone1", "zone2"},
				TabletType:          topodatapb.TabletType_REPLICA,
				DisableQueryService: true,
			},
			expected: &vtctldatapb.SetShardTabletControlResponse{
				Shard: &topodatapb.Shard{
					TabletControls: []*topodatapb.Shard_TabletControl{
						{
							TabletType: topodatapb.TabletType_REPLICA,
							Cells:      []string{"zone1", "zone2"},
						},
					},
					IsPrimaryServing: true,
					KeyRange:         &topodatapb.KeyRange{},
				},
			},
		},
		{
			name: "keyspace lock error",
			setup: func(t *testing.T, tt *testcase) {
				var cancel func()
				tt.ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*50)
				tt.ts = memorytopo.NewServer("zone1")
				testutil.AddShards(tt.ctx, t, tt.ts, &vtctldatapb.Shard{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard:    &topodatapb.Shard{},
				})

				_, unlock, err := tt.ts.LockKeyspace(tt.ctx, "testkeyspace", "test lock")
				require.NoError(t, err)
				tt.teardown = func(t *testing.T, tt *testcase) {
					var err error
					unlock(&err)
					assert.NoError(t, err)
					cancel()
				}
			},
			req: &vtctldatapb.SetShardTabletControlRequest{
				Keyspace:     "testkeyspace",
				Shard:        "-",
				DeniedTables: []string{"t1"},
				TabletType:   topodatapb.TabletType_REPLICA,
			},
			shouldErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.setup != nil {
				tt.setup(t, tt)
			}
			if tt.teardown != nil {
				defer tt.teardown(t, tt)
			}

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, tt.ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			resp, err := vtctld.SetShardTabletControl(tt.ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestSetWritable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cells     []string
		tablets   []*topodatapb.Tablet
		tmc       testutil.TabletManagerClient
		req       *vtctldatapb.SetWritableRequest
		shouldErr bool
	}{
		{
			name:  "writable ok",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_REPLICA,
				},
			},
			tmc: testutil.TabletManagerClient{
				SetReadOnlyResults: map[string]error{
					"zone1-0000000100": assert.AnError,
				},
				SetReadWriteResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			req: &vtctldatapb.SetWritableRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				Writable: true,
			},
			shouldErr: false,
		},
		{
			name:  "writable fail",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_RDONLY,
				},
			},
			tmc: testutil.TabletManagerClient{
				SetReadOnlyResults: map[string]error{
					"zone1-0000000100": nil,
				},
				SetReadWriteResults: map[string]error{
					"zone1-0000000100": assert.AnError,
				},
			},
			req: &vtctldatapb.SetWritableRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				Writable: true,
			},
			shouldErr: true,
		},
		{
			name:  "read only ok",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_REPLICA,
				},
			},
			tmc: testutil.TabletManagerClient{
				SetReadOnlyResults: map[string]error{
					"zone1-0000000100": nil,
				},
				SetReadWriteResults: map[string]error{
					"zone1-0000000100": assert.AnError,
				},
			},
			req: &vtctldatapb.SetWritableRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				Writable: false,
			},
			shouldErr: false,
		},
		{
			name:  "read only fail",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_PRIMARY,
				},
			},
			tmc: testutil.TabletManagerClient{
				SetReadOnlyResults: map[string]error{
					"zone1-0000000100": assert.AnError,
				},
				SetReadWriteResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			req: &vtctldatapb.SetWritableRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				Writable: false,
			},
			shouldErr: true,
		},
		{
			name:  "no such tablet",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_PRIMARY,
				},
			},
			req: &vtctldatapb.SetWritableRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone2",
					Uid:  200,
				},
				Writable: false,
			},
			shouldErr: true,
		},
		{
			name:  "bad request",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_PRIMARY,
				},
			},
			req:       &vtctldatapb.SetWritableRequest{},
			shouldErr: true,
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := memorytopo.NewServer(tt.cells...)
			defer ts.Close()

			testutil.AddTablets(ctx, t, ts, nil, tt.tablets...)
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, &tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			_, err := vtctld.SetWritable(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestShardReplicationAdd(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ts := memorytopo.NewServer("zone1")
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	tablets := []*topodatapb.Tablet{
		{
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  100,
			},
			Keyspace: "ks",
			Shard:    "-",
		},
		{
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  101,
			},
			Keyspace: "ks",
			Shard:    "-",
		},
	}
	testutil.AddTablets(ctx, t, ts, nil, tablets...)

	_, err := vtctld.ShardReplicationAdd(ctx, &vtctldatapb.ShardReplicationAddRequest{
		Keyspace: "ks",
		Shard:    "-",
		TabletAlias: &topodatapb.TabletAlias{
			Cell: "zone1",
			Uid:  404,
		},
	})
	assert.NoError(t, err)

	resp, err := vtctld.ShardReplicationFix(ctx, &vtctldatapb.ShardReplicationFixRequest{
		Keyspace: "ks",
		Shard:    "-",
		Cell:     "zone1",
	})
	require.NoError(t, err, "ShardReplicationFix failed")
	utils.MustMatch(t, &topodatapb.ShardReplicationError{
		TabletAlias: &topodatapb.TabletAlias{
			Cell: "zone1",
			Uid:  404,
		},
		Type: topodatapb.ShardReplicationError_NOT_FOUND,
	}, resp.Error)
}

func TestShardReplicationPositions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ts         *topo.Server
		tablets    []*topodatapb.Tablet
		tmc        tmclient.TabletManagerClient
		ctxTimeout time.Duration
		req        *vtctldatapb.ShardReplicationPositionsRequest
		expected   *vtctldatapb.ShardReplicationPositionsResponse
		shouldErr  bool
	}{
		{
			name: "success",
			ts:   memorytopo.NewServer("zone1"),
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_PRIMARY,
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_REPLICA,
				},
			},
			tmc: &testutil.TabletManagerClient{
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "primary_tablet_position",
					},
				},
				ReplicationStatusResults: map[string]struct {
					Position *replicationdatapb.Status
					Error    error
				}{
					"zone1-0000000101": {
						Position: &replicationdatapb.Status{
							Position: "replica_tablet_position",
						},
					},
				},
			},
			req: &vtctldatapb.ShardReplicationPositionsRequest{
				Keyspace: "testkeyspace",
				Shard:    "-",
			},
			expected: &vtctldatapb.ShardReplicationPositionsResponse{
				ReplicationStatuses: map[string]*replicationdatapb.Status{
					"zone1-0000000100": {
						Position: "primary_tablet_position",
					},
					"zone1-0000000101": {
						Position: "replica_tablet_position",
					},
				},
				TabletMap: map[string]*topodatapb.Tablet{
					"zone1-0000000100": {
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Keyspace: "testkeyspace",
						Shard:    "-",
						Type:     topodatapb.TabletType_PRIMARY,
					},
					"zone1-0000000101": {
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
						Keyspace: "testkeyspace",
						Shard:    "-",
						Type:     topodatapb.TabletType_REPLICA,
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "timeouts are nonfatal",
			ts:   memorytopo.NewServer("zone1"),
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_PRIMARY,
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_REPLICA,
				},
			},
			tmc: &testutil.TabletManagerClient{
				PrimaryPositionDelays: map[string]time.Duration{
					"zone1-0000000100": time.Millisecond * 100,
				},
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "primary_tablet_position",
					},
				},
				ReplicationStatusDelays: map[string]time.Duration{
					"zone1-0000000101": time.Millisecond * 100,
				},
				ReplicationStatusResults: map[string]struct {
					Position *replicationdatapb.Status
					Error    error
				}{
					"zone1-0000000101": {
						Position: &replicationdatapb.Status{
							Position: "replica_tablet_position",
						},
					},
				},
			},
			ctxTimeout: time.Millisecond * 10,
			req: &vtctldatapb.ShardReplicationPositionsRequest{
				Keyspace: "testkeyspace",
				Shard:    "-",
			},
			expected: &vtctldatapb.ShardReplicationPositionsResponse{
				ReplicationStatuses: map[string]*replicationdatapb.Status{
					"zone1-0000000100": nil,
					"zone1-0000000101": nil,
				},
				TabletMap: map[string]*topodatapb.Tablet{
					"zone1-0000000100": {
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Keyspace: "testkeyspace",
						Shard:    "-",
						Type:     topodatapb.TabletType_PRIMARY,
					},
					"zone1-0000000101": {
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
						Keyspace: "testkeyspace",
						Shard:    "-",
						Type:     topodatapb.TabletType_REPLICA,
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "other rpc errors are fatal",
			ts:   memorytopo.NewServer("zone1"),
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_PRIMARY,
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_REPLICA,
				},
			},
			tmc: &testutil.TabletManagerClient{
				PrimaryPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Error: assert.AnError,
					},
				},
				ReplicationStatusResults: map[string]struct {
					Position *replicationdatapb.Status
					Error    error
				}{
					"zone1-0000000101": {
						Position: &replicationdatapb.Status{
							Position: "replica_tablet_position",
						},
					},
				},
			},
			req: &vtctldatapb.ShardReplicationPositionsRequest{
				Keyspace: "testkeyspace",
				Shard:    "-",
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "nonexistent shard",
			ts:   memorytopo.NewServer("zone1"),
			req: &vtctldatapb.ShardReplicationPositionsRequest{
				Keyspace: "testkeyspace",
				Shard:    "-",
			},
			expected:  nil,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()

			testutil.AddTablets(ctx, t, tt.ts, &testutil.AddTabletOptions{
				AlsoSetShardPrimary: true,
				SkipShardCreation:   false,
			}, tt.tablets...)

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, tt.ts, tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			if tt.ctxTimeout > 0 {
				_ctx, cancel := context.WithTimeout(ctx, tt.ctxTimeout)
				defer cancel()

				ctx = _ctx
			}

			resp, err := vtctld.ShardReplicationPositions(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestShardReplicationRemove(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ts := memorytopo.NewServer("zone1")
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	tablets := []*topodatapb.Tablet{
		{
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  100,
			},
			Keyspace: "ks",
			Shard:    "-",
		},
		{
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  101,
			},
			Keyspace: "ks",
			Shard:    "-",
		},
	}
	testutil.AddTablets(ctx, t, ts, nil, tablets...)

	_, err := vtctld.ShardReplicationRemove(ctx, &vtctldatapb.ShardReplicationRemoveRequest{
		Keyspace: "ks",
		Shard:    "-",
		TabletAlias: &topodatapb.TabletAlias{
			Cell: "zone1",
			Uid:  101,
		},
	})
	assert.NoError(t, err)

	sri, err := ts.GetShardReplication(ctx, "zone1", "ks", "-")
	require.NoError(t, err, "GetShardReplication failed")

	utils.MustMatch(t, sri.Nodes, []*topodatapb.ShardReplication_Node{{
		TabletAlias: &topodatapb.TabletAlias{
			Cell: "zone1",
			Uid:  100,
		},
	}})
}

func TestSourceShardAdd(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		shards       []*vtctldatapb.Shard
		topoIsLocked bool
		req          *vtctldatapb.SourceShardAddRequest
		expected     *vtctldatapb.SourceShardAddResponse
		shouldErr    bool
		assertion    func(ctx context.Context, t *testing.T, ts *topo.Server)
	}{
		{
			name: "ok",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "ks",
					Name:     "-",
				},
			},
			req: &vtctldatapb.SourceShardAddRequest{
				Keyspace:       "ks",
				Shard:          "-",
				Uid:            1,
				SourceKeyspace: "otherks",
				SourceShard:    "-80",
			},
			expected: &vtctldatapb.SourceShardAddResponse{
				Shard: &topodatapb.Shard{
					IsPrimaryServing: true,
					KeyRange:         &topodatapb.KeyRange{},
					SourceShards: []*topodatapb.Shard_SourceShard{
						{
							Uid:      1,
							Keyspace: "otherks",
							Shard:    "-80",
						},
					},
				},
			},
		},
		{
			name: "uid already used",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "ks",
					Name:     "-",
					Shard: &topodatapb.Shard{
						SourceShards: []*topodatapb.Shard_SourceShard{
							{
								Uid:      1,
								Keyspace: "otherks",
								Shard:    "-80",
							},
						},
					},
				},
			},
			req: &vtctldatapb.SourceShardAddRequest{
				Keyspace:       "ks",
				Shard:          "-",
				Uid:            1,
				SourceKeyspace: "otherks",
				SourceShard:    "80-",
			},
			expected: &vtctldatapb.SourceShardAddResponse{},
			assertion: func(ctx context.Context, t *testing.T, ts *topo.Server) {
				si, err := ts.GetShard(ctx, "ks", "-")
				require.NoError(t, err, "failed to get shard ks/-")
				utils.MustMatch(t, []*topodatapb.Shard_SourceShard{
					{
						Uid:      1,
						Keyspace: "otherks",
						Shard:    "-80",
					},
				}, si.SourceShards, "SourceShards should not have changed")
			},
		},
		{
			name: "cannot lock keyspace",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "ks",
					Name:     "-",
					Shard: &topodatapb.Shard{
						SourceShards: []*topodatapb.Shard_SourceShard{
							{
								Uid:      1,
								Keyspace: "otherks",
								Shard:    "-80",
							},
						},
					},
				},
			},
			topoIsLocked: true,
			req: &vtctldatapb.SourceShardAddRequest{
				Keyspace:       "ks",
				Shard:          "-",
				Uid:            1,
				SourceKeyspace: "otherks",
				SourceShard:    "80-",
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			ts := memorytopo.NewServer("zone1")
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			testutil.AddShards(ctx, t, ts, tt.shards...)
			if tt.topoIsLocked {
				lctx, unlock, lerr := ts.LockKeyspace(ctx, tt.req.Keyspace, "test lock")
				require.NoError(t, lerr, "failed to lock %s for test setup", tt.req.Keyspace)

				defer func() {
					var err error
					unlock(&err)
					assert.NoError(t, err, "failed to unlock %s after test", tt.req.Keyspace)
				}()

				ctx = lctx
			}

			resp, err := vtctld.SourceShardAdd(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
			if tt.assertion != nil {
				func() {
					t.Helper()
					tt.assertion(ctx, t, ts)
				}()
			}
		})
	}
}

func TestSourceShardDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		shards       []*vtctldatapb.Shard
		topoIsLocked bool
		req          *vtctldatapb.SourceShardDeleteRequest
		expected     *vtctldatapb.SourceShardDeleteResponse
		shouldErr    bool
		assertion    func(ctx context.Context, t *testing.T, ts *topo.Server)
	}{
		{
			name: "ok",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "ks",
					Name:     "-",
					Shard: &topodatapb.Shard{
						SourceShards: []*topodatapb.Shard_SourceShard{
							{
								Uid:      1,
								Keyspace: "otherks",
								Shard:    "-80",
							},
						},
					},
				},
			},
			req: &vtctldatapb.SourceShardDeleteRequest{
				Keyspace: "ks",
				Shard:    "-",
				Uid:      1,
			},
			expected: &vtctldatapb.SourceShardDeleteResponse{
				Shard: &topodatapb.Shard{},
			},
		},
		{
			name: "no SourceShard with uid",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "ks",
					Name:     "-",
					Shard: &topodatapb.Shard{
						SourceShards: []*topodatapb.Shard_SourceShard{
							{
								Uid:      1,
								Keyspace: "otherks",
								Shard:    "-80",
							},
						},
					},
				},
			},
			req: &vtctldatapb.SourceShardDeleteRequest{
				Keyspace: "ks",
				Shard:    "-",
				Uid:      2,
			},
			expected: &vtctldatapb.SourceShardDeleteResponse{},
			assertion: func(ctx context.Context, t *testing.T, ts *topo.Server) {
				si, err := ts.GetShard(ctx, "ks", "-")
				require.NoError(t, err, "failed to get shard ks/-")
				utils.MustMatch(t, []*topodatapb.Shard_SourceShard{
					{
						Uid:      1,
						Keyspace: "otherks",
						Shard:    "-80",
					},
				}, si.SourceShards, "SourceShards should not have changed")
			},
		},
		{
			name: "cannot lock keyspace",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "ks",
					Name:     "-",
				},
			},
			topoIsLocked: true,
			req: &vtctldatapb.SourceShardDeleteRequest{
				Keyspace: "ks",
				Shard:    "-",
				Uid:      1,
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			ts := memorytopo.NewServer("zone1")
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			testutil.AddShards(ctx, t, ts, tt.shards...)
			if tt.topoIsLocked {
				lctx, unlock, lerr := ts.LockKeyspace(ctx, tt.req.Keyspace, "test lock")
				require.NoError(t, lerr, "failed to lock %s for test setup", tt.req.Keyspace)

				defer func() {
					var err error
					unlock(&err)
					assert.NoError(t, err, "failed to unlock %s after test", tt.req.Keyspace)
				}()

				ctx = lctx
			}

			resp, err := vtctld.SourceShardDelete(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
			if tt.assertion != nil {
				func() {
					t.Helper()
					tt.assertion(ctx, t, ts)
				}()
			}
		})
	}
}

func TestStartReplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cells     []string
		tablets   []*topodatapb.Tablet
		tmc       testutil.TabletManagerClient
		req       *vtctldatapb.StartReplicationRequest
		shouldErr bool
	}{
		{
			name:  "ok",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_REPLICA,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_PRIMARY,
				},
			},
			tmc: testutil.TabletManagerClient{
				StartReplicationResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			req: &vtctldatapb.StartReplicationRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
		},
		{
			name:  "fail",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_REPLICA,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_PRIMARY,
				},
			},
			tmc: testutil.TabletManagerClient{
				StartReplicationResults: map[string]error{
					"zone1-0000000100": assert.AnError,
				},
			},
			req: &vtctldatapb.StartReplicationRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			shouldErr: true,
		},
		{
			name:  "no such tablet",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_REPLICA,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_PRIMARY,
				},
			},
			tmc: testutil.TabletManagerClient{
				StartReplicationResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			req: &vtctldatapb.StartReplicationRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone2",
					Uid:  200,
				},
			},
			shouldErr: true,
		},
		{
			name:  "bad request",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_REPLICA,
				}, {
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_PRIMARY,
				},
			},
			req:       &vtctldatapb.StartReplicationRequest{},
			shouldErr: true,
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := memorytopo.NewServer(tt.cells...)
			defer ts.Close()

			testutil.AddTablets(ctx, t, ts, &testutil.AddTabletOptions{
				AlsoSetShardPrimary: true,
			}, tt.tablets...)
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, &tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			_, err := vtctld.StartReplication(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestStopReplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cells     []string
		tablets   []*topodatapb.Tablet
		tmc       testutil.TabletManagerClient
		req       *vtctldatapb.StopReplicationRequest
		shouldErr bool
	}{
		{
			name:  "ok",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_REPLICA,
				},
			},
			tmc: testutil.TabletManagerClient{
				StopReplicationResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			req: &vtctldatapb.StopReplicationRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
		},
		{
			name:  "fail",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_REPLICA,
				},
			},
			tmc: testutil.TabletManagerClient{
				StopReplicationResults: map[string]error{
					"zone1-0000000100": assert.AnError,
				},
			},
			req: &vtctldatapb.StopReplicationRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			shouldErr: true,
		},
		{
			name:  "no such tablet",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_REPLICA,
				},
			},
			tmc: testutil.TabletManagerClient{
				StopReplicationResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			req: &vtctldatapb.StopReplicationRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone2",
					Uid:  200,
				},
			},
			shouldErr: true,
		},
		{
			name:  "bad request",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_REPLICA,
				},
			},
			req:       &vtctldatapb.StopReplicationRequest{},
			shouldErr: true,
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := memorytopo.NewServer(tt.cells...)
			defer ts.Close()

			testutil.AddTablets(ctx, t, ts, nil, tt.tablets...)
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, &tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			_, err := vtctld.StopReplication(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestTabletExternallyReparented(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		topo         []*topodatapb.Tablet
		topoErr      error
		tmcHasNoTopo bool
		req          *vtctldatapb.TabletExternallyReparentedRequest
		expected     *vtctldatapb.TabletExternallyReparentedResponse
		shouldErr    bool
		expectedTopo []*topodatapb.Tablet
	}{
		{
			name: "success",
			topo: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "-",
					PrimaryTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone3",
						Uid:  300,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			topoErr: nil,
			req: &vtctldatapb.TabletExternallyReparentedRequest{
				Tablet: &topodatapb.TabletAlias{
					Cell: "zone2",
					Uid:  200,
				},
			},
			expected: &vtctldatapb.TabletExternallyReparentedResponse{
				Keyspace: "testkeyspace",
				Shard:    "-",
				NewPrimary: &topodatapb.TabletAlias{
					Cell: "zone2",
					Uid:  200,
				},
				OldPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			shouldErr: false,
			// NOTE: this seems weird, right? Why is the old primary still a
			// PRIMARY, and why is the new primary's term start 0,0? Well, our
			// test client implementation is a little incomplete. See
			// ./testutil/test_tmclient.go for reference.
			expectedTopo: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "-",
					PrimaryTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:                 topodatapb.TabletType_UNKNOWN,
					Keyspace:             "testkeyspace",
					Shard:                "-",
					PrimaryTermStartTime: &vttime.Time{},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone3",
						Uid:  300,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
		},
		{
			name:    "tablet is nil",
			topo:    nil,
			topoErr: nil,
			req: &vtctldatapb.TabletExternallyReparentedRequest{
				Tablet: nil,
			},
			expected:     nil,
			shouldErr:    true,
			expectedTopo: nil,
		},
		{
			name: "topo is down",
			topo: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "-",
					PrimaryTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone3",
						Uid:  300,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			topoErr: assert.AnError,
			req: &vtctldatapb.TabletExternallyReparentedRequest{
				Tablet: &topodatapb.TabletAlias{
					Cell: "zone2",
					Uid:  200,
				},
			},
			expected:  nil,
			shouldErr: true,
			expectedTopo: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "-",
					PrimaryTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone3",
						Uid:  300,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
		},
		{
			name: "tablet is already primary",
			topo: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "-",
					PrimaryTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone3",
						Uid:  300,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			topoErr: nil,
			req: &vtctldatapb.TabletExternallyReparentedRequest{
				Tablet: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected: &vtctldatapb.TabletExternallyReparentedResponse{
				Keyspace: "testkeyspace",
				Shard:    "-",
				NewPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				OldPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			shouldErr: false,
			expectedTopo: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "-",
					PrimaryTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone3",
						Uid:  300,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
		},
		{
			name: "cannot change tablet type",
			topo: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "-",
					PrimaryTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone3",
						Uid:  300,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			topoErr:      nil,
			tmcHasNoTopo: true,
			req: &vtctldatapb.TabletExternallyReparentedRequest{
				Tablet: &topodatapb.TabletAlias{
					Cell: "zone2",
					Uid:  200,
				},
			},
			expected:  nil,
			shouldErr: true,
			expectedTopo: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_PRIMARY,
					Keyspace: "testkeyspace",
					Shard:    "-",
					PrimaryTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone3",
						Uid:  300,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cells := []string{"zone1", "zone2", "zone3"}

			ctx := context.Background()
			ts, topofactory := memorytopo.NewServerAndFactory(cells...)
			tmc := testutil.TabletManagerClient{
				TopoServer: ts,
			}
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, &tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			if tt.tmcHasNoTopo {
				// For certain test cases, we want specifically just the
				// ChangeType call to fail, which is why we rely on a separate
				// bool rather than using tt.topoErr.
				tmc.TopoServer = nil
			}

			testutil.AddTablets(ctx, t, ts, &testutil.AddTabletOptions{
				AlsoSetShardPrimary: true,
			}, tt.topo...)

			if tt.topoErr != nil {
				topofactory.SetError(tt.topoErr)
			}

			if tt.expectedTopo != nil {
				// assert on expectedTopo state when we've fininished the rest
				// of the test.
				defer func() {
					topofactory.SetError(nil)

					ctx, cancel := context.WithTimeout(ctx, time.Millisecond*10)
					defer cancel()

					resp, err := vtctld.GetTablets(ctx, &vtctldatapb.GetTabletsRequest{})
					require.NoError(t, err, "cannot get all tablets in the topo")
					testutil.AssertSameTablets(t, tt.expectedTopo, resp.Tablets)
				}()
			}

			resp, err := vtctld.TabletExternallyReparented(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestUpdateCellInfo(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		name           string
		cells          map[string]*topodatapb.CellInfo
		forceTopoError bool
		req            *vtctldatapb.UpdateCellInfoRequest
		expected       *vtctldatapb.UpdateCellInfoResponse
		shouldErr      bool
	}{
		{
			name: "update",
			cells: map[string]*topodatapb.CellInfo{
				"zone1": {
					ServerAddress: ":1111",
					Root:          "/zone1",
				},
			},
			req: &vtctldatapb.UpdateCellInfoRequest{
				Name: "zone1",
				CellInfo: &topodatapb.CellInfo{
					ServerAddress: ":0101",
					Root:          "/zones/zone1",
				},
			},
			expected: &vtctldatapb.UpdateCellInfoResponse{
				Name: "zone1",
				CellInfo: &topodatapb.CellInfo{
					ServerAddress: ":0101",
					Root:          "/zones/zone1",
				},
			},
		},
		{
			name: "partial update",
			cells: map[string]*topodatapb.CellInfo{
				"zone1": {
					ServerAddress: ":1111",
					Root:          "/zone1",
				},
			},
			req: &vtctldatapb.UpdateCellInfoRequest{
				Name: "zone1",
				CellInfo: &topodatapb.CellInfo{
					Root: "/zones/zone1",
				},
			},
			expected: &vtctldatapb.UpdateCellInfoResponse{
				Name: "zone1",
				CellInfo: &topodatapb.CellInfo{
					ServerAddress: ":1111",
					Root:          "/zones/zone1",
				},
			},
		},
		{
			name: "no update",
			cells: map[string]*topodatapb.CellInfo{
				"zone1": {
					ServerAddress: ":1111",
					Root:          "/zone1",
				},
			},
			req: &vtctldatapb.UpdateCellInfoRequest{
				Name: "zone1",
				CellInfo: &topodatapb.CellInfo{
					Root: "/zone1",
				},
			},
			expected: &vtctldatapb.UpdateCellInfoResponse{
				Name: "zone1",
				CellInfo: &topodatapb.CellInfo{
					ServerAddress: ":1111",
					Root:          "/zone1",
				},
			},
		},
		{
			name: "cell not found",
			cells: map[string]*topodatapb.CellInfo{
				"zone1": {
					ServerAddress: ":1111",
					Root:          "/zone1",
				},
			},
			req: &vtctldatapb.UpdateCellInfoRequest{
				Name: "zone404",
				CellInfo: &topodatapb.CellInfo{
					ServerAddress: ":4040",
					Root:          "/zone404",
				},
			},
			expected: &vtctldatapb.UpdateCellInfoResponse{
				Name: "zone404",
				CellInfo: &topodatapb.CellInfo{
					ServerAddress: ":4040",
					Root:          "/zone404",
				},
			},
		},
		{
			name: "cannot update",
			cells: map[string]*topodatapb.CellInfo{
				"zone1": {
					ServerAddress: ":1111",
					Root:          "/zone1",
				},
			},
			forceTopoError: true,
			req: &vtctldatapb.UpdateCellInfoRequest{
				Name: "zone1",
				CellInfo: &topodatapb.CellInfo{
					Root: "/zone1",
				},
			},
			expected:  nil,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts, factory := memorytopo.NewServerAndFactory()
			for name, cell := range tt.cells {
				err := ts.CreateCellInfo(ctx, name, cell)
				require.NoError(t, err, "failed to create cell %s: %+v for test", name, cell)
			}

			if tt.forceTopoError {
				factory.SetError(fmt.Errorf("%w: topo down for testing", assert.AnError))
			}

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			resp, err := vtctld.UpdateCellInfo(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestUpdateCellsAlias(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		name      string
		cells     []string
		aliases   map[string][]string
		req       *vtctldatapb.UpdateCellsAliasRequest
		expected  *vtctldatapb.UpdateCellsAliasResponse
		shouldErr bool
	}{
		{
			name: "remove one cell",
			aliases: map[string][]string{
				"zone": {
					"zone1",
					"zone2",
					"zone3",
				},
			},
			req: &vtctldatapb.UpdateCellsAliasRequest{
				Name: "zone",
				CellsAlias: &topodatapb.CellsAlias{
					Cells: []string{"zone1", "zone2"},
				},
			},
			expected: &vtctldatapb.UpdateCellsAliasResponse{
				Name: "zone",
				CellsAlias: &topodatapb.CellsAlias{
					Cells: []string{"zone1", "zone2"},
				},
			},
		},
		{
			name:  "add one cell",
			cells: []string{"zone4"}, // all other cells get created via the aliases map
			aliases: map[string][]string{
				"zone": {
					"zone1",
					"zone2",
					"zone3",
				},
			},
			req: &vtctldatapb.UpdateCellsAliasRequest{
				Name: "zone",
				CellsAlias: &topodatapb.CellsAlias{
					Cells: []string{
						"zone1",
						"zone2",
						"zone3",
						"zone4",
					},
				},
			},
			expected: &vtctldatapb.UpdateCellsAliasResponse{
				Name: "zone",
				CellsAlias: &topodatapb.CellsAlias{
					Cells: []string{
						"zone1",
						"zone2",
						"zone3",
						"zone4",
					},
				},
			},
		},
		{
			name:  "alias does not exist",
			cells: []string{"zone1", "zone2"},
			req: &vtctldatapb.UpdateCellsAliasRequest{
				Name: "zone",
				CellsAlias: &topodatapb.CellsAlias{
					Cells: []string{"zone1", "zone2"},
				},
			},
			expected: &vtctldatapb.UpdateCellsAliasResponse{
				Name: "zone",
				CellsAlias: &topodatapb.CellsAlias{
					Cells: []string{"zone1", "zone2"},
				},
			},
		},
		{
			name: "invalid alias list",
			aliases: map[string][]string{
				"zone_a": {
					"zone1",
					"zone2",
				},
				"zone_b": {
					"zone3",
					"zone4",
				},
			},
			req: &vtctldatapb.UpdateCellsAliasRequest{
				Name: "zone_a",
				CellsAlias: &topodatapb.CellsAlias{
					Cells: []string{
						"zone1",
						"zone2",
						"zone3", // this is invalid because it belongs to alias zone_b
					},
				},
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := memorytopo.NewServer(tt.cells...)
			for name, cells := range tt.aliases {
				for _, cell := range cells {
					// We use UpdateCellInfoFields rather than CreateCellInfo
					// for the update-or-create behavior.
					err := ts.UpdateCellInfoFields(ctx, cell, func(ci *topodatapb.CellInfo) error {
						ci.Root = "/" + cell
						ci.ServerAddress = cell + ":8080"
						return nil
					})
					require.NoError(t, err, "failed to create cell %v", cell)
				}

				err := ts.CreateCellsAlias(ctx, name, &topodatapb.CellsAlias{
					Cells: cells,
				})
				require.NoError(t, err, "failed to create cell alias %v (cells = %v)", name, cells)
			}

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			resp, err := vtctld.UpdateCellsAlias(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ts := memorytopo.NewServer("zone1", "zone2", "zone3")
	tablets := []*topodatapb.Tablet{
		{
			Keyspace: "ks1",
			Shard:    "-",
			Type:     topodatapb.TabletType_PRIMARY,
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  100,
			},
			Hostname: "ks1-00-00-primary",
		},
		{
			Keyspace: "ks1",
			Shard:    "-",
			Type:     topodatapb.TabletType_REPLICA,
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  101,
			},
			Hostname: "ks1-00-00-replica",
		},
		{
			Keyspace: "ks2",
			Shard:    "-80",
			Type:     topodatapb.TabletType_PRIMARY,
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  102,
			},
			Hostname: "ks2-00-80-primary",
		},
		{
			Keyspace: "ks2",
			Shard:    "-80",
			Type:     topodatapb.TabletType_REPLICA,
			Alias: &topodatapb.TabletAlias{
				Cell: "zone2",
				Uid:  200,
			},
			Hostname: "ks2-00-80-replica1",
		},
		{
			Keyspace: "ks2",
			Shard:    "-80",
			Type:     topodatapb.TabletType_REPLICA,
			Alias: &topodatapb.TabletAlias{
				Cell: "zone3",
				Uid:  300,
			},
			Hostname: "ks2-00-80-replica2",
		},
		{
			Keyspace: "ks2",
			Shard:    "80-",
			Type:     topodatapb.TabletType_PRIMARY,
			Alias: &topodatapb.TabletAlias{
				Cell: "zone3",
				Uid:  301,
			},
			Hostname: "ks2-80-00-primary",
		},
		{
			Keyspace: "ks2",
			Shard:    "80-",
			Type:     topodatapb.TabletType_REPLICA,
			Alias: &topodatapb.TabletAlias{
				Cell: "zone2",
				Uid:  201,
			},
			Hostname: "ks2-80-00-replica1",
		},
		{
			Keyspace: "ks2",
			Shard:    "80-",
			Type:     topodatapb.TabletType_RDONLY,
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  103,
			},
			Hostname: "ks2-80-00-rdonly1",
		},
	}
	testutil.AddTablets(ctx, t, ts, &testutil.AddTabletOptions{
		AlsoSetShardPrimary:  true,
		ForceSetShardPrimary: true,
		SkipShardCreation:    false,
	}, tablets...)
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	resp, err := vtctld.Validate(ctx, &vtctldatapb.ValidateRequest{
		PingTablets: false,
	})
	require.NoError(t, err)
	assert.Equal(t, &vtctldatapb.ValidateResponse{
		ResultsByKeyspace: map[string]*vtctldatapb.ValidateKeyspaceResponse{
			"ks1": {
				ResultsByShard: map[string]*vtctldatapb.ValidateShardResponse{
					"-": {},
				},
			},
			"ks2": {
				ResultsByShard: map[string]*vtctldatapb.ValidateShardResponse{
					"-80": {},
					"80-": {},
				},
			},
		},
	}, resp)
}

func TestValidateSchemaKeyspace(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("zone1", "zone2", "zone3")
	tmc := testutil.TabletManagerClient{
		GetSchemaResults: map[string]struct {
			Schema *tabletmanagerdatapb.SchemaDefinition
			Error  error
		}{},
	}
	testutil.AddKeyspace(ctx, t, ts, &vtctldatapb.Keyspace{
		Name: "ks1",
		Keyspace: &topodatapb.Keyspace{
			KeyspaceType: topodatapb.KeyspaceType_NORMAL,
		},
	})
	testutil.AddKeyspace(ctx, t, ts, &vtctldatapb.Keyspace{
		Name: "ks2",
		Keyspace: &topodatapb.Keyspace{
			KeyspaceType: topodatapb.KeyspaceType_NORMAL,
		},
	})
	tablets := []*topodatapb.Tablet{
		{
			Keyspace: "ks1",
			Shard:    "-",
			Type:     topodatapb.TabletType_PRIMARY,
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  100,
			},
			Hostname: "ks1-00-00-primary",
		},
		{
			Keyspace: "ks1",
			Shard:    "-",
			Type:     topodatapb.TabletType_REPLICA,
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  101,
			},
			Hostname: "ks1-00-00-replica",
		},
		// ks2 shard -80 has no Primary intentionally for testing
		{
			Keyspace: "ks2",
			Shard:    "-80",
			Type:     topodatapb.TabletType_REPLICA,
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  102,
			},
			Hostname: "ks2-00-80-replica0",
		},
		{
			Keyspace: "ks2",
			Shard:    "-80",
			Type:     topodatapb.TabletType_REPLICA,
			Alias: &topodatapb.TabletAlias{
				Cell: "zone2",
				Uid:  200,
			},
			Hostname: "ks2-00-80-replica1",
		},
		//
		{
			Keyspace: "ks2",
			Shard:    "80-",
			Type:     topodatapb.TabletType_PRIMARY,
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  103,
			},
			Hostname: "ks2-80-00-primary1",
		},
		{
			Keyspace: "ks2",
			Shard:    "80-",
			Type:     topodatapb.TabletType_REPLICA,
			Alias: &topodatapb.TabletAlias{
				Cell: "zone2",
				Uid:  201,
			},
			Hostname: "ks2-80-00-replica1",
		},
	}
	testutil.AddTablets(ctx, t, ts, &testutil.AddTabletOptions{
		AlsoSetShardPrimary:  true,
		ForceSetShardPrimary: true,
		SkipShardCreation:    false,
	}, tablets...)

	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, &tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	schema1 := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "not_in_vschema",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}

	schema2 := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
			{
				Name:    "t1",
				Columns: []string{"c1"},
			},
			{
				Name:    "t2",
				Columns: []string{"c1"},
			},
			{
				Name:    "t3",
				Columns: []string{"c1"},
			},
		},
	}

	// we need to run this on each test case or they will pollute each other
	setupSchema := func(tablet *topodatapb.TabletAlias, schema *tabletmanagerdatapb.SchemaDefinition) {
		tmc.GetSchemaResults[topoproto.TabletAliasString(tablet)] = struct {
			Schema *tabletmanagerdatapb.SchemaDefinition
			Error  error
		}{
			Schema: schema,
			Error:  nil,
		}
	}

	tests := []*struct {
		name      string
		req       *vtctldatapb.ValidateSchemaKeyspaceRequest
		expected  *vtctldatapb.ValidateSchemaKeyspaceResponse
		setup     func()
		shouldErr bool
	}{
		{
			name: "valid schemas",
			req: &vtctldatapb.ValidateSchemaKeyspaceRequest{
				Keyspace: "ks1",
			},
			expected: &vtctldatapb.ValidateSchemaKeyspaceResponse{
				Results: []string{},
				ResultsByShard: map[string]*vtctldatapb.ValidateShardResponse{
					"-": {Results: []string{}},
				},
			},
			setup: func() {
				setupSchema(&topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				}, schema1)
				setupSchema(&topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  101,
				}, schema1)
			},
			shouldErr: false,
		},
		{
			name: "different schemas",
			req: &vtctldatapb.ValidateSchemaKeyspaceRequest{
				Keyspace: "ks1",
			},
			expected: &vtctldatapb.ValidateSchemaKeyspaceResponse{
				Results: []string{"zone1-0000000100 has an extra table named not_in_vschema"},
				ResultsByShard: map[string]*vtctldatapb.ValidateShardResponse{
					"-": {Results: []string{"zone1-0000000100 has an extra table named not_in_vschema"}},
				},
			},
			setup: func() {
				setupSchema(&topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				}, schema1)
				setupSchema(&topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  101,
				}, schema2)
			},
			shouldErr: false,
		},
		{
			name: "skip-no-primary: no primary",
			req: &vtctldatapb.ValidateSchemaKeyspaceRequest{
				Keyspace:      "ks2",
				SkipNoPrimary: false,
			},
			expected: &vtctldatapb.ValidateSchemaKeyspaceResponse{
				Results: []string{"no primary in shard ks2/-80"},
				ResultsByShard: map[string]*vtctldatapb.ValidateShardResponse{
					"-80": {Results: []string{"no primary in shard ks2/-80"}},
					"80-": {Results: []string{}},
				},
			},
			setup: func() {
				setupSchema(&topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  103,
				}, schema1)
				setupSchema(&topodatapb.TabletAlias{
					Cell: "zone2",
					Uid:  201,
				}, schema1)
			},
			shouldErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			resp, err := vtctld.ValidateSchemaKeyspace(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestValidateVersionKeyspace(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("zone1", "zone2")
	tmc := testutil.TabletManagerClient{
		GetSchemaResults: map[string]struct {
			Schema *tabletmanagerdatapb.SchemaDefinition
			Error  error
		}{},
	}
	testutil.AddKeyspace(ctx, t, ts, &vtctldatapb.Keyspace{
		Name: "ks1",
		Keyspace: &topodatapb.Keyspace{
			KeyspaceType: topodatapb.KeyspaceType_NORMAL,
		},
	})
	testutil.AddKeyspace(ctx, t, ts, &vtctldatapb.Keyspace{
		Name: "ks2",
		Keyspace: &topodatapb.Keyspace{
			KeyspaceType: topodatapb.KeyspaceType_NORMAL,
		},
	})
	tablets := []*topodatapb.Tablet{
		{
			Keyspace: "ks1",
			Shard:    "-",
			Type:     topodatapb.TabletType_PRIMARY,
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  100,
			},
			Hostname: "primary",
		},
		{
			Keyspace: "ks1",
			Shard:    "-",
			Type:     topodatapb.TabletType_REPLICA,
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  101,
			},
			Hostname: "replica",
		},
	}
	testutil.AddTablets(ctx, t, ts, &testutil.AddTabletOptions{
		AlsoSetShardPrimary:  true,
		ForceSetShardPrimary: true,
		SkipShardCreation:    false,
	}, tablets...)

	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, &tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	tests := []*struct {
		name      string
		req       *vtctldatapb.ValidateVersionKeyspaceRequest
		expected  *vtctldatapb.ValidateVersionKeyspaceResponse
		setup     func()
		shouldErr bool
	}{
		{
			name: "valid versions",
			req: &vtctldatapb.ValidateVersionKeyspaceRequest{
				Keyspace: "ks1",
			},
			expected: &vtctldatapb.ValidateVersionKeyspaceResponse{
				Results: []string{},
				ResultsByShard: map[string]*vtctldatapb.ValidateShardResponse{
					"-": {Results: []string{}},
				},
			},
			setup: func() {
				addrVersionMap := map[string]string{
					"primary:0": "version1",
					"replica:0": "version1",
				}
				SetVersionFunc(testutil.MockGetVersionFromTablet(addrVersionMap))
			},
			shouldErr: false,
		},
		{
			name: "different versions",
			req: &vtctldatapb.ValidateVersionKeyspaceRequest{
				Keyspace: "ks1",
			},
			expected: &vtctldatapb.ValidateVersionKeyspaceResponse{
				Results: []string{"primary zone1-0000000100 version version:\"version1\" is different than replica zone1-0000000101 version version:\"version2\""},
				ResultsByShard: map[string]*vtctldatapb.ValidateShardResponse{
					"-": {Results: []string{"primary zone1-0000000100 version version:\"version1\" is different than replica zone1-0000000101 version version:\"version2\""}},
				},
			},
			setup: func() {
				addrVersionMap := map[string]string{
					"primary:0": "version1",
					"replica:0": "version2",
				}
				SetVersionFunc(testutil.MockGetVersionFromTablet(addrVersionMap))
			},
			shouldErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			resp, err := vtctld.ValidateVersionKeyspace(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestValidateVersionShard(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ts := memorytopo.NewServer("zone1", "zone2")
	tmc := testutil.TabletManagerClient{
		GetSchemaResults: map[string]struct {
			Schema *tabletmanagerdatapb.SchemaDefinition
			Error  error
		}{},
	}
	testutil.AddKeyspace(ctx, t, ts, &vtctldatapb.Keyspace{
		Name: "ks",
		Keyspace: &topodatapb.Keyspace{
			KeyspaceType: topodatapb.KeyspaceType_NORMAL,
		},
	})

	tablets := []*topodatapb.Tablet{
		{
			Keyspace: "ks",
			Shard:    "-",
			Type:     topodatapb.TabletType_PRIMARY,
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  100,
			},
			Hostname: "primary",
		},
		{
			Keyspace: "ks",
			Shard:    "-",
			Type:     topodatapb.TabletType_REPLICA,
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  101,
			},
			Hostname: "replica",
		},
	}
	testutil.AddTablets(ctx, t, ts, &testutil.AddTabletOptions{
		AlsoSetShardPrimary:  true,
		ForceSetShardPrimary: true,
		SkipShardCreation:    false,
	}, tablets...)

	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, &tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	tests := []*struct {
		name      string
		req       *vtctldatapb.ValidateVersionShardRequest
		expected  *vtctldatapb.ValidateVersionShardResponse
		setup     func()
		shouldErr bool
	}{
		{
			name: "valid versions",
			req: &vtctldatapb.ValidateVersionShardRequest{
				Keyspace: "ks",
				Shard:    "-",
			},
			expected: &vtctldatapb.ValidateVersionShardResponse{
				Results: []string{},
			},
			setup: func() {
				addrVersionMap := map[string]string{
					"primary:0": "version1",
					"replica:0": "version1",
				}
				SetVersionFunc(testutil.MockGetVersionFromTablet(addrVersionMap))
			},
			shouldErr: false,
		},
		{
			name: "different versions",
			req: &vtctldatapb.ValidateVersionShardRequest{
				Keyspace: "ks",
				Shard:    "-",
			},
			expected: &vtctldatapb.ValidateVersionShardResponse{
				Results: []string{"primary zone1-0000000100 version version1 is different than replica zone1-0000000101 version version:\"version2\""},
			},
			setup: func() {
				addrVersionMap := map[string]string{
					"primary:0": "version1",
					"replica:0": "version2",
				}
				SetVersionFunc(testutil.MockGetVersionFromTablet(addrVersionMap))
			},
			shouldErr: false,
		},
	}

	for _, tt := range tests {
		curT := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			curT.setup()
			resp, err := vtctld.ValidateVersionShard(ctx, curT.req)
			if curT.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			utils.MustMatch(t, curT.expected, resp)
		})
	}
}

func TestValidateShard(t *testing.T) {
	t.Parallel()

	type testcase struct {
		name      string
		ts        *topo.Server
		tmc       *testutil.TabletManagerClient
		setup     func(t *testing.T, tt *testcase)
		req       *vtctldatapb.ValidateShardRequest
		expected  *vtctldatapb.ValidateShardResponse
		shouldErr bool
	}

	ctx := context.Background()
	tests := []*testcase{
		{
			name: "ok",
			ts:   memorytopo.NewServer("zone1"),
			tmc:  nil,
			setup: func(t *testing.T, tt *testcase) {
				tablets := []*topodatapb.Tablet{
					{
						Keyspace: "ks1",
						Shard:    "-",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Type:     topodatapb.TabletType_PRIMARY,
						Hostname: "ks1-primary",
					},
					{
						Keyspace: "ks1",
						Shard:    "-",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
						Type:     topodatapb.TabletType_REPLICA,
						Hostname: "ks1-replica",
					},
				}
				testutil.AddTablets(ctx, t, tt.ts, &testutil.AddTabletOptions{
					AlsoSetShardPrimary: true,
				}, tablets...)
			},
			req: &vtctldatapb.ValidateShardRequest{
				Keyspace: "ks1",
				Shard:    "-",
			},
			expected: &vtctldatapb.ValidateShardResponse{},
		},
		{
			name: "no shard",
			ts:   memorytopo.NewServer("zone1"),
			tmc:  nil,
			setup: func(t *testing.T, tt *testcase) {
				tablets := []*topodatapb.Tablet{
					{
						Keyspace: "ks1",
						Shard:    "-",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Type:     topodatapb.TabletType_PRIMARY,
						Hostname: "ks1-primary",
					},
					{
						Keyspace: "ks1",
						Shard:    "-",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
						Type:     topodatapb.TabletType_REPLICA,
						Hostname: "ks1-replica",
					},
				}
				testutil.AddTablets(ctx, t, tt.ts, &testutil.AddTabletOptions{
					SkipShardCreation: true,
				}, tablets...)
			},
			req: &vtctldatapb.ValidateShardRequest{
				Keyspace: "ks1",
				Shard:    "-",
			},
			expected: &vtctldatapb.ValidateShardResponse{
				Results: []string{
					"TopologyServer.GetShard(ks1, -) failed: node doesn't exist: keyspaces/ks1/shards/-/Shard",
				},
			},
		},
		{
			name: "no primary in shard",
			ts:   memorytopo.NewServer("zone1"),
			tmc:  nil,
			setup: func(t *testing.T, tt *testcase) {
				tablets := []*topodatapb.Tablet{
					{
						Keyspace: "ks1",
						Shard:    "-",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
						Type:     topodatapb.TabletType_REPLICA,
						Hostname: "ks1-replica",
					},
				}
				testutil.AddTablets(ctx, t, tt.ts, nil, tablets...)
			},
			req: &vtctldatapb.ValidateShardRequest{
				Keyspace: "ks1",
				Shard:    "-",
			},
			expected: &vtctldatapb.ValidateShardResponse{
				Results: []string{"no primary for shard ks1/-"},
			},
		},
		{
			name: "two primaries in shard",
			ts:   memorytopo.NewServer("zone1"),
			tmc:  nil,
			setup: func(t *testing.T, tt *testcase) {
				tablets := []*topodatapb.Tablet{
					{
						Keyspace: "ks1",
						Shard:    "-",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Type:     topodatapb.TabletType_PRIMARY,
						Hostname: "ks1-primary",
					},
					{
						Keyspace: "ks1",
						Shard:    "-",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
						Type:     topodatapb.TabletType_PRIMARY,
						Hostname: "ks1-primary2",
					},
				}
				testutil.AddTablets(ctx, t, tt.ts, &testutil.AddTabletOptions{
					AlsoSetShardPrimary:  true,
					ForceSetShardPrimary: true,
				}, tablets...)
			},
			req: &vtctldatapb.ValidateShardRequest{
				Keyspace: "ks1",
				Shard:    "-",
			},
			expected: &vtctldatapb.ValidateShardResponse{
				Results: []string{
					"shard ks1/- already has primary zone1-0000000100 but found other primary zone1-0000000101",
					"primary mismatch for shard ks1/-: found zone1-0000000100, expected zone1-0000000101",
				},
			},
		},
		{
			name: "ping_tablets/ok",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				GetReplicasResults: map[string]struct {
					Replicas []string
					Error    error
				}{
					"zone1-0000000100": {
						Replicas: []string{"11.21.31.41", "12.22.32.42"},
					},
				},
				PingResults: map[string]error{
					"zone1-0000000100": nil,
					"zone1-0000000101": nil,
					"zone1-0000000102": nil,
				},
			},
			setup: func(t *testing.T, tt *testcase) {
				tablets := []*topodatapb.Tablet{
					{
						Keyspace: "ks1",
						Shard:    "-",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Type:     topodatapb.TabletType_PRIMARY,
						Hostname: "ks1-primary",
						// note: we don't actually use this IP, we just need to
						// resolve _something_ for the testcase. The IPs are
						// used by the validateReplication function to
						// disambiguate/deduplicate tablets.
						MysqlHostname: "10.20.30.40",
					},
					{
						Keyspace: "ks1",
						Shard:    "-",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
						Type:          topodatapb.TabletType_REPLICA,
						Hostname:      "ks1-replica",
						MysqlHostname: "11.21.31.41",
					},
					{
						Keyspace: "ks1",
						Shard:    "-",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
						Type:          topodatapb.TabletType_RDONLY,
						Hostname:      "ks1-rdonly",
						MysqlHostname: "12.22.32.42",
					},
				}
				testutil.AddTablets(ctx, t, tt.ts, &testutil.AddTabletOptions{
					AlsoSetShardPrimary: true,
				}, tablets...)
			},
			req: &vtctldatapb.ValidateShardRequest{
				Keyspace:    "ks1",
				Shard:       "-",
				PingTablets: true,
			},
			expected: &vtctldatapb.ValidateShardResponse{},
		},
		{
			name: "ping_tablets/GetReplicas failed",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				GetReplicasResults: map[string]struct {
					Replicas []string
					Error    error
				}{
					"zone1-0000000100": {
						Error: assert.AnError,
					},
				},
				PingResults: map[string]error{
					"zone1-0000000100": nil,
					"zone1-0000000101": nil,
					"zone1-0000000102": nil,
				},
			},
			setup: func(t *testing.T, tt *testcase) {
				tablets := []*topodatapb.Tablet{
					{
						Keyspace: "ks1",
						Shard:    "-",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Type:     topodatapb.TabletType_PRIMARY,
						Hostname: "ks1-primary",
						// note: we don't actually use this IP, we just need to
						// resolve _something_ for the testcase. The IPs are
						// used by the validateReplication function to
						// disambiguate/deduplicate tablets.
						MysqlHostname: "10.20.30.40",
					},
					{
						Keyspace: "ks1",
						Shard:    "-",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
						Type:          topodatapb.TabletType_REPLICA,
						Hostname:      "ks1-replica",
						MysqlHostname: "11.21.31.41",
					},
					{
						Keyspace: "ks1",
						Shard:    "-",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
						Type:          topodatapb.TabletType_RDONLY,
						Hostname:      "ks1-rdonly",
						MysqlHostname: "12.22.32.42",
					},
				}
				testutil.AddTablets(ctx, t, tt.ts, &testutil.AddTabletOptions{
					AlsoSetShardPrimary: true,
				}, tablets...)
			},
			req: &vtctldatapb.ValidateShardRequest{
				Keyspace:    "ks1",
				Shard:       "-",
				PingTablets: true,
			},
			expected: &vtctldatapb.ValidateShardResponse{
				Results: []string{"GetReplicas(Tablet{zone1-0000000100}) failed: assert.AnError general error for testing"},
			},
		},
		{
			name: "ping_tablets/no replicas",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				GetReplicasResults: map[string]struct {
					Replicas []string
					Error    error
				}{
					"zone1-0000000100": {
						Replicas: []string{},
					},
				},
				PingResults: map[string]error{
					"zone1-0000000100": nil,
					"zone1-0000000101": nil,
					"zone1-0000000102": nil,
				},
			},
			setup: func(t *testing.T, tt *testcase) {
				tablets := []*topodatapb.Tablet{
					{
						Keyspace: "ks1",
						Shard:    "-",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Type:     topodatapb.TabletType_PRIMARY,
						Hostname: "ks1-primary",
						// note: we don't actually use this IP, we just need to
						// resolve _something_ for the testcase. The IPs are
						// used by the validateReplication function to
						// disambiguate/deduplicate tablets.
						MysqlHostname: "10.20.30.40",
					},
					{
						Keyspace: "ks1",
						Shard:    "-",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
						Type:          topodatapb.TabletType_REPLICA,
						Hostname:      "ks1-replica",
						MysqlHostname: "11.21.31.41",
					},
					{
						Keyspace: "ks1",
						Shard:    "-",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
						Type:          topodatapb.TabletType_RDONLY,
						Hostname:      "ks1-rdonly",
						MysqlHostname: "12.22.32.42",
					},
				}
				testutil.AddTablets(ctx, t, tt.ts, &testutil.AddTabletOptions{
					AlsoSetShardPrimary: true,
				}, tablets...)
			},
			req: &vtctldatapb.ValidateShardRequest{
				Keyspace:    "ks1",
				Shard:       "-",
				PingTablets: true,
			},
			expected: &vtctldatapb.ValidateShardResponse{
				Results: []string{"no replicas of tablet zone1-0000000100 found"},
			},
		},
		{
			name: "ping_tablets/orphaned replica",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				GetReplicasResults: map[string]struct {
					Replicas []string
					Error    error
				}{
					"zone1-0000000100": {
						Replicas: []string{"11.21.31.41", "100.200.200.100" /* not in set of tablet addrs below */},
					},
				},
				PingResults: map[string]error{
					"zone1-0000000100": nil,
					"zone1-0000000101": nil,
					"zone1-0000000102": nil,
				},
			},
			setup: func(t *testing.T, tt *testcase) {
				tablets := []*topodatapb.Tablet{
					{
						Keyspace: "ks1",
						Shard:    "-",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Type:     topodatapb.TabletType_PRIMARY,
						Hostname: "ks1-primary",
						// note: we don't actually use this IP, we just need to
						// resolve _something_ for the testcase. The IPs are
						// used by the validateReplication function to
						// disambiguate/deduplicate tablets.
						MysqlHostname: "10.20.30.40",
					},
					{
						Keyspace: "ks1",
						Shard:    "-",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
						Type:          topodatapb.TabletType_REPLICA,
						Hostname:      "ks1-replica",
						MysqlHostname: "11.21.31.41",
					},
					{
						Keyspace: "ks1",
						Shard:    "-",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
						Type:          topodatapb.TabletType_RDONLY,
						Hostname:      "ks1-rdonly",
						MysqlHostname: "12.22.32.42",
					},
				}
				testutil.AddTablets(ctx, t, tt.ts, &testutil.AddTabletOptions{
					AlsoSetShardPrimary: true,
				}, tablets...)
			},
			req: &vtctldatapb.ValidateShardRequest{
				Keyspace:    "ks1",
				Shard:       "-",
				PingTablets: true,
			},
			expected: &vtctldatapb.ValidateShardResponse{
				Results: []string{
					"replica 100.200.200.100 not in replication graph for shard ks1/- (mysql instance without vttablet?)",
					"replica zone1-0000000102 not replicating: 12.22.32.42 replica list: [\"11.21.31.41\" \"100.200.200.100\"]",
				},
			},
		},
		{
			name: "ping_tablets/Ping failed",
			ts:   memorytopo.NewServer("zone1"),
			tmc: &testutil.TabletManagerClient{
				GetReplicasResults: map[string]struct {
					Replicas []string
					Error    error
				}{
					"zone1-0000000100": {
						Replicas: []string{"11.21.31.41", "12.22.32.42"},
					},
				},
				PingResults: map[string]error{
					"zone1-0000000100": nil,
					"zone1-0000000101": assert.AnError,
					"zone1-0000000102": nil,
				},
			},
			setup: func(t *testing.T, tt *testcase) {
				tablets := []*topodatapb.Tablet{
					{
						Keyspace: "ks1",
						Shard:    "-",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Type:     topodatapb.TabletType_PRIMARY,
						Hostname: "ks1-primary",
						// note: we don't actually use this IP, we just need to
						// resolve _something_ for the testcase. The IPs are
						// used by the validateReplication function to
						// disambiguate/deduplicate tablets.
						MysqlHostname: "10.20.30.40",
					},
					{
						Keyspace: "ks1",
						Shard:    "-",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
						Type:          topodatapb.TabletType_REPLICA,
						Hostname:      "ks1-replica",
						MysqlHostname: "11.21.31.41",
					},
					{
						Keyspace: "ks1",
						Shard:    "-",
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  102,
						},
						Type:          topodatapb.TabletType_RDONLY,
						Hostname:      "ks1-rdonly",
						MysqlHostname: "12.22.32.42",
					},
				}
				testutil.AddTablets(ctx, t, tt.ts, &testutil.AddTabletOptions{
					AlsoSetShardPrimary: true,
				}, tablets...)
			},
			req: &vtctldatapb.ValidateShardRequest{
				Keyspace:    "ks1",
				Shard:       "-",
				PingTablets: true,
			},
			expected: &vtctldatapb.ValidateShardResponse{
				Results: []string{"Ping(zone1-0000000101) failed: assert.AnError general error for testing tablet hostname: ks1-replica"},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.setup != nil {
				tt.setup(t, tt)
			}

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, tt.ts, tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			resp, err := vtctld.ValidateShard(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, resp)
		})
	}
}
func TestMain(m *testing.M) {
	_flag.ParseFlagsForTest()
	os.Exit(m.Run())
}
