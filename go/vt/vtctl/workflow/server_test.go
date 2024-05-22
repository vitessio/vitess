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

package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

type fakeTMC struct {
	tmclient.TabletManagerClient
	vrepQueriesByTablet map[string]map[string]*querypb.QueryResult
}

func (fake *fakeTMC) VReplicationExec(ctx context.Context, tablet *topodatapb.Tablet, query string) (*querypb.QueryResult, error) {
	alias := topoproto.TabletAliasString(tablet.Alias)
	tabletQueries, ok := fake.vrepQueriesByTablet[alias]
	if !ok {
		return nil, fmt.Errorf("no query map registered on fake for %s", alias)
	}

	p3qr, ok := tabletQueries[query]
	if !ok {
		return nil, fmt.Errorf("no result on fake for query %q on tablet %s", query, alias)
	}

	return p3qr, nil
}

func TestCheckReshardingJournalExistsOnTablet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone1",
			Uid:  100,
		},
	}
	journal := &binlogdatapb.Journal{
		Id:            1,
		MigrationType: binlogdatapb.MigrationType_SHARDS,
		Tables:        []string{"t1", "t2"},
	}
	journalBytes, err := prototext.Marshal(journal)
	require.NoError(t, err, "could not marshal journal %+v into bytes", journal)

	// get some bytes that will fail to unmarshal into a binlogdatapb.Journal
	tabletBytes, err := prototext.Marshal(tablet)
	require.NoError(t, err, "could not marshal tablet %+v into bytes", tablet)

	p3qr := sqltypes.ResultToProto3(sqltypes.MakeTestResult([]*querypb.Field{
		{
			Name: "val",
			Type: querypb.Type_BLOB,
		},
	}, string(journalBytes)))

	tests := []struct {
		name        string
		tablet      *topodatapb.Tablet
		result      *querypb.QueryResult
		journal     *binlogdatapb.Journal
		shouldExist bool
		shouldErr   bool
	}{
		{
			name:        "journal exists",
			tablet:      tablet,
			result:      p3qr,
			shouldExist: true,
			journal:     journal,
		},
		{
			name:        "journal does not exist",
			tablet:      tablet,
			result:      sqltypes.ResultToProto3(sqltypes.MakeTestResult(nil)),
			journal:     &binlogdatapb.Journal{},
			shouldExist: false,
		},
		{
			name:   "cannot unmarshal into journal",
			tablet: tablet,
			result: sqltypes.ResultToProto3(sqltypes.MakeTestResult([]*querypb.Field{
				{
					Name: "val",
					Type: querypb.Type_BLOB,
				},
			}, string(tabletBytes))),
			shouldErr: true,
		},
		{
			name: "VReplicationExec fails on tablet",
			tablet: &topodatapb.Tablet{ // Here we use a different tablet to force the fake to return an error
				Alias: &topodatapb.TabletAlias{
					Cell: "zone2",
					Uid:  200,
				},
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tmc := &fakeTMC{
				vrepQueriesByTablet: map[string]map[string]*querypb.QueryResult{
					topoproto.TabletAliasString(tablet.Alias): { // always use the tablet shared by these tests cases
						"select val from _vt.resharding_journal where id=1": tt.result,
					},
				},
			}

			ws := NewServer(vtenv.NewTestEnv(), nil, tmc)
			journal, exists, err := ws.CheckReshardingJournalExistsOnTablet(ctx, tt.tablet, 1)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			existAssertionMsg := "expected journal to "
			if tt.shouldExist {
				existAssertionMsg += "already exist on tablet"
			} else {
				existAssertionMsg += "not exist"
			}

			assert.Equal(t, tt.shouldExist, exists, existAssertionMsg)
			utils.MustMatch(t, tt.journal, journal, "journal in resharding_journal did not match")
		})
	}
}

// TestVDiffCreate performs some basic tests of the VDiffCreate function
// to ensure that it behaves as expected given a specific request.
func TestVDiffCreate(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell")
	tmc := &fakeTMC{}
	s := NewServer(vtenv.NewTestEnv(), ts, tmc)

	tests := []struct {
		name    string
		req     *vtctldatapb.VDiffCreateRequest
		wantErr string
	}{
		{
			name: "no values",
			req:  &vtctldatapb.VDiffCreateRequest{},
			// We did not provide any keyspace or shard.
			wantErr: "FindAllShardsInKeyspace() invalid keyspace name: UnescapeID err: invalid input identifier ''",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.VDiffCreate(ctx, tt.req)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, got)
			require.NotEmpty(t, got.UUID)
		})
	}
}

func TestMirrorTraffic(t *testing.T) {
	ctx := context.Background()
	sourceKs := "source"
	sourceShards := []string{"-"}
	targetKs := "target"
	targetShards := []string{"-80", "80-"}
	table1 := "table1"
	table2 := "table2"
	workflow := "src2target"

	mirrorRules := map[string]map[string]float32{}
	routingRules := map[string][]string{
		fmt.Sprintf("%s.%s@rdonly", sourceKs, table1): {fmt.Sprintf("%s.%s", targetKs, table1)},
		fmt.Sprintf("%s.%s@rdonly", sourceKs, table2): {fmt.Sprintf("%s.%s", targetKs, table2)},
	}

	tabletTypes := []topodatapb.TabletType{
		topodatapb.TabletType_PRIMARY,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_RDONLY,
	}

	tests := []struct {
		name string

		req            *vtctldatapb.WorkflowMirrorTrafficRequest
		mirrorRules    map[string]map[string]float32
		routingRules   map[string][]string
		setup          func(*testing.T, context.Context, *testEnv)
		sourceKeyspace string
		sourceShards   []string
		targetKeyspace string
		targetShards   []string

		wantErr         string
		wantMirrorRules map[string]map[string]float32
	}{
		{
			name: "no such keyspace",
			req: &vtctldatapb.WorkflowMirrorTrafficRequest{
				Keyspace:    "no_ks",
				Workflow:    workflow,
				TabletTypes: tabletTypes,
				Percent:     50.0,
			},
			wantErr:         "FindAllShardsInKeyspace(no_ks): List: node doesn't exist: keyspaces/no_ks/shards",
			wantMirrorRules: make(map[string]map[string]float32),
		},
		{
			name: "no such workflow",
			req: &vtctldatapb.WorkflowMirrorTrafficRequest{
				Keyspace:    targetKs,
				Workflow:    "no_workflow",
				TabletTypes: tabletTypes,
				Percent:     50.0,
			},
			setup: func(t *testing.T, ctx context.Context, te *testEnv) {
				te.tmc.readVReplicationWorkflow = func(
					ctx context.Context,
					tablet *topodatapb.Tablet,
					request *tabletmanagerdatapb.ReadVReplicationWorkflowRequest,
				) (*tabletmanagerdatapb.ReadVReplicationWorkflowResponse, error) {
					return nil, nil
				}
			},
			wantErr:         "no streams found in keyspace target for no_workflow",
			wantMirrorRules: make(map[string]map[string]float32),
		},
		{
			name: "cannot mirror traffic for migrate workflows",
			req: &vtctldatapb.WorkflowMirrorTrafficRequest{
				Keyspace:    targetKs,
				Workflow:    "migrate",
				TabletTypes: tabletTypes,
				Percent:     50.0,
			},
			setup: func(t *testing.T, ctx context.Context, te *testEnv) {
				te.tmc.readVReplicationWorkflow = createReadVReplicationWorkflowFunc(t, binlogdatapb.VReplicationWorkflowType_Migrate, nil, te.tmc.keyspace, sourceShards, []string{table1, table2})
			},
			wantErr:         "invalid action for Migrate workflow: MirrorTraffic",
			wantMirrorRules: make(map[string]map[string]float32),
		},
		{
			name: "cannot mirror traffic for reshard workflows",
			req: &vtctldatapb.WorkflowMirrorTrafficRequest{
				Keyspace:    sourceKs,
				Workflow:    "reshard",
				TabletTypes: tabletTypes,
				Percent:     50.0,
			},
			sourceKeyspace: sourceKs,
			sourceShards:   []string{"-80", "80-"},
			targetKeyspace: sourceKs,
			targetShards:   []string{"-55", "55-aa", "55-"},
			setup: func(t *testing.T, ctx context.Context, te *testEnv) {
				te.tmc.readVReplicationWorkflow = createReadVReplicationWorkflowFunc(t, binlogdatapb.VReplicationWorkflowType_Reshard, nil, sourceKs, []string{"-80", "80-"}, []string{table1, table2})
			},
			wantErr:         "invalid action for Reshard workflow: MirrorTraffic",
			wantMirrorRules: make(map[string]map[string]float32),
		},
		{
			name: "cannot mirror traffic after switch rdonly traffic",
			req: &vtctldatapb.WorkflowMirrorTrafficRequest{
				Keyspace:    targetKs,
				Workflow:    workflow,
				TabletTypes: tabletTypes,
				Percent:     50.0,
			},
			routingRules: map[string][]string{
				fmt.Sprintf("%s.%s@rdonly", targetKs, table1): {fmt.Sprintf("%s.%s@rdonly", targetKs, table1)},
				fmt.Sprintf("%s.%s@rdonly", targetKs, table2): {fmt.Sprintf("%s.%s@rdonly", targetKs, table2)},
			},
			wantErr:         "cannot mirror traffic for workflow src2target at this time: traffic is switched",
			wantMirrorRules: make(map[string]map[string]float32),
		},
		{
			name: "cannot mirror traffic after switch replica traffic",
			req: &vtctldatapb.WorkflowMirrorTrafficRequest{
				Keyspace:    targetKs,
				Workflow:    workflow,
				TabletTypes: tabletTypes,
				Percent:     50.0,
			},
			routingRules: map[string][]string{
				fmt.Sprintf("%s.%s@replica", targetKs, table1): {fmt.Sprintf("%s.%s@replica", targetKs, table1)},
				fmt.Sprintf("%s.%s@replica", targetKs, table2): {fmt.Sprintf("%s.%s@replica", targetKs, table2)},
			},
			wantErr:         "cannot mirror traffic for workflow src2target at this time: traffic is switched",
			wantMirrorRules: make(map[string]map[string]float32),
		},
		{
			name: "cannot mirror traffic after switch traffic",
			req: &vtctldatapb.WorkflowMirrorTrafficRequest{
				Keyspace:    targetKs,
				Workflow:    workflow,
				TabletTypes: tabletTypes,
				Percent:     50.0,
			},
			routingRules: map[string][]string{
				table1: {fmt.Sprintf("%s.%s", targetKs, table1)},
				table2: {fmt.Sprintf("%s.%s", targetKs, table2)},
			},
			wantErr:         "cannot mirror traffic for workflow src2target at this time: traffic is switched",
			wantMirrorRules: make(map[string]map[string]float32),
		},
		{
			name: "does not mirror traffic for partial move tables",
			req: &vtctldatapb.WorkflowMirrorTrafficRequest{
				Keyspace:    targetKs,
				Workflow:    workflow,
				TabletTypes: tabletTypes,
				Percent:     50.0,
			},
			setup: func(t *testing.T, ctx context.Context, te *testEnv) {
				te.tmc.readVReplicationWorkflow = func(
					ctx context.Context,
					tablet *topodatapb.Tablet,
					request *tabletmanagerdatapb.ReadVReplicationWorkflowRequest,
				) (*tabletmanagerdatapb.ReadVReplicationWorkflowResponse, error) {
					if tablet.Shard != "-80" {
						return nil, nil
					}
					return &tabletmanagerdatapb.ReadVReplicationWorkflowResponse{
						Workflow:     request.Workflow,
						WorkflowType: binlogdatapb.VReplicationWorkflowType_MoveTables,
						Streams: []*tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream{
							{
								Id: 1,
								Bls: &binlogdatapb.BinlogSource{
									Keyspace: sourceKs,
									Shard:    "-80",
									Filter: &binlogdatapb.Filter{
										Rules: []*binlogdatapb.Rule{
											{Match: table1},
											{Match: table2},
										},
									},
								},
							},
						},
					}, nil
				}
			},
			sourceShards:    []string{"-80", "80-"},
			targetShards:    []string{"-80", "80-"},
			wantErr:         "invalid action for partial migration: MirrorTraffic",
			wantMirrorRules: make(map[string]map[string]float32),
		},
		{
			name: "does not mirror traffic for multi-tenant move tables",
			req: &vtctldatapb.WorkflowMirrorTrafficRequest{
				Keyspace:    targetKs,
				Workflow:    workflow,
				TabletTypes: tabletTypes,
				Percent:     50.0,
			},
			setup: func(t *testing.T, ctx context.Context, te *testEnv) {
				te.tmc.readVReplicationWorkflow = createReadVReplicationWorkflowFunc(t, binlogdatapb.VReplicationWorkflowType_MoveTables, &vtctldatapb.WorkflowOptions{TenantId: "123"}, te.tmc.keyspace, sourceShards, []string{table1, table2})
			},
			wantErr:         "invalid action for multi-tenant migration: MirrorTraffic",
			wantMirrorRules: make(map[string]map[string]float32),
		},
		{
			name: "does not mirror traffic for reverse move tables",
			req: &vtctldatapb.WorkflowMirrorTrafficRequest{
				Keyspace:    targetKs,
				Workflow:    workflow + "_reverse",
				TabletTypes: tabletTypes,
				Percent:     50.0,
			},
			wantErr:         "invalid action for reverse workflow: MirrorTraffic",
			wantMirrorRules: make(map[string]map[string]float32),
		},
		{
			name: "ok",
			req: &vtctldatapb.WorkflowMirrorTrafficRequest{
				Keyspace:    targetKs,
				Workflow:    workflow,
				TabletTypes: tabletTypes,
				Percent:     50.0,
			},
			wantMirrorRules: map[string]map[string]float32{
				fmt.Sprintf("%s.%s", sourceKs, table1): {
					fmt.Sprintf("%s.%s", targetKs, table1): 50.0,
				},
				fmt.Sprintf("%s.%s@replica", sourceKs, table1): {
					fmt.Sprintf("%s.%s", targetKs, table1): 50.0,
				},
				fmt.Sprintf("%s.%s@rdonly", sourceKs, table1): {
					fmt.Sprintf("%s.%s", targetKs, table1): 50.0,
				},
				fmt.Sprintf("%s.%s", sourceKs, table2): {
					fmt.Sprintf("%s.%s", targetKs, table2): 50.0,
				},
				fmt.Sprintf("%s.%s@replica", sourceKs, table2): {
					fmt.Sprintf("%s.%s", targetKs, table2): 50.0,
				},
				fmt.Sprintf("%s.%s@rdonly", sourceKs, table2): {
					fmt.Sprintf("%s.%s", targetKs, table2): 50.0,
				},
			},
		},
		{
			name: "does not overwrite unrelated mirror rules",
			mirrorRules: map[string]map[string]float32{
				"other_source.table2": {
					fmt.Sprintf("%s.table2", targetKs): 25.0,
				},
			},
			req: &vtctldatapb.WorkflowMirrorTrafficRequest{
				Keyspace:    targetKs,
				Workflow:    workflow,
				TabletTypes: tabletTypes,
				Percent:     50.0,
			},
			wantMirrorRules: map[string]map[string]float32{
				fmt.Sprintf("%s.%s", sourceKs, table1): {
					fmt.Sprintf("%s.%s", targetKs, table1): 50.0,
				},
				fmt.Sprintf("%s.%s@replica", sourceKs, table1): {
					fmt.Sprintf("%s.%s", targetKs, table1): 50.0,
				},
				fmt.Sprintf("%s.%s@rdonly", sourceKs, table1): {
					fmt.Sprintf("%s.%s", targetKs, table1): 50.0,
				},
				fmt.Sprintf("%s.%s", sourceKs, table2): {
					fmt.Sprintf("%s.%s", targetKs, table2): 50.0,
				},
				fmt.Sprintf("%s.%s@replica", sourceKs, table2): {
					fmt.Sprintf("%s.%s", targetKs, table2): 50.0,
				},
				fmt.Sprintf("%s.%s@rdonly", sourceKs, table2): {
					fmt.Sprintf("%s.%s", targetKs, table2): 50.0,
				},
				"other_source.table2": {
					fmt.Sprintf("%s.table2", targetKs): 25.0,
				},
			},
		},
		{
			name: "does not overwrite when some but not all mirror rules already exist",
			mirrorRules: map[string]map[string]float32{
				fmt.Sprintf("%s.%s", sourceKs, table1): {
					fmt.Sprintf("%s.%s", targetKs, table1): 25.0,
				},
				fmt.Sprintf("%s.%s@replica", sourceKs, table1): {
					fmt.Sprintf("%s.%s", targetKs, table1): 25.0,
				},
				fmt.Sprintf("%s.%s@rdonly", sourceKs, table1): {
					fmt.Sprintf("%s.%s", targetKs, table1): 25.0,
				},
			},
			req: &vtctldatapb.WorkflowMirrorTrafficRequest{
				Keyspace:    targetKs,
				Workflow:    workflow,
				TabletTypes: tabletTypes,
				Percent:     50.0,
			},
			wantErr: "wrong number of pre-existing mirror rules",
			wantMirrorRules: map[string]map[string]float32{
				fmt.Sprintf("%s.%s", sourceKs, table1): {
					fmt.Sprintf("%s.%s", targetKs, table1): 25.0,
				},
				fmt.Sprintf("%s.%s@replica", sourceKs, table1): {
					fmt.Sprintf("%s.%s", targetKs, table1): 25.0,
				},
				fmt.Sprintf("%s.%s@rdonly", sourceKs, table1): {
					fmt.Sprintf("%s.%s", targetKs, table1): 25.0,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.mirrorRules == nil {
				tt.mirrorRules = mirrorRules
			}
			if tt.routingRules == nil {
				tt.routingRules = routingRules
			}
			if tt.sourceKeyspace == "" {
				tt.sourceKeyspace = sourceKs
			}
			if tt.sourceShards == nil {
				tt.sourceShards = sourceShards
			}
			if tt.targetKeyspace == "" {
				tt.targetKeyspace = targetKs
			}
			if tt.targetShards == nil {
				tt.targetShards = targetShards
			}

			te := newTestEnv(t, ctx, &vtctldatapb.MaterializeSettings{
				SourceKeyspace: tt.sourceKeyspace,
				TargetKeyspace: tt.targetKeyspace,
				Workflow:       workflow,
				TableSettings: []*vtctldatapb.TableMaterializeSettings{
					{
						TargetTable:      table1,
						SourceExpression: fmt.Sprintf("select * from %s", table1),
					},
					{
						TargetTable:      table2,
						SourceExpression: fmt.Sprintf("select * from %s", table2),
					},
				},
			}, tt.sourceShards, tt.targetShards)

			require.NoError(t, topotools.SaveMirrorRules(ctx, te.topoServ, tt.mirrorRules))
			require.NoError(t, topotools.SaveRoutingRules(ctx, te.topoServ, tt.routingRules))
			require.NoError(t, te.topoServ.RebuildSrvVSchema(ctx, []string{te.cell}))

			if tt.setup != nil {
				tt.setup(t, ctx, te)
			}

			got, err := te.ws.WorkflowMirrorTraffic(ctx, tt.req)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				require.NotNil(t, got)
			}
			mr, err := topotools.GetMirrorRules(ctx, te.topoServ)
			require.NoError(t, err)
			wantMirrorRules := tt.mirrorRules
			if tt.wantMirrorRules != nil {
				wantMirrorRules = tt.wantMirrorRules
			}
			require.Equal(t, wantMirrorRules, mr)
		})
	}
}

func createReadVReplicationWorkflowFunc(t *testing.T, workflowType binlogdatapb.VReplicationWorkflowType, workflowOptions *vtctldatapb.WorkflowOptions, sourceKeyspace string, sourceShards []string, sourceTables []string) readVReplicationWorkflowFunc {
	return func(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.ReadVReplicationWorkflowRequest) (*tabletmanagerdatapb.ReadVReplicationWorkflowResponse, error) {
		streams := make([]*tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream, 0)
		for i, shard := range sourceShards {
			if shard == tablet.Shard {
				return nil, nil
			}
			rules := make([]*binlogdatapb.Rule, len(sourceTables))
			for i, table := range sourceTables {
				rules[i] = &binlogdatapb.Rule{Match: table}
			}
			streams = append(streams, &tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream{
				Id: int32(i + 1),
				Bls: &binlogdatapb.BinlogSource{
					Keyspace: sourceKeyspace,
					Shard:    shard,
					Filter:   &binlogdatapb.Filter{Rules: rules},
				},
			})
		}

		var err error
		var options []byte
		if workflowOptions != nil {
			options, err = json.Marshal(workflowOptions)
			require.NoError(t, err)
		}

		return &tabletmanagerdatapb.ReadVReplicationWorkflowResponse{
			Workflow:     request.Workflow,
			Options:      string(options),
			WorkflowType: workflowType,
			Streams:      streams,
		}, nil
	}
}
