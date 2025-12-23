/*
Copyright 2024 The Vitess Authors.

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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"

	"vitess.io/vitess/go/ptr"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

const eol = "$"

func TestReshardCreate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	workflowName := "wf1"
	tableName := "t1"
	sourceKeyspaceName := "targetks"
	targetKeyspaceName := "targetks"
	tabletTypes := []topodatapb.TabletType{
		topodatapb.TabletType_PRIMARY,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_RDONLY,
	}
	tabletTypesStr := topoproto.MakeStringTypeCSV(tabletTypes)
	schema := map[string]*tabletmanagerdatapb.SchemaDefinition{
		tableName: {
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:   tableName,
					Schema: fmt.Sprintf("CREATE TABLE %s (id BIGINT, name VARCHAR(64), PRIMARY KEY (id))", tableName),
				},
			},
		},
	}

	testcases := []struct {
		name                           string
		sourceKeyspace, targetKeyspace *testKeyspace
		preFunc                        func(env *testEnv)
		want                           *vtctldatapb.WorkflowStatusResponse
		updateVReplicationRequest      *tabletmanagerdatapb.UpdateVReplicationWorkflowsRequest
		autoStart                      bool
		wantErr                        string
	}{
		{
			name: "basic",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"0"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			autoStart: true,
			updateVReplicationRequest: &tabletmanagerdatapb.UpdateVReplicationWorkflowsRequest{
				AllWorkflows: true,
				State:        ptr.Of(binlogdatapb.VReplicationWorkflowState_Running),
			},
			want: &vtctldatapb.WorkflowStatusResponse{
				ShardStreams: map[string]*vtctldatapb.WorkflowStatusResponse_ShardStreams{
					"targetks/-80": {
						Streams: []*vtctldatapb.WorkflowStatusResponse_ShardStreamState{
							{
								Id:          1,
								Tablet:      &topodatapb.TabletAlias{Cell: defaultCellName, Uid: startingTargetTabletUID},
								SourceShard: "targetks/0", Position: gtid(position), Status: "Running", Info: "VStream Lag: 0s",
							},
						},
					},
					"targetks/80-": {
						Streams: []*vtctldatapb.WorkflowStatusResponse_ShardStreamState{
							{
								Id:          1,
								Tablet:      &topodatapb.TabletAlias{Cell: defaultCellName, Uid: startingTargetTabletUID + tabletUIDStep},
								SourceShard: "targetks/0", Position: gtid(position), Status: "Running", Info: "VStream Lag: 0s",
							},
						},
					},
				},
				TrafficState: "Reads Not Switched. Writes Not Switched",
			},
		},
		{
			name: "no primary",
			sourceKeyspace: &testKeyspace{
				KeyspaceName: sourceKeyspaceName,
				ShardNames:   []string{"0"},
			},
			targetKeyspace: &testKeyspace{
				KeyspaceName: targetKeyspaceName,
				ShardNames:   []string{"-80", "80-"},
			},
			preFunc: func(env *testEnv) {
				_, err := env.ts.UpdateShardFields(ctx, targetKeyspaceName, "-80", func(si *topo.ShardInfo) error {
					si.PrimaryAlias = nil
					return nil
				})
				require.NoError(t, err)
			},
			wantErr: "buildResharder: target shard -80 has no primary tablet",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			require.NotNil(t, tc.sourceKeyspace)
			require.NotNil(t, tc.targetKeyspace)

			env := newTestEnv(t, ctx, defaultCellName, tc.sourceKeyspace, tc.targetKeyspace)
			defer env.close()
			env.tmc.schema = schema

			req := &vtctldatapb.ReshardCreateRequest{
				Keyspace:     targetKeyspaceName,
				Workflow:     workflowName,
				TabletTypes:  tabletTypes,
				SourceShards: tc.sourceKeyspace.ShardNames,
				TargetShards: tc.targetKeyspace.ShardNames,
				Cells:        []string{env.cell},
				AutoStart:    tc.autoStart,
			}

			for i := range tc.sourceKeyspace.ShardNames {
				tabletUID := startingSourceTabletUID + (tabletUIDStep * i)
				env.tmc.expectVRQuery(
					tabletUID,
					"select distinct table_name from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = 1",
					&sqltypes.Result{},
				)
				env.tmc.expectVRQuery(
					tabletUID,
					"select vrepl_id, table_name, lastpk from _vt.copy_state where vrepl_id in (1) and id in (select max(id) from _vt.copy_state where vrepl_id in (1) group by vrepl_id, table_name)",
					&sqltypes.Result{},
				)
			}

			for i, target := range tc.targetKeyspace.ShardNames {
				tabletUID := startingTargetTabletUID + (tabletUIDStep * i)
				env.tmc.expectVRQuery(
					tabletUID,
					insertPrefix+
						`\('`+workflowName+`', 'keyspace:"`+targetKeyspaceName+`" shard:"0" filter:{rules:{match:"/.*" filter:"`+target+`"}}', '', [0-9]*, [0-9]*, '`+
						env.cell+`', '`+tabletTypesStr+`', [0-9]*, 0, 'Stopped', 'vt_`+targetKeyspaceName+`', 4, 0, false, '{}'\)`+eol,
					&sqltypes.Result{},
				)
				env.tmc.expectVRQuery(
					tabletUID,
					"select distinct table_name from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = 1",
					&sqltypes.Result{},
				)
				env.tmc.expectVRQuery(
					tabletUID,
					"select vrepl_id, table_name, lastpk from _vt.copy_state where vrepl_id in (1) and id in (select max(id) from _vt.copy_state where vrepl_id in (1) group by vrepl_id, table_name)",
					&sqltypes.Result{},
				)
				if tc.updateVReplicationRequest != nil {
					env.tmc.AddUpdateVReplicationRequests(uint32(tabletUID), tc.updateVReplicationRequest)
				}
			}

			if tc.preFunc != nil {
				tc.preFunc(env)
			}

			res, err := env.ws.ReshardCreate(ctx, req)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			if tc.want != nil {
				require.Equal(t, tc.want, res)
			}

			// Expect updateVReplicationWorklowsRequests to be empty,
			// if AutoStart is enabled. This is because we delete the specific
			// key from the map in the testTMC, once updateVReplicationWorklows()
			// with the expected request is called.
			if tc.autoStart {
				assert.Len(t, env.tmc.updateVReplicationWorklowsRequests, 0)
			}
		})
	}
}

func TestReadRefStreams(t *testing.T) {
	ctx := context.Background()

	sourceKeyspace := &testKeyspace{
		KeyspaceName: "sourceKeyspace",
		ShardNames:   []string{"-"},
	}
	targetKeyspace := &testKeyspace{
		KeyspaceName: "targetKeyspace",
		ShardNames:   []string{"-"},
	}

	env := newTestEnv(t, ctx, defaultCellName, sourceKeyspace, targetKeyspace)
	defer env.close()

	s1, err := env.ts.UpdateShardFields(ctx, targetKeyspace.KeyspaceName, "-", func(si *topo.ShardInfo) error {
		return nil
	})
	require.NoError(t, err)

	sourceTablet, ok := env.tablets[sourceKeyspace.KeyspaceName][100]
	require.True(t, ok)

	env.tmc.schema = map[string]*tabletmanagerdatapb.SchemaDefinition{
		"t1": {},
	}

	rules := make([]*binlogdatapb.Rule, len(env.tmc.schema))
	for i, table := range maps.Keys(env.tmc.schema) {
		rules[i] = &binlogdatapb.Rule{
			Match:  table,
			Filter: "select * from " + table,
		}
	}

	refKey := fmt.Sprintf("wf:%s:-", sourceKeyspace.KeyspaceName)

	testCases := []struct {
		name                             string
		addVReplicationWorkflowsResponse *tabletmanagerdatapb.ReadVReplicationWorkflowsResponse
		preRefStreams                    map[string]*refStream
		wantRefStreamKeys                []string
		wantErr                          bool
		errContains                      string
	}{
		{
			name: "error for unnamed workflow",
			addVReplicationWorkflowsResponse: &tabletmanagerdatapb.ReadVReplicationWorkflowsResponse{
				Workflows: []*tabletmanagerdatapb.ReadVReplicationWorkflowResponse{
					{
						Workflow:     "",
						WorkflowType: binlogdatapb.VReplicationWorkflowType_Reshard,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "populate ref streams",
			addVReplicationWorkflowsResponse: &tabletmanagerdatapb.ReadVReplicationWorkflowsResponse{
				Workflows: []*tabletmanagerdatapb.ReadVReplicationWorkflowResponse{
					{
						Workflow:     "wf",
						WorkflowType: binlogdatapb.VReplicationWorkflowType_Reshard,
						Streams: []*tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream{
							{

								Bls: &binlogdatapb.BinlogSource{
									Keyspace: sourceKeyspace.KeyspaceName,
									Shard:    "-",
									Tables:   maps.Keys(env.tmc.schema),
									Filter: &binlogdatapb.Filter{
										Rules: rules,
									},
								},
							},
						},
					},
				},
			},
			wantRefStreamKeys: []string{refKey},
		},
		{
			name:          "mismatched streams with empty map",
			preRefStreams: map[string]*refStream{},
			addVReplicationWorkflowsResponse: &tabletmanagerdatapb.ReadVReplicationWorkflowsResponse{
				Workflows: []*tabletmanagerdatapb.ReadVReplicationWorkflowResponse{
					{
						Workflow:     "wf",
						WorkflowType: binlogdatapb.VReplicationWorkflowType_Reshard,
						Streams: []*tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream{
							{

								Bls: &binlogdatapb.BinlogSource{
									Keyspace: sourceKeyspace.KeyspaceName,
									Shard:    "-",
									Tables:   maps.Keys(env.tmc.schema),
									Filter: &binlogdatapb.Filter{
										Rules: rules,
									},
								},
							},
						},
					},
				},
			},
			wantErr:     true,
			errContains: "mismatch",
		},
		{
			name: "mismatched streams",
			preRefStreams: map[string]*refStream{
				refKey:        nil,
				"nonexisting": nil,
			},
			addVReplicationWorkflowsResponse: &tabletmanagerdatapb.ReadVReplicationWorkflowsResponse{
				Workflows: []*tabletmanagerdatapb.ReadVReplicationWorkflowResponse{
					{
						Workflow:     "wf",
						WorkflowType: binlogdatapb.VReplicationWorkflowType_Reshard,
						Streams: []*tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream{
							{

								Bls: &binlogdatapb.BinlogSource{
									Keyspace: sourceKeyspace.KeyspaceName,
									Shard:    "-",
									Tables:   maps.Keys(env.tmc.schema),
									Filter: &binlogdatapb.Filter{
										Rules: rules,
									},
								},
							},
						},
					},
				},
			},
			wantErr:     true,
			errContains: "mismatch",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rs := &resharder{
				s:            env.ws,
				keyspace:     targetKeyspace.KeyspaceName,
				sourceShards: []*topo.ShardInfo{s1},
				sourcePrimaries: map[string]*topo.TabletInfo{
					"-": {
						Tablet: sourceTablet,
					},
				},
				workflow: "wf",
				vschema: &topo.KeyspaceVSchemaInfo{
					Name: targetKeyspace.KeyspaceName,
					Keyspace: &vschemapb.Keyspace{
						Tables: map[string]*vschemapb.Table{
							"t1": {
								Type: vindexes.TypeReference,
							},
						},
					},
				},
				refStreams: tc.preRefStreams,
			}

			workflowKey := env.tmc.GetWorkflowKey(sourceKeyspace.KeyspaceName, "-")

			env.tmc.AddVReplicationWorkflowsResponse(workflowKey, tc.addVReplicationWorkflowsResponse)

			err := rs.readRefStreams(ctx)
			if !tc.wantErr {
				assert.NoError(t, err)
				for _, rk := range tc.wantRefStreamKeys {
					assert.Contains(t, rs.refStreams, rk)
				}
				return
			}

			assert.Error(t, err)
			assert.ErrorContains(t, err, tc.errContains)
		})
	}
}

func TestBlsIsReference(t *testing.T) {
	testCases := []struct {
		name        string
		bls         *binlogdatapb.BinlogSource
		tables      map[string]*vschemapb.Table
		expected    bool
		wantErr     bool
		errContains string
	}{
		{
			name: "all references",
			bls: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{
						{Match: "ref_table1"},
						{Match: "ref_table2"},
					},
				},
			},
			tables: map[string]*vschemapb.Table{
				"ref_table1": {Type: vindexes.TypeReference},
				"ref_table2": {Type: vindexes.TypeReference},
			},
			expected: true,
		},
		{
			name: "all sharded",
			bls: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{
						{Match: "sharded_table1"},
						{Match: "sharded_table2"},
					},
				},
			},
			tables: map[string]*vschemapb.Table{
				"sharded_table1": {Type: vindexes.TypeTable},
				"sharded_table2": {Type: vindexes.TypeTable},
			},
			expected: false,
		},
		{
			name: "mixed reference and sharded tables",
			bls: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{
						{Match: "ref_table"},
						{Match: "sharded_table"},
					},
				},
			},
			tables: map[string]*vschemapb.Table{
				"ref_table":     {Type: vindexes.TypeReference},
				"sharded_table": {Type: vindexes.TypeTable},
			},
			wantErr: true,
		},
		{
			name: "rule table not found in vschema",
			bls: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{
						{Match: "unknown_table"},
					},
				},
			},
			tables:      map[string]*vschemapb.Table{},
			wantErr:     true,
			errContains: "unknown_table",
		},
		{
			name: "internal operation table ignored",
			bls: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{
						{Match: "_vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_"},
					},
				},
			},
			tables:   map[string]*vschemapb.Table{},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rs := &resharder{
				vschema: &topo.KeyspaceVSchemaInfo{
					Name: "ks",
					Keyspace: &vschemapb.Keyspace{
						Tables: tc.tables,
					},
				},
			}

			result, err := rs.blsIsReference(tc.bls)

			if tc.wantErr {
				assert.ErrorContains(t, err, tc.errContains)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}
