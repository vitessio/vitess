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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
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
		})
	}
}
