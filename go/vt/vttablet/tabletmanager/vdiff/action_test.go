/*
Copyright 2022 The Vitess Authors.

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

package vdiff

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/ptr"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vterrors"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func TestPerformVDiffAction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	vdiffenv := newTestVDiffEnv(t)
	defer vdiffenv.close()
	keyspace := "ks"
	workflow := "wf"
	uuid := uuid.New().String()
	type queryAndResult struct {
		query  string
		result *sqltypes.Result // Optional if you need a non-empty result
	}

	tests := []struct {
		name          string
		vde           *Engine
		req           *tabletmanagerdatapb.VDiffRequest
		preFunc       func() error
		postFunc      func() error
		want          *tabletmanagerdatapb.VDiffResponse
		expectQueries []queryAndResult
		wantErr       error
	}{
		{
			name:    "nil request",
			wantErr: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "nil vdiff request"),
		},
		{
			name:    "engine not open",
			req:     &tabletmanagerdatapb.VDiffRequest{},
			vde:     &Engine{isOpen: false},
			wantErr: vterrors.New(vtrpcpb.Code_UNAVAILABLE, "vdiff engine is closed"),
		},
		{
			name: "create with defaults",
			req: &tabletmanagerdatapb.VDiffRequest{
				Action:    string(CreateAction),
				VdiffUuid: uuid,
				Options: &tabletmanagerdatapb.VDiffOptions{
					PickerOptions: &tabletmanagerdatapb.VDiffPickerOptions{},
				},
			},
			// Add a second cell. The default for source_cell and target_cell is all
			// available cells, so this additional cell should then show up in the
			// created vdiff record.
			preFunc: func() error {
				return tstenv.TopoServ.CreateCellInfo(ctx, "zone100_test", &topodatapb.CellInfo{})
			},
			expectQueries: []queryAndResult{
				{
					query: "select id as id from _vt.vdiff where vdiff_uuid = " + encodeString(uuid),
				},
				{
					query: fmt.Sprintf(`insert into _vt.vdiff(keyspace, workflow, state, options, shard, db_name, vdiff_uuid) values('', '', 'pending', '{"picker_options":{"source_cell":"cell1,zone100_test","target_cell":"cell1,zone100_test"}}', '0', 'vt_vttest', %s)`, encodeString(uuid)),
				},
			},
			postFunc: func() error {
				return tstenv.TopoServ.DeleteCellInfo(ctx, "zone100_test", true)
			},
		},
		{
			name: "create without starting",
			req: &tabletmanagerdatapb.VDiffRequest{
				Action:    string(CreateAction),
				VdiffUuid: uuid,
				Options: &tabletmanagerdatapb.VDiffOptions{
					PickerOptions: &tabletmanagerdatapb.VDiffPickerOptions{},
					CoreOptions: &tabletmanagerdatapb.VDiffCoreOptions{
						AutoStart: ptr.Of(false),
					},
				},
			},
			expectQueries: []queryAndResult{
				{
					query: "select id as id from _vt.vdiff where vdiff_uuid = " + encodeString(uuid),
				},
				{
					query: fmt.Sprintf(`insert into _vt.vdiff(keyspace, workflow, state, options, shard, db_name, vdiff_uuid) values('', '', 'stopped', '{"picker_options":{"source_cell":"cell1","target_cell":"cell1"},"core_options":{"auto_start":false}}', '0', 'vt_vttest', %s)`, encodeString(uuid)),
				},
			},
		},
		{
			name: "create with cell alias",
			req: &tabletmanagerdatapb.VDiffRequest{
				Action:    string(CreateAction),
				VdiffUuid: uuid,
				Options: &tabletmanagerdatapb.VDiffOptions{
					PickerOptions: &tabletmanagerdatapb.VDiffPickerOptions{
						SourceCell: "all",
						TargetCell: "all",
					},
				},
			},
			// Add a second cell and create an cell alias that contains it.
			preFunc: func() error {
				if err := tstenv.TopoServ.CreateCellInfo(ctx, "zone100_test", &topodatapb.CellInfo{}); err != nil {
					return err
				}
				cells := append(tstenv.Cells, "zone100_test")
				return tstenv.TopoServ.CreateCellsAlias(ctx, "all", &topodatapb.CellsAlias{
					Cells: cells,
				})
			},
			expectQueries: []queryAndResult{
				{
					query: "select id as id from _vt.vdiff where vdiff_uuid = " + encodeString(uuid),
				},
				{
					query: fmt.Sprintf(`insert into _vt.vdiff(keyspace, workflow, state, options, shard, db_name, vdiff_uuid) values('', '', 'pending', '{"picker_options":{"source_cell":"all","target_cell":"all"}}', '0', 'vt_vttest', %s)`, encodeString(uuid)),
				},
			},
			postFunc: func() error {
				if err := tstenv.TopoServ.DeleteCellInfo(ctx, "zone100_test", true); err != nil {
					return err
				}
				return tstenv.TopoServ.DeleteCellsAlias(ctx, "all")
			},
		},
		{
			name: "resume never started vdiff",
			req: &tabletmanagerdatapb.VDiffRequest{
				Action:    string(ResumeAction),
				VdiffUuid: uuid,
				Keyspace:  keyspace,
				Workflow:  workflow,
			},
			expectQueries: []queryAndResult{
				{
					query: "select id as id from _vt.vdiff where vdiff_uuid = " + encodeString(uuid),
					result: sqltypes.MakeTestResult(
						sqltypes.MakeTestFields(
							"id",
							"int64",
						),
						"1",
					),
				},
				{
					query: fmt.Sprintf(`update _vt.vdiff as vd, _vt.vdiff_table as vdt set vd.started_at = NULL, vd.completed_at = NULL, vd.state = 'pending',
					vdt.state = 'pending' where vd.vdiff_uuid = %s and vd.id = vdt.vdiff_id and vd.state in ('completed', 'stopped')
					and vdt.state in ('completed', 'stopped')`, encodeString(uuid)),
					result: &sqltypes.Result{
						RowsAffected: 0, // No _vt.vdiff_table records
					},
				},
				{
					query: fmt.Sprintf(`update _vt.vdiff as vd set vd.state = 'pending' where vd.vdiff_uuid = %s and vd.state = 'stopped' and
					vd.started_at is NULL and vd.completed_at is NULL and
					(select count(*) as cnt from _vt.vdiff_table as vdt where vd.id = vdt.vdiff_id) = 0`,
						encodeString(uuid)),
					result: &sqltypes.Result{
						RowsAffected: 1,
					},
				},
				{
					query: "select * from _vt.vdiff where id = 1",
				},
			},
		},
		{
			name: "resume completed vdiff",
			req: &tabletmanagerdatapb.VDiffRequest{
				Action:    string(ResumeAction),
				VdiffUuid: uuid,
				Keyspace:  keyspace,
				Workflow:  workflow,
			},
			expectQueries: []queryAndResult{
				{
					query: "select id as id from _vt.vdiff where vdiff_uuid = " + encodeString(uuid),
					result: sqltypes.MakeTestResult(
						sqltypes.MakeTestFields(
							"id",
							"int64",
						),
						"1",
					),
				},
				{
					query: fmt.Sprintf(`update _vt.vdiff as vd, _vt.vdiff_table as vdt set vd.started_at = NULL, vd.completed_at = NULL, vd.state = 'pending',
					vdt.state = 'pending' where vd.vdiff_uuid = %s and vd.id = vdt.vdiff_id and vd.state in ('completed', 'stopped')
					and vdt.state in ('completed', 'stopped')`, encodeString(uuid)),
					result: &sqltypes.Result{
						RowsAffected: 1,
					},
				},
				{
					query: "select * from _vt.vdiff where id = 1",
				},
			},
		},
		{
			name: "delete by uuid",
			req: &tabletmanagerdatapb.VDiffRequest{
				Action:    string(DeleteAction),
				ActionArg: uuid,
			},
			expectQueries: []queryAndResult{
				{
					query: "select id as id from _vt.vdiff where vdiff_uuid = " + encodeString(uuid),
					result: sqltypes.MakeTestResult(
						sqltypes.MakeTestFields(
							"id",
							"int64",
						),
						"1",
					),
				},
				{
					query: "delete from vd, vdt using _vt.vdiff as vd left join _vt.vdiff_table as vdt on (vd.id = vdt.vdiff_id)\n\t\t\t\t\t\t\twhere vd.vdiff_uuid = " + encodeString(uuid),
				},
			},
		},
		{
			name: "delete all",
			req: &tabletmanagerdatapb.VDiffRequest{
				Action:    string(DeleteAction),
				ActionArg: "all",
				Keyspace:  keyspace,
				Workflow:  workflow,
			},
			expectQueries: []queryAndResult{
				{
					query: fmt.Sprintf("select id as id from _vt.vdiff where keyspace = %s and workflow = %s", encodeString(keyspace), encodeString(workflow)),
					result: sqltypes.MakeTestResult(
						sqltypes.MakeTestFields(
							"id",
							"int64",
						),
						"1",
						"2",
					),
				},
				{
					query: fmt.Sprintf(`delete from vd, vdt, vdl using _vt.vdiff as vd left join _vt.vdiff_table as vdt on (vd.id = vdt.vdiff_id)
										left join _vt.vdiff_log as vdl on (vd.id = vdl.vdiff_id)
										where vd.keyspace = %s and vd.workflow = %s`, encodeString(keyspace), encodeString(workflow)),
				},
			},
		},
		{
			name: "show last",
			req: &tabletmanagerdatapb.VDiffRequest{
				Action:    string(ShowAction),
				ActionArg: "last",
				Keyspace:  keyspace,
				Workflow:  workflow,
			},
			expectQueries: []queryAndResult{
				{
					query: fmt.Sprintf("select * from _vt.vdiff where keyspace = %s and workflow = %s order by id desc limit %d",
						encodeString(keyspace), encodeString(workflow), 1),
					result: noResults,
				},
			},
		},
		{
			name: "show all",
			req: &tabletmanagerdatapb.VDiffRequest{
				Action:    string(ShowAction),
				ActionArg: "all",
				Keyspace:  keyspace,
				Workflow:  workflow,
			},
			expectQueries: []queryAndResult{
				{
					query: fmt.Sprintf("select * from _vt.vdiff where keyspace = %s and workflow = %s order by id desc limit %d",
						encodeString(keyspace), encodeString(workflow), maxVDiffsToReport),
					result: noResults,
				},
			},
		},
	}

	errCount := int64(0)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preFunc != nil {
				err := tt.preFunc()
				require.NoError(t, err, "pre function failed: %v", err)
			}
			if tt.vde == nil {
				tt.vde = vdiffenv.vde
			}
			for _, queryResult := range tt.expectQueries {
				if queryResult.result == nil {
					queryResult.result = &sqltypes.Result{}
				}
				vdiffenv.dbClient.ExpectRequest(queryResult.query, queryResult.result, nil)
			}
			got, err := tt.vde.PerformVDiffAction(ctx, tt.req)
			if err != nil {
				errCount++
			}
			vdiffenv.dbClient.Wait()
			if tt.wantErr != nil && !vterrors.Equals(err, tt.wantErr) {
				t.Errorf("Engine.PerformVDiffAction() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.want != nil && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Engine.PerformVDiffAction() = %v, want %v", got, tt.want)
			}
			if tt.postFunc != nil {
				err := tt.postFunc()
				require.NoError(t, err, "post function failed: %v", err)
			}
			// No VDiffs should be running anymore.
			require.Equal(t, 0, len(vdiffenv.vde.controllers), "expected no controllers to be running, but found %d",
				len(vdiffenv.vde.controllers))
			require.Equal(t, int64(0), globalStats.numControllers(), "expected no controllers, but found %d")
		})
		require.Equal(t, errCount, globalStats.ErrorCount.Get(), "expected error count %d, got %d", errCount, globalStats.ErrorCount.Get())
	}
}
