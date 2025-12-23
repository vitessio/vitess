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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/topo"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

func TestGetStreamState(t *testing.T) {
	testCases := []struct {
		name    string
		stream  *vtctldatapb.Workflow_Stream
		rstream *tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream
		want    string
	}{
		{
			name: "error state",
			stream: &vtctldatapb.Workflow_Stream{
				Message: "test error",
			},
			want: "Error",
		},
		{
			name: "copying state",
			stream: &vtctldatapb.Workflow_Stream{
				State: "Running",
				CopyStates: []*vtctldatapb.Workflow_Stream_CopyState{
					{
						Table: "table1",
					},
				},
			},
			want: "Copying",
		},
		{
			name: "lagging state",
			stream: &vtctldatapb.Workflow_Stream{
				State: "Running",
			},
			rstream: &tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream{
				TimeUpdated: &vttime.Time{
					Seconds: int64(time.Now().Second()) - 11,
				},
			},
			want: "Lagging",
		},
		{
			name: "non-running and error free",
			stream: &vtctldatapb.Workflow_Stream{
				State: "Stopped",
			},
			rstream: &tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream{
				State: binlogdata.VReplicationWorkflowState_Stopped,
			},
			want: "Stopped",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			state := getStreamState(tt.stream, tt.rstream)
			assert.Equal(t, tt.want, state)
		})
	}
}

func TestGetWorkflowCopyStates(t *testing.T) {
	ctx := context.Background()

	sourceShards := []string{"-"}
	targetShards := []string{"-"}

	te := newTestMaterializerEnv(t, ctx, &vtctldatapb.MaterializeSettings{
		SourceKeyspace: "source_keyspace",
		TargetKeyspace: "target_keyspace",
		Workflow:       "test_workflow",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{
			{
				TargetTable:      "table1",
				SourceExpression: "select * from " + "table1",
			},
			{
				TargetTable:      "table2",
				SourceExpression: "select * from " + "table2",
			},
		},
	}, sourceShards, targetShards)

	wf := workflowFetcher{
		ts:  te.ws.ts,
		tmc: te.tmc,
	}

	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone1",
			Uid:  100,
		},
	}

	query := "select vrepl_id, table_name, lastpk from _vt.copy_state where vrepl_id in (1) and id in (select max(id) from _vt.copy_state where vrepl_id in (1) group by vrepl_id, table_name)"
	te.tmc.expectVRQuery(100, query, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("vrepl_id|table_name|lastpk", "int64|varchar|varchar"),
		"1|table1|2", "1|table2|1",
	))

	copyStates, err := wf.getWorkflowCopyStates(ctx, &topo.TabletInfo{
		Tablet: tablet,
	}, []int32{1})
	assert.NoError(t, err)
	assert.Len(t, copyStates, 2)

	state1 := &vtctldatapb.Workflow_Stream_CopyState{
		Table:    "table1",
		LastPk:   "2",
		StreamId: 1,
	}
	state2 := &vtctldatapb.Workflow_Stream_CopyState{
		Table:    "table2",
		LastPk:   "1",
		StreamId: 1,
	}
	assert.Contains(t, copyStates, state1)
	assert.Contains(t, copyStates, state2)
}

func TestFetchCopyStatesByShardStream(t *testing.T) {
	ctx := context.Background()

	sourceShards := []string{"-"}
	targetShards := []string{"-"}

	te := newTestMaterializerEnv(t, ctx, &vtctldatapb.MaterializeSettings{
		SourceKeyspace: "source_keyspace",
		TargetKeyspace: "target_keyspace",
		Workflow:       "test_workflow",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{
			{
				TargetTable:      "table1",
				SourceExpression: "select * from " + "table1",
			},
			{
				TargetTable:      "table2",
				SourceExpression: "select * from " + "table2",
			},
		},
	}, sourceShards, targetShards)

	wf := workflowFetcher{
		ts:  te.ws.ts,
		tmc: te.tmc,
	}

	tablet := &topodatapb.Tablet{
		Shard: "-80",
		Alias: &topodatapb.TabletAlias{
			Cell: "zone1",
			Uid:  100,
		},
	}
	tablet2 := &topodatapb.Tablet{
		Shard: "80-",
		Alias: &topodatapb.TabletAlias{
			Cell: "zone1",
			Uid:  101,
		},
	}

	query := "select vrepl_id, table_name, lastpk from _vt.copy_state where vrepl_id in (1, 2) and id in (select max(id) from _vt.copy_state where vrepl_id in (1, 2) group by vrepl_id, table_name)"
	te.tmc.expectVRQuery(100, query, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("vrepl_id|table_name|lastpk", "int64|varchar|varchar"),
		"1|table1|2", "2|table2|1", "2|table1|1",
	))

	te.tmc.expectVRQuery(101, query, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("vrepl_id|table_name|lastpk", "int64|varchar|varchar"),
		"1|table1|2", "1|table2|1",
	))

	ti := &topo.TabletInfo{
		Tablet: tablet,
	}
	ti2 := &topo.TabletInfo{
		Tablet: tablet2,
	}

	readVReplicationResponse := map[*topo.TabletInfo]*tabletmanagerdatapb.ReadVReplicationWorkflowsResponse{
		ti: {
			Workflows: []*tabletmanagerdatapb.ReadVReplicationWorkflowResponse{
				{
					Streams: []*tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream{
						{
							Id: 1,
						}, {
							Id: 2,
						},
					},
				},
			},
		},
		ti2: {
			Workflows: []*tabletmanagerdatapb.ReadVReplicationWorkflowResponse{
				{
					Streams: []*tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream{
						{
							Id: 1,
						}, {
							Id: 2,
						},
					},
				},
			},
		},
	}
	copyStatesByStreamId, err := wf.fetchCopyStatesByShardStream(ctx, readVReplicationResponse)
	assert.NoError(t, err)

	copyStates1 := copyStatesByStreamId["-80/1"]
	copyStates2 := copyStatesByStreamId["-80/2"]
	copyStates3 := copyStatesByStreamId["80-/1"]

	require.NotNil(t, copyStates1)
	require.NotNil(t, copyStates2)
	require.NotNil(t, copyStates3)

	assert.Len(t, copyStates1, 1)
	assert.Len(t, copyStates2, 2)
	assert.Len(t, copyStates3, 2)

	assert.Nil(t, copyStatesByStreamId["80-/2"])
}
