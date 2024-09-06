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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func setupMoveTables(t *testing.T) (context.Context, *testEnv) {
	ctx := context.Background()
	schema := map[string]*tabletmanagerdata.SchemaDefinition{
		"t1": {
			TableDefinitions: []*tabletmanagerdata.TableDefinition{
				{
					Name:   "t1",
					Schema: fmt.Sprintf("CREATE TABLE %s (id BIGINT, name VARCHAR(64), PRIMARY KEY (id))", "t1"),
				},
			},
		},
	}
	sourceKeyspace := &testKeyspace{
		KeyspaceName: "source",
		ShardNames:   []string{"0"},
	}
	targetKeyspace := &testKeyspace{
		KeyspaceName: "target",
		ShardNames:   []string{"0"},
	}
	te := newTestEnv(t, ctx, "zone1", sourceKeyspace, targetKeyspace)
	te.tmc.schema = schema
	for k := range te.tablets {
		for k2 := range te.tablets[k] {
			fmt.Println(k, k2)
		}
	}
	var wfs tabletmanagerdata.ReadVReplicationWorkflowsResponse
	wfName := "wf1"
	id := int32(1)
	wfs.Workflows = append(wfs.Workflows, &tabletmanagerdata.ReadVReplicationWorkflowResponse{
		Workflow:     wfName,
		WorkflowType: binlogdatapb.VReplicationWorkflowType_MoveTables,
	})
	wfs.Workflows[0].Streams = append(wfs.Workflows[0].Streams, &tabletmanagerdata.ReadVReplicationWorkflowResponse_Stream{
		Id: id,
		Bls: &binlogdatapb.BinlogSource{
			Keyspace: te.sourceKeyspace.KeyspaceName,
			Shard:    "0",
			Filter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{
					{Match: "t1", Filter: "select * from t1"},
				},
			},
			Tables: []string{"t1"},
		},
		Pos:   position,
		State: binlogdatapb.VReplicationWorkflowState_Running,
	})
	workflowKey := te.tmc.GetWorkflowKey("target", "wf1")
	workflowResponses := []*tabletmanagerdata.ReadVReplicationWorkflowsResponse{
		nil,              // this is the response for getting stopped workflows
		&wfs, &wfs, &wfs, // return the full list for subsequent GetWorkflows calls
	}
	for _, resp := range workflowResponses {
		te.tmc.AddVReplicationWorkflowsResponse(workflowKey, resp)
	}
	te.tmc.readVReplicationWorkflowRequests[200] = &tabletmanagerdata.ReadVReplicationWorkflowRequest{
		Workflow: wfName,
	}
	te.updateTableRoutingRules(t, ctx, nil, []string{"t1"}, te.sourceKeyspace.KeyspaceName)
	return ctx, te
}

func TestWorkflowStateMoveTables(t *testing.T) {
	ctx, te := setupMoveTables(t)
	require.NotNil(t, te)
	type testCase struct {
		name        string
		tabletTypes []topodatapb.TabletType
		wantState   string
	}
	testCases := []testCase{
		{
			name:        "switch reads",
			tabletTypes: []topodatapb.TabletType{topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY},
			wantState:   "All Reads Switched. Writes Not Switched",
		},
		{
			name:        "switch writes",
			tabletTypes: []topodatapb.TabletType{topodatapb.TabletType_PRIMARY},
			wantState:   "Reads Not Switched. Writes Switched",
		},
		{
			name:        "switch reads and writes",
			tabletTypes: []topodatapb.TabletType{topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY},
			wantState:   "All Reads Switched. Writes Switched",
		},
		{
			name:        "switch rdonly only",
			tabletTypes: []topodatapb.TabletType{topodatapb.TabletType_RDONLY},
			wantState:   "Reads partially switched. Replica not switched. All Rdonly Reads Switched. Writes Not Switched",
		},
		{
			name:        "switch replica only",
			tabletTypes: []topodatapb.TabletType{topodatapb.TabletType_REPLICA},
			wantState:   "Reads partially switched. All Replica Reads Switched. Rdonly not switched. Writes Not Switched",
		},
	}
	tables := []string{"t1"}

	getStateString := func() string {
		tsw, state, err := te.ws.getWorkflowState(ctx, te.targetKeyspace.KeyspaceName, "wf1")
		require.NoError(t, err)
		require.NotNil(t, tsw)
		require.NotNil(t, state)
		return state.String()
	}
	initState := getStateString()
	require.Equal(t, "Reads Not Switched. Writes Not Switched", initState)
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			te.updateTableRoutingRules(t, ctx, tc.tabletTypes, tables, te.targetKeyspace.KeyspaceName)
			require.Equal(t, tc.wantState, getStateString())
			// reset to initial state
			te.updateTableRoutingRules(t, ctx, tc.tabletTypes, tables, te.sourceKeyspace.KeyspaceName)
		})
	}
}
