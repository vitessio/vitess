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

	"github.com/stretchr/testify/require"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func setupMoveTables(t *testing.T, ctx context.Context) *testEnv {
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
	var wfs tabletmanagerdata.ReadVReplicationWorkflowsResponse
	id := int32(1)
	wfs.Workflows = append(wfs.Workflows, &tabletmanagerdata.ReadVReplicationWorkflowResponse{
		Workflow:     "wf1",
		WorkflowType: binlogdatapb.VReplicationWorkflowType_MoveTables,
	}, &tabletmanagerdata.ReadVReplicationWorkflowResponse{
		Workflow:     "wf2",
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
		nil,                                // this is the response for getting stopped workflows
		&wfs, &wfs, &wfs, &wfs, &wfs, &wfs, // return the full list for subsequent GetWorkflows calls
	}
	for _, resp := range workflowResponses {
		te.tmc.AddVReplicationWorkflowsResponse(workflowKey, resp)
	}
	te.tmc.readVReplicationWorkflowRequests[200] = &tabletmanagerdata.ReadVReplicationWorkflowRequest{
		Workflow: "wf1",
	}
	te.updateTableRoutingRules(t, ctx, nil, []string{"t1"},
		"source", te.targetKeyspace.KeyspaceName, "source")
	return te
}

// TestWorkflowStateMoveTables tests the logic used to determine the state of a MoveTables workflow based on the
// routing rules. We setup two workflows with the same table in both source and target keyspaces.
func TestWorkflowStateMoveTables(t *testing.T) {
	ctx := context.Background()
	te := setupMoveTables(t, ctx)
	require.NotNil(t, te)
	type testCase struct {
		name                   string
		wf1SwitchedTabletTypes []topodatapb.TabletType
		wf1ExpectedState       string
		// Simulate a second workflow to validate that the logic used to determine the state of the first workflow
		// from the routing rules is not affected by the presence of other workflows in different states.
		wf2SwitchedTabletTypes []topodatapb.TabletType
	}
	testCases := []testCase{
		{
			name:                   "switch reads",
			wf1SwitchedTabletTypes: []topodatapb.TabletType{topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY},
			wf1ExpectedState:       "All Reads Switched. Writes Not Switched",
			wf2SwitchedTabletTypes: []topodatapb.TabletType{topodatapb.TabletType_PRIMARY},
		},
		{
			name:                   "switch writes",
			wf1SwitchedTabletTypes: []topodatapb.TabletType{topodatapb.TabletType_PRIMARY},
			wf1ExpectedState:       "Reads Not Switched. Writes Switched",
			wf2SwitchedTabletTypes: []topodatapb.TabletType{topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY},
		},
		{
			name:                   "switch reads and writes",
			wf1SwitchedTabletTypes: defaultTabletTypes,
			wf1ExpectedState:       "All Reads Switched. Writes Switched",
		},
		{
			name:                   "switch rdonly only",
			wf1SwitchedTabletTypes: []topodatapb.TabletType{topodatapb.TabletType_RDONLY},
			wf1ExpectedState:       "Reads partially switched. Replica not switched. All Rdonly Reads Switched. Writes Not Switched",
			wf2SwitchedTabletTypes: []topodatapb.TabletType{topodatapb.TabletType_PRIMARY},
		},
		{
			name:                   "switch replica only",
			wf1SwitchedTabletTypes: []topodatapb.TabletType{topodatapb.TabletType_REPLICA},
			wf1ExpectedState:       "Reads partially switched. All Replica Reads Switched. Rdonly not switched. Writes Not Switched",
			wf2SwitchedTabletTypes: defaultTabletTypes,
		},
	}
	tables := []string{"t1"}

	getStateString := func(targetKeyspace, wfName string) string {
		tsw, state, err := te.ws.getWorkflowState(ctx, targetKeyspace, wfName)
		require.NoError(t, err)
		require.NotNil(t, tsw)
		require.NotNil(t, state)
		return state.String()
	}
	require.Equal(t, "Reads Not Switched. Writes Not Switched", getStateString("target", "wf1"))

	resetRoutingRules := func() {
		te.updateTableRoutingRules(t, ctx, nil, tables,
			"source", te.targetKeyspace.KeyspaceName, "source")
		te.updateTableRoutingRules(t, ctx, nil, tables,
			"source2", "target2", "source2")
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resetRoutingRules()
			te.updateTableRoutingRules(t, ctx, tc.wf1SwitchedTabletTypes, tables,
				"source", te.targetKeyspace.KeyspaceName, te.targetKeyspace.KeyspaceName)
			te.updateTableRoutingRules(t, ctx, tc.wf2SwitchedTabletTypes, tables,
				"source2", "target2", "target2")
			require.Equal(t, tc.wf1ExpectedState, getStateString("target", "wf1"))
		})
	}
}
