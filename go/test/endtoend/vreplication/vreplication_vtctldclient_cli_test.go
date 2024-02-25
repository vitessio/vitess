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

package vreplication

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/test/endtoend/cluster"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// TestVtctldclientCLI tests the vreplication vtctldclient CLI commands, primarily to check that non-standard flags
// are being handled correctly. The other end-to-end tests are expected to test the various common workflows.
func TestVtctldclientCLI(t *testing.T) {
	setSidecarDBName("_vt")
	var err error
	origDefaultRdonly := defaultRdonly
	defer func() {
		defaultRdonly = origDefaultRdonly
	}()
	defaultRdonly = 0
	vc = setupMinimalCluster(t)

	err = vc.Vtctl.AddCellInfo("zone2")
	require.NoError(t, err)
	zone2, err := vc.AddCell(t, "zone2")
	require.NoError(t, err)
	require.NotNil(t, zone2)
	defer vc.TearDown()

	sourceKeyspaceName := "product"
	targetKeyspaceName := "customer"
	var mt iMoveTables
	workflowName := "wf1"
	targetTabs := setupMinimalCustomerKeyspace(t)

	t.Run("MoveTablesCreateFlags1", func(t *testing.T) {
		testMoveTablesFlags1(t, &mt, sourceKeyspaceName, targetKeyspaceName, workflowName, targetTabs)
	})
	t.Run("MoveTablesCreateFlags2", func(t *testing.T) {
		testMoveTablesFlags2(t, &mt, sourceKeyspaceName, targetKeyspaceName, workflowName, targetTabs)
	})
	t.Run("MoveTablesCompleteFlags3", func(t *testing.T) {
		testMoveTablesFlags3(t, sourceKeyspaceName, targetKeyspaceName, targetTabs)
	})
	t.Run("Reshard", func(t *testing.T) {
		cell := vc.Cells["zone1"]
		targetKeyspace := cell.Keyspaces[targetKeyspaceName]
		sourceShard := "-80"
		newShards := "-40,40-80"
		require.NoError(t, vc.AddShards(t, []*Cell{cell}, targetKeyspace, newShards, 1, 0, 400, nil))
		reshardWorkflowName := "reshard"
		tablets := map[string]*cluster.VttabletProcess{
			"-40":   targetKeyspace.Shards["-40"].Tablets["zone1-400"].Vttablet,
			"40-80": targetKeyspace.Shards["40-80"].Tablets["zone1-500"].Vttablet,
		}
		splitShard(t, targetKeyspaceName, reshardWorkflowName, sourceShard, newShards, tablets)
	})
}

// Tests several create flags and some complete flags and validates that some of them are set correctly for the workflow.
func testMoveTablesFlags1(t *testing.T, mt *iMoveTables, sourceKeyspace, targetKeyspace, workflowName string, targetTabs map[string]*cluster.VttabletProcess) {
	tables := "customer,customer2"
	createFlags := []string{"--auto-start=false", "--defer-secondary-keys=false", "--stop-after-copy",
		"--no-routing-rules", "--on-ddl", "STOP", "--exclude-tables", "customer2",
		"--tablet-types", "primary,rdonly", "--tablet-types-in-preference-order=true",
		"--all-cells",
	}
	completeFlags := []string{"--keep-routing-rules", "--keep-data"}
	switchFlags := []string{}
	// Test one set of MoveTable flags.
	*mt = createMoveTables(t, sourceKeyspace, targetKeyspace, workflowName, tables, createFlags, completeFlags, switchFlags)
	(*mt).Show()
	moveTablesResponse := getMoveTablesShowResponse(mt)
	workflowResponse := getWorkflow(targetKeyspace, workflowName)

	// also validates that MoveTables Show and Workflow Show return the same output.
	require.EqualValues(t, moveTablesResponse.CloneVT(), workflowResponse)

	// Validate that the flags are set correctly in the database.
	validateMoveTablesWorkflow(t, workflowResponse.Workflows)
	// Since we used --no-routing-rules, there should be no routing rules.
	confirmNoRoutingRules(t)
}

func getMoveTablesShowResponse(mt *iMoveTables) *vtctldatapb.GetWorkflowsResponse {
	moveTablesOutput := (*mt).GetLastOutput()
	var moveTablesResponse vtctldatapb.GetWorkflowsResponse
	err := protojson.Unmarshal([]byte(moveTablesOutput), &moveTablesResponse)
	require.NoError(vc.t, err)
	moveTablesResponse.Workflows[0].MaxVReplicationTransactionLag = 0
	moveTablesResponse.Workflows[0].MaxVReplicationLag = 0
	return moveTablesResponse.CloneVT()
}

// Validates some of the flags created from the previous test.
func testMoveTablesFlags2(t *testing.T, mt *iMoveTables, sourceKeyspace, targetKeyspace, workflowName string, targetTabs map[string]*cluster.VttabletProcess) {
	ksWorkflow := fmt.Sprintf("%s.%s", targetKeyspace, workflowName)
	(*mt).Start() // Need to start because we set auto-start to false.
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Stopped.String())
	confirmNoRoutingRules(t)
	for _, tab := range targetTabs {
		alias := fmt.Sprintf("zone1-%d", tab.TabletUID)
		query := "update _vt.vreplication set source := replace(source, 'stop_after_copy:true', 'stop_after_copy:false') where db_name = 'vt_customer' and workflow = 'wf1'"
		output, err := vc.VtctlClient.ExecuteCommandWithOutput("ExecuteFetchAsDba", alias, query)
		require.NoError(t, err, output)
	}
	confirmNoRoutingRules(t)
	(*mt).Start() // Need to start because we set stop-after-copy to true.
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())
	(*mt).Stop() // Test stopping workflow.
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Stopped.String())
	(*mt).Start()
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())
	for _, tab := range targetTabs {
		catchup(t, tab, workflowName, "MoveTables")
	}
	(*mt).SwitchReadsAndWrites()
	(*mt).Complete()
	confirmRoutingRulesExist(t)
	// Confirm that --keep-data was honored.
	require.True(t, checkTablesExist(t, "zone1-100", []string{"customer", "customer2"}))
}

// Tests SwitchTraffic and Complete flags
func testMoveTablesFlags3(t *testing.T, sourceKeyspace, targetKeyspace string, targetTabs map[string]*cluster.VttabletProcess) {
	for _, tab := range targetTabs {
		alias := fmt.Sprintf("zone1-%d", tab.TabletUID)
		output, err := vc.VtctlClient.ExecuteCommandWithOutput("ExecuteFetchAsDba", alias, "drop table customer")
		require.NoError(t, err, output)
	}
	createFlags := []string{}
	completeFlags := []string{"--rename-tables"}
	tables := "customer2"
	switchFlags := []string{"--enable-reverse-replication=false"}
	mt := createMoveTables(t, sourceKeyspace, targetKeyspace, workflowName, tables, createFlags, completeFlags, switchFlags)
	mt.Start() // Need to start because we set stop-after-copy to true.
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())
	mt.Stop() // Test stopping workflow.
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Stopped.String())
	mt.Start()
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())
	for _, tab := range targetTabs {
		catchup(t, tab, workflowName, "MoveTables")
	}
	mt.SwitchReadsAndWrites()
	mt.Complete()
	// Confirm that the source tables were renamed.
	require.True(t, checkTablesExist(t, "zone1-100", []string{"_customer2_old"}))
	require.False(t, checkTablesExist(t, "zone1-100", []string{"customer2"}))
}

func createMoveTables(t *testing.T, sourceKeyspace, targetKeyspace, workflowName, tables string,
	createFlags, completeFlags, switchFlags []string) iMoveTables {
	mt := newMoveTables(vc, &moveTablesWorkflow{
		workflowInfo: &workflowInfo{
			vc:             vc,
			workflowName:   workflowName,
			targetKeyspace: targetKeyspace,
		},
		sourceKeyspace: sourceKeyspace,
		tables:         tables,
		createFlags:    createFlags,
		completeFlags:  completeFlags,
		switchFlags:    switchFlags,
	}, workflowFlavorVtctld)
	mt.Create()
	return mt
}

// reshard helpers

func splitShard(t *testing.T, keyspace, workflowName, sourceShards, targetShards string, targetTabs map[string]*cluster.VttabletProcess) {
	createFlags := []string{"--auto-start=false", "--defer-secondary-keys=false", "--stop-after-copy",
		"--on-ddl", "STOP", "--tablet-types", "primary,rdonly", "--tablet-types-in-preference-order=true",
		"--all-cells", "--format=json",
	}
	rs := newReshard(vc, &reshardWorkflow{
		workflowInfo: &workflowInfo{
			vc:             vc,
			workflowName:   workflowName,
			targetKeyspace: keyspace,
		},
		sourceShards: sourceShards,
		targetShards: targetShards,
		createFlags:  createFlags,
	}, workflowFlavorVtctld)

	ksWorkflow := fmt.Sprintf("%s.%s", keyspace, workflowName)
	rs.Create()
	validateReshardResponse(rs)
	workflowResponse := getWorkflow(keyspace, workflowName)
	reshardShowResponse := getReshardShowResponse(&rs)
	require.EqualValues(t, reshardShowResponse, workflowResponse)
	validateReshardWorkflow(t, workflowResponse.Workflows)
	waitForWorkflowState(t, vc, fmt.Sprintf("%s.%s", keyspace, workflowName), binlogdatapb.VReplicationWorkflowState_Stopped.String())
	rs.Start()
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Stopped.String())
	for _, tab := range targetTabs {
		alias := fmt.Sprintf("zone1-%d", tab.TabletUID)
		query := "update _vt.vreplication set source := replace(source, 'stop_after_copy:true', 'stop_after_copy:false') where db_name = 'vt_customer' and workflow = '" + workflowName + "'"
		output, err := vc.VtctlClient.ExecuteCommandWithOutput("ExecuteFetchAsDba", alias, query)
		require.NoError(t, err, output)
	}
	rs.Start()
	waitForWorkflowState(t, vc, fmt.Sprintf("%s.%s", keyspace, workflowName), binlogdatapb.VReplicationWorkflowState_Running.String())
	rs.Stop()
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Stopped.String())
	rs.Start()
	waitForWorkflowState(t, vc, fmt.Sprintf("%s.%s", keyspace, workflowName), binlogdatapb.VReplicationWorkflowState_Running.String())
	for _, targetTab := range targetTabs {
		catchup(t, targetTab, workflowName, "Reshard")
	}
	vdiff(t, keyspace, workflowName, "zone1", false, true, nil)

	rs.SwitchReadsAndWrites()
	waitForLowLag(t, keyspace, workflowName+"_reverse")
	vdiff(t, keyspace, workflowName+"_reverse", "zone1", true, false, nil)

	rs.ReverseReadsAndWrites()
	waitForLowLag(t, keyspace, workflowName)
	vdiff(t, keyspace, workflowName, "zone1", false, true, nil)
	rs.SwitchReadsAndWrites()
	rs.Complete()
}

func getReshardShowResponse(rs *iReshard) *vtctldatapb.GetWorkflowsResponse {
	(*rs).Show()
	reshardOutput := (*rs).GetLastOutput()
	var reshardResponse vtctldatapb.GetWorkflowsResponse
	err := protojson.Unmarshal([]byte(reshardOutput), &reshardResponse)
	require.NoError(vc.t, err)
	reshardResponse.Workflows[0].MaxVReplicationTransactionLag = 0
	reshardResponse.Workflows[0].MaxVReplicationLag = 0
	return reshardResponse.CloneVT()
}

func validateReshardResponse(rs iReshard) {
	resp := getReshardResponse(rs)
	require.NotNil(vc.t, resp)
	require.NotNil(vc.t, resp.ShardStreams)
	require.Equal(vc.t, len(resp.ShardStreams), 2)
	keyspace := "customer"
	for _, shard := range []string{"-40", "40-80"} {
		streams := resp.ShardStreams[fmt.Sprintf("%s/%s", keyspace, shard)]
		require.Equal(vc.t, 1, len(streams.Streams))
		require.Equal(vc.t, binlogdatapb.VReplicationWorkflowState_Stopped.String(), streams.Streams[0].Status)
	}
}

func validateReshardWorkflow(t *testing.T, workflows []*vtctldatapb.Workflow) {
	require.Equal(t, 1, len(workflows))
	wf := workflows[0]
	require.Equal(t, "reshard", wf.Name)
	require.Equal(t, binlogdatapb.VReplicationWorkflowType_Reshard.String(), wf.WorkflowType)
	require.Equal(t, "None", wf.WorkflowSubType)
	require.Equal(t, "customer", wf.Target.Keyspace)
	require.Equal(t, 2, len(wf.Target.Shards))
	require.Equal(t, "customer", wf.Source.Keyspace)
	require.Equal(t, 1, len(wf.Source.Shards))
	require.False(t, wf.DeferSecondaryKeys)

	require.GreaterOrEqual(t, len(wf.ShardStreams), int(1))
	oneStream := maps.Values(wf.ShardStreams)[0]
	require.NotNil(t, oneStream)

	stream := oneStream.Streams[0]
	require.Equal(t, binlogdatapb.VReplicationWorkflowState_Stopped.String(), stream.State)
	require.Equal(t, stream.TabletSelectionPreference, tabletmanagerdatapb.TabletSelectionPreference_INORDER)
	require.True(t, slices.Equal([]topodatapb.TabletType{topodatapb.TabletType_PRIMARY, topodatapb.TabletType_RDONLY}, stream.TabletTypes))
	require.True(t, slices.Equal([]string{"zone1", "zone2"}, stream.Cells))

	bls := stream.BinlogSource
	require.Equal(t, binlogdatapb.OnDDLAction_STOP, bls.OnDdl)
	require.True(t, bls.StopAfterCopy)

}

func getReshardResponse(rs iReshard) *vtctldatapb.WorkflowStatusResponse {
	reshardOutput := rs.GetLastOutput()
	var reshardResponse vtctldatapb.WorkflowStatusResponse
	err := protojson.Unmarshal([]byte(reshardOutput), &reshardResponse)
	require.NoError(vc.t, err)
	return reshardResponse.CloneVT()
}

// helper functions

func getWorkflow(targetKeyspace, workflow string) *vtctldatapb.GetWorkflowsResponse {
	workflowOutput, err := vc.VtctldClient.ExecuteCommandWithOutput("Workflow", "--keyspace", targetKeyspace, "show", "--workflow", workflow)
	require.NoError(vc.t, err)
	var workflowResponse vtctldatapb.GetWorkflowsResponse
	err = protojson.Unmarshal([]byte(workflowOutput), &workflowResponse)
	require.NoError(vc.t, err)
	workflowResponse.Workflows[0].MaxVReplicationTransactionLag = 0
	workflowResponse.Workflows[0].MaxVReplicationLag = 0
	return workflowResponse.CloneVT()
}

func checkTablesExist(t *testing.T, tabletAlias string, tables []string) bool {
	tablesResponse, err := vc.VtctldClient.ExecuteCommandWithOutput("GetSchema", tabletAlias, "--tables", strings.Join(tables, ","), "--table-names-only")
	require.NoError(t, err)
	tablesFound := strings.Split(tablesResponse, "\n")
	for _, table := range tables {
		found := false
		for _, tableFound := range tablesFound {
			if tableFound == table {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func getRoutingRules(t *testing.T) *vschemapb.RoutingRules {
	routingRules, err := vc.VtctldClient.ExecuteCommandWithOutput("GetRoutingRules")
	require.NoError(t, err)
	var routingRulesResponse vschemapb.RoutingRules
	err = protojson.Unmarshal([]byte(routingRules), &routingRulesResponse)
	require.NoError(t, err)
	return &routingRulesResponse
}

func confirmNoRoutingRules(t *testing.T) {
	routingRulesResponse := getRoutingRules(t)
	require.Zero(t, len(routingRulesResponse.Rules))
}

func confirmRoutingRulesExist(t *testing.T) {
	routingRulesResponse := getRoutingRules(t)
	require.NotZero(t, len(routingRulesResponse.Rules))
}

// We only want to validate non-standard attributes that are set by the CLI. The other end-to-end tests validate the rest.
// We also check some of the standard attributes to make sure they are set correctly.
func validateMoveTablesWorkflow(t *testing.T, workflows []*vtctldatapb.Workflow) {
	require.Equal(t, 1, len(workflows))
	wf := workflows[0]
	require.Equal(t, "wf1", wf.Name)
	require.Equal(t, binlogdatapb.VReplicationWorkflowType_MoveTables.String(), wf.WorkflowType)
	require.Equal(t, "None", wf.WorkflowSubType)
	require.Equal(t, "customer", wf.Target.Keyspace)
	require.Equal(t, 2, len(wf.Target.Shards))
	require.Equal(t, "product", wf.Source.Keyspace)
	require.Equal(t, 1, len(wf.Source.Shards))
	require.False(t, wf.DeferSecondaryKeys)

	require.GreaterOrEqual(t, len(wf.ShardStreams), int(1))
	oneStream := maps.Values(wf.ShardStreams)[0]
	require.NotNil(t, oneStream)

	stream := oneStream.Streams[0]
	require.Equal(t, binlogdatapb.VReplicationWorkflowState_Stopped.String(), stream.State)
	require.Equal(t, stream.TabletSelectionPreference, tabletmanagerdatapb.TabletSelectionPreference_INORDER)
	require.True(t, slices.Equal([]topodatapb.TabletType{topodatapb.TabletType_PRIMARY, topodatapb.TabletType_RDONLY}, stream.TabletTypes))
	require.True(t, slices.Equal([]string{"zone1", "zone2"}, stream.Cells))

	bls := stream.BinlogSource
	require.Equalf(t, 1, len(bls.Filter.Rules), "Rules are %+v", bls.Filter.Rules) // only customer, customer2 should be excluded
	require.Equal(t, binlogdatapb.OnDDLAction_STOP, bls.OnDdl)
	require.True(t, bls.StopAfterCopy)
}
