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

package vreplication

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/vt/wrangler"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

const (
	workflowName      = "wf1"
	sourceKs          = "product"
	targetKs          = "customer"
	ksWorkflow        = targetKs + "." + workflowName
	reverseKsWorkflow = sourceKs + "." + workflowName + "_reverse"
	tablesToMove      = "customer"
	defaultCellName   = "zone1"
	readQuery         = "select * from customer"
)

const (
	workflowActionCreate         = "Create"
	workflowActionSwitchTraffic  = "SwitchTraffic"
	workflowActionReverseTraffic = "ReverseTraffic"
	workflowActionComplete       = "Complete"
	workflowActionCancel         = "Cancel"
)

var (
	targetTab1, targetTab2, targetReplicaTab1 *cluster.VttabletProcess
	sourceReplicaTab, sourceTab               *cluster.VttabletProcess

	lastOutput          string
	currentWorkflowType wrangler.VReplicationWorkflowType
)

func createReshardWorkflow(t *testing.T, sourceShards, targetShards string) error {
	err := tstWorkflowExec(t, defaultCellName, workflowName, targetKs, targetKs,
		"", workflowActionCreate, "", sourceShards, targetShards)
	require.NoError(t, err)
	time.Sleep(1 * time.Second)
	catchup(t, targetTab1, workflowName, "Reshard")
	catchup(t, targetTab2, workflowName, "Reshard")
	vdiff(t, ksWorkflow, "")
	return nil
}

func createMoveTablesWorkflow(t *testing.T, tables string) error {
	if tables == "" {
		tables = tablesToMove
	}
	err := tstWorkflowExec(t, defaultCellName, workflowName, sourceKs, targetKs,
		tables, workflowActionCreate, "", "", "")
	require.NoError(t, err)
	catchup(t, targetTab1, workflowName, "MoveTables")
	catchup(t, targetTab2, workflowName, "MoveTables")
	time.Sleep(1 * time.Second)
	vdiff(t, ksWorkflow, "")
	return nil
}

func tstWorkflowAction(t *testing.T, action, tabletTypes, cells string) error {
	return tstWorkflowExec(t, cells, workflowName, sourceKs, targetKs, tablesToMove, action, tabletTypes, "", "")
}

func tstWorkflowExec(t *testing.T, cells, workflow, sourceKs, targetKs, tables, action, tabletTypes, sourceShards, targetShards string) error {
	var args []string
	if currentWorkflowType == wrangler.MoveTablesWorkflow {
		args = append(args, "MoveTables")
	} else {
		args = append(args, "Reshard")
	}
	args = append(args, "-v2")
	switch action {
	case workflowActionCreate:
		if currentWorkflowType == wrangler.MoveTablesWorkflow {
			args = append(args, "-source", sourceKs, "-tables", tables)
		} else {
			args = append(args, "-source_shards", sourceShards, "-target_shards", targetShards)
		}
	}
	if cells != "" {
		args = append(args, "-cells", cells)
	}
	if tabletTypes != "" {
		args = append(args, "-tablet_types", tabletTypes)
	}
	ksWorkflow := fmt.Sprintf("%s.%s", targetKs, workflow)
	args = append(args, action, ksWorkflow)
	output, err := vc.VtctlClient.ExecuteCommandWithOutput(args...)
	lastOutput = output
	if err != nil {
		return fmt.Errorf("%s: %s", err, output)
	}
	fmt.Printf("----------\n%+v\n%s----------\n", args, output)
	return nil
}

func tstWorkflowSwitchReads(t *testing.T, tabletTypes, cells string) {
	if tabletTypes == "" {
		tabletTypes = "replica,rdonly"
	}
	require.NoError(t, tstWorkflowAction(t, workflowActionSwitchTraffic, tabletTypes, cells))
}

func tstWorkflowReverseReads(t *testing.T, tabletTypes, cells string) {
	if tabletTypes == "" {
		tabletTypes = "replica,rdonly"
	}
	require.NoError(t, tstWorkflowAction(t, workflowActionReverseTraffic, tabletTypes, cells))
}

func tstWorkflowSwitchWrites(t *testing.T) {
	require.NoError(t, tstWorkflowAction(t, workflowActionSwitchTraffic, "master", ""))
}

func tstWorkflowReverseWrites(t *testing.T) {
	require.NoError(t, tstWorkflowAction(t, workflowActionReverseTraffic, "master", ""))
}

func tstWorkflowSwitchReadsAndWrites(t *testing.T) {
	require.NoError(t, tstWorkflowAction(t, workflowActionSwitchTraffic, "replica,rdonly,master", ""))
}

func tstWorkflowReverseReadsAndWrites(t *testing.T) {
	require.NoError(t, tstWorkflowAction(t, workflowActionReverseTraffic, "replica,rdonly,master", ""))
}

func tstWorkflowComplete(t *testing.T) error {
	return tstWorkflowAction(t, workflowActionComplete, "", "")
}

func tstWorkflowCancel(t *testing.T) error {
	return tstWorkflowAction(t, workflowActionCancel, "", "")
}

func validateReadsRoute(t *testing.T, tabletTypes string, tablet *cluster.VttabletProcess) {
	if tabletTypes == "" {
		tabletTypes = "replica,rdonly"
	}
	for _, tt := range []string{"replica", "rdonly"} {
		destination := fmt.Sprintf("%s:%s@%s", tablet.Keyspace, tablet.Shard, tt)
		if strings.Contains(tabletTypes, tt) {
			require.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, tablet, destination, readQuery, readQuery))
		}
	}
}

func validateReadsRouteToSource(t *testing.T, tabletTypes string) {
	validateReadsRoute(t, tabletTypes, sourceReplicaTab)
}

func validateReadsRouteToTarget(t *testing.T, tabletTypes string) {
	validateReadsRoute(t, tabletTypes, targetReplicaTab1)
}

func validateWritesRouteToSource(t *testing.T) {
	insertQuery := "insert into customer(name, cid) values('tempCustomer2', 200)"
	matchInsertQuery := "insert into customer(name, cid) values"
	require.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, sourceTab, "customer", insertQuery, matchInsertQuery))
	execVtgateQuery(t, vtgateConn, "customer", "delete from customer where cid > 100")
}

func validateWritesRouteToTarget(t *testing.T) {
	insertQuery := "insert into customer(name, cid) values('tempCustomer3', 101)"
	matchInsertQuery := "insert into customer(name, cid) values"
	require.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, targetTab2, "customer", insertQuery, matchInsertQuery))
	insertQuery = "insert into customer(name, cid) values('tempCustomer3', 102)"
	require.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, targetTab1, "customer", insertQuery, matchInsertQuery))
	execVtgateQuery(t, vtgateConn, "customer", "delete from customer where cid > 100")
}

func revert(t *testing.T) {
	switchWrites(t, reverseKsWorkflow, false)
	validateWritesRouteToSource(t)
	switchReadsNew(t, allCellNames, ksWorkflow, true)
	validateReadsRouteToSource(t, "replica")
	queries := []string{
		"delete from _vt.vreplication",
		"delete from _vt.resharding_journal",
	}

	for _, query := range queries {
		targetTab1.QueryTablet(query, "customer", true)
		targetTab2.QueryTablet(query, "customer", true)
		sourceTab.QueryTablet(query, "product", true)
	}
	targetTab1.QueryTablet("drop table vt_customer.customer", "customer", true)
	targetTab2.QueryTablet("drop table vt_customer.customer", "customer", true)

	clearRoutingRules(t, vc)
}

func checkStates(t *testing.T, startState, endState string) {
	require.Contains(t, lastOutput, fmt.Sprintf("Start State: %s", startState))
	require.Contains(t, lastOutput, fmt.Sprintf("Current State: %s", endState))
}

func getCurrentState(t *testing.T) string {
	if err := tstWorkflowAction(t, "GetState", "", ""); err != nil {
		return err.Error()
	}
	return strings.TrimSpace(strings.Trim(lastOutput, "\n"))
}

// ideally this should be broken up into multiple tests for full flow, replica/rdonly flow, reverse flows etc
// but CI currently fails on creating multiple clusters even after the previous ones are torn down

func TestBasicV2Workflows(t *testing.T) {
	vc = setupCluster(t)
	defer vtgateConn.Close()
	defer vc.TearDown()

	testMoveTablesV2Workflow(t)
	testReshardV2Workflow(t)
	log.Flush()
}

func testReshardV2Workflow(t *testing.T) {
	currentWorkflowType = wrangler.ReshardWorkflow

	createAdditionalCustomerShards(t, "-40,40-80,80-c0,c0-")
	createReshardWorkflow(t, "-80,80-", "-40,40-80,80-c0,c0-")
	if !strings.Contains(lastOutput, "Workflow started successfully") {
		t.Fail()
	}
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToSource(t)

	testRestOfWorkflow(t)
}

func testMoveTablesV2Workflow(t *testing.T) {
	currentWorkflowType = wrangler.MoveTablesWorkflow

	// test basic forward and reverse flows
	setupCustomerKeyspace(t)
	createMoveTablesWorkflow(t, "customer")
	if !strings.Contains(lastOutput, "Workflow started successfully") {
		t.Fail()
	}
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToSource(t)

	testRestOfWorkflow(t)

	listAllArgs := []string{"workflow", "customer", "listall"}
	output, _ := vc.VtctlClient.ExecuteCommandWithOutput(listAllArgs...)
	require.Contains(t, output, "No workflows found in keyspace customer")

	createMoveTablesWorkflow(t, "customer2")
	output, _ = vc.VtctlClient.ExecuteCommandWithOutput(listAllArgs...)
	require.Contains(t, output, "Following workflow(s) found in keyspace customer: wf1")

	err := tstWorkflowCancel(t)
	require.NoError(t, err)

	output, _ = vc.VtctlClient.ExecuteCommandWithOutput(listAllArgs...)
	require.Contains(t, output, "No workflows found in keyspace customer")
}

func testPartialSwitches(t *testing.T) {
	//nothing switched
	require.Equal(t, getCurrentState(t), wrangler.WorkflowStateNotSwitched)
	tstWorkflowSwitchReads(t, "replica,rdonly", "zone1")
	nextState := "Reads partially switched. Replica switched in cells: zone1. Rdonly switched in cells: zone1. Writes Not Switched"
	checkStates(t, wrangler.WorkflowStateNotSwitched, nextState)

	tstWorkflowSwitchReads(t, "replica,rdonly", "zone2")
	currentState := nextState
	nextState = wrangler.WorkflowStateReadsSwitched
	checkStates(t, currentState, nextState)

	tstWorkflowSwitchReads(t, "", "")
	checkStates(t, nextState, nextState) //idempotency

	tstWorkflowSwitchWrites(t)
	currentState = nextState
	nextState = wrangler.WorkflowStateAllSwitched
	checkStates(t, currentState, nextState)

	tstWorkflowSwitchWrites(t)
	checkStates(t, nextState, nextState) //idempotency

	tstWorkflowReverseReads(t, "replica,rdonly", "zone1")
	currentState = nextState
	nextState = "Reads partially switched. Replica switched in cells: zone2. Rdonly switched in cells: zone2. Writes Switched"
	checkStates(t, currentState, nextState)

	tstWorkflowReverseReads(t, "replica,rdonly", "zone2")
	currentState = nextState
	nextState = wrangler.WorkflowStateWritesSwitched
	checkStates(t, currentState, nextState)

	tstWorkflowReverseWrites(t)
	currentState = nextState
	nextState = wrangler.WorkflowStateNotSwitched
	checkStates(t, currentState, nextState)
}

func testRestOfWorkflow(t *testing.T) {
	testPartialSwitches(t)

	// test basic forward and reverse flows
	tstWorkflowSwitchReads(t, "", "")
	checkStates(t, wrangler.WorkflowStateNotSwitched, wrangler.WorkflowStateReadsSwitched)
	validateReadsRouteToTarget(t, "replica")
	validateWritesRouteToSource(t)

	tstWorkflowSwitchWrites(t)
	checkStates(t, wrangler.WorkflowStateReadsSwitched, wrangler.WorkflowStateAllSwitched)
	validateReadsRouteToTarget(t, "replica")
	validateWritesRouteToTarget(t)

	tstWorkflowReverseReads(t, "", "")
	checkStates(t, wrangler.WorkflowStateAllSwitched, wrangler.WorkflowStateWritesSwitched)
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToTarget(t)

	tstWorkflowReverseWrites(t)
	checkStates(t, wrangler.WorkflowStateWritesSwitched, wrangler.WorkflowStateNotSwitched)
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToSource(t)

	tstWorkflowSwitchWrites(t)
	checkStates(t, wrangler.WorkflowStateNotSwitched, wrangler.WorkflowStateWritesSwitched)
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToTarget(t)

	tstWorkflowReverseWrites(t)
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToSource(t)

	tstWorkflowSwitchReads(t, "", "")
	validateReadsRouteToTarget(t, "replica")
	validateWritesRouteToSource(t)

	tstWorkflowReverseReads(t, "", "")
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToSource(t)

	tstWorkflowSwitchReadsAndWrites(t)
	validateReadsRouteToTarget(t, "replica")
	validateWritesRouteToTarget(t)
	tstWorkflowReverseReadsAndWrites(t)
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToSource(t)

	// trying to complete an unswitched workflow should error
	err := tstWorkflowComplete(t)
	require.Error(t, err)
	require.Contains(t, err.Error(), wrangler.ErrWorkflowNotFullySwitched)

	// fully switch and complete
	tstWorkflowSwitchReadsAndWrites(t)
	validateReadsRouteToTarget(t, "replica")
	validateWritesRouteToTarget(t)

	err = tstWorkflowComplete(t)
	require.NoError(t, err)
}

func setupCluster(t *testing.T) *VitessCluster {
	cells := []string{"zone1", "zone2"}

	vc = NewVitessCluster(t, "TestBasicVreplicationWorkflow", cells, mainClusterConfig)
	require.NotNil(t, vc)
	defaultCellName := "zone1"
	allCellNames = defaultCellName
	defaultCell = vc.Cells[defaultCellName]

	zone1 := vc.Cells["zone1"]
	zone2 := vc.Cells["zone2"]

	vc.AddKeyspace(t, []*Cell{zone1, zone2}, "product", "0", initialProductVSchema, initialProductSchema, defaultReplicas, defaultRdonly, 100)

	vtgate = zone1.Vtgates[0]
	require.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "product", "0"), 1)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", "product", "0"), 2)

	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	verifyClusterHealth(t, vc)
	insertInitialData(t)

	sourceReplicaTab = vc.Cells[defaultCell.Name].Keyspaces["product"].Shards["0"].Tablets["zone1-101"].Vttablet
	sourceTab = vc.Cells[defaultCell.Name].Keyspaces["product"].Shards["0"].Tablets["zone1-100"].Vttablet

	return vc
}

func setupCustomerKeyspace(t *testing.T) {
	if _, err := vc.AddKeyspace(t, []*Cell{vc.Cells["zone1"], vc.Cells["zone2"]}, "customer", "-80,80-",
		customerVSchema, customerSchema, defaultReplicas, defaultRdonly, 200); err != nil {
		t.Fatal(err)
	}
	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "customer", "-80"), 1); err != nil {
		t.Fatal(err)
	}
	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "customer", "80-"), 1); err != nil {
		t.Fatal(err)
	}
	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", "customer", "-80"), 2); err != nil {
		t.Fatal(err)
	}
	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", "customer", "80-"), 2); err != nil {
		t.Fatal(err)
	}
	custKs := vc.Cells[defaultCell.Name].Keyspaces["customer"]
	targetTab1 = custKs.Shards["-80"].Tablets["zone1-200"].Vttablet
	targetTab2 = custKs.Shards["80-"].Tablets["zone1-300"].Vttablet
	targetReplicaTab1 = custKs.Shards["-80"].Tablets["zone1-201"].Vttablet
}

func TestSwitchReadsWritesInAnyOrder(t *testing.T) {
	vc = setupCluster(t)
	defer vc.TearDown()
	moveCustomerTableSwitchFlows(t, []*Cell{vc.Cells["zone1"]}, "zone1")
}

func switchReadsNew(t *testing.T, cells, ksWorkflow string, reverse bool) {
	output, err := vc.VtctlClient.ExecuteCommandWithOutput("SwitchReads", "-cells="+cells,
		"-tablet_types=rdonly,replica", fmt.Sprintf("-reverse=%t", reverse), ksWorkflow)
	require.NoError(t, err, fmt.Sprintf("SwitchReads Error: %s: %s", err, output))
	if output != "" {
		fmt.Printf("SwitchReads output: %s\n", output)
	}
}

func moveCustomerTableSwitchFlows(t *testing.T, cells []*Cell, sourceCellOrAlias string) {
	workflow := "wf1"
	sourceKs := "product"
	targetKs := "customer"
	ksWorkflow := fmt.Sprintf("%s.%s", targetKs, workflow)
	tables := "customer"
	setupCustomerKeyspace(t)

	var moveTablesAndWait = func() {
		moveTables(t, sourceCellOrAlias, workflow, sourceKs, targetKs, tables)
		catchup(t, targetTab1, workflow, "MoveTables")
		catchup(t, targetTab2, workflow, "MoveTables")
		vdiff(t, ksWorkflow, "")
	}

	var switchReadsFollowedBySwitchWrites = func() {
		moveTablesAndWait()

		validateReadsRouteToSource(t, "replica")
		switchReadsNew(t, allCellNames, ksWorkflow, false)
		validateReadsRouteToTarget(t, "replica")

		validateWritesRouteToSource(t)
		switchWrites(t, ksWorkflow, false)
		validateWritesRouteToTarget(t)

		revert(t)
	}
	var switchWritesFollowedBySwitchReads = func() {
		moveTablesAndWait()

		validateWritesRouteToSource(t)
		switchWrites(t, ksWorkflow, false)
		validateWritesRouteToTarget(t)

		validateReadsRouteToSource(t, "replica")
		switchReadsNew(t, allCellNames, ksWorkflow, false)
		validateReadsRouteToTarget(t, "replica")

		revert(t)
	}

	var switchReadsReverseSwitchWritesSwitchReads = func() {
		moveTablesAndWait()

		validateReadsRouteToSource(t, "replica")
		switchReadsNew(t, allCellNames, ksWorkflow, false)
		validateReadsRouteToTarget(t, "replica")

		switchReadsNew(t, allCellNames, ksWorkflow, true)
		validateReadsRouteToSource(t, "replica")
		printRoutingRules(t, vc, "After reversing SwitchReads")

		validateWritesRouteToSource(t)
		switchWrites(t, ksWorkflow, false)
		validateWritesRouteToTarget(t)

		printRoutingRules(t, vc, "After SwitchWrites and reversing SwitchReads")
		validateReadsRouteToSource(t, "replica")
		switchReadsNew(t, allCellNames, ksWorkflow, false)
		validateReadsRouteToTarget(t, "replica")

		revert(t)
	}

	var switchWritesReverseSwitchReadsSwitchWrites = func() {
		moveTablesAndWait()

		validateWritesRouteToSource(t)
		switchWrites(t, ksWorkflow, false)
		validateWritesRouteToTarget(t)

		switchWrites(t, reverseKsWorkflow, true)
		validateWritesRouteToSource(t)

		validateReadsRouteToSource(t, "replica")
		switchReadsNew(t, allCellNames, ksWorkflow, false)
		validateReadsRouteToTarget(t, "replica")

		validateWritesRouteToSource(t)
		switchWrites(t, ksWorkflow, false)
		validateWritesRouteToTarget(t)

		revert(t)

	}
	switchReadsFollowedBySwitchWrites()
	switchWritesFollowedBySwitchReads()
	switchReadsReverseSwitchWritesSwitchReads()
	switchWritesReverseSwitchReadsSwitchWrites()
}

func createAdditionalCustomerShards(t *testing.T, shards string) {
	ksName := "customer"
	keyspace := vc.Cells[defaultCell.Name].Keyspaces[ksName]
	require.NoError(t, vc.AddShards(t, []*Cell{defaultCell, vc.Cells["zone2"]}, keyspace, shards, defaultReplicas, defaultRdonly, 400))
	arrTargetShardNames := strings.Split(shards, ",")

	for _, shardName := range arrTargetShardNames {
		if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", ksName, shardName), 1); err != nil {
			t.Fatal(err)
		}
		if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", ksName, shardName), 2); err != nil {
			t.Fatal(err)
		}
	}
	custKs := vc.Cells[defaultCell.Name].Keyspaces[ksName]
	targetTab2 = custKs.Shards["80-c0"].Tablets["zone1-600"].Vttablet
	targetTab1 = custKs.Shards["40-80"].Tablets["zone1-500"].Vttablet
	targetReplicaTab1 = custKs.Shards["-40"].Tablets["zone1-401"].Vttablet

	sourceReplicaTab = custKs.Shards["-80"].Tablets["zone1-201"].Vttablet
	sourceTab = custKs.Shards["-80"].Tablets["zone1-200"].Vttablet
}
