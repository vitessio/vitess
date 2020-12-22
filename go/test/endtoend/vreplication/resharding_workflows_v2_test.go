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
	workflowActionStart          = "Start"
	workflowActionSwitchTraffic  = "SwitchTraffic"
	workflowActionReverseTraffic = "ReverseTraffic"
	workflowActionComplete       = "Complete"
	workflowActionAbort          = "Abort"
)

var (
	customerTab1, customerTab2, productReplicaTab, customerReplicaTab1, productTab *cluster.VttabletProcess
	lastOutput                                                                     string
	currentWorkflowType                                                            wrangler.VReplicationWorkflowType
)

func reshard2Start(t *testing.T, sourceShards, targetShards string) error {
	err := tstWorkflowExec(t, defaultCellName, workflowName, targetKs, targetKs,
		"", workflowActionStart, "", sourceShards, targetShards)
	require.NoError(t, err)
	time.Sleep(1 * time.Second)
	catchup(t, customerTab1, workflowName, "Reshard")
	catchup(t, customerTab2, workflowName, "Reshard")
	vdiff(t, ksWorkflow)
	return nil
}

func moveTables2Start(t *testing.T, tables string) error {
	if tables == "" {
		tables = tablesToMove
	}
	err := tstWorkflowExec(t, defaultCellName, workflowName, sourceKs, targetKs,
		tables, workflowActionStart, "", "", "")
	require.NoError(t, err)
	catchup(t, customerTab1, workflowName, "MoveTables")
	catchup(t, customerTab2, workflowName, "MoveTables")
	time.Sleep(1 * time.Second)
	vdiff(t, ksWorkflow)
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
	case workflowActionStart:
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
		t.Logf("%s command failed with %+v\n", args[0], err)
		return fmt.Errorf("%s: %s", err, output)
	}
	fmt.Printf("----------\n%+v\n%s\n----------\n", args, output)
	return nil
}

func tstWorkflowSwitchReads(t *testing.T, tabletTypes string) {
	require.NoError(t, tstWorkflowAction(t, workflowActionSwitchTraffic, "replica,rdonly", ""))
}

func tstWorkflowReverseReads(t *testing.T, tabletTypes string) {
	require.NoError(t, tstWorkflowAction(t, workflowActionReverseTraffic, "replica,rdonly", ""))
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

func tstWorkflowAbort(t *testing.T) error {
	return tstWorkflowAction(t, workflowActionAbort, "", "")
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
	validateReadsRoute(t, tabletTypes, productReplicaTab)
}

func validateReadsRouteToTarget(t *testing.T, tabletTypes string) {
	validateReadsRoute(t, tabletTypes, customerReplicaTab1)
}

func validateWritesRouteToSource(t *testing.T) {
	insertQuery := "insert into customer(name, cid) values('tempCustomer2', 200)"
	matchInsertQuery := "insert into customer(name, cid) values"
	require.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productTab, "customer", insertQuery, matchInsertQuery))
	execVtgateQuery(t, vtgateConn, "customer", "delete from customer where cid > 100")
}
func validateWritesRouteToTarget(t *testing.T) {
	insertQuery := "insert into customer(name, cid) values('tempCustomer3', 101)"
	matchInsertQuery := "insert into customer(name, cid) values"
	require.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, customerTab2, "customer", insertQuery, matchInsertQuery))
	insertQuery = "insert into customer(name, cid) values('tempCustomer3', 102)"
	require.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, customerTab1, "customer", insertQuery, matchInsertQuery))
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
		customerTab1.QueryTablet(query, "customer", true)
		customerTab2.QueryTablet(query, "customer", true)
		productTab.QueryTablet(query, "product", true)
	}
	customerTab1.QueryTablet("drop table vt_customer.customer", "customer", true)
	customerTab2.QueryTablet("drop table vt_customer.customer", "customer", true)

	clearRoutingRules(t, vc)
}

func checkStates(t *testing.T, startState, endState string) {
	require.Contains(t, lastOutput, fmt.Sprintf("Start State: %s", startState))
	require.Contains(t, lastOutput, fmt.Sprintf("Current State: %s", endState))
}

// ideally this should be broken up into multiple tests for full flow, replica/rdonly flow, reverse flows etc
// but CI currently fails on creating multiple clusters even after the previous ones are torn down

func TestBasicV2Workflows(t *testing.T) {
	vc = setupCluster(t)
	defer vtgateConn.Close()
	//defer vc.TearDown()

	testMoveTablesV2Workflow(t)
	testReshardV2Workflow(t)
}

func testReshardV2Workflow(t *testing.T) {
	currentWorkflowType = wrangler.ReshardWorkflow

	createAdditionalCustomerShards(t, "-40,40-80,80-c0,c0-")
	reshard2Start(t, "-80,80-", "-40,40-80,80-c0,c0-")

	checkStates(t, "Not Started", "Not Started")
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToSource(t)

	testRestOfWorkflow(t)

}

func testMoveTablesV2Workflow(t *testing.T) {
	currentWorkflowType = wrangler.MoveTablesWorkflow

	// test basic forward and reverse flows
	setupCustomerKeyspace(t)
	moveTables2Start(t, "customer")
	checkStates(t, "Not Started", "Not Started")
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToSource(t)

	testRestOfWorkflow(t)

	listAllArgs := []string{"workflow", "customer", "listall"}
	output, _ := vc.VtctlClient.ExecuteCommandWithOutput(listAllArgs...)
	require.Contains(t, output, "No workflows found in keyspace customer")

	moveTables2Start(t, "customer2")
	output, _ = vc.VtctlClient.ExecuteCommandWithOutput(listAllArgs...)
	require.Contains(t, output, "Following workflow(s) found in keyspace customer: wf1")

	err := tstWorkflowAbort(t)
	require.NoError(t, err)

	output, _ = vc.VtctlClient.ExecuteCommandWithOutput(listAllArgs...)
	require.Contains(t, output, "No workflows found in keyspace customer")

}
func testRestOfWorkflow(t *testing.T) {
	// test basic forward and reverse flows
	tstWorkflowSwitchReads(t, "")
	checkStates(t, "Reads Not Switched. Writes Not Switched", "All Reads Switched. Writes Not Switched")
	validateReadsRouteToTarget(t, "replica")
	validateWritesRouteToSource(t)

	tstWorkflowSwitchWrites(t)
	checkStates(t, "All Reads Switched. Writes Not Switched", "All Reads Switched. Writes Switched")
	validateReadsRouteToTarget(t, "replica")
	validateWritesRouteToTarget(t)

	tstWorkflowReverseReads(t, "")
	checkStates(t, "All Reads Switched. Writes Switched", "Reads Not Switched. Writes Switched")
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToTarget(t)

	tstWorkflowReverseWrites(t)
	checkStates(t, "Reads Not Switched. Writes Switched", "Reads Not Switched. Writes Not Switched")
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToSource(t)

	tstWorkflowSwitchWrites(t)
	checkStates(t, "Reads Not Switched. Writes Not Switched", "Reads Not Switched. Writes Switched")
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToTarget(t)

	tstWorkflowReverseWrites(t)
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToSource(t)

	tstWorkflowSwitchReads(t, "")
	validateReadsRouteToTarget(t, "replica")
	validateWritesRouteToSource(t)

	tstWorkflowReverseReads(t, "")
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToSource(t)

	tstWorkflowSwitchReadsAndWrites(t)
	validateReadsRouteToTarget(t, "replica")
	validateWritesRouteToTarget(t)
	tstWorkflowReverseReadsAndWrites(t)
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToSource(t)

	// test complete and abort
	var err error

	err = tstWorkflowComplete(t)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot complete workflow because you have not yet switched all read and write traffic")

	tstWorkflowSwitchReadsAndWrites(t)
	validateReadsRouteToTarget(t, "replica")
	validateWritesRouteToTarget(t)

	err = tstWorkflowComplete(t)
	require.NoError(t, err)

}

func setupCluster(t *testing.T) *VitessCluster {
	cells := []string{"zone1"}

	vc = InitCluster(t, cells)
	require.NotNil(t, vc)
	defaultCellName := "zone1"
	allCellNames = defaultCellName
	defaultCell = vc.Cells[defaultCellName]

	cell1 := vc.Cells["zone1"]
	vc.AddKeyspace(t, []*Cell{cell1}, "product", "0", initialProductVSchema, initialProductSchema, defaultReplicas, defaultRdonly, 100)

	vtgate = cell1.Vtgates[0]
	require.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "product", "0"), 1)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", "product", "0"), 2)

	vtgateConn = getConnection(t, globalConfig.vtgateMySQLPort)
	verifyClusterHealth(t)
	insertInitialData(t)

	productReplicaTab = vc.Cells[defaultCell.Name].Keyspaces["product"].Shards["0"].Tablets["zone1-101"].Vttablet
	productTab = vc.Cells[defaultCell.Name].Keyspaces["product"].Shards["0"].Tablets["zone1-100"].Vttablet

	return vc
}

func setupCustomerKeyspace(t *testing.T) {
	if _, err := vc.AddKeyspace(t, []*Cell{vc.Cells[defaultCellName]}, "customer", "-80,80-",
		customerVSchema, customerSchema, defaultReplicas, defaultRdonly, 200); err != nil {
		t.Fatal(err)
	}
	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "customer", "-80"), 1); err != nil {
		t.Fatal(err)
	}
	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "customer", "80-"), 1); err != nil {
		t.Fatal(err)
	}
	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", "customer", "-80"), 1); err != nil {
		t.Fatal(err)
	}
	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", "customer", "80-"), 1); err != nil {
		t.Fatal(err)
	}
	custKs := vc.Cells[defaultCell.Name].Keyspaces["customer"]
	customerTab1 = custKs.Shards["-80"].Tablets["zone1-200"].Vttablet
	customerTab2 = custKs.Shards["80-"].Tablets["zone1-300"].Vttablet
	customerReplicaTab1 = custKs.Shards["-80"].Tablets["zone1-201"].Vttablet
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
		catchup(t, customerTab1, workflow, "MoveTables")
		catchup(t, customerTab2, workflow, "MoveTables")
		vdiff(t, ksWorkflow)
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

		switchWrites(t, ksWorkflow, true)
		validateWritesRouteToSource(t)

		validateReadsRouteToSource(t, "replica")
		switchReadsNew(t, allCellNames, ksWorkflow, false)
		validateReadsRouteToTarget(t, "replica")

		validateWritesRouteToSource(t)
		switchWrites(t, ksWorkflow, false)
		validateWritesRouteToTarget(t)

		revert(t)

	}
	_ = switchReadsFollowedBySwitchWrites
	_ = switchWritesFollowedBySwitchReads
	_ = switchReadsReverseSwitchWritesSwitchReads
	_ = switchWritesReverseSwitchReadsSwitchWrites
	switchReadsFollowedBySwitchWrites()
	switchWritesFollowedBySwitchReads()
	switchReadsReverseSwitchWritesSwitchReads()
	switchWritesReverseSwitchReadsSwitchWrites()
}

func createAdditionalCustomerShards(t *testing.T, shards string) {
	ksName := "customer"
	keyspace := vc.Cells[defaultCell.Name].Keyspaces[ksName]
	require.NoError(t, vc.AddShards(t, []*Cell{defaultCell}, keyspace, shards, defaultReplicas, defaultRdonly, 400))
	arrTargetShardNames := strings.Split(shards, ",")

	for _, shardName := range arrTargetShardNames {
		if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", ksName, shardName), 1); err != nil {
			t.Fatal(err)
		}
	}
	//FIXME
	custKs := vc.Cells[defaultCell.Name].Keyspaces[ksName]
	customerTab2 = custKs.Shards["80-c0"].Tablets["zone1-600"].Vttablet
	customerTab1 = custKs.Shards["40-80"].Tablets["zone1-500"].Vttablet
	customerReplicaTab1 = custKs.Shards["-40"].Tablets["zone1-401"].Vttablet

	productReplicaTab = vc.Cells[defaultCell.Name].Keyspaces["customer"].Shards["-80"].Tablets["zone1-201"].Vttablet
	productTab = vc.Cells[defaultCell.Name].Keyspaces["customer"].Shards["-80"].Tablets["zone1-200"].Vttablet

}
