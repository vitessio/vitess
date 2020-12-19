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

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

const (
	moveTablesWorkflowName = "p2c"
	sourceKs               = "product"
	targetKs               = "customer"
	ksWorkflow             = targetKs + "." + moveTablesWorkflowName
	reverseKsWorkflow      = sourceKs + "." + moveTablesWorkflowName + "_reverse"
	tablesToMove           = "customer"
	defaultCellName        = "zone1"
	readQuery              = "select * from customer"
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
)

func moveTables2Start(t *testing.T, tables string) error {
	if tables == "" {
		tables = tablesToMove
	}
	err := moveTables2(t, defaultCellName, moveTablesWorkflowName, sourceKs, targetKs, tables, workflowActionStart, "")
	require.NoError(t, err)
	catchup(t, customerTab1, moveTablesWorkflowName, "MoveTables")
	catchup(t, customerTab2, moveTablesWorkflowName, "MoveTables")
	vdiff(t, ksWorkflow)
	return nil
}

func moveTables2Action(t *testing.T, action, tabletTypes, cells string) error {
	return moveTables2(t, cells, moveTablesWorkflowName, sourceKs, targetKs, tablesToMove, action, tabletTypes)
}

func moveTables2(t *testing.T, cells, workflow, sourceKs, targetKs, tables, action, tabletTypes string) error {
	var args []string
	args = append(args, "MoveTables", "-v2")
	switch action {
	case workflowActionStart:
		args = append(args, "-source", sourceKs, "-tables", tables)
	}
	if cells != "" {
		args = append(args, "-cells", cells)
	}
	if tabletTypes != "" {
		args = append(args, "-tablet_types", tabletTypes)
	}
	ksWorkflow := fmt.Sprintf("%s.%s", targetKs, workflow)
	args = append(args, action, ksWorkflow)
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput(args...); err != nil {
		t.Logf("MoveTables command failed with %+v\n", err)
		return fmt.Errorf("%s: %s", err, output)
	}
	return nil
}

func moveTablesSwitchReads(t *testing.T, tabletTypes string) {
	require.NoError(t, moveTables2Action(t, workflowActionSwitchTraffic, "replica,rdonly", ""))
}

func moveTablesReverseReads(t *testing.T, tabletTypes string) {
	require.NoError(t, moveTables2Action(t, workflowActionReverseTraffic, "replica,rdonly", ""))
}

func moveTablesSwitchWrites(t *testing.T) {
	require.NoError(t, moveTables2Action(t, workflowActionSwitchTraffic, "master", ""))
}

func moveTablesReverseWrites(t *testing.T) {
	require.NoError(t, moveTables2Action(t, workflowActionReverseTraffic, "master", ""))
}

func moveTablesSwitchReadsAndWrites(t *testing.T) {
	require.NoError(t, moveTables2Action(t, workflowActionSwitchTraffic, "replica,rdonly,master", ""))
}

func moveTablesReverseReadsAndWrites(t *testing.T) {
	require.NoError(t, moveTables2Action(t, workflowActionReverseTraffic, "replica,rdonly,master", ""))
}

func moveTablesComplete(t *testing.T) error {
	return moveTables2Action(t, workflowActionComplete, "", "")
}

func moveTablesAbort(t *testing.T) error {
	return moveTables2Action(t, workflowActionAbort, "", "")
}

func validateReadsRoute(t *testing.T, tabletTypes string, tablet *cluster.VttabletProcess) {
	if tabletTypes == "" {
		tabletTypes = "replica,rdonly"
	}
	for _, tt := range []string{"replica", "rdonly"} {
		if strings.Contains(tabletTypes, tt) {
			require.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, tablet, "product@"+tt, readQuery, readQuery))
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
	matchInsertQuery := "insert into customer(name, cid) values (:vtg1, :_cid0)"
	require.False(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productTab, "customer", insertQuery, matchInsertQuery))
	execVtgateQuery(t, vtgateConn, "customer", "delete from customer where cid > 100")
}
func validateWritesRouteToTarget(t *testing.T) {
	insertQuery := "insert into customer(name, cid) values('tempCustomer3', 101)"
	matchInsertQuery := "insert into customer(name, cid) values (:vtg1, :_cid0)"
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

func TestMoveTablesV2Workflow(t *testing.T) {
	vc = setupCluster(t)
	defer vtgateConn.Close()
	//defer vc.TearDown()

	setupCustomerKeyspace(t)
	moveTables2Start(t, "customer")
	//printRoutingRules(t, vc, "After MoveTables Started")
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToSource(t)

	moveTablesSwitchReads(t, "")
	//printRoutingRules(t, vc, "After SwitchReads")
	validateReadsRouteToTarget(t, "replica")
	validateWritesRouteToSource(t)

	moveTablesSwitchWrites(t)
	//printRoutingRules(t, vc, "After SwitchWrites")
	validateReadsRouteToTarget(t, "replica")
	validateWritesRouteToTarget(t)

	moveTablesReverseReads(t, "")
	//printRoutingRules(t, vc, "After ReverseReads")
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToTarget(t)

	moveTablesReverseWrites(t)
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToSource(t)
	//printRoutingRules(t, vc, "After ReverseWrites")

	moveTablesSwitchWrites(t)
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToTarget(t)

	moveTablesReverseWrites(t)
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToSource(t)

	moveTablesSwitchReads(t, "")
	validateReadsRouteToTarget(t, "replica")
	validateWritesRouteToSource(t)

	moveTablesReverseReads(t, "")
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToSource(t)

	moveTablesSwitchReadsAndWrites(t)
	validateReadsRouteToTarget(t, "replica")
	validateWritesRouteToTarget(t)
	moveTablesReverseReadsAndWrites(t)
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToSource(t)

	var err error
	var output string

	err = moveTablesComplete(t)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot complete workflow because you have not yet switched all read and write traffic")

	moveTablesSwitchReadsAndWrites(t)
	validateReadsRouteToTarget(t, "replica")
	validateWritesRouteToTarget(t)

	err = moveTablesComplete(t)
	require.NoError(t, err)

	listAllArgs := []string{"workflow", "customer", "listall"}
	output, _ = vc.VtctlClient.ExecuteCommandWithOutput(listAllArgs...)
	require.Contains(t, output, "No workflows found in keyspace customer")

	moveTables2Start(t, "customer2")
	output, _ = vc.VtctlClient.ExecuteCommandWithOutput(listAllArgs...)
	require.Contains(t, output, "Following workflow(s) found in keyspace customer: p2c")

	err = moveTablesAbort(t)
	require.NoError(t, err)

	output, _ = vc.VtctlClient.ExecuteCommandWithOutput(listAllArgs...)
	require.Contains(t, output, "No workflows found in keyspace customer")
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
	workflow := "p2c"
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
