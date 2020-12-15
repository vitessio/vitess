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

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/wrangler"

	"github.com/stretchr/testify/require"
)

const (
	moveTablesWorkflowName = "p2c"
	sourceKs               = "product"
	targetKs               = "customer"
	ksWorkflow             = targetKs + "." + moveTablesWorkflowName
	reverseKsWorkflow      = sourceKs + "." + moveTablesWorkflowName + "_reverse"
	tablesToMove           = "customer"
	defaultCellName        = "zone1"
	query                  = "select * from customer"
)

var (
	customerTab1, customerTab2, productReplicaTab, customerReplicaTab1, productTab *cluster.VttabletProcess
)

func moveTablesStart(t *testing.T) {
	moveTables2(t, defaultCellName, moveTablesWorkflowName, sourceKs, targetKs, tablesToMove, wrangler.WorkflowEventStart)
	catchup(t, customerTab1, moveTablesWorkflowName, "MoveTables")
	catchup(t, customerTab2, moveTablesWorkflowName, "MoveTables")
	vdiff(t, ksWorkflow)
}

func moveTables2(t *testing.T, cells, workflow, sourceKs, targetKs, tables, action string) {
	var args []string
	args = append(args, "MoveTables", "-v2")
	action = strings.ToLower(action)
	switch action {
	case "start":
		args = append(args, "-source", sourceKs, "-tables", tables)
	case "switchreads":
	case "switchwrites":
	}
	if cells != "" {
		args = append(args, "-cells", cells)
	}
	ksWorkflow := fmt.Sprintf("%s.%s", targetKs, workflow)
	args = append(args, action, ksWorkflow)
	if err := vc.VtctlClient.ExecuteCommand(args...); err != nil {
		t.Fatalf("MoveTables command failed with %+v\n", err)
	}
}

func moveTablesSwitchReads(t *testing.T, typ string) {
	var action string
	switch typ {
	case "replica":
		action = wrangler.WorkflowEventSwitchReplicaReads
	case "rdonly":
		action = wrangler.WorkflowEventSwitchRdonlyReads
	default:
		action = wrangler.WorkflowEventSwitchReads
	}
	moveTables2(t, defaultCellName, moveTablesWorkflowName, "", targetKs, "", action)
}

func moveTablesSwitchWrites(t *testing.T) {
	moveTables2(t, defaultCellName, moveTablesWorkflowName, "", targetKs, "", wrangler.WorkflowEventSwitchWrites)
}

func moveTablesReverseWrites(t *testing.T) {
	moveTables2(t, defaultCellName, moveTablesWorkflowName, "", targetKs, "", wrangler.WorkflowEventReverseWrites)
}

func moveTablesReverseReads(t *testing.T) {
	moveTables2(t, defaultCellName, moveTablesWorkflowName, "", targetKs, "", wrangler.WorkflowEventReverseReads)
}

func validateReadsRouteToSource(t *testing.T) {
	require.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productReplicaTab, "product@replica", query, query))
}

func validateReadsRouteToTarget(t *testing.T) {
	require.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, customerReplicaTab1, "product@replica", query, query))
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
	validateReadsRouteToSource(t)
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
	moveTablesStart(t)
	printRoutingRules(t, vc, "After MoveTables Started")
	validateReadsRouteToSource(t)
	validateWritesRouteToSource(t)

	moveTablesSwitchReads(t, "")
	printRoutingRules(t, vc, "After SwitchReads")
	validateReadsRouteToTarget(t)
	validateWritesRouteToSource(t)

	moveTablesSwitchWrites(t)
	printRoutingRules(t, vc, "After SwitchWrites")
	validateReadsRouteToTarget(t)
	validateWritesRouteToTarget(t)

	moveTablesReverseReads(t)
	printRoutingRules(t, vc, "After ReverseReads")
	validateReadsRouteToSource(t)
	validateWritesRouteToTarget(t)

	moveTablesReverseWrites(t)
	validateReadsRouteToSource(t)
	validateWritesRouteToSource(t)
	printRoutingRules(t, vc, "After ReverseWrites")

	moveTablesSwitchWrites(t)
	validateReadsRouteToSource(t)
	validateWritesRouteToTarget(t)

	moveTablesReverseWrites(t)
	validateReadsRouteToSource(t)
	validateWritesRouteToSource(t)

	moveTablesSwitchReads(t, "")
	validateReadsRouteToTarget(t)
	validateWritesRouteToSource(t)

	moveTablesReverseReads(t)
	validateReadsRouteToSource(t)
	validateWritesRouteToSource(t)
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

		validateReadsRouteToSource(t)
		switchReadsNew(t, allCellNames, ksWorkflow, false)
		validateReadsRouteToTarget(t)

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

		validateReadsRouteToSource(t)
		switchReadsNew(t, allCellNames, ksWorkflow, false)
		validateReadsRouteToTarget(t)

		revert(t)
	}

	var switchReadsReverseSwitchWritesSwitchReads = func() {
		moveTablesAndWait()

		validateReadsRouteToSource(t)
		switchReadsNew(t, allCellNames, ksWorkflow, false)
		validateReadsRouteToTarget(t)

		switchReadsNew(t, allCellNames, ksWorkflow, true)
		validateReadsRouteToSource(t)
		printRoutingRules(t, vc, "After reversing SwitchReads")

		validateWritesRouteToSource(t)
		switchWrites(t, ksWorkflow, false)
		validateWritesRouteToTarget(t)

		printRoutingRules(t, vc, "After SwitchWrites and reversing SwitchReads")
		validateReadsRouteToSource(t)
		switchReadsNew(t, allCellNames, ksWorkflow, false)
		validateReadsRouteToTarget(t)

		revert(t)
	}

	var switchWritesReverseSwitchReadsSwitchWrites = func() {
		moveTablesAndWait()

		validateWritesRouteToSource(t)
		switchWrites(t, ksWorkflow, false)
		validateWritesRouteToTarget(t)

		switchWrites(t, ksWorkflow, true)
		validateWritesRouteToSource(t)

		validateReadsRouteToSource(t)
		switchReadsNew(t, allCellNames, ksWorkflow, false)
		validateReadsRouteToTarget(t)

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
