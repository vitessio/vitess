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
	"testing"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/wrangler"

	"github.com/stretchr/testify/require"
)

func TestNewMoveTablesWorkflow(t *testing.T) {
	vc = setupCluster(t)
	setupCustomerKeyspace(t)
	moveTablesNew(t) //moveTables(t, defaultCellName, moveTablesWorkflowName, sourceKs, targetKs, tablesToMove, wrangler.WorkflowEventStart)

}

func setupCluster(t *testing.T) *VitessCluster {
	cells := []string{"zone1"}

	vc = InitCluster(t, cells)
	require.NotNil(t, vc)
	defaultCellName := "zone1"
	allCellNames = defaultCellName
	defaultCell = vc.Cells[defaultCellName]

	//defer vc.TearDown()

	cell1 := vc.Cells["zone1"]
	vc.AddKeyspace(t, []*Cell{cell1}, "product", "0", initialProductVSchema, initialProductSchema, defaultReplicas, defaultRdonly, 100)

	vtgate = cell1.Vtgates[0]
	require.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "product", "0"), 1)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", "product", "0"), 2)

	vtgateConn = getConnection(t, globalConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t)
	insertInitialData(t)
	return vc
}

const (
	moveTablesWorkflowName = "p2c"
	sourceKs               = "product"
	targetKs               = "customer"
	ksWorkflow             = targetKs + "." + moveTablesWorkflowName
	reverseKsWorkflow      = sourceKs + "." + moveTablesWorkflowName + "_reverse"
	tablesToMove           = "customer"
	defaultCellName        = "zone1"
)

var (
	customerTab1, customerTab2 *cluster.VttabletProcess
)

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

var moveTablesNew = func(t *testing.T) {
	moveTables(t, defaultCellName, moveTablesWorkflowName, sourceKs, targetKs, tablesToMove, wrangler.WorkflowEventStart)
	catchup(t, customerTab1, moveTablesWorkflowName, "MoveTables")
	catchup(t, customerTab2, moveTablesWorkflowName, "MoveTables")
	vdiff(t, ksWorkflow)
}

func moveCustomerTableSwitchFlows(t *testing.T, cells []*Cell, sourceCellOrAlias string) {
	workflow := "p2c"
	sourceKs := "product"
	targetKs := "customer"
	ksWorkflow := fmt.Sprintf("%s.%s", targetKs, workflow)
	reverseKsWorkflow := fmt.Sprintf("%s.%s_reverse", sourceKs, workflow)

	if _, err := vc.AddKeyspace(t, cells, "customer", "-80,80-", customerVSchema, customerSchema, defaultReplicas, defaultRdonly, 200); err != nil {
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
	tables := "customer"

	// Assume we are operating on first cell
	defaultCell := cells[0]
	custKs := vc.Cells[defaultCell.Name].Keyspaces["customer"]
	customerTab1 := custKs.Shards["-80"].Tablets["zone1-200"].Vttablet
	customerReplicaTab1 := custKs.Shards["-80"].Tablets["zone1-201"].Vttablet
	customerTab2 := custKs.Shards["80-"].Tablets["zone1-300"].Vttablet

	productTab := vc.Cells[defaultCell.Name].Keyspaces["product"].Shards["0"].Tablets["zone1-100"].Vttablet
	productReplicaTab := vc.Cells[defaultCell.Name].Keyspaces["product"].Shards["0"].Tablets["zone1-101"].Vttablet
	query := "select * from customer"

	validateReadsRouteToSource := func() {
		require.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productReplicaTab, "product@replica", query, query))
	}

	validateReadsRouteToTarget := func() {
		require.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, customerReplicaTab1, "product@replica", query, query))
	}

	validateWritesRouteToSource := func() {
		insertQuery := "insert into customer(name, cid) values('tempCustomer2', 200)"
		matchInsertQuery := "insert into customer(name, cid) values (:vtg1, :_cid0)"
		require.False(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productTab, "customer", insertQuery, matchInsertQuery))
		execVtgateQuery(t, vtgateConn, "customer", "delete from customer where cid > 100")
	}
	validateWritesRouteToTarget := func() {
		insertQuery := "insert into customer(name, cid) values('tempCustomer3', 101)"
		matchInsertQuery := "insert into customer(name, cid) values (:vtg1, :_cid0)"
		require.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, customerTab2, "customer", insertQuery, matchInsertQuery))
		insertQuery = "insert into customer(name, cid) values('tempCustomer3', 102)"
		require.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, customerTab1, "customer", insertQuery, matchInsertQuery))
		execVtgateQuery(t, vtgateConn, "customer", "delete from customer where cid > 100")
	}

	revert := func() {
		switchWrites(t, reverseKsWorkflow, false)
		validateWritesRouteToSource()
		switchReadsNew(t, allCellNames, ksWorkflow, true)
		validateReadsRouteToSource()
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

	var moveTablesNew = func() {
		moveTables(t, sourceCellOrAlias, workflow, sourceKs, targetKs, tables, wrangler.WorkflowEventStart)
		catchup(t, customerTab1, workflow, "MoveTables")
		catchup(t, customerTab2, workflow, "MoveTables")
		vdiff(t, ksWorkflow)
	}

	var switchReadsFollowedBySwitchWrites = func() {
		moveTablesNew()

		validateReadsRouteToSource()
		switchReadsNew(t, allCellNames, ksWorkflow, false)
		validateReadsRouteToTarget()

		validateWritesRouteToSource()
		switchWrites(t, ksWorkflow, false)
		validateWritesRouteToTarget()

		revert()
	}
	var switchWritesFollowedBySwitchReads = func() {
		moveTablesNew()

		validateWritesRouteToSource()
		switchWrites(t, ksWorkflow, false)
		validateWritesRouteToTarget()

		validateReadsRouteToSource()
		switchReadsNew(t, allCellNames, ksWorkflow, false)
		validateReadsRouteToTarget()

		revert()
	}

	var switchReadsReverseSwitchWritesSwitchReads = func() {
		moveTablesNew()

		validateReadsRouteToSource()
		switchReadsNew(t, allCellNames, ksWorkflow, false)
		validateReadsRouteToTarget()

		switchReadsNew(t, allCellNames, ksWorkflow, true)
		validateReadsRouteToSource()
		printRoutingRules(t, vc, "After reversing SwitchReads")

		validateWritesRouteToSource()
		switchWrites(t, ksWorkflow, false)
		validateWritesRouteToTarget()

		printRoutingRules(t, vc, "After SwitchWrites and reversing SwitchReads")
		validateReadsRouteToSource()
		switchReadsNew(t, allCellNames, ksWorkflow, false)
		validateReadsRouteToTarget()

		revert()
	}

	var switchWritesReverseSwitchReadsSwitchWrites = func() {
		moveTablesNew()

		validateWritesRouteToSource()
		switchWrites(t, ksWorkflow, false)
		validateWritesRouteToTarget()

		switchWrites(t, ksWorkflow, true)
		validateWritesRouteToSource()

		validateReadsRouteToSource()
		switchReadsNew(t, allCellNames, ksWorkflow, false)
		validateReadsRouteToTarget()

		validateWritesRouteToSource()
		switchWrites(t, ksWorkflow, false)
		validateWritesRouteToTarget()

		revert()
	}
	_ = switchReadsFollowedBySwitchWrites
	_ = switchWritesFollowedBySwitchReads
	_ = switchReadsReverseSwitchWritesSwitchReads
	_ = switchWritesReverseSwitchReadsSwitchWrites
	switchReadsFollowedBySwitchWrites()
	//switchWritesFollowedBySwitchReads()
	//switchReadsReverseSwitchWritesSwitchReads()
	//switchWritesReverseSwitchReadsSwitchWrites()
}
