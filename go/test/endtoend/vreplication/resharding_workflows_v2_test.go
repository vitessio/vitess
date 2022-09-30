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
	"net"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/wrangler"
)

const (
	workflowName      = "wf1"
	sourceKs          = "product"
	targetKs          = "customer"
	ksWorkflow        = targetKs + "." + workflowName
	reverseKsWorkflow = sourceKs + "." + workflowName + "_reverse"
	tablesToMove      = "customer"
	defaultCellName   = "zone1"
	readQuery         = "select cid from customer"
)

const (
	workflowActionCreate         = "Create"
	workflowActionSwitchTraffic  = "SwitchTraffic"
	workflowActionReverseTraffic = "ReverseTraffic"
	workflowActionComplete       = "Complete"
	workflowActionCancel         = "Cancel"
)

var (
	targetTab1, targetTab2, targetReplicaTab1, targetRdonlyTab1 *cluster.VttabletProcess
	sourceTab, sourceReplicaTab, sourceRdonlyTab                *cluster.VttabletProcess

	lastOutput          string
	currentWorkflowType wrangler.VReplicationWorkflowType
)

func createReshardWorkflow(t *testing.T, sourceShards, targetShards string) error {
	err := tstWorkflowExec(t, defaultCellName, workflowName, targetKs, targetKs,
		"", workflowActionCreate, "", sourceShards, targetShards)
	require.NoError(t, err)
	waitForWorkflowState(t, vc, ksWorkflow, workflowStateRunning)
	catchup(t, targetTab1, workflowName, "Reshard")
	catchup(t, targetTab2, workflowName, "Reshard")
	vdiff1(t, ksWorkflow, "")
	return nil
}

func createMoveTablesWorkflow(t *testing.T, tables string) {
	if tables == "" {
		tables = tablesToMove
	}
	err := tstWorkflowExec(t, defaultCellName, workflowName, sourceKs, targetKs,
		tables, workflowActionCreate, "", "", "")
	require.NoError(t, err)
	waitForWorkflowState(t, vc, ksWorkflow, workflowStateRunning)
	catchup(t, targetTab1, workflowName, "MoveTables")
	catchup(t, targetTab2, workflowName, "MoveTables")
	vdiff1(t, ksWorkflow, "")
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

	args = append(args, "--")

	if BypassLagCheck {
		args = append(args, "--max_replication_lag_allowed=2542087h")
	}

	switch action {
	case workflowActionCreate:
		if currentWorkflowType == wrangler.MoveTablesWorkflow {
			args = append(args, "--source", sourceKs, "--tables", tables)
			if sourceShards != "" {
				args = append(args, "--source_shards", sourceShards)
			}
		} else {
			args = append(args, "--source_shards", sourceShards, "--target_shards", targetShards)
		}
	}
	if cells != "" {
		args = append(args, "--cells", cells)
	}
	if tabletTypes != "" {
		args = append(args, "--tablet_types", tabletTypes)
	}
	ksWorkflow := fmt.Sprintf("%s.%s", targetKs, workflow)
	args = append(args, action, ksWorkflow)
	output, err := vc.VtctlClient.ExecuteCommandWithOutput(args...)
	lastOutput = output
	if err != nil {
		return fmt.Errorf("%s: %s", err, output)
	}
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
	require.NoError(t, tstWorkflowAction(t, workflowActionSwitchTraffic, "primary", ""))
}

func tstWorkflowReverseWrites(t *testing.T) {
	require.NoError(t, tstWorkflowAction(t, workflowActionReverseTraffic, "primary", ""))
}

// tstWorkflowSwitchReadsAndWrites tests that SwitchWrites w/o any user provided --tablet_types
// value switches all traffic
func tstWorkflowSwitchReadsAndWrites(t *testing.T) {
	require.NoError(t, tstWorkflowAction(t, workflowActionSwitchTraffic, "", ""))
}

// tstWorkflowReversesReadsAndWrites tests that SwitchWrites w/o any user provided --tablet_types
// value switches all traffic in reverse
func tstWorkflowReverseReadsAndWrites(t *testing.T) {
	require.NoError(t, tstWorkflowAction(t, workflowActionReverseTraffic, "", ""))
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
	defaultRdonly = 1
	defer func() { defaultRdonly = 0 }()
	vc = setupCluster(t)
	defer vtgateConn.Close()
	defer vc.TearDown(t)

	// Internal tables like the lifecycle ones for OnlineDDL should be ignored
	ddlSQL := "ALTER TABLE customer MODIFY cid bigint UNSIGNED"
	tstApplySchemaOnlineDDL(t, ddlSQL, sourceKs)

	testMoveTablesV2Workflow(t)
	testReshardV2Workflow(t)
	log.Flush()
}

// TestPartialMoveTables tests partial move tables by moving just one shard
// 80- from customer to customer2.
func TestPartialMoveTables(t *testing.T) {
	defaultRdonly = 1
	origExtraVTGateArgs := extraVTGateArgs
	// We need to enable shard routing for partial movetables routing.
	// And we need to disable schema change tracking in vtgate as we want
	// to test query routing using a query we know will fail as it's
	// using a column that doesn't exist in the schema -- this way we
	// get the target shard details back in the error message. If schema
	// tracking is enabled then vtgate will produce an error about the
	// unknown symbol before attempting to route the query.
	extraVTGateArgs = append(extraVTGateArgs, []string{
		"--enable-partial-keyspace-migration",
		"--schema_change_signal=false",
	}...)
	defer func() {
		extraVTGateArgs = origExtraVTGateArgs
	}()
	vc = setupCluster(t)
	defer vtgateConn.Close()
	defer vc.TearDown(t)
	setupCustomerKeyspace(t)

	// Move customer table from unsharded product keyspace to
	// sharded customer keyspace.
	createMoveTablesWorkflow(t, "customer")
	tstWorkflowSwitchReadsAndWrites(t)
	tstWorkflowComplete(t)

	// Now setup the customer2 keyspace so we can do a partial
	// move tables for one of the two shards: 80-.
	defaultRdonly = 0
	setupCustomer2Keyspace(t)
	currentWorkflowType = wrangler.MoveTablesWorkflow
	wfName := "partial"
	moveToKs := "customer2"
	shard := "80-"
	ksWf := fmt.Sprintf("%s.%s", moveToKs, wfName)
	err := tstWorkflowExec(t, defaultCellName, wfName, targetKs, moveToKs,
		"customer", workflowActionCreate, "", shard, "")
	require.NoError(t, err)
	targetTab1 = vc.getPrimaryTablet(t, moveToKs, shard)
	catchup(t, targetTab1, wfName, "Partial MoveTables Customer to Customer2")
	vdiff1(t, ksWf, "")

	waitForRowCount(t, vtgateConn, "customer", "customer", 3)      // customer: all shards
	waitForRowCount(t, vtgateConn, "customer2", "customer", 3)     // customer: all shards
	waitForRowCount(t, vtgateConn, "customer2:80-", "customer", 2) // customer2: 80-

	// Remove any manually applied shard routing rules as these
	// should be set by SwitchTraffic.
	emptyRules := `{"rules":[]}`
	applyShardRoutingRules(t, emptyRules)
	require.Equal(t, emptyRules, getShardRoutingRules(t))

	// switch all traffic
	require.NoError(t, tstWorkflowExec(t, "", wfName, "", moveToKs, "", workflowActionSwitchTraffic, "", "", ""))
	expectedSwitchOutput := fmt.Sprintf("SwitchTraffic was successful for workflow customer2.partial\nStart State: Reads Not Switched. Writes Not Switched\nCurrent State: Reads partially switched, for shards: %s. Writes partially switched, for shards: %s\n\n",
		shard, shard)
	require.Equal(t, expectedSwitchOutput, lastOutput)

	// Confirm global routing rules -- everything should still be routed
	// to the source side, customer, globally.
	output, err := vc.VtctlClient.ExecuteCommandWithOutput("GetRoutingRules")
	require.NoError(t, err)
	result := gjson.Get(output, "rules")
	result.ForEach(func(attributeKey, attributeValue gjson.Result) bool {
		// 0 is the keyspace and 1 is optional tablename[@tablettype]
		fromKsTbl := strings.Split(attributeValue.Get("fromTable").String(), ".")
		// 0 is the keyspace and 1 is the tablename
		toKsTbl := strings.Split(attributeValue.Get("toTables.0").String(), ".")
		// All tables in the customer and customer2 keyspaces should be
		// routed to the customer keyspace.
		if fromKsTbl[0] == "customer" || fromKsTbl[0] == "customer2" {
			require.Equal(t, "customer", toKsTbl[0])
		}
		return true
	})
	// Confirm shard routing rules -- all traffic for the 80- shard should be
	// routed into the customer2 keyspace, overriding the global routing rules.
	expectedShardRoutingRules := `{"rules":[{"from_keyspace":"customer","to_keyspace":"customer2","shard":"80-"}]}`
	require.Equal(t, expectedShardRoutingRules, getShardRoutingRules(t))

	// This query uses an ID that should always get routed to customer2:80-
	targetRoutedQuery := "select name from customer where cid = 1 and noexistcol = 'foo'"
	// This query uses an ID that should always get routed to customer:-80
	sourceRoutedQuery := "select name from customer where cid = 2 and noexistcol = 'foo'"

	// reset any existing vtgate connection state
	vtgateConn.Close()
	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()

	// No shard targeting
	_, err = vtgateConn.ExecuteFetch(targetRoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "target: customer2.80-.primary")
	_, err = vtgateConn.ExecuteFetch(sourceRoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "target: customer.-80.primary")

	// Shard targeting
	_, err = vtgateConn.ExecuteFetch("use `customer2:80-`", 0, false)
	require.NoError(t, err)
	_, err = vtgateConn.ExecuteFetch(targetRoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "target: customer2.80-.primary")
	_, err = vtgateConn.ExecuteFetch("use `customer:80-`", 0, false)
	require.NoError(t, err)
	_, err = vtgateConn.ExecuteFetch(targetRoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "target: customer2.80-.primary")

	// Tablet type targeting
	_, err = vtgateConn.ExecuteFetch("use `customer2@replica`", 0, false)
	require.NoError(t, err)
	_, err = vtgateConn.ExecuteFetch(targetRoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "target: customer2.80-.replica")
	_, err = vtgateConn.ExecuteFetch(sourceRoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "target: customer.-80.replica")
	_, err = vtgateConn.ExecuteFetch("use `customer@replica`", 0, false)
	require.NoError(t, err)
	_, err = vtgateConn.ExecuteFetch(targetRoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "target: customer2.80-.replica")
	_, err = vtgateConn.ExecuteFetch(sourceRoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "target: customer.-80.replica")

	// We cannot Complete a partial move tables at the moment because it will
	// find that all traffic has (obviously) not been switched we need to
	// cleanup using Workflow delete.
	err = tstWorkflowExec(t, "", wfName, "", moveToKs, "", workflowActionComplete, "", "", "")
	require.Error(t, err)
	require.Equal(t, expectedShardRoutingRules, getShardRoutingRules(t))
	_, err = vc.VtctlClient.ExecuteCommandWithOutput("Workflow", ksWf, "delete")
	require.NoError(t, err)
	output, err = vc.VtctlClient.ExecuteCommandWithOutput("Workflow", ksWf, "show")
	require.Error(t, err)
	require.Contains(t, output, "no streams found")

}

func getVtctldGRPCURL() string {
	return net.JoinHostPort("localhost", strconv.Itoa(vc.Vtctld.GrpcPort))
}

func applyShardRoutingRules(t *testing.T, rules string) {
	output, err := osExec(t, "vtctldclient", []string{"--server", getVtctldGRPCURL(), "ApplyShardRoutingRules", "--rules", rules})
	log.Infof("ApplyShardRoutingRules err: %+v, output: %+v", err, output)
	require.NoError(t, err, output)
	require.NotNil(t, output)
}

func getShardRoutingRules(t *testing.T) string {
	output, err := osExec(t, "vtctldclient", []string{"--server", getVtctldGRPCURL(), "GetShardRoutingRules"})
	log.Infof("GetShardRoutingRules err: %+v, output: %+v", err, output)
	require.Nilf(t, err, output)
	require.NotNil(t, output)
	re := regexp.MustCompile(`[\n\s]+`)
	output = re.ReplaceAllString(output, "")
	output = strings.TrimSpace(output)
	return output
}

/*
testVSchemaForSequenceAfterMoveTables checks that the related sequence tag is migrated correctly in the vschema
while moving a table with an auto-increment from sharded to unsharded.
*/
func testVSchemaForSequenceAfterMoveTables(t *testing.T) {
	// at this point the unsharded product and sharded customer keyspaces are created by previous tests

	// use MoveTables to move customer2 from product to customer using
	currentWorkflowType = wrangler.MoveTablesWorkflow
	err := tstWorkflowExec(t, defaultCellName, "wf2", sourceKs, targetKs,
		"customer2", workflowActionCreate, "", "", "")
	require.NoError(t, err)

	waitForWorkflowState(t, vc, "customer.wf2", workflowStateRunning)
	waitForLowLag(t, "customer", "wf2")

	err = tstWorkflowExec(t, defaultCellName, "wf2", sourceKs, targetKs,
		"", workflowActionSwitchTraffic, "", "", "")
	require.NoError(t, err)
	err = tstWorkflowExec(t, defaultCellName, "wf2", sourceKs, targetKs,
		"", workflowActionComplete, "", "", "")
	require.NoError(t, err)

	// sanity check
	output, err := vc.VtctlClient.ExecuteCommandWithOutput("GetVSchema", "product")
	require.NoError(t, err)
	assert.NotContains(t, output, "customer2\"", "customer2 still found in keyspace product")
	waitForRowCount(t, vtgateConn, "customer", "customer2", 3)

	// check that customer2 has the sequence tag
	output, err = vc.VtctlClient.ExecuteCommandWithOutput("GetVSchema", "customer")
	require.NoError(t, err)
	assert.Contains(t, output, "\"sequence\": \"customer_seq2\"", "customer2 sequence missing in keyspace customer")

	// ensure sequence is available to vtgate
	num := 5
	for i := 0; i < num; i++ {
		execVtgateQuery(t, vtgateConn, "customer", "insert into customer2(name) values('a')")
	}
	waitForRowCount(t, vtgateConn, "customer", "customer2", 3+num)
	want := fmt.Sprintf("[[INT32(%d)]]", 100+num-1)
	waitForQueryResult(t, vtgateConn, "customer", "select max(cid) from customer2", want)

	// use MoveTables to move customer2 back to product. Note that now the table has an associated sequence
	err = tstWorkflowExec(t, defaultCellName, "wf3", targetKs, sourceKs,
		"customer2", workflowActionCreate, "", "", "")
	require.NoError(t, err)
	waitForWorkflowState(t, vc, "product.wf3", workflowStateRunning)

	waitForLowLag(t, "product", "wf3")
	err = tstWorkflowExec(t, defaultCellName, "wf3", targetKs, sourceKs,
		"", workflowActionSwitchTraffic, "", "", "")
	require.NoError(t, err)
	err = tstWorkflowExec(t, defaultCellName, "wf3", targetKs, sourceKs,
		"", workflowActionComplete, "", "", "")
	require.NoError(t, err)

	// sanity check
	output, err = vc.VtctlClient.ExecuteCommandWithOutput("GetVSchema", "product")
	require.NoError(t, err)
	assert.Contains(t, output, "customer2\"", "customer2 not found in keyspace product ")

	// check that customer2 still has the sequence tag
	output, err = vc.VtctlClient.ExecuteCommandWithOutput("GetVSchema", "product")
	require.NoError(t, err)
	assert.Contains(t, output, "\"sequence\": \"customer_seq2\"", "customer2 still found in keyspace product")

	// ensure sequence is available to vtgate
	for i := 0; i < num; i++ {
		execVtgateQuery(t, vtgateConn, "product", "insert into customer2(name) values('a')")
	}
	waitForRowCount(t, vtgateConn, "product", "customer2", 3+num+num)
	want = fmt.Sprintf("[[INT32(%d)]]", 100+num+num-1)
	waitForQueryResult(t, vtgateConn, "product", "select max(cid) from customer2", want)
}

// testReplicatingWithPKEnumCols ensures that we properly apply binlog events
// in the stream where the PK contains an ENUM column
func testReplicatingWithPKEnumCols(t *testing.T) {
	// At this point we have an ongoing MoveTables operation for the customer table
	// from the product to the customer keyspace. Let's delete and insert a row to
	// ensure that the PK -- which is on (cid, typ) with typ being an ENUM -- is
	// managed correctly in the WHERE clause for the delete. The end result is that
	// we should see the proper deletes propogate and not get a duplicate key error
	// when we re-insert the same row values and ultimately VDiff shows the table as
	// being identical in both keyspaces.

	// typ is an enum, with soho having a stored and binlogged value of 2
	deleteQuery := "delete from customer where cid = 2 and typ = 'soho'"
	insertQuery := "insert into customer(cid, name, typ, sport, meta) values(2, 'Paül','soho','cricket',convert(x'7b7d' using utf8mb4))"
	execVtgateQuery(t, vtgateConn, sourceKs, deleteQuery)
	waitForNoWorkflowLag(t, vc, targetKs, workflowName)
	vdiff1(t, ksWorkflow, "")
	execVtgateQuery(t, vtgateConn, sourceKs, insertQuery)
	waitForNoWorkflowLag(t, vc, targetKs, workflowName)
	vdiff1(t, ksWorkflow, "")
}

func testReshardV2Workflow(t *testing.T) {
	currentWorkflowType = wrangler.ReshardWorkflow

	// create internal tables on the original customer shards that should be
	// ignored and not show up on the new shards
	execMultipleQueries(t, vtgateConn, targetKs+"/-80", internalSchema)
	execMultipleQueries(t, vtgateConn, targetKs+"/80-", internalSchema)

	createAdditionalCustomerShards(t, "-40,40-80,80-c0,c0-")
	createReshardWorkflow(t, "-80,80-", "-40,40-80,80-c0,c0-")
	if !strings.Contains(lastOutput, "Workflow started successfully") {
		t.Fail()
	}
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToSource(t)

	// Verify that we've properly ignored any internal operational tables
	// and that they were not copied to the new target shards
	verifyNoInternalTables(t, vtgateConn, targetKs+"/-40")
	verifyNoInternalTables(t, vtgateConn, targetKs+"/c0-")

	testRestOfWorkflow(t)
}

func testMoveTablesV2Workflow(t *testing.T) {
	currentWorkflowType = wrangler.MoveTablesWorkflow

	// test basic forward and reverse flows
	setupCustomerKeyspace(t)
	// The purge table should get skipped/ignored
	// If it's not then we'll get an error as the table doesn't exist in the vschema
	createMoveTablesWorkflow(t, "customer,_vt_PURGE_4f9194b43b2011eb8a0104ed332e05c2_20221210194431")
	if !strings.Contains(lastOutput, "Workflow started successfully") {
		t.Fail()
	}
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToSource(t)

	// Verify that we've properly ignored any internal operational tables
	// and that they were not copied to the new target keyspace
	verifyNoInternalTables(t, vtgateConn, targetKs)

	testReplicatingWithPKEnumCols(t)

	testRestOfWorkflow(t)

	listAllArgs := []string{"workflow", "customer", "listall"}
	output, _ := vc.VtctlClient.ExecuteCommandWithOutput(listAllArgs...)
	require.Contains(t, output, "No workflows found in keyspace customer")

	testVSchemaForSequenceAfterMoveTables(t)

	createMoveTablesWorkflow(t, "Lead,Lead-1")
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

	keyspace := "product"
	if currentWorkflowType == wrangler.ReshardWorkflow {
		keyspace = "customer"
	}
	waitForLowLag(t, keyspace, "wf1_reverse")
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
	waitForLowLag(t, "customer", "wf1")
	tstWorkflowSwitchReads(t, "", "")
	checkStates(t, wrangler.WorkflowStateNotSwitched, wrangler.WorkflowStateReadsSwitched)
	validateReadsRouteToTarget(t, "replica")
	validateWritesRouteToSource(t)

	tstWorkflowSwitchWrites(t)
	checkStates(t, wrangler.WorkflowStateReadsSwitched, wrangler.WorkflowStateAllSwitched)
	validateReadsRouteToTarget(t, "replica")
	validateWritesRouteToTarget(t)

	// this function is called for both MoveTables and Reshard, so the reverse workflows exist in different keyspaces
	keyspace := "product"
	if currentWorkflowType == wrangler.ReshardWorkflow {
		keyspace = "customer"
	}
	waitForLowLag(t, keyspace, "wf1_reverse")
	tstWorkflowReverseReads(t, "", "")
	checkStates(t, wrangler.WorkflowStateAllSwitched, wrangler.WorkflowStateWritesSwitched)
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToTarget(t)

	tstWorkflowReverseWrites(t)
	checkStates(t, wrangler.WorkflowStateWritesSwitched, wrangler.WorkflowStateNotSwitched)
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToSource(t)

	waitForLowLag(t, "customer", "wf1")
	tstWorkflowSwitchWrites(t)
	checkStates(t, wrangler.WorkflowStateNotSwitched, wrangler.WorkflowStateWritesSwitched)
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToTarget(t)

	waitForLowLag(t, keyspace, "wf1_reverse")
	tstWorkflowReverseWrites(t)
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToSource(t)

	waitForLowLag(t, "customer", "wf1")
	tstWorkflowSwitchReads(t, "", "")
	validateReadsRouteToTarget(t, "replica")
	validateWritesRouteToSource(t)

	tstWorkflowReverseReads(t, "", "")
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToSource(t)

	tstWorkflowSwitchReadsAndWrites(t)
	validateReadsRouteToTarget(t, "replica")
	validateReadsRoute(t, "rdonly", targetRdonlyTab1)
	validateWritesRouteToTarget(t)
	waitForLowLag(t, keyspace, "wf1_reverse")
	tstWorkflowReverseReadsAndWrites(t)
	validateReadsRoute(t, "rdonly", sourceRdonlyTab)
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToSource(t)

	// trying to complete an unswitched workflow should error
	err := tstWorkflowComplete(t)
	require.Error(t, err)
	require.Contains(t, err.Error(), wrangler.ErrWorkflowNotFullySwitched)

	// fully switch and complete
	waitForLowLag(t, "customer", "wf1")
	tstWorkflowSwitchReadsAndWrites(t)
	validateReadsRoute(t, "rdonly", targetRdonlyTab1)
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

	vc.AddKeyspace(t, []*Cell{zone1, zone2}, "product", "0", initialProductVSchema, initialProductSchema, defaultReplicas, defaultRdonly, 100, nil)

	vtgate = zone1.Vtgates[0]
	require.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", "product", "0"), 1)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", "product", "0"), 2)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.rdonly", "product", "0"), 1)

	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	verifyClusterHealth(t, vc)
	insertInitialData(t)

	sourceTab = vc.Cells[defaultCell.Name].Keyspaces["product"].Shards["0"].Tablets["zone1-100"].Vttablet
	sourceReplicaTab = vc.Cells[defaultCell.Name].Keyspaces["product"].Shards["0"].Tablets["zone1-101"].Vttablet
	sourceRdonlyTab = vc.Cells[defaultCell.Name].Keyspaces["product"].Shards["0"].Tablets["zone1-102"].Vttablet

	return vc
}

func setupCustomerKeyspace(t *testing.T) {
	if _, err := vc.AddKeyspace(t, []*Cell{vc.Cells["zone1"], vc.Cells["zone2"]}, "customer", "-80,80-",
		customerVSchema, customerSchema, defaultReplicas, defaultRdonly, 200, nil); err != nil {
		t.Fatal(err)
	}
	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", "customer", "-80"), 1); err != nil {
		t.Fatal(err)
	}
	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", "customer", "80-"), 1); err != nil {
		t.Fatal(err)
	}
	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", "customer", "-80"), 2); err != nil {
		t.Fatal(err)
	}
	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", "customer", "80-"), 2); err != nil {
		t.Fatal(err)
	}
	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.rdonly", "customer", "-80"), 1); err != nil {
		t.Fatal(err)
	}
	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.rdonly", "customer", "80-"), 1); err != nil {
		t.Fatal(err)
	}
	custKs := vc.Cells[defaultCell.Name].Keyspaces["customer"]
	targetTab1 = custKs.Shards["-80"].Tablets["zone1-200"].Vttablet
	targetTab2 = custKs.Shards["80-"].Tablets["zone1-300"].Vttablet
	targetReplicaTab1 = custKs.Shards["-80"].Tablets["zone1-201"].Vttablet
	targetRdonlyTab1 = custKs.Shards["-80"].Tablets["zone1-202"].Vttablet
}

func setupCustomer2Keyspace(t *testing.T) {
	c2shards := []string{"-80", "80-"}
	c2keyspace := "customer2"
	if _, err := vc.AddKeyspace(t, []*Cell{vc.Cells["zone1"]}, c2keyspace, strings.Join(c2shards, ","),
		customerVSchema, customerSchema, defaultReplicas, defaultRdonly, 1200, nil); err != nil {
		t.Fatal(err)
	}
	for _, c2shard := range c2shards {
		if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", c2keyspace, c2shard), 1); err != nil {
			t.Fatal(err)
		}
		if defaultReplicas > 0 {
			if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", c2keyspace, c2shard), defaultReplicas); err != nil {
				t.Fatal(err)
			}
		}
		if defaultRdonly > 0 {
			if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.rdonly", c2keyspace, c2shard), defaultRdonly); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestSwitchReadsWritesInAnyOrder(t *testing.T) {
	vc = setupCluster(t)
	defer vc.TearDown(t)
	moveCustomerTableSwitchFlows(t, []*Cell{vc.Cells["zone1"]}, "zone1")
}

func switchReadsNew(t *testing.T, cells, ksWorkflow string, reverse bool) {
	output, err := vc.VtctlClient.ExecuteCommandWithOutput("SwitchReads", "--", "--cells="+cells,
		"--tablet_types=rdonly,replica", fmt.Sprintf("--reverse=%t", reverse), ksWorkflow)
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
		vdiff1(t, ksWorkflow, "")
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
	require.NoError(t, vc.AddShards(t, []*Cell{defaultCell, vc.Cells["zone2"]}, keyspace, shards, defaultReplicas, defaultRdonly, 400, targetKsOpts))
	arrTargetShardNames := strings.Split(shards, ",")

	for _, shardName := range arrTargetShardNames {
		if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", ksName, shardName), 1); err != nil {
			require.NoError(t, err)
		}
		if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", ksName, shardName), 2); err != nil {
			require.NoError(t, err)
		}
		if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.rdonly", ksName, shardName), 1); err != nil {
			require.NoError(t, err)
		}
	}
	custKs := vc.Cells[defaultCell.Name].Keyspaces[ksName]
	targetTab2 = custKs.Shards["80-c0"].Tablets["zone1-600"].Vttablet
	targetTab1 = custKs.Shards["40-80"].Tablets["zone1-500"].Vttablet
	targetReplicaTab1 = custKs.Shards["-40"].Tablets["zone1-401"].Vttablet

	sourceTab = custKs.Shards["-80"].Tablets["zone1-200"].Vttablet
	sourceReplicaTab = custKs.Shards["-80"].Tablets["zone1-201"].Vttablet
	sourceRdonlyTab = custKs.Shards["-80"].Tablets["zone1-202"].Vttablet
}

func tstApplySchemaOnlineDDL(t *testing.T, sql string, keyspace string) {
	err := vc.VtctlClient.ExecuteCommand("ApplySchema", "--", "--skip_preflight", "--ddl_strategy=online",
		"--sql", sql, keyspace)
	require.NoError(t, err, fmt.Sprintf("ApplySchema Error: %s", err))
}
