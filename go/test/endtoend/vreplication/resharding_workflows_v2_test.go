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
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/throttler"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/wrangler"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

const (
	workflowActionCreate         = "Create"
	workflowActionMirrorTraffic  = "Mirror"
	workflowActionSwitchTraffic  = "SwitchTraffic"
	workflowActionReverseTraffic = "ReverseTraffic"
	workflowActionComplete       = "Complete"
	workflowActionCancel         = "Cancel"
)

var (
	targetTab1, targetTab2, targetReplicaTab1, targetRdonlyTab1 *cluster.VttabletProcess
	sourceTab, sourceReplicaTab, sourceRdonlyTab                *cluster.VttabletProcess

	lastOutput          string
	currentWorkflowType binlogdatapb.VReplicationWorkflowType
)

type workflowExecOptions struct {
	deferSecondaryKeys bool
	atomicCopy         bool
	shardSubset        string
	percent            float32
}

var defaultWorkflowExecOptions = &workflowExecOptions{
	deferSecondaryKeys: true,
}

func createReshardWorkflow(t *testing.T, sourceShards, targetShards string) error {
	err := tstWorkflowExec(t, defaultCellName, defaultWorkflowName, defaultTargetKs, defaultTargetKs,
		"", workflowActionCreate, "", sourceShards, targetShards, defaultWorkflowExecOptions)
	require.NoError(t, err)
	waitForWorkflowState(t, vc, defaultKsWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())
	confirmTablesHaveSecondaryKeys(t, []*cluster.VttabletProcess{targetTab1}, defaultTargetKs, "")
	catchup(t, targetTab1, defaultWorkflowName, "Reshard")
	catchup(t, targetTab2, defaultWorkflowName, "Reshard")
	doVDiff(t, defaultKsWorkflow, "")
	return nil
}

func createMoveTablesWorkflow(t *testing.T, tables string) {
	if tables == "" {
		tables = "customer"
	}
	err := tstWorkflowExec(t, defaultCellName, defaultWorkflowName, defaultSourceKs, defaultTargetKs,
		tables, workflowActionCreate, "", "", "", defaultWorkflowExecOptions)
	require.NoError(t, err)
	waitForWorkflowState(t, vc, defaultKsWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())
	confirmTablesHaveSecondaryKeys(t, []*cluster.VttabletProcess{targetTab1}, defaultTargetKs, tables)
	catchup(t, targetTab1, defaultWorkflowName, "MoveTables")
	catchup(t, targetTab2, defaultWorkflowName, "MoveTables")
	doVDiff(t, defaultKsWorkflow, "")
}

func tstWorkflowAction(t *testing.T, action, tabletTypes, cells string) error {
	return tstWorkflowExec(t, cells, defaultWorkflowName, defaultSourceKs, defaultTargetKs, "customer", action, tabletTypes, "", "", defaultWorkflowExecOptions)
}

// tstWorkflowExec executes a MoveTables or Reshard workflow command using
// vtctldclient.
// tstWorkflowExecVtctl instead.
func tstWorkflowExec(t *testing.T, cells, workflow, defaultSourceKs, defaultTargetKs, tables, action, tabletTypes,
	sourceShards, targetShards string, options *workflowExecOptions) error {
	var args []string
	if currentWorkflowType == binlogdatapb.VReplicationWorkflowType_MoveTables {
		args = append(args, "MoveTables")
	} else {
		args = append(args, "Reshard")
	}

	args = append(args, "--workflow", workflow, "--target-keyspace", defaultTargetKs, action)

	switch action {
	case workflowActionCreate:
		if currentWorkflowType == binlogdatapb.VReplicationWorkflowType_MoveTables {
			args = append(args, "--source-keyspace", defaultSourceKs)
			if tables != "" {
				args = append(args, "--tables", tables)
			} else {
				args = append(args, "--all-tables")
			}
			if sourceShards != "" {
				args = append(args, "--source-shards", sourceShards)
			}
		} else {
			args = append(args, "--source-shards", sourceShards, "--target-shards", targetShards)
		}
		// Test new experimental --defer-secondary-keys flag
		switch currentWorkflowType {
		case binlogdatapb.VReplicationWorkflowType_MoveTables, binlogdatapb.VReplicationWorkflowType_Migrate, binlogdatapb.VReplicationWorkflowType_Reshard:
			if !options.atomicCopy && options.deferSecondaryKeys {
				args = append(args, "--defer-secondary-keys")
			}
		}
	default:
		if options.shardSubset != "" {
			args = append(args, "--shards", options.shardSubset)
		}
	}
	if currentWorkflowType == binlogdatapb.VReplicationWorkflowType_MoveTables && action == workflowActionSwitchTraffic {
		args = append(args, "--initialize-target-sequences")
	}
	if action == workflowActionSwitchTraffic || action == workflowActionReverseTraffic {
		if BypassLagCheck {
			args = append(args, "--max-replication-lag-allowed=2542087h")
		}
		args = append(args, "--timeout=90s")
	}
	if currentWorkflowType == binlogdatapb.VReplicationWorkflowType_MoveTables && action == workflowActionCreate && options.atomicCopy {
		args = append(args, "--atomic-copy")
	}
	if (action == workflowActionCreate || action == workflowActionSwitchTraffic || action == workflowActionReverseTraffic) && cells != "" {
		args = append(args, "--cells", cells)
	}
	if action != workflowActionComplete && tabletTypes != "" {
		args = append(args, "--tablet-types", tabletTypes)
	}
	args = append(args, "--action_timeout=10m") // At this point something is up so fail the test
	t.Logf("Executing workflow command: vtctldclient %s", strings.Join(args, " "))
	output, err := vc.VtctldClient.ExecuteCommandWithOutput(args...)
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

// tstWorkflowSwitchReadsAndWrites tests that switching traffic w/o any user provided --tablet_types
// value switches all traffic
func tstWorkflowSwitchReadsAndWrites(t *testing.T) {
	require.NoError(t, tstWorkflowAction(t, workflowActionSwitchTraffic, "", ""))
}

// tstWorkflowReversesReadsAndWrites tests that ReverseTraffic w/o any user provided --tablet_types
// value switches all traffic in reverse
func tstWorkflowReverseReadsAndWrites(t *testing.T) {
	require.NoError(t, tstWorkflowAction(t, workflowActionReverseTraffic, "", ""))
}

func tstWorkflowComplete(t *testing.T) error {
	return tstWorkflowAction(t, workflowActionComplete, "", "")
}

// testWorkflowUpdate is a very simple test of the workflow update
// vtctldclient command.
// It performs a non-behavior impacting update, setting tablet-types
// to primary,replica,rdonly (the only applicable types in these tests).
func testWorkflowUpdate(t *testing.T) {
	tabletTypes := "primary,replica,rdonly"
	_, err := vc.VtctldClient.ExecuteCommandWithOutput("workflow", "--keyspace", "noexist", "update", "--workflow", "noexist", "--tablet-types", tabletTypes)
	require.Error(t, err)
	// Change the tablet-types to rdonly.
	resp, err := vc.VtctldClient.ExecuteCommandWithOutput("workflow", "--keyspace", defaultTargetKs, "update", "--workflow", defaultWorkflowName, "--tablet-types", "rdonly")
	require.NoError(t, err, err)
	// Confirm that we changed the workflow.
	var ures vtctldatapb.WorkflowUpdateResponse
	require.NoError(t, err)
	err = protojson.Unmarshal([]byte(resp), &ures)
	require.NoError(t, err)
	require.Greater(t, len(ures.Details), 0)
	require.True(t, ures.Details[0].Changed)
	// Change tablet-types back to primary,replica,rdonly.
	resp, err = vc.VtctldClient.ExecuteCommandWithOutput("workflow", "--keyspace", defaultTargetKs, "update", "--workflow", defaultWorkflowName, "--tablet-types", tabletTypes)
	require.NoError(t, err, err)
	// Confirm that we changed the workflow.
	err = protojson.Unmarshal([]byte(resp), &ures)
	require.NoError(t, err)
	require.Greater(t, len(ures.Details), 0)
	require.True(t, ures.Details[0].Changed)
	// Execute a no-op as tablet-types is already primary,replica,rdonly.
	resp, err = vc.VtctldClient.ExecuteCommandWithOutput("workflow", "--keyspace", defaultTargetKs, "update", "--workflow", defaultWorkflowName, "--tablet-types", tabletTypes)
	require.NoError(t, err, err)
	// Confirm that we didn't change the workflow.
	err = protojson.Unmarshal([]byte(resp), &ures)
	require.NoError(t, err)
	require.Greater(t, len(ures.Details), 0)
	require.False(t, ures.Details[0].Changed)
}

func tstWorkflowCancel(t *testing.T) error {
	return tstWorkflowAction(t, workflowActionCancel, "", "")
}

func validateReadsRoute(t *testing.T, tabletType string, tablet *cluster.VttabletProcess) {
	if tablet == nil {
		return
	}
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	// We do NOT want to target a shard as that goes around the routing rules and
	// defeats the purpose here. We are using a query w/o a WHERE clause so for
	// sharded keyspaces it should hit all shards as a SCATTER query. So all we
	// care about is the keyspace and tablet type.
	destination := fmt.Sprintf("%s@%s", tablet.Keyspace, strings.ToLower(tabletType))
	readQuery := "select cid from customer limit 50"
	assertQueryExecutesOnTablet(t, vtgateConn, tablet, destination, readQuery, "select cid from customer limit :vtg1")
}

func validateReadsRouteToSource(t *testing.T, tabletTypes string) {
	tt, err := topoproto.ParseTabletTypes(tabletTypes)
	require.NoError(t, err)
	if slices.Contains(tt, topodatapb.TabletType_REPLICA) {
		require.NotNil(t, sourceReplicaTab)
		validateReadsRoute(t, topodatapb.TabletType_REPLICA.String(), sourceReplicaTab)
	}
	if slices.Contains(tt, topodatapb.TabletType_RDONLY) {
		require.NotNil(t, sourceRdonlyTab)
		validateReadsRoute(t, topodatapb.TabletType_RDONLY.String(), sourceRdonlyTab)
	}
}

func validateReadsRouteToTarget(t *testing.T, tabletTypes string) {
	tt, err := topoproto.ParseTabletTypes(tabletTypes)
	require.NoError(t, err)
	if slices.Contains(tt, topodatapb.TabletType_REPLICA) {
		require.NotNil(t, targetReplicaTab1)
		validateReadsRoute(t, topodatapb.TabletType_REPLICA.String(), targetReplicaTab1)
	}
	if slices.Contains(tt, topodatapb.TabletType_RDONLY) {
		require.NotNil(t, targetRdonlyTab1)
		validateReadsRoute(t, topodatapb.TabletType_RDONLY.String(), targetRdonlyTab1)
	}
}

func validateWritesRouteToSource(t *testing.T) {
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	insertQuery := "insert into customer(name, cid) values('tempCustomer2', 200)"
	matchInsertQuery := "insert into customer(`name`, cid) values"
	assertQueryExecutesOnTablet(t, vtgateConn, sourceTab, defaultTargetKs, insertQuery, matchInsertQuery)
	execVtgateQuery(t, vtgateConn, defaultTargetKs, "delete from customer where cid = 200")
}

func validateWritesRouteToTarget(t *testing.T) {
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	insertQuery := "insert into customer(name, cid) values('tempCustomer3', 101)"
	matchInsertQuery := "insert into customer(`name`, cid) values"
	assertQueryExecutesOnTablet(t, vtgateConn, targetTab2, defaultTargetKs, insertQuery, matchInsertQuery)
	insertQuery = "insert into customer(name, cid) values('tempCustomer3', 102)"
	assertQueryExecutesOnTablet(t, vtgateConn, targetTab1, defaultTargetKs, insertQuery, matchInsertQuery)
	execVtgateQuery(t, vtgateConn, defaultTargetKs, "delete from customer where cid in (101, 102)")
}

func revert(t *testing.T, workflowType string) {
	switchWrites(t, workflowType, defaultKsWorkflow, true)
	validateWritesRouteToSource(t)
	switchReadsNew(t, workflowType, getCellNames(nil), defaultKsWorkflow, true)
	validateReadsRouteToSource(t, "replica")

	// cancel the workflow to cleanup
	_, err := vc.VtctldClient.ExecuteCommandWithOutput(workflowType, "--target-keyspace", defaultTargetKs, "--workflow", defaultWorkflowName, "cancel")
	require.NoError(t, err, fmt.Sprintf("%s Cancel error: %v", workflowType, err))
}

func checkStates(t *testing.T, startState, endState string) {
	require.Contains(t, lastOutput, "Start State: "+startState)
	require.Contains(t, lastOutput, "Current State: "+endState)
}

func getCurrentStatus(t *testing.T) string {
	if err := tstWorkflowAction(t, "status", "", ""); err != nil {
		return err.Error()
	}
	return strings.TrimSpace(strings.Trim(lastOutput, "\n"))
}

// ideally this should be broken up into multiple tests for full flow, replica/rdonly flow, reverse flows etc
// but CI currently fails on creating multiple clusters even after the previous ones are torn down

func TestBasicV2Workflows(t *testing.T) {
	ogReplicas := defaultReplicas
	ogRdOnly := defaultRdonly
	defer func() {
		defaultReplicas = ogReplicas
		defaultRdonly = ogRdOnly
	}()
	defaultReplicas = 1
	defaultRdonly = 1
	extraVTTabletArgs = []string{
		parallelInsertWorkers,
	}
	defer func() {
		defaultRdonly = 0
		extraVTTabletArgs = []string{}
	}()

	vc = setupCluster(t)
	defer vc.TearDown()

	// Internal tables like the lifecycle ones for OnlineDDL should be ignored
	ddlSQL := "ALTER TABLE customer MODIFY cid bigint UNSIGNED"
	tstApplySchemaOnlineDDL(t, ddlSQL, defaultSourceKs)

	testMoveTablesV2Workflow(t)
	testReshardV2Workflow(t)
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

/*
testVSchemaForSequenceAfterMoveTables checks that the related sequence tag is migrated correctly in the vschema
while moving a table with an auto-increment from sharded to unsharded.
*/
func testVSchemaForSequenceAfterMoveTables(t *testing.T) {
	// at this point the unsharded product and sharded customer keyspaces are created by previous tests

	// use MoveTables to move customer2 from product to customer using
	currentWorkflowType = binlogdatapb.VReplicationWorkflowType_MoveTables
	err := tstWorkflowExec(t, defaultCellName, "wf2", defaultSourceKs, defaultTargetKs,
		"customer2", workflowActionCreate, "", "", "", defaultWorkflowExecOptions)
	require.NoError(t, err)

	waitForWorkflowState(t, vc, defaultTargetKs+".wf2", binlogdatapb.VReplicationWorkflowState_Running.String())
	waitForLowLag(t, defaultTargetKs, "wf2")

	err = tstWorkflowExec(t, defaultCellName, "wf2", defaultSourceKs, defaultTargetKs,
		"", workflowActionSwitchTraffic, "", "", "", defaultWorkflowExecOptions)
	require.NoError(t, err)
	err = tstWorkflowExec(t, defaultCellName, "wf2", defaultSourceKs, defaultTargetKs,
		"", workflowActionComplete, "", "", "", defaultWorkflowExecOptions)
	require.NoError(t, err)

	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	// sanity check
	output, err := vc.VtctldClient.ExecuteCommandWithOutput("GetVSchema", defaultSourceKs)
	require.NoError(t, err)
	assert.NotContains(t, output, "customer2\"", "customer2 still found in keyspace product")
	waitForRowCount(t, vtgateConn, defaultTargetKs, "customer2", 3)

	// check that customer2 has the sequence tag
	output, err = vc.VtctldClient.ExecuteCommandWithOutput("GetVSchema", defaultTargetKs)
	require.NoError(t, err)
	assert.Contains(t, output, "\"sequence\": \"customer_seq2\"", "customer2 sequence missing in keyspace customer")

	// ensure sequence is available to vtgate
	num := 5
	for i := 0; i < num; i++ {
		execVtgateQuery(t, vtgateConn, defaultTargetKs, "insert into customer2(name) values('a')")
	}
	waitForRowCount(t, vtgateConn, defaultTargetKs, "customer2", 3+num)
	want := fmt.Sprintf("[[INT32(%d)]]", 100+num-1)
	waitForQueryResult(t, vtgateConn, defaultTargetKs, "select max(cid) from customer2", want)

	// use MoveTables to move customer2 back to product. Note that now the table has an associated sequence
	err = tstWorkflowExec(t, defaultCellName, "wf3", defaultTargetKs, defaultSourceKs,
		"customer2", workflowActionCreate, "", "", "", defaultWorkflowExecOptions)
	require.NoError(t, err)
	waitForWorkflowState(t, vc, defaultSourceKs+".wf3", binlogdatapb.VReplicationWorkflowState_Running.String())

	waitForLowLag(t, defaultSourceKs, "wf3")
	err = tstWorkflowExec(t, defaultCellName, "wf3", defaultTargetKs, defaultSourceKs,
		"", workflowActionSwitchTraffic, "", "", "", defaultWorkflowExecOptions)
	require.NoError(t, err)
	err = tstWorkflowExec(t, defaultCellName, "wf3", defaultTargetKs, defaultSourceKs,
		"", workflowActionComplete, "", "", "", defaultWorkflowExecOptions)
	require.NoError(t, err)

	// sanity check
	output, err = vc.VtctldClient.ExecuteCommandWithOutput("GetVSchema", defaultSourceKs)
	require.NoError(t, err)
	assert.Contains(t, output, "customer2\"", "customer2 not found in keyspace product ")

	// check that customer2 still has the sequence tag
	output, err = vc.VtctldClient.ExecuteCommandWithOutput("GetVSchema", defaultSourceKs)
	require.NoError(t, err)
	assert.Contains(t, output, "\"sequence\": \"customer_seq2\"", "customer2 still found in keyspace product")

	// ensure sequence is available to vtgate
	for i := 0; i < num; i++ {
		execVtgateQuery(t, vtgateConn, defaultSourceKs, "insert into customer2(name) values('a')")
	}
	waitForRowCount(t, vtgateConn, defaultSourceKs, "customer2", 3+num+num)
	res := execVtgateQuery(t, vtgateConn, defaultSourceKs, "select max(cid) from customer2")
	cid, err := res.Rows[0][0].ToInt()
	require.NoError(t, err)
	require.GreaterOrEqual(t, cid, 100+num+num-1)
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

	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	// typ is an enum, with soho having a stored and binlogged value of 2
	deleteQuery := "delete from customer where cid = 2 and typ = 'soho'"
	insertQuery := "insert into customer(cid, name, typ, sport, meta) values(2, 'Paül','soho','cricket',convert(x'7b7d' using utf8mb4))"
	execVtgateQuery(t, vtgateConn, defaultSourceKs, deleteQuery)
	waitForNoWorkflowLag(t, vc, defaultTargetKs, defaultWorkflowName)
	doVDiff(t, defaultKsWorkflow, "")
	execVtgateQuery(t, vtgateConn, defaultSourceKs, insertQuery)
	waitForNoWorkflowLag(t, vc, defaultTargetKs, defaultWorkflowName)
	doVDiff(t, defaultKsWorkflow, "")
}

func testReshardV2Workflow(t *testing.T) {
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	currentWorkflowType = binlogdatapb.VReplicationWorkflowType_Reshard

	// Generate customer records in the background for the rest of the test
	// in order to confirm that no writes are lost in either the customer
	// table or the customer_name and enterprise_customer materializations
	// against it during the Reshard and all of the traffic switches.
	dataGenCtx, dataGenCancel := context.WithCancel(context.Background())
	defer dataGenCancel()
	dataGenConn, dataGenCloseConn := getVTGateConn()
	defer dataGenCloseConn()
	dataGenWg := sync.WaitGroup{}
	dataGenWg.Add(1)
	go func() {
		defer dataGenWg.Done()
		id := 1000
		for {
			select {
			case <-dataGenCtx.Done():
				return
			default:
				// Use a random customer type for each record.
				_ = execVtgateQuery(t, dataGenConn, defaultTargetKs, fmt.Sprintf("insert into customer (cid, name, typ) values (%d, 'tempCustomer%d', %s)",
					id, id, customerTypes[rand.IntN(len(customerTypes))]))
			}
			time.Sleep(1 * time.Millisecond)
			id++
		}
	}()

	// create internal tables on the original customer shards that should be
	// ignored and not show up on the new shards
	execMultipleQueries(t, vtgateConn, defaultTargetKs+"/-80", internalSchema)
	execMultipleQueries(t, vtgateConn, defaultTargetKs+"/80-", internalSchema)

	createAdditionalTargetShards(t, "-40,40-80,80-c0,c0-")
	createReshardWorkflow(t, "-80,80-", "-40,40-80,80-c0,c0-")
	validateReadsRouteToSource(t, "replica")
	validateWritesRouteToSource(t)

	// Verify that we've properly ignored any internal operational tables
	// and that they were not copied to the new target shards
	verifyNoInternalTables(t, vtgateConn, defaultTargetKs+"/-40")
	verifyNoInternalTables(t, vtgateConn, defaultTargetKs+"/c0-")

	// Confirm that updating Reshard workflows works.
	testWorkflowUpdate(t)

	testRestOfWorkflow(t)

	// Confirm that we lost no customer related writes during the Reshard.
	dataGenCancel()
	dataGenWg.Wait()
	cres := execVtgateQuery(t, dataGenConn, defaultTargetKs, "select count(*) from customer")
	require.Len(t, cres.Rows, 1)
	waitForNoWorkflowLag(t, vc, defaultTargetKs, "customer_name")
	cnres := execVtgateQuery(t, dataGenConn, defaultTargetKs, "select count(*) from customer_name")
	require.Len(t, cnres.Rows, 1)
	require.EqualValues(t, cres.Rows, cnres.Rows)
	if debugMode {
		// We expect the row count to differ in enterprise_customer because it is
		// using a `where typ='enterprise'` filter. So the count is only for debug
		// info.
		ecres := execVtgateQuery(t, dataGenConn, defaultTargetKs, "select count(*) from enterprise_customer")
		t.Logf("Done inserting customer data. Record counts in customer: %s, customer_name: %s, enterprise_customer: %s",
			cres.Rows[0][0].ToString(), cnres.Rows[0][0].ToString(), ecres.Rows[0][0].ToString())
	}
	// We also do a vdiff on the materialize workflows for good measure.
	doVtctldclientVDiff(t, defaultTargetKs, "customer_name", "", nil)
	doVtctldclientVDiff(t, defaultTargetKs, "enterprise_customer", "", nil)
}

func testMoveTablesV2Workflow(t *testing.T) {
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	currentWorkflowType = binlogdatapb.VReplicationWorkflowType_MoveTables

	materializeShow := func() {
		if !debugMode {
			return
		}
		output, err := vc.VtctldClient.ExecuteCommandWithOutput("materialize", "--target-keyspace", defaultTargetKs, "show", "--workflow=customer_name", "--compact", "--include-logs=false")
		require.NoError(t, err)
		t.Logf("Materialize show output: %s", output)
	}

	// Test basic forward and reverse flows.
	setupTargetKeyspace(t)

	listOutputContainsWorkflow := func(output string, workflow string) bool {
		workflows := []string{}
		err := json.Unmarshal([]byte(output), &workflows)
		require.NoError(t, err)
		for _, w := range workflows {
			if w == workflow {
				return true
			}
		}
		return false
	}
	listOutputIsEmpty := func(output string) bool {
		workflows := []string{}
		err := json.Unmarshal([]byte(output), &workflows)
		require.NoError(t, err)
		return len(workflows) == 0
	}
	listAllArgs := []string{"workflow", "--keyspace", defaultTargetKs, "list"}

	output, err := vc.VtctldClient.ExecuteCommandWithOutput(listAllArgs...)
	require.NoError(t, err)
	require.True(t, listOutputIsEmpty(output))

	// The purge table should get skipped/ignored
	// If it's not then we'll get an error as the table doesn't exist in the vschema
	createMoveTablesWorkflow(t, "customer,loadtest,vdiff_order,reftable,_vt_prg_4f9194b43b2011eb8a0104ed332e05c2_20221210194431_")
	waitForWorkflowState(t, vc, defaultKsWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())
	validateReadsRouteToSource(t, "replica,rdonly")
	validateWritesRouteToSource(t)

	// Verify that we've properly ignored any internal operational tables
	// and that they were not copied to the new target keyspace
	verifyNoInternalTables(t, vtgateConn, defaultTargetKs)

	testReplicatingWithPKEnumCols(t)

	// Confirm that updating MoveTable workflows works.
	testWorkflowUpdate(t)

	testRestOfWorkflow(t)
	// Create our primary intra-keyspace materialization.
	materialize(t, materializeCustomerNameSpec)
	// Create a second one to confirm that multiple ones get migrated correctly.
	materialize(t, materializeCustomerTypeSpec)
	materializeShow()

	output, err = vc.VtctldClient.ExecuteCommandWithOutput(listAllArgs...)
	require.NoError(t, err)
	require.True(t, listOutputContainsWorkflow(output, "customer_name") && listOutputContainsWorkflow(output, "enterprise_customer") && !listOutputContainsWorkflow(output, "wf1"))

	testVSchemaForSequenceAfterMoveTables(t)

	// Confirm that the auto_increment clause on customer.cid was removed.
	cs, err := vtgateConn.ExecuteFetch("show create table customer", 1, false)
	require.NoError(t, err)
	require.Len(t, cs.Rows, 1)
	require.Len(t, cs.Rows[0], 2) // Table and "Create Table"
	csddl := strings.ToLower(cs.Rows[0][1].ToString())
	require.NotContains(t, csddl, "auto_increment", "customer table still has auto_increment clause: %s", csddl)

	createMoveTablesWorkflow(t, "Lead,Lead-1")
	output, err = vc.VtctldClient.ExecuteCommandWithOutput(listAllArgs...)
	require.NoError(t, err)
	require.True(t, listOutputContainsWorkflow(output, "wf1") && listOutputContainsWorkflow(output, "customer_name") && listOutputContainsWorkflow(output, "enterprise_customer"))

	err = tstWorkflowCancel(t)
	require.NoError(t, err)

	output, err = vc.VtctldClient.ExecuteCommandWithOutput(listAllArgs...)
	require.NoError(t, err)
	require.True(t, listOutputContainsWorkflow(output, "customer_name") && listOutputContainsWorkflow(output, "enterprise_customer") && !listOutputContainsWorkflow(output, "wf1"))
}

func testPartialSwitches(t *testing.T) {
	// nothing switched
	require.Contains(t, getCurrentStatus(t), wrangler.WorkflowStateNotSwitched)
	tstWorkflowSwitchReads(t, "replica,rdonly", "zone1")
	nextState := "Reads partially switched. Replica switched in cells: zone1. Rdonly switched in cells: zone1. Writes Not Switched"
	checkStates(t, wrangler.WorkflowStateNotSwitched, nextState)

	tstWorkflowSwitchReads(t, "replica,rdonly", "zone2")
	currentState := nextState
	nextState = wrangler.WorkflowStateReadsSwitched
	checkStates(t, currentState, nextState)

	tstWorkflowSwitchReads(t, "", "")
	checkStates(t, nextState, nextState) // idempotency

	tstWorkflowReverseReads(t, "replica,rdonly", "")
	checkStates(t, wrangler.WorkflowStateReadsSwitched, wrangler.WorkflowStateNotSwitched)

	tstWorkflowSwitchReads(t, "", "")
	checkStates(t, wrangler.WorkflowStateNotSwitched, wrangler.WorkflowStateReadsSwitched)

	tstWorkflowSwitchWrites(t)
	currentState = nextState
	nextState = wrangler.WorkflowStateAllSwitched
	checkStates(t, currentState, nextState)

	tstWorkflowSwitchWrites(t)
	checkStates(t, nextState, nextState) // idempotency

	keyspace := defaultSourceKs
	if currentWorkflowType == binlogdatapb.VReplicationWorkflowType_Reshard {
		keyspace = defaultTargetKs
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
	// Relax the throttler so that it does not cause switches to fail because it can block
	// the catchup for the intra-keyspace materialization.
	req := &vtctldatapb.UpdateThrottlerConfigRequest{
		Enable:      true,
		Threshold:   throttlerConfig.Threshold * 5,
		CustomQuery: throttlerConfig.Query,
	}
	res, err := throttler.UpdateThrottlerTopoConfigRaw(vc.VtctldClient, defaultTargetKs, req, nil, nil)
	require.NoError(t, err, res)

	testPartialSwitches(t)

	// test basic forward and reverse flows
	waitForLowLag(t, defaultTargetKs, "wf1")
	tstWorkflowSwitchReads(t, "", "")
	checkStates(t, wrangler.WorkflowStateNotSwitched, wrangler.WorkflowStateReadsSwitched)
	validateReadsRouteToTarget(t, "replica,rdonly")
	validateWritesRouteToSource(t)

	tstWorkflowSwitchWrites(t)
	checkStates(t, wrangler.WorkflowStateReadsSwitched, wrangler.WorkflowStateAllSwitched)
	validateReadsRouteToTarget(t, "replica,rdonly")
	validateWritesRouteToTarget(t)

	// this function is called for both MoveTables and Reshard, so the reverse workflows exist in different keyspaces
	keyspace := defaultSourceKs
	if currentWorkflowType == binlogdatapb.VReplicationWorkflowType_Reshard {
		keyspace = defaultTargetKs
	}
	waitForLowLag(t, keyspace, "wf1_reverse")
	tstWorkflowReverseReads(t, "", "")
	checkStates(t, wrangler.WorkflowStateAllSwitched, wrangler.WorkflowStateWritesSwitched)
	validateReadsRouteToSource(t, "replica,rdonly")
	validateWritesRouteToTarget(t)

	tstWorkflowReverseWrites(t)
	checkStates(t, wrangler.WorkflowStateWritesSwitched, wrangler.WorkflowStateNotSwitched)
	validateReadsRouteToSource(t, "replica,rdonly")
	validateWritesRouteToSource(t)

	waitForLowLag(t, defaultTargetKs, "wf1")
	tstWorkflowSwitchWrites(t)
	checkStates(t, wrangler.WorkflowStateNotSwitched, wrangler.WorkflowStateWritesSwitched)
	validateReadsRouteToSource(t, "replica,rdonly")
	validateWritesRouteToTarget(t)

	waitForLowLag(t, keyspace, "wf1_reverse")
	tstWorkflowReverseWrites(t)
	checkStates(t, wrangler.WorkflowStateWritesSwitched, wrangler.WorkflowStateNotSwitched)
	validateReadsRouteToSource(t, "replica,rdonly")
	validateWritesRouteToSource(t)

	waitForLowLag(t, defaultTargetKs, "wf1")
	tstWorkflowSwitchReads(t, "", "")
	checkStates(t, wrangler.WorkflowStateNotSwitched, wrangler.WorkflowStateReadsSwitched)
	validateReadsRouteToTarget(t, "replica,rdonly")
	validateWritesRouteToSource(t)

	tstWorkflowReverseReads(t, "", "")
	checkStates(t, wrangler.WorkflowStateReadsSwitched, wrangler.WorkflowStateNotSwitched)
	validateReadsRouteToSource(t, "replica,rdonly")
	validateWritesRouteToSource(t)

	tstWorkflowSwitchReadsAndWrites(t)
	checkStates(t, wrangler.WorkflowStateNotSwitched, wrangler.WorkflowStateAllSwitched)
	validateReadsRouteToTarget(t, "replica,rdonly")
	validateWritesRouteToTarget(t)
	waitForLowLag(t, keyspace, "wf1_reverse")
	tstWorkflowReverseReadsAndWrites(t)
	checkStates(t, wrangler.WorkflowStateAllSwitched, wrangler.WorkflowStateNotSwitched)
	validateReadsRouteToSource(t, "replica,rdonly")
	validateWritesRouteToSource(t)

	// trying to complete an unswitched workflow should error
	err = tstWorkflowComplete(t)
	require.Error(t, err)
	require.Contains(t, err.Error(), wrangler.ErrWorkflowNotFullySwitched)

	// fully switch and complete
	waitForLowLag(t, defaultTargetKs, "wf1")
	waitForLowLag(t, defaultTargetKs, "customer_name")
	waitForLowLag(t, defaultTargetKs, "enterprise_customer")
	tstWorkflowSwitchReadsAndWrites(t)
	validateReadsRouteToTarget(t, "replica,rdonly")
	validateWritesRouteToTarget(t)

	err = tstWorkflowComplete(t)
	require.NoError(t, err)
}

func setupCluster(t *testing.T) *VitessCluster {
	vc = NewVitessCluster(t, &clusterOptions{cells: []string{"zone1", "zone2"}})

	zone1 := vc.Cells["zone1"]
	zone2 := vc.Cells["zone2"]

	vc.AddKeyspace(t, []*Cell{zone1, zone2}, defaultSourceKs, "0", initialProductVSchema, initialProductSchema, defaultReplicas, defaultRdonly, 100, nil)

	defer getVTGateConn()
	verifyClusterHealth(t, vc)
	insertInitialData(t)
	defaultCell := vc.Cells[vc.CellNames[0]]
	sourceTab = vc.Cells[defaultCell.Name].Keyspaces[defaultSourceKs].Shards["0"].Tablets["zone1-100"].Vttablet
	if defaultReplicas > 0 {
		sourceReplicaTab = vc.Cells[defaultCell.Name].Keyspaces[defaultSourceKs].Shards["0"].Tablets["zone1-101"].Vttablet
	}
	if defaultRdonly > 0 {
		sourceRdonlyTab = vc.Cells[defaultCell.Name].Keyspaces[defaultSourceKs].Shards["0"].Tablets["zone1-102"].Vttablet
	}

	return vc
}

func setupTargetKeyspace(t *testing.T) {
	if _, err := vc.AddKeyspace(t, []*Cell{vc.Cells["zone1"], vc.Cells["zone2"]}, defaultTargetKs, "-80,80-",
		customerVSchema, customerSchema, defaultReplicas, defaultRdonly, 200, nil); err != nil {
		t.Fatal(err)
	}
	defaultCell := vc.Cells[vc.CellNames[0]]
	custKs := vc.Cells[defaultCell.Name].Keyspaces[defaultTargetKs]
	targetTab1 = custKs.Shards["-80"].Tablets["zone1-200"].Vttablet
	targetTab2 = custKs.Shards["80-"].Tablets["zone1-300"].Vttablet
	if defaultReplicas > 0 {
		targetReplicaTab1 = custKs.Shards["-80"].Tablets["zone1-201"].Vttablet
	}
	if defaultRdonly > 0 {
		targetRdonlyTab1 = custKs.Shards["-80"].Tablets["zone1-202"].Vttablet
	}
}

func setupCustomer2Keyspace(t *testing.T) {
	c2shards := []string{"-80", "80-"}
	c2keyspace := "customer2"
	if _, err := vc.AddKeyspace(t, []*Cell{vc.Cells["zone1"]}, c2keyspace, strings.Join(c2shards, ","),
		customerVSchema, customerSchema, 0, 0, 1200, nil); err != nil {
		t.Fatal(err)
	}
}

func setupMinimalCluster(t *testing.T) *VitessCluster {
	vc = NewVitessCluster(t, nil)

	defaultCell := vc.Cells[vc.CellNames[0]]

	zone1 := vc.Cells["zone1"]

	vc.AddKeyspace(t, []*Cell{zone1}, defaultSourceKs, "0", initialProductVSchema, initialProductSchema, defaultReplicas, defaultRdonly, 100, nil)

	verifyClusterHealth(t, vc)
	insertInitialData(t)

	sourceTab = vc.Cells[defaultCell.Name].Keyspaces[defaultSourceKs].Shards["0"].Tablets["zone1-100"].Vttablet

	return vc
}

func setupMinimalTargetKeyspace(t *testing.T) map[string]*cluster.VttabletProcess {
	tablets := make(map[string]*cluster.VttabletProcess)
	if _, err := vc.AddKeyspace(t, []*Cell{vc.Cells["zone1"]}, defaultTargetKs, "-80,80-",
		customerVSchema, customerSchema, defaultReplicas, defaultRdonly, 200, nil); err != nil {
		t.Fatal(err)
	}
	defaultCell := vc.Cells[vc.CellNames[0]]
	custKs := vc.Cells[defaultCell.Name].Keyspaces[defaultTargetKs]
	targetTab1 = custKs.Shards["-80"].Tablets["zone1-200"].Vttablet
	targetTab2 = custKs.Shards["80-"].Tablets["zone1-300"].Vttablet
	tablets["-80"] = targetTab1
	tablets["80-"] = targetTab2
	return tablets
}

func TestSwitchReadsWritesInAnyOrder(t *testing.T) {
	vc = setupCluster(t)
	defer vc.TearDown()
	moveCustomerTableSwitchFlows(t, []*Cell{vc.Cells["zone1"]}, "zone1")
}

func switchReadsNew(t *testing.T, workflowType, cells, ksWorkflow string, reverse bool) {
	command := "SwitchTraffic"
	if reverse {
		command = "ReverseTraffic"
	}
	parts := strings.Split(ksWorkflow, ".")
	require.Len(t, parts, 2)
	ks, wf := parts[0], parts[1]
	output, err := vc.VtctldClient.ExecuteCommandWithOutput(workflowType, "--workflow", wf, "--target-keyspace", ks, command,
		"--cells", cells, "--tablet-types=rdonly,replica")
	require.NoError(t, err, fmt.Sprintf("SwitchReads Error: %s: %s", err, output))
	if output != "" {
		fmt.Printf("SwitchReads output: %s\n", output)
	}
}

func moveCustomerTableSwitchFlows(t *testing.T, cells []*Cell, sourceCellOrAlias string) {
	workflow := "wf1"
	ksWorkflow := fmt.Sprintf("%s.%s", defaultTargetKs, workflow)
	tables := "customer"
	setupTargetKeyspace(t)
	workflowType := "MoveTables"

	var moveTablesAndWait = func() {
		moveTablesAction(t, "Create", sourceCellOrAlias, workflow, defaultSourceKs, defaultTargetKs, tables)
		catchup(t, targetTab1, workflow, workflowType)
		catchup(t, targetTab2, workflow, workflowType)
		doVDiff(t, ksWorkflow, "")
	}
	allCellNames := getCellNames(cells)
	var switchReadsFollowedBySwitchWrites = func() {
		moveTablesAndWait()

		validateReadsRouteToSource(t, "replica")
		switchReadsNew(t, workflowType, allCellNames, ksWorkflow, false)
		validateReadsRouteToTarget(t, "replica")

		validateWritesRouteToSource(t)
		switchWrites(t, workflowType, ksWorkflow, false)
		validateWritesRouteToTarget(t)

		revert(t, workflowType)
	}
	var switchWritesFollowedBySwitchReads = func() {
		moveTablesAndWait()

		validateWritesRouteToSource(t)
		switchWrites(t, workflowType, ksWorkflow, false)
		validateWritesRouteToTarget(t)

		validateReadsRouteToSource(t, "replica")
		switchReadsNew(t, workflowType, allCellNames, ksWorkflow, false)
		validateReadsRouteToTarget(t, "replica")

		revert(t, workflowType)
	}

	var switchReadsReverseSwitchWritesSwitchReads = func() {
		moveTablesAndWait()

		validateReadsRouteToSource(t, "replica")
		switchReadsNew(t, workflowType, allCellNames, ksWorkflow, false)
		validateReadsRouteToTarget(t, "replica")

		switchReadsNew(t, workflowType, allCellNames, ksWorkflow, true)
		validateReadsRouteToSource(t, "replica")
		printRoutingRules(t, vc, "After reversing read traffic")

		validateWritesRouteToSource(t)
		switchWrites(t, workflowType, ksWorkflow, false)
		validateWritesRouteToTarget(t)

		printRoutingRules(t, vc, "After switching writes and reversing reads")
		validateReadsRouteToSource(t, "replica")
		switchReadsNew(t, workflowType, allCellNames, ksWorkflow, false)
		validateReadsRouteToTarget(t, "replica")

		revert(t, workflowType)
	}

	var switchWritesReverseSwitchReadsSwitchWrites = func() {
		moveTablesAndWait()

		validateWritesRouteToSource(t)
		switchWrites(t, workflowType, ksWorkflow, false)
		validateWritesRouteToTarget(t)

		switchWrites(t, workflowType, defaultReverseKsWorkflow, true)
		validateWritesRouteToSource(t)

		validateReadsRouteToSource(t, "replica")
		switchReadsNew(t, workflowType, allCellNames, ksWorkflow, false)
		validateReadsRouteToTarget(t, "replica")

		validateWritesRouteToSource(t)
		switchWrites(t, workflowType, ksWorkflow, false)
		validateWritesRouteToTarget(t)

		revert(t, workflowType)
	}
	switchReadsFollowedBySwitchWrites()
	switchWritesFollowedBySwitchReads()
	switchReadsReverseSwitchWritesSwitchReads()
	switchWritesReverseSwitchReadsSwitchWrites()
}

func createAdditionalTargetShards(t *testing.T, shards string) {
	defaultCell := vc.Cells[vc.CellNames[0]]
	keyspace := vc.Cells[defaultCell.Name].Keyspaces[defaultTargetKs]
	require.NoError(t, vc.AddShards(t, []*Cell{defaultCell, vc.Cells["zone2"]}, keyspace, shards, defaultReplicas, defaultRdonly, 400, defaultTargetKsOpts))
	custKs := vc.Cells[defaultCell.Name].Keyspaces[defaultTargetKs]
	targetTab2 = custKs.Shards["80-c0"].Tablets["zone1-600"].Vttablet
	targetTab1 = custKs.Shards["40-80"].Tablets["zone1-500"].Vttablet
	targetReplicaTab1 = custKs.Shards["-40"].Tablets["zone1-401"].Vttablet
	targetRdonlyTab1 = custKs.Shards["-40"].Tablets["zone1-402"].Vttablet

	sourceTab = custKs.Shards["-80"].Tablets["zone1-200"].Vttablet
	sourceReplicaTab = custKs.Shards["-80"].Tablets["zone1-201"].Vttablet
	sourceRdonlyTab = custKs.Shards["-80"].Tablets["zone1-202"].Vttablet
}

func tstApplySchemaOnlineDDL(t *testing.T, sql string, keyspace string) {
	err := vc.VtctldClient.ExecuteCommand("ApplySchema", utils.GetFlagVariantForTests("--ddl-strategy")+"=online",
		"--sql", sql, keyspace)
	require.NoError(t, err, fmt.Sprintf("ApplySchema Error: %s", err))
}

func validateTableRoutingRule(t *testing.T, table, tabletType, fromKeyspace, toKeyspace string) {
	tabletType = strings.ToLower(strings.TrimSpace(tabletType))
	rr := getRoutingRules(t)
	// We set matched = true by default because it is possible, if --no-routing-rules is set while creating
	// a workflow, that the routing rules are empty when the workflow starts.
	// We set it to false below when the rule is found, but before matching the routed keyspace.
	matched := true
	for _, r := range rr.GetRules() {
		fromRule := fmt.Sprintf("%s.%s", fromKeyspace, table)
		if tabletType != "" && tabletType != "primary" {
			fromRule = fmt.Sprintf("%s@%s", fromRule, tabletType)
		}
		if r.FromTable == fromRule {
			// We found the rule, so we can set matched to false here and check for the routed keyspace below.
			matched = false
			require.NotEmpty(t, r.ToTables)
			toTable := r.ToTables[0]
			// The ToTables value is of the form "routedKeyspace.table".
			routedKeyspace, routedTable, ok := strings.Cut(toTable, ".")
			require.True(t, ok)
			require.Equal(t, table, routedTable)
			if routedKeyspace == toKeyspace {
				// We found the rule, the table and keyspace matches, so our search is done.
				matched = true
				break
			}
		}
	}
	require.Truef(t, matched, "routing rule for %s.%s from %s to %s not found", fromKeyspace, table, tabletType, toKeyspace)
}
