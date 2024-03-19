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
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buger/jsonparser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	throttlebase "vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
)

var (
	vc                     *VitessCluster
	defaultRdonly          int
	defaultReplicas        int
	sourceKsOpts           = make(map[string]string)
	targetKsOpts           = make(map[string]string)
	httpClient             = throttlebase.SetupHTTPClient(time.Second)
	sourceThrottlerAppName = throttlerapp.VStreamerName
	targetThrottlerAppName = throttlerapp.VReplicationName
)

const (
	// for some tests we keep an open transaction during a Switch writes and commit it afterwards, to reproduce https://github.com/vitessio/vitess/issues/9400
	// we also then delete the extra row (if) added so that the row counts for the future count comparisons stay the same
	openTxQuery       = "insert into customer(cid, name, typ, sport, meta) values(4, 'openTxQuery',1,'football,baseball','{}');"
	deleteOpenTxQuery = "delete from customer where name = 'openTxQuery'"

	historyLenQuery = "select count as history_len from information_schema.INNODB_METRICS where name = 'trx_rseg_history_len'"

	merchantKeyspace            = "merchant-type"
	maxWait                     = 60 * time.Second
	BypassLagCheck              = true                         // temporary fix for flakiness seen only in CI when lag check is introduced
	throttlerStatusThrottled    = http.StatusExpectationFailed // 417
	throttlerStatusNotThrottled = http.StatusOK                // 200
)

func init() {
	defaultRdonly = 0
	defaultReplicas = 1
}

func throttleResponse(tablet *cluster.VttabletProcess, path string) (respBody string, err error) {
	apiURL := fmt.Sprintf("http://%s:%d/%s", tablet.TabletHostname, tablet.Port, path)
	resp, err := httpClient.Get(apiURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	respBody = string(b)
	return respBody, err
}

func throttleApp(tablet *cluster.VttabletProcess, throttlerApp throttlerapp.Name) (string, error) {
	return throttleResponse(tablet, fmt.Sprintf("throttler/throttle-app?app=%s&duration=1h", throttlerApp.String()))
}

func unthrottleApp(tablet *cluster.VttabletProcess, throttlerApp throttlerapp.Name) (string, error) {
	return throttleResponse(tablet, fmt.Sprintf("throttler/unthrottle-app?app=%s", throttlerApp.String()))
}

func throttlerCheckSelf(tablet *cluster.VttabletProcess, throttlerApp throttlerapp.Name) (respBody string, err error) {
	apiURL := fmt.Sprintf("http://%s:%d/throttler/check-self?app=%s", tablet.TabletHostname, tablet.Port, throttlerApp.String())
	resp, err := httpClient.Get(apiURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	respBody = string(b)
	return respBody, err
}

// TestVReplicationDDLHandling tests the DDL handling in
// VReplication for the values of IGNORE, STOP, and EXEC.
// NOTE: this is a manual test. It is not executed in the
// CI.
func TestVReplicationDDLHandling(t *testing.T) {
	var err error
	workflow := "onddl_test"
	ksWorkflow := fmt.Sprintf("%s.%s", targetKs, workflow)
	table := "orders"
	newColumn := "ddltest"
	cell := "zone1"
	shard := "0"
	vc = NewVitessCluster(t, nil)
	defer vc.TearDown()
	defaultCell := vc.Cells[cell]

	if _, err := vc.AddKeyspace(t, []*Cell{defaultCell}, sourceKs, shard, initialProductVSchema, initialProductSchema, 0, 0, 100, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := vc.AddKeyspace(t, []*Cell{defaultCell}, targetKs, shard, "", "", 0, 0, 200, nil); err != nil {
		t.Fatal(err)
	}
	vtgate := defaultCell.Vtgates[0]
	require.NotNil(t, vtgate)

	verifyClusterHealth(t, vc)

	vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	sourceTab = vc.getPrimaryTablet(t, sourceKs, shard)
	targetTab := vc.getPrimaryTablet(t, targetKs, shard)

	insertInitialData(t)

	_, err = vtgateConn.ExecuteFetch(fmt.Sprintf("use %s", sourceKs), 1, false)
	require.NoError(t, err)

	addColDDL := fmt.Sprintf("alter table %s add column %s varchar(64)", table, newColumn)
	dropColDDL := fmt.Sprintf("alter table %s drop column %s", table, newColumn)
	checkColQuerySource := fmt.Sprintf("select count(column_name) from information_schema.columns where table_schema='vt_%s' and table_name='%s' and column_name='%s'",
		sourceKs, table, newColumn)
	checkColQueryTarget := fmt.Sprintf("select count(column_name) from information_schema.columns where table_schema='vt_%s' and table_name='%s' and column_name='%s'",
		targetKs, table, newColumn)

	// Test IGNORE behavior
	moveTablesAction(t, "Create", defaultCellName, workflow, sourceKs, targetKs, table, "--on-ddl=IGNORE")
	// Wait until we get through the copy phase...
	catchup(t, targetTab, workflow, "MoveTables")
	// Add new col on source
	_, err = vtgateConn.ExecuteFetch(addColDDL, 1, false)
	require.NoError(t, err, "error executing %q: %v", addColDDL, err)
	// Confirm workflow is still running fine
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())
	// Confirm new col does not exist on target
	waitForQueryResult(t, vtgateConn, targetKs, checkColQueryTarget, "[[INT64(0)]]")
	// Confirm new col does exist on source
	waitForQueryResult(t, vtgateConn, sourceKs, checkColQuerySource, "[[INT64(1)]]")
	// Also test Cancel --keep_routing_rules
	moveTablesAction(t, "Cancel", defaultCellName, workflow, sourceKs, targetKs, table, "--keep_routing_rules")
	// Confirm that the routing rules were NOT cleared
	rr, err := vc.VtctldClient.ExecuteCommandWithOutput("GetRoutingRules")
	require.NoError(t, err)
	require.Greater(t, len(gjson.Get(rr, "rules").Array()), 0)
	// Manually clear the routing rules
	err = vc.VtctldClient.ExecuteCommand("ApplyRoutingRules", "--rules", "{}")
	require.NoError(t, err)
	// Confirm that the routing rules are gone
	rr, err = vc.VtctldClient.ExecuteCommandWithOutput("GetRoutingRules")
	require.NoError(t, err)
	require.Equal(t, len(gjson.Get(rr, "rules").Array()), 0)
	// Drop the column on source to start fresh again
	_, err = vtgateConn.ExecuteFetch(dropColDDL, 1, false)
	require.NoError(t, err, "error executing %q: %v", dropColDDL, err)

	// Test STOP behavior (new col now exists nowhere)
	moveTablesAction(t, "Create", defaultCellName, workflow, sourceKs, targetKs, table, "--on-ddl=STOP")
	// Wait until we get through the copy phase...
	catchup(t, targetTab, workflow, "MoveTables")
	// Add new col on the source
	_, err = vtgateConn.ExecuteFetch(addColDDL, 1, false)
	require.NoError(t, err, "error executing %q: %v", addColDDL, err)
	// Confirm that the worfklow stopped because of the DDL
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Stopped.String(), fmt.Sprintf("Message==Stopped at DDL %s", addColDDL))
	// Confirm that the target does not have new col
	waitForQueryResult(t, vtgateConn, targetKs, checkColQueryTarget, "[[INT64(0)]]")
	moveTablesAction(t, "Cancel", defaultCellName, workflow, sourceKs, targetKs, table)

	// Test EXEC behavior (new col now exists on source)
	moveTablesAction(t, "Create", defaultCellName, workflow, sourceKs, targetKs, table, "--on-ddl=EXEC")
	// Wait until we get through the copy phase...
	catchup(t, targetTab, workflow, "MoveTables")
	// Confirm target has new col from copy phase
	waitForQueryResult(t, vtgateConn, targetKs, checkColQueryTarget, "[[INT64(1)]]")
	// Drop col on source
	_, err = vtgateConn.ExecuteFetch(dropColDDL, 1, false)
	require.NoError(t, err, "error executing %q: %v", dropColDDL, err)
	// Confirm workflow is still running fine
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())
	// Confirm new col was dropped on target
	waitForQueryResult(t, vtgateConn, targetKs, checkColQueryTarget, "[[INT64(0)]]")
	moveTablesAction(t, "Cancel", defaultCellName, workflow, sourceKs, targetKs, table)
}

// TestVreplicationCopyThrottling tests the logic that is used
// by vstreamer when starting a copy phase cycle.
// This logic today supports waiting for MySQL replication lag
// and/or InnoDB MVCC history to be below a certain threshold
// before starting the next copy phase. This test focuses on
// the innodb history list length check.
// NOTE: this is a manual test. It is not executed in the CI.
func TestVreplicationCopyThrottling(t *testing.T) {
	workflow := "copy-throttling"
	cell := "zone1"
	table := "customer"
	shard := "0"
	vc = NewVitessCluster(t, nil)
	defer vc.TearDown()
	defaultCell := vc.Cells[cell]
	// To test vstreamer source throttling for the MoveTables operation
	maxSourceTrxHistory := int64(5)
	extraVTTabletArgs = []string{
		// We rely on holding open transactions to generate innodb history so extend the timeout
		// to avoid flakiness when the CI is very slow.
		fmt.Sprintf("--queryserver-config-transaction-timeout=%s", (defaultTimeout * 3).String()),
		fmt.Sprintf("--vreplication_copy_phase_max_innodb_history_list_length=%d", maxSourceTrxHistory),
		parallelInsertWorkers,
	}

	if _, err := vc.AddKeyspace(t, []*Cell{defaultCell}, sourceKs, shard, initialProductVSchema, initialProductSchema, 0, 0, 100, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := vc.AddKeyspace(t, []*Cell{defaultCell}, targetKs, shard, "", "", 0, 0, 200, nil); err != nil {
		t.Fatal(err)
	}
	vtgate := defaultCell.Vtgates[0]
	require.NotNil(t, vtgate)

	// Confirm that the initial copy table phase does not proceed until the source tablet(s)
	// have an InnoDB History List length that is less than specified in the tablet's config.
	// We update rows in a table not part of the MoveTables operation so that we're not blocking
	// on the LOCK TABLE call but rather the InnoDB History List length.
	trxConn := generateInnoDBRowHistory(t, sourceKs, maxSourceTrxHistory)
	// History should have been generated on the source primary tablet
	waitForInnoDBHistoryLength(t, vc.getPrimaryTablet(t, sourceKs, shard), maxSourceTrxHistory)
	// We need to force primary tablet types as the history list has been increased on the source primary
	// We use a small timeout and ignore errors as we don't expect the MoveTables to start here
	// because of the InnoDB History List length.
	moveTablesActionWithTabletTypes(t, "Create", defaultCell.Name, workflow, sourceKs, targetKs, table, "primary", true)
	// Wait for the copy phase to start
	waitForWorkflowState(t, vc, fmt.Sprintf("%s.%s", targetKs, workflow), binlogdatapb.VReplicationWorkflowState_Copying.String())
	// The initial copy phase should be blocking on the history list
	confirmWorkflowHasCopiedNoData(t, targetKs, workflow)
	releaseInnoDBRowHistory(t, trxConn)
	trxConn.Close()
}

func TestBasicVreplicationWorkflow(t *testing.T) {
	defer setAllVTTabletExperimentalFlags()
	sourceKsOpts["DBTypeVersion"] = "mysql-8.0"
	targetKsOpts["DBTypeVersion"] = "mysql-8.0"
	testBasicVreplicationWorkflow(t, "noblob")
}

func TestVreplicationCopyParallel(t *testing.T) {
	sourceKsOpts["DBTypeVersion"] = "mysql-5.7"
	targetKsOpts["DBTypeVersion"] = "mysql-5.7"
	extraVTTabletArgs = []string{
		parallelInsertWorkers,
	}
	testBasicVreplicationWorkflow(t, "")
}

func testBasicVreplicationWorkflow(t *testing.T, binlogRowImage string) {
	testVreplicationWorkflows(t, false, binlogRowImage)
}

// If limited == true, we only run a limited set of workflows.
func testVreplicationWorkflows(t *testing.T, limited bool, binlogRowImage string) {
	var err error
	defaultCellName := "zone1"
	vc = NewVitessCluster(t, nil)
	defer vc.TearDown()
	// Keep the cluster processes minimal to deal with CI resource constraints
	defaultReplicas = 0
	defaultRdonly = 0
	defer func() { defaultReplicas = 1 }()

	if binlogRowImage != "" {
		require.NoError(t, utils.SetBinlogRowImageMode("noblob", vc.ClusterConfig.tmpDir))
		defer utils.SetBinlogRowImageMode("", vc.ClusterConfig.tmpDir)
	}

	defaultCell := vc.Cells[defaultCellName]
	vc.AddKeyspace(t, []*Cell{defaultCell}, "product", "0", initialProductVSchema, initialProductSchema, defaultReplicas, defaultRdonly, 100, sourceKsOpts)

	vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)
	insertInitialData(t)
	materializeRollup(t, true)

	shardCustomer(t, true, []*Cell{defaultCell}, defaultCellName, false)

	// the Lead and Lead-1 tables tested a specific case with binary sharding keys. Drop it now so that we don't
	// have to update the rest of the tests
	execVtgateQuery(t, vtgateConn, "customer", "drop table `Lead`,`Lead-1`")
	validateRollupReplicates(t)
	shardOrders(t)
	shardMerchant(t)

	if limited {
		return
	}

	materializeProduct(t, true)

	materializeMerchantOrders(t, true)
	materializeSales(t, true)
	materializeMerchantSales(t, true)

	reshardMerchant2to3SplitMerge(t)
	reshardMerchant3to1Merge(t)

	insertMoreCustomers(t, 16)
	reshardCustomer2to4Split(t, nil, "")
	confirmAllStreamsRunning(t, vtgateConn, "customer:-40")
	expectNumberOfStreams(t, vtgateConn, "Customer2to4", "sales", "product:0", 4)
	reshardCustomer3to2SplitMerge(t)
	confirmAllStreamsRunning(t, vtgateConn, "customer:-60")
	expectNumberOfStreams(t, vtgateConn, "Customer3to2", "sales", "product:0", 3)
	reshardCustomer3to1Merge(t)
	confirmAllStreamsRunning(t, vtgateConn, "customer:0")
	expectNumberOfStreams(t, vtgateConn, "Customer3to1", "sales", "product:0", 1)

	t.Run("Verify CopyState Is Optimized Afterwards", func(t *testing.T) {
		tabletMap := vc.getVttabletsInKeyspace(t, defaultCell, "customer", topodatapb.TabletType_PRIMARY.String())
		require.NotNil(t, tabletMap)
		require.Greater(t, len(tabletMap), 0)
		for _, tablet := range tabletMap {
			verifyCopyStateIsOptimized(t, tablet)
		}
	})

	t.Run("Test LookupVindex", func(t *testing.T) {
		// LookupVindex does not support noblob images.
		if strings.ToLower(binlogRowImage) == "noblob" {
			return
		}
		_, err = vtgateConn.ExecuteFetch("use customer", 1, false)
		require.NoError(t, err, "error using customer keyspace: %v", err)
		res, err := vtgateConn.ExecuteFetch("select count(*) from customer where name is not null", 1, false)
		require.NoError(t, err, "error getting current row count in customer: %v", err)
		require.Equal(t, 1, len(res.Rows), "expected 1 row in count(*) query, got %d", len(res.Rows))
		rows, _ := res.Rows[0][0].ToInt32()
		// Insert a couple of rows with a NULL name to confirm that they
		// are ignored.
		insert := "insert into customer (cid, name, typ, sport, meta) values (100, NULL, 'soho', 'football','{}'), (101, NULL, 'enterprise','baseball','{}')"
		_, err = vtgateConn.ExecuteFetch(insert, -1, false)
		require.NoError(t, err, "error executing %q: %v", insert, err)

		vindexName := "customer_name_keyspace_id"
		err = vc.VtctldClient.ExecuteCommand("LookupVindex", "--name", vindexName, "--table-keyspace=product", "create", "--keyspace=customer",
			"--type=consistent_lookup", "--table-owner=customer", "--table-owner-columns=name,cid", "--ignore-nulls", "--tablet-types=PRIMARY")
		require.NoError(t, err, "error executing LookupVindex create: %v", err)
		waitForWorkflowState(t, vc, fmt.Sprintf("product.%s", vindexName), binlogdatapb.VReplicationWorkflowState_Running.String())
		waitForRowCount(t, vtgateConn, "product", vindexName, int(rows))
		customerVSchema, err = vc.VtctldClient.ExecuteCommandWithOutput("GetVSchema", "customer")
		require.NoError(t, err, "error executing GetVSchema: %v", err)
		vdx := gjson.Get(customerVSchema, fmt.Sprintf("vindexes.%s", vindexName))
		require.NotNil(t, vdx, "lookup vindex %s not found", vindexName)
		require.Equal(t, "true", vdx.Get("params.write_only").String(), "expected write_only parameter to be true")

		err = vc.VtctldClient.ExecuteCommand("LookupVindex", "--name", vindexName, "--table-keyspace=product", "externalize", "--keyspace=customer")
		require.NoError(t, err, "error executing LookupVindex externalize: %v", err)
		customerVSchema, err = vc.VtctldClient.ExecuteCommandWithOutput("GetVSchema", "customer")
		require.NoError(t, err, "error executing GetVSchema: %v", err)
		vdx = gjson.Get(customerVSchema, fmt.Sprintf("vindexes.%s", vindexName))
		require.NotNil(t, vdx, "lookup vindex %s not found", vindexName)
		require.NotEqual(t, "true", vdx.Get("params.write_only").String(), "did not expect write_only parameter to be true")
	})
}

func TestV2WorkflowsAcrossDBVersions(t *testing.T) {
	sourceKsOpts["DBTypeVersion"] = "mysql-5.7"
	targetKsOpts["DBTypeVersion"] = "mysql-8.0"
	testBasicVreplicationWorkflow(t, "")
}

// TestMoveTablesMariaDBToMySQL tests that MoveTables works between a MariaDB source
// and a MySQL target as while MariaDB is not supported in Vitess v14+ we want
// MariaDB users to have a way to migrate into Vitess.
func TestMoveTablesMariaDBToMySQL(t *testing.T) {
	sourceKsOpts["DBTypeVersion"] = "mariadb-10.10"
	targetKsOpts["DBTypeVersion"] = "mysql-8.0"
	testVreplicationWorkflows(t, true /* only do MoveTables */, "")
}

func TestVStreamFlushBinlog(t *testing.T) {
	defaultCellName := "zone1"
	workflow := "test_vstream_p2c"
	shard := "0"
	vc = NewVitessCluster(t, nil)
	require.NotNil(t, vc)
	defer vc.TearDown()
	defaultCell := vc.Cells[defaultCellName]

	// Keep the cluster processes minimal (no rdonly and no replica tablets)
	// to deal with CI resource constraints.
	// This also makes it easier to confirm the behavior as we know exactly
	// what tablets will be involved.
	if _, err := vc.AddKeyspace(t, []*Cell{defaultCell}, sourceKs, shard, initialProductVSchema, initialProductSchema, 0, 0, 100, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := vc.AddKeyspace(t, []*Cell{defaultCell}, targetKs, shard, "", "", 0, 0, 200, nil); err != nil {
		t.Fatal(err)
	}
	verifyClusterHealth(t, vc)

	sourceTab = vc.getPrimaryTablet(t, sourceKs, shard)

	insertInitialData(t)

	tables := "product,customer,merchant,orders"
	moveTablesAction(t, "Create", defaultCellName, workflow, sourceKs, targetKs, tables)
	// Wait until we get through the copy phase...
	catchup(t, vc.getPrimaryTablet(t, targetKs, shard), workflow, "MoveTables")

	// So far, we should not have rotated any binlogs
	flushCount := int64(sourceTab.GetVars()["VStreamerFlushedBinlogs"].(float64))
	require.Equal(t, flushCount, int64(0), "VStreamerFlushedBinlogs should be 0")

	// Generate a lot of binlog event bytes
	targetBinlogSize := vstreamer.GetBinlogRotationThreshold() + 1024
	vtgateConn := vc.GetVTGateConn(t)
	defer vtgateConn.Close()

	queryF := "insert into db_order_test (c_uuid, dbstuff, created_at) values ('%d', '%s', now())"
	for i := 100; i < 10000; i++ {
		randStr, err := randHex(6500)
		require.NoError(t, err)
		res, err := vtgateConn.ExecuteFetch(fmt.Sprintf(queryF, i, randStr), -1, false)
		require.NoError(t, err)
		require.Greater(t, res.RowsAffected, uint64(0))

		if i%100 == 0 {
			res, err := sourceTab.QueryTablet("show binary logs", sourceKs, false)
			require.NoError(t, err)
			require.NotNil(t, res)
			require.Greater(t, len(res.Rows), 0)
			lastRow := res.Rows[len(res.Rows)-1]
			size, err := lastRow[1].ToInt64()
			require.NoError(t, err)
			if size > targetBinlogSize {
				break
			}
		}
	}

	// Now we should rotate the binary logs ONE time on the source, even
	// though we're opening up multiple result streams (1 per table).
	runVDiffsSideBySide = false
	vdiff(t, targetKs, workflow, defaultCellName, true, false, nil)
	flushCount = int64(sourceTab.GetVars()["VStreamerFlushedBinlogs"].(float64))
	require.Equal(t, flushCount, int64(1), "VStreamerFlushedBinlogs should now be 1")

	// Now if we do another vdiff, we should NOT rotate the binlogs again
	// as we haven't been generating a lot of new binlog events.
	vdiff(t, targetKs, workflow, defaultCellName, true, false, nil)
	flushCount = int64(sourceTab.GetVars()["VStreamerFlushedBinlogs"].(float64))
	require.Equal(t, flushCount, int64(1), "VStreamerFlushedBinlogs should still be 1")
}

func testVStreamCellFlag(t *testing.T) {
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: "product",
			Shard:    "0",
			Gtid:     "",
		}}}
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "product",
			Filter: "select * from product",
		}},
	}
	ctx := context.Background()

	type vstreamTestCase struct {
		cells       string
		expectError bool
	}
	nonExistingCell := "zone7"
	vstreamTestCases := []vstreamTestCase{
		{"zone1,zone2", false},
		{nonExistingCell, true},
		{"", false},
	}

	for _, tc := range vstreamTestCases {
		t.Run("VStreamCellsFlag/"+tc.cells, func(t *testing.T) {
			conn, err := vtgateconn.Dial(ctx, fmt.Sprintf("localhost:%d", vc.ClusterConfig.vtgateGrpcPort))
			require.NoError(t, err)
			defer conn.Close()

			flags := &vtgatepb.VStreamFlags{}
			if tc.cells != "" {
				flags.Cells = tc.cells
				flags.CellPreference = "onlyspecified"
			}

			ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
			reader, err := conn.VStream(ctx2, topodatapb.TabletType_REPLICA, vgtid, filter, flags)
			require.NoError(t, err)

			rowsReceived := false
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer cancel()

				events, err := reader.Recv()
				switch err {
				case nil:
					if len(events) > 0 {
						log.Infof("received %d events", len(events))
						rowsReceived = true
					}
				case io.EOF:
					log.Infof("stream ended without data")
				default:
					log.Infof("%s:: remote error: %v", time.Now(), err)
				}
			}()
			wg.Wait()

			if tc.expectError {
				require.False(t, rowsReceived)

				// if no tablet was found the tablet picker adds a key which includes the cell name to the vtgate TabletPickerNoTabletFoundErrorCount stat
				pickerErrorStat, err := getDebugVar(t, vc.ClusterConfig.vtgatePort, []string{"TabletPickerNoTabletFoundErrorCount"})
				require.NoError(t, err)
				require.Contains(t, pickerErrorStat, nonExistingCell)
			} else {
				require.True(t, rowsReceived)
			}
		})
	}
}

// TestCellAliasVreplicationWorkflow tests replication from a cell with an alias to test the tablet picker's alias functionality
// We also reuse the setup of this test to validate that the "vstream * from" vtgate query functionality is functional
func TestCellAliasVreplicationWorkflow(t *testing.T) {
	cells := []string{"zone1", "zone2"}
	defer mainClusterConfig.enableGTIDCompression()
	defer setAllVTTabletExperimentalFlags()
	vc = NewVitessCluster(t, &clusterOptions{cells: cells})
	defer vc.TearDown()

	keyspace := "product"
	shard := "0"

	require.NoError(t, utils.SetBinlogRowImageMode("noblob", vc.ClusterConfig.tmpDir))
	defer utils.SetBinlogRowImageMode("", vc.ClusterConfig.tmpDir)

	cell1 := vc.Cells["zone1"]
	cell2 := vc.Cells["zone2"]
	vc.AddKeyspace(t, []*Cell{cell1, cell2}, keyspace, shard, initialProductVSchema, initialProductSchema, defaultReplicas, defaultRdonly, 100, sourceKsOpts)

	// Add cell alias containing only zone2
	result, err := vc.VtctlClient.ExecuteCommandWithOutput("AddCellsAlias", "--", "--cells", "zone2", "alias")
	require.NoError(t, err, "command failed with output: %v", result)

	verifyClusterHealth(t, vc)
	insertInitialData(t)

	vtgate := cell1.Vtgates[0]
	t.Run("VStreamFrom", func(t *testing.T) {
		testVStreamFrom(t, vtgate, keyspace, 2)
	})
	shardCustomer(t, true, []*Cell{cell1, cell2}, "alias", false)
	isTableInDenyList(t, vc, "product:0", "customer")
	// we tag along this test so as not to create the overhead of creating another cluster
	testVStreamCellFlag(t)
}

// testVStreamFrom confirms that the "vstream * from" endpoint is serving data
func testVStreamFrom(t *testing.T, vtgate *cluster.VtgateProcess, table string, expectedRowCount int) {
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: vtgate.MySQLServerPort,
	}
	ch := make(chan bool, 1)
	go func() {
		streamConn, err := mysql.Connect(ctx, &vtParams)
		require.NoError(t, err)
		defer streamConn.Close()
		_, err = streamConn.ExecuteFetch("set workload='olap'", 1000, false)
		require.NoError(t, err)

		query := fmt.Sprintf("vstream * from %s", table)
		err = streamConn.ExecuteStreamFetch(query)
		require.NoError(t, err)

		wantFields := []*querypb.Field{{
			Name: "op",
			Type: sqltypes.VarChar,
		}, {
			Name: "pid",
			Type: sqltypes.Int32,
		}, {
			Name: "description",
			Type: sqltypes.VarBinary,
		}, {
			Name: "date1",
			Type: sqltypes.Datetime,
		}, {
			Name: "date2",
			Type: sqltypes.Datetime,
		}}
		gotFields, err := streamConn.Fields()
		require.NoError(t, err)
		for i, field := range gotFields {
			gotFields[i] = &querypb.Field{
				Name: field.Name,
				Type: field.Type,
			}
		}
		utils.MustMatch(t, wantFields, gotFields)

		gotRows, err := streamConn.FetchNext(nil)
		require.NoError(t, err)
		log.Infof("QR1:%v\n", gotRows)

		gotRows, err = streamConn.FetchNext(nil)
		require.NoError(t, err)
		log.Infof("QR2:%+v\n", gotRows)

		ch <- true
	}()

	select {
	case <-ch:
		return
	case <-time.After(5 * time.Second):
		t.Fatal("nothing streamed within timeout")
	}
}

func shardCustomer(t *testing.T, testReverse bool, cells []*Cell, sourceCellOrAlias string, withOpenTx bool) {
	t.Run("shardCustomer", func(t *testing.T) {
		workflow := "p2c"
		sourceKs := "product"
		targetKs := "customer"
		ksWorkflow := fmt.Sprintf("%s.%s", targetKs, workflow)
		if _, err := vc.AddKeyspace(t, cells, "customer", "-80,80-", customerVSchema, customerSchema, defaultReplicas, defaultRdonly, 200, targetKsOpts); err != nil {
			t.Fatal(err)
		}
		// Assume we are operating on first cell
		defaultCell := cells[0]
		custKs := vc.Cells[defaultCell.Name].Keyspaces["customer"]

		tables := "customer,loadtest,Lead,Lead-1,db_order_test,geom_tbl,json_tbl,blüb_tbl,vdiff_order,reftable"
		moveTablesAction(t, "Create", sourceCellOrAlias, workflow, sourceKs, targetKs, tables)

		customerTab1 := custKs.Shards["-80"].Tablets["zone1-200"].Vttablet
		customerTab2 := custKs.Shards["80-"].Tablets["zone1-300"].Vttablet
		productTab := vc.Cells[defaultCell.Name].Keyspaces["product"].Shards["0"].Tablets["zone1-100"].Vttablet

		// Wait to finish the copy phase for all tables
		workflowType := "MoveTables"
		catchup(t, customerTab1, workflow, workflowType)
		catchup(t, customerTab2, workflow, workflowType)

		// The wait in the next code block which checks that customer.dec80 is updated, also confirms that the
		// blob-related dmls we execute here are vreplicated.
		insertIntoBlobTable(t)
		vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
		defer vtgateConn.Close()
		// Confirm that the 0 scale decimal field, dec80, is replicated correctly
		dec80Replicated := false
		execVtgateQuery(t, vtgateConn, sourceKs, "update customer set dec80 = 0")
		execVtgateQuery(t, vtgateConn, sourceKs, "update customer set blb = \"new blob data\" where cid=3")
		execVtgateQuery(t, vtgateConn, sourceKs, "update json_tbl set j1 = null, j2 = 'null', j3 = '\"null\"'")
		execVtgateQuery(t, vtgateConn, sourceKs, "insert into json_tbl(id, j1, j2, j3) values (7, null, 'null', '\"null\"')")
		waitForNoWorkflowLag(t, vc, targetKs, workflow)
		for _, shard := range []string{"-80", "80-"} {
			shardTarget := fmt.Sprintf("%s:%s", targetKs, shard)
			if res := execVtgateQuery(t, vtgateConn, shardTarget, "select cid from customer"); len(res.Rows) > 0 {
				waitForQueryResult(t, vtgateConn, shardTarget, "select distinct dec80 from customer", `[[DECIMAL(0)]]`)
				dec80Replicated = true
			}
		}
		require.Equal(t, true, dec80Replicated)

		// Insert multiple rows in the loadtest table and immediately delete them to confirm that bulk delete
		// works the same way with the vplayer optimization enabled and disabled. Currently this optimization
		// is disabled by default, but enabled in TestCellAliasVreplicationWorkflow.
		execVtgateQuery(t, vtgateConn, sourceKs, "insert into loadtest(id, name) values(10001, 'tempCustomer'), (10002, 'tempCustomer2'), (10003, 'tempCustomer3'), (10004, 'tempCustomer4')")
		execVtgateQuery(t, vtgateConn, sourceKs, "delete from loadtest where id > 10000")

		// Confirm that all partial query metrics get updated when we are testing the noblob mode.
		t.Run("validate partial query counts", func(t *testing.T) {
			if !isBinlogRowImageNoBlob(t, productTab) {
				return
			}

			// the two primaries of the new reshard targets
			tablet200 := custKs.Shards["-80"].Tablets["zone1-200"].Vttablet
			tablet300 := custKs.Shards["80-"].Tablets["zone1-300"].Vttablet

			totalInserts, totalUpdates, totalInsertQueries, totalUpdateQueries := 0, 0, 0, 0
			for _, tab := range []*cluster.VttabletProcess{tablet200, tablet300} {
				insertCount, updateCount, insertQueries, updateQueries := getPartialMetrics(t, "product.0.p2c.1", tab)
				totalInserts += insertCount
				totalUpdates += updateCount
				totalInsertQueries += insertQueries
				totalUpdateQueries += updateQueries
			}
			// Counts are total queries from `blobTableQueries` across shards + customer updates from above.
			require.NotZero(t, totalInserts)
			require.NotZero(t, totalUpdates)
			require.NotZero(t, totalInsertQueries)
			require.NotZero(t, totalUpdateQueries)
		})

		query := "select cid from customer"
		assertQueryExecutesOnTablet(t, vtgateConn, productTab, "product", query, query)
		insertQuery1 := "insert into customer(cid, name) values(1001, 'tempCustomer1')"
		matchInsertQuery1 := "insert into customer(cid, `name`) values (:vtg1 /* INT64 */, :vtg2 /* VARCHAR */)"
		assertQueryExecutesOnTablet(t, vtgateConn, productTab, "product", insertQuery1, matchInsertQuery1)

		// FIXME for some reason, these inserts fails on mac, need to investigate, some
		// vreplication bug because of case insensitiveness of table names on mac?
		if runtime.GOOS == "linux" {
			// Confirm that the backticking of table names in the routing rules works.
			tbls := []string{"Lead", "Lead-1"}
			for _, tbl := range tbls {
				output, err := osExec(t, "mysql", []string{"-u", "vtdba", "-P", fmt.Sprintf("%d", vc.ClusterConfig.vtgateMySQLPort),
					"--host=127.0.0.1", "--default-character-set=utf8mb4", "-e", fmt.Sprintf("select * from `%s`", tbl)})
				if err != nil {
					require.FailNow(t, output)
				}
				execVtgateQuery(t, vtgateConn, "product", fmt.Sprintf("update `%s` set name='xyz'", tbl))
			}
		}
		vdiffSideBySide(t, ksWorkflow, "")
		cellNames := getCellNames(cells)
		switchReadsDryRun(t, workflowType, cellNames, ksWorkflow, dryRunResultsReadCustomerShard)
		switchReads(t, workflowType, cellNames, ksWorkflow, false)
		assertQueryExecutesOnTablet(t, vtgateConn, productTab, "customer", query, query)

		var commit func(t *testing.T)
		if withOpenTx {
			commit, _ = vc.startQuery(t, openTxQuery)
		}
		switchWritesDryRun(t, workflowType, ksWorkflow, dryRunResultsSwitchWritesCustomerShard)
		switchWrites(t, workflowType, ksWorkflow, false)

		checkThatVDiffFails(t, targetKs, workflow)

		// The original unsharded customer data included an insert with the
		// vindex column (cid) of 999999, so the backing sequence table should
		// now have a next_id of 1000000 after SwitchTraffic.
		res := execVtgateQuery(t, vtgateConn, sourceKs, "select next_id from customer_seq where id = 0")
		require.Equal(t, "1000000", res.Rows[0][0].ToString())

		if withOpenTx && commit != nil {
			commit(t)
		}

		catchup(t, productTab, workflow, "MoveTables")

		vdiffSideBySide(t, "product.p2c_reverse", "")
		if withOpenTx {
			execVtgateQuery(t, vtgateConn, "", deleteOpenTxQuery)
		}

		ksShards := []string{"product/0", "customer/-80", "customer/80-"}
		printShardPositions(vc, ksShards)
		insertQuery2 := "insert into customer(name, cid) values('tempCustomer2', 100)"
		matchInsertQuery2 := "insert into customer(`name`, cid) values (:vtg1 /* VARCHAR */, :_cid_0)"
		assertQueryDoesNotExecutesOnTablet(t, vtgateConn, productTab, "customer", insertQuery2, matchInsertQuery2)

		insertQuery2 = "insert into customer(name, cid) values('tempCustomer3', 101)" // ID 101, hence due to reverse_bits in shard 80-
		assertQueryExecutesOnTablet(t, vtgateConn, customerTab2, "customer", insertQuery2, matchInsertQuery2)

		insertQuery2 = "insert into customer(name, cid) values('tempCustomer4', 102)" // ID 102, hence due to reverse_bits in shard -80
		assertQueryExecutesOnTablet(t, vtgateConn, customerTab1, "customer", insertQuery2, matchInsertQuery2)

		execVtgateQuery(t, vtgateConn, "customer", "update customer set meta = convert(x'7b7d' using utf8mb4) where cid = 1")
		if testReverse {
			// Reverse Replicate
			switchReads(t, workflowType, cellNames, ksWorkflow, true)
			printShardPositions(vc, ksShards)
			switchWrites(t, workflowType, ksWorkflow, true)

			output, err := vc.VtctlClient.ExecuteCommandWithOutput("Workflow", ksWorkflow, "show")
			require.NoError(t, err)
			require.Contains(t, output, "'customer.reverse_bits'")
			require.Contains(t, output, "'customer.bmd5'")

			insertQuery1 = "insert into customer(cid, name) values(1002, 'tempCustomer5')"
			assertQueryExecutesOnTablet(t, vtgateConn, productTab, "product", insertQuery1, matchInsertQuery1)
			// both inserts go into 80-, this tests the edge-case where a stream (-80) has no relevant new events after the previous switch
			insertQuery1 = "insert into customer(cid, name) values(1003, 'tempCustomer6')"
			assertQueryDoesNotExecutesOnTablet(t, vtgateConn, customerTab1, "customer", insertQuery1, matchInsertQuery1)
			insertQuery1 = "insert into customer(cid, name) values(1004, 'tempCustomer7')"
			assertQueryDoesNotExecutesOnTablet(t, vtgateConn, customerTab2, "customer", insertQuery1, matchInsertQuery1)

			waitForNoWorkflowLag(t, vc, targetKs, workflow)

			// Go forward again
			switchReads(t, workflowType, cellNames, ksWorkflow, false)
			switchWrites(t, workflowType, ksWorkflow, false)

			var exists bool
			exists, err = isTableInDenyList(t, vc, "product:0", "customer")
			require.NoError(t, err, "Error getting denylist for customer:0")
			require.True(t, exists)

			moveTablesAction(t, "Complete", cellNames, workflow, sourceKs, targetKs, tables)

			exists, err = isTableInDenyList(t, vc, "product:0", "customer")
			require.NoError(t, err, "Error getting denylist for customer:0")
			require.False(t, exists)

			for _, shard := range strings.Split("-80,80-", ",") {
				expectNumberOfStreams(t, vtgateConn, "shardCustomerTargetStreams", "p2c", "customer:"+shard, 0)
			}

			expectNumberOfStreams(t, vtgateConn, "shardCustomerReverseStreams", "p2c_reverse", "product:0", 0)

			var found bool
			found, err = checkIfTableExists(t, vc, "zone1-100", "customer")
			assert.NoError(t, err, "Customer table not deleted from zone1-100")
			require.False(t, found)

			found, err = checkIfTableExists(t, vc, "zone1-200", "customer")
			assert.NoError(t, err, "Customer table not deleted from zone1-200")
			require.True(t, found)

			insertQuery2 = "insert into customer(name, cid) values('tempCustomer8', 103)" // ID 103, hence due to reverse_bits in shard 80-
			assertQueryDoesNotExecutesOnTablet(t, vtgateConn, productTab, "customer", insertQuery2, matchInsertQuery2)
			insertQuery2 = "insert into customer(name, cid) values('tempCustomer10', 104)" // ID 105, hence due to reverse_bits in shard -80
			assertQueryExecutesOnTablet(t, vtgateConn, customerTab1, "customer", insertQuery2, matchInsertQuery2)
			insertQuery2 = "insert into customer(name, cid) values('tempCustomer9', 105)" // ID 104, hence due to reverse_bits in shard 80-
			assertQueryExecutesOnTablet(t, vtgateConn, customerTab2, "customer", insertQuery2, matchInsertQuery2)

			execVtgateQuery(t, vtgateConn, "customer", "delete from customer where name like 'tempCustomer%'")
			waitForRowCountInTablet(t, customerTab1, "customer", "customer", 1)
			waitForRowCountInTablet(t, customerTab2, "customer", "customer", 2)
			waitForRowCount(t, vtgateConn, "customer", "customer.customer", 3)

			query = "insert into customer (name, cid) values('george', 5)"
			execVtgateQuery(t, vtgateConn, "customer", query)
			waitForRowCountInTablet(t, customerTab1, "customer", "customer", 1)
			waitForRowCountInTablet(t, customerTab2, "customer", "customer", 3)
			waitForRowCount(t, vtgateConn, "customer", "customer.customer", 4)
		}
	})
}

func validateRollupReplicates(t *testing.T) {
	t.Run("validateRollupReplicates", func(t *testing.T) {
		insertMoreProducts(t)
		vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
		defer vtgateConn.Close()
		waitForRowCount(t, vtgateConn, "product", "rollup", 1)
		waitForQueryResult(t, vtgateConn, "product:0", "select rollupname, kount from rollup",
			`[[VARCHAR("total") INT32(5)]]`)
	})
}

func reshardCustomer2to4Split(t *testing.T, cells []*Cell, sourceCellOrAlias string) {
	t.Run("reshardCustomer2to4Split", func(t *testing.T) {
		vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
		defer vtgateConn.Close()
		ksName := "customer"
		counts := map[string]int{"zone1-600": 4, "zone1-700": 5, "zone1-800": 6, "zone1-900": 5}
		reshard(t, ksName, "customer", "c2c4", "-80,80-", "-40,40-80,80-c0,c0-",
			600, counts, nil, nil, cells, sourceCellOrAlias, 1)
		waitForRowCount(t, vtgateConn, ksName, "customer", 20)
		query := "insert into customer (name) values('yoko')"
		execVtgateQuery(t, vtgateConn, ksName, query)
		waitForRowCount(t, vtgateConn, ksName, "customer", 21)
	})
}

func reshardMerchant2to3SplitMerge(t *testing.T) {
	t.Run("reshardMerchant2to3SplitMerge", func(t *testing.T) {
		vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
		defer vtgateConn.Close()
		ksName := merchantKeyspace
		counts := map[string]int{"zone1-1600": 0, "zone1-1700": 2, "zone1-1800": 0}
		reshard(t, ksName, "merchant", "m2m3", "-80,80-", "-40,40-c0,c0-",
			1600, counts, dryRunResultsSwitchReadM2m3, dryRunResultsSwitchWritesM2m3, nil, "", 1)
		waitForRowCount(t, vtgateConn, ksName, "merchant", 2)
		query := "insert into merchant (mname, category) values('amazon', 'electronics')"
		execVtgateQuery(t, vtgateConn, ksName, query)
		waitForRowCount(t, vtgateConn, ksName, "merchant", 3)

		var output string
		var err error

		for _, shard := range strings.Split("-80,80-", ",") {
			output, err = vc.VtctlClient.ExecuteCommandWithOutput("GetShard", "merchant:"+shard)
			if err == nil {
				t.Fatal("GetShard merchant:-80 failed")
			}
			assert.Contains(t, output, "node doesn't exist", "GetShard succeeded for dropped shard merchant:"+shard)
		}

		for _, shard := range strings.Split("-40,40-c0,c0-", ",") {
			ksShard := fmt.Sprintf("%s:%s", merchantKeyspace, shard)
			output, err = vc.VtctlClient.ExecuteCommandWithOutput("GetShard", ksShard)
			if err != nil {
				t.Fatalf("GetShard merchant failed for: %s: %v", shard, err)
			}
			assert.NotContains(t, output, "node doesn't exist", "GetShard failed for valid shard "+ksShard)
			assert.Contains(t, output, "primary_alias", "GetShard failed for valid shard "+ksShard)
		}

		for _, shard := range strings.Split("-40,40-c0,c0-", ",") {
			ksShard := fmt.Sprintf("%s:%s", merchantKeyspace, shard)
			expectNumberOfStreams(t, vtgateConn, "reshardMerchant2to3SplitMerge", "m2m3", ksShard, 0)
		}

		var found bool
		found, err = checkIfTableExists(t, vc, "zone1-1600", "customer")
		assert.NoError(t, err, "Customer table found incorrectly in zone1-1600")
		require.False(t, found)
		found, err = checkIfTableExists(t, vc, "zone1-1600", "merchant")
		assert.NoError(t, err, "Merchant table not found in zone1-1600")
		require.True(t, found)
	})
}

func reshardMerchant3to1Merge(t *testing.T) {
	t.Run("reshardMerchant3to1Merge", func(t *testing.T) {
		vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
		defer vtgateConn.Close()
		ksName := merchantKeyspace
		counts := map[string]int{"zone1-2000": 3}
		reshard(t, ksName, "merchant", "m3m1", "-40,40-c0,c0-", "0",
			2000, counts, nil, nil, nil, "", 1)
		waitForRowCount(t, vtgateConn, ksName, "merchant", 3)
		query := "insert into merchant (mname, category) values('flipkart', 'electronics')"
		execVtgateQuery(t, vtgateConn, ksName, query)
		waitForRowCount(t, vtgateConn, ksName, "merchant", 4)
	})
}

func reshardCustomer3to2SplitMerge(t *testing.T) { // -40,40-80,80-c0 => merge/split, c0- stays the same  ending up with 3
	t.Run("reshardCustomer3to2SplitMerge", func(t *testing.T) {
		ksName := "customer"
		counts := map[string]int{"zone1-1000": 8, "zone1-1100": 8, "zone1-1200": 5}
		reshard(t, ksName, "customer", "c4c3", "-40,40-80,80-c0", "-60,60-c0",
			1000, counts, nil, nil, nil, "", 1)
	})
}

func reshardCustomer3to1Merge(t *testing.T) { // to unsharded
	t.Run("reshardCustomer3to1Merge", func(t *testing.T) {
		ksName := "customer"
		counts := map[string]int{"zone1-1500": 21}
		reshard(t, ksName, "customer", "c3c1", "-60,60-c0,c0-", "0",
			1500, counts, nil, nil, nil, "", 3)
	})
}

func reshard(t *testing.T, ksName string, tableName string, workflow string, sourceShards string, targetShards string,
	tabletIDBase int, counts map[string]int, dryRunResultSwitchReads, dryRunResultSwitchWrites []string, cells []*Cell, sourceCellOrAlias string,
	autoIncrementStep int) {
	t.Run("reshard", func(t *testing.T) {
		defaultCell := vc.Cells[vc.CellNames[0]]
		if cells == nil {
			cells = []*Cell{defaultCell}
		}
		if sourceCellOrAlias == "" {
			sourceCellOrAlias = defaultCell.Name
		}
		callNames := getCellNames(cells)
		ksWorkflow := ksName + "." + workflow
		keyspace := vc.Cells[defaultCell.Name].Keyspaces[ksName]
		require.NoError(t, vc.AddShards(t, cells, keyspace, targetShards, defaultReplicas, defaultRdonly, tabletIDBase, targetKsOpts))

		tablets := vc.getVttabletsInKeyspace(t, defaultCell, ksName, "primary")

		// Test multi-primary setups, like a Galera cluster, which have auto increment steps > 1.
		for _, tablet := range tablets {
			autoIncrementSetQuery := fmt.Sprintf("set @@session.auto_increment_increment = %d; set @@global.auto_increment_increment = %d",
				autoIncrementStep, autoIncrementStep)
			tablet.QueryTablet(autoIncrementSetQuery, "", false)
		}
		reshardAction(t, "Create", workflow, ksName, sourceShards, targetShards, sourceCellOrAlias, "replica,primary")

		targetShards = "," + targetShards + ","
		for _, tab := range tablets {
			if strings.Contains(targetShards, ","+tab.Shard+",") {
				log.Infof("Waiting for vrepl to catch up on %s since it IS a target shard", tab.Shard)
				catchup(t, tab, workflow, "Reshard")
			} else {
				log.Infof("Not waiting for vrepl to catch up on %s since it is NOT a target shard", tab.Shard)
				continue
			}
		}
		restartWorkflow(t, ksWorkflow)
		vdiffSideBySide(t, ksWorkflow, "")
		if dryRunResultSwitchReads != nil {
			reshardAction(t, "SwitchTraffic", workflow, ksName, "", "", callNames, "rdonly,replica", "--dry-run")
		}
		reshardAction(t, "SwitchTraffic", workflow, ksName, "", "", callNames, "rdonly,replica")
		if dryRunResultSwitchWrites != nil {
			reshardAction(t, "SwitchTraffic", workflow, ksName, "", "", callNames, "primary", "--dry-run")
		}
		reshardAction(t, "SwitchTraffic", workflow, ksName, "", "", callNames, "primary")
		reshardAction(t, "Complete", workflow, ksName, "", "", "", "")
		for tabletName, count := range counts {
			if tablets[tabletName] == nil {
				continue
			}
			waitForRowCountInTablet(t, tablets[tabletName], ksName, tableName, count)
		}
	})
}

func shardOrders(t *testing.T) {
	t.Run("shardOrders", func(t *testing.T) {
		vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
		defer vtgateConn.Close()
		defaultCell := vc.Cells[vc.CellNames[0]]
		workflow := "o2c"
		cell := defaultCell.Name
		sourceKs := "product"
		targetKs := "customer"
		tables := "orders"
		ksWorkflow := fmt.Sprintf("%s.%s", targetKs, workflow)
		applyVSchema(t, ordersVSchema, targetKs)
		moveTablesAction(t, "Create", cell, workflow, sourceKs, targetKs, tables)

		custKs := vc.Cells[defaultCell.Name].Keyspaces["customer"]
		customerTab1 := custKs.Shards["-80"].Tablets["zone1-200"].Vttablet
		customerTab2 := custKs.Shards["80-"].Tablets["zone1-300"].Vttablet
		workflowType := "MoveTables"
		catchup(t, customerTab1, workflow, workflowType)
		catchup(t, customerTab2, workflow, workflowType)
		vdiffSideBySide(t, ksWorkflow, "")
		switchReads(t, workflowType, strings.Join(vc.CellNames, ","), ksWorkflow, false)
		switchWrites(t, workflowType, ksWorkflow, false)
		moveTablesAction(t, "Complete", cell, workflow, sourceKs, targetKs, tables)
		waitForRowCountInTablet(t, customerTab1, "customer", "orders", 1)
		waitForRowCountInTablet(t, customerTab2, "customer", "orders", 2)
		waitForRowCount(t, vtgateConn, "customer", "orders", 3)
	})
}

func checkThatVDiffFails(t *testing.T, keyspace, workflow string) {
	ksWorkflow := fmt.Sprintf("%s.%s", keyspace, workflow)
	t.Run("check that vdiffSideBySide won't run", func(t2 *testing.T) {
		output, err := vc.VtctlClient.ExecuteCommandWithOutput("VDiff", "--", "--v1", ksWorkflow)
		require.Error(t, err)
		require.Contains(t, output, "invalid VDiff run")
	})
	t.Run("check that vdiff2 won't run", func(t2 *testing.T) {
		output, err := vc.VtctlClient.ExecuteCommandWithOutput("VDiff", "--", ksWorkflow)
		require.Error(t, err)
		require.Contains(t, output, "invalid VDiff run")

	})
}

func shardMerchant(t *testing.T) {
	t.Run("shardMerchant", func(t *testing.T) {
		vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
		defer vtgateConn.Close()
		workflow := "p2m"
		defaultCell := vc.Cells[vc.CellNames[0]]
		cell := defaultCell.Name
		sourceKs := "product"
		targetKs := merchantKeyspace
		tables := "merchant"
		ksWorkflow := fmt.Sprintf("%s.%s", targetKs, workflow)
		if _, err := vc.AddKeyspace(t, []*Cell{defaultCell}, merchantKeyspace, "-80,80-", merchantVSchema, "", defaultReplicas, defaultRdonly, 400, targetKsOpts); err != nil {
			t.Fatal(err)
		}
		moveTablesAction(t, "Create", cell, workflow, sourceKs, targetKs, tables)
		merchantKs := vc.Cells[defaultCell.Name].Keyspaces[merchantKeyspace]
		merchantTab1 := merchantKs.Shards["-80"].Tablets["zone1-400"].Vttablet
		merchantTab2 := merchantKs.Shards["80-"].Tablets["zone1-500"].Vttablet
		workflowType := "MoveTables"
		catchup(t, merchantTab1, workflow, workflowType)
		catchup(t, merchantTab2, workflow, workflowType)

		vdiffSideBySide(t, fmt.Sprintf("%s.%s", merchantKeyspace, workflow), "")
		switchReads(t, workflowType, strings.Join(vc.CellNames, ","), ksWorkflow, false)
		switchWrites(t, workflowType, ksWorkflow, false)
		printRoutingRules(t, vc, "After merchant movetables")

		// confirm that the backticking of keyspaces in the routing rules works
		output, err := osExec(t, "mysql", []string{"-u", "vtdba", "-P", fmt.Sprintf("%d", vc.ClusterConfig.vtgateMySQLPort),
			fmt.Sprintf("--host=%s", vc.ClusterConfig.hostname), "--default-character-set=utf8mb4", "-e", "select * from merchant"})
		if err != nil {
			require.FailNow(t, output)
		}
		moveTablesAction(t, "Complete", cell, workflow, sourceKs, targetKs, tables)

		waitForRowCountInTablet(t, merchantTab1, merchantKeyspace, "merchant", 1)
		waitForRowCountInTablet(t, merchantTab2, merchantKeyspace, "merchant", 1)
		waitForRowCount(t, vtgateConn, merchantKeyspace, "merchant", 2)
	})
}

func materialize(t *testing.T, spec string, useVtctldClient bool) {
	if useVtctldClient {
		t.Run("vtctldclient materialize", func(t *testing.T) {
			// Split out the parameters from the JSON spec for
			// use in the vtctldclient command flags.
			// This allows us to test both clients with the same
			// input.
			sj := gjson.Parse(spec)
			workflow := sj.Get("workflow").String()
			require.NotEmpty(t, workflow, "workflow not found in spec: %s", spec)
			sourceKeyspace := sj.Get("source_keyspace").String()
			require.NotEmpty(t, sourceKeyspace, "source_keyspace not found in spec: %s", spec)
			targetKeyspace := sj.Get("target_keyspace").String()
			require.NotEmpty(t, targetKeyspace, "target_keyspace not found in spec: %s", spec)
			tableSettings := sj.Get("table_settings").String()
			require.NotEmpty(t, tableSettings, "table_settings not found in spec: %s", spec)
			stopAfterCopy := sj.Get("stop-after-copy").Bool() // Optional
			err := vc.VtctldClient.ExecuteCommand("materialize", "--workflow", workflow, "--target-keyspace", targetKeyspace,
				"create", "--source-keyspace", sourceKeyspace, "--table-settings", tableSettings,
				fmt.Sprintf("--stop-after-copy=%t", stopAfterCopy))
			require.NoError(t, err, "Materialize")
		})
	} else {
		t.Run("materialize", func(t *testing.T) {
			err := vc.VtctlClient.ExecuteCommand("Materialize", spec)
			require.NoError(t, err, "Materialize")
		})
	}
}

func materializeProduct(t *testing.T, useVtctldClient bool) {
	t.Run("materializeProduct", func(t *testing.T) {
		// Materializing from "product" keyspace to "customer" keyspace.
		workflow := "cproduct"
		keyspace := "customer"
		defaultCell := vc.Cells[vc.CellNames[0]]
		applyVSchema(t, materializeProductVSchema, keyspace)
		materialize(t, materializeProductSpec, useVtctldClient)
		customerTablets := vc.getVttabletsInKeyspace(t, defaultCell, keyspace, "primary")
		for _, tab := range customerTablets {
			catchup(t, tab, workflow, "Materialize")
			waitForRowCountInTablet(t, tab, keyspace, workflow, 5)
		}

		productTablets := vc.getVttabletsInKeyspace(t, defaultCell, "product", "primary")
		t.Run("throttle-app-product", func(t *testing.T) {
			// Now, throttle the source side component (vstreamer), and insert some rows.
			for _, tab := range productTablets {
				body, err := throttleApp(tab, sourceThrottlerAppName)
				assert.NoError(t, err)
				assert.Contains(t, body, sourceThrottlerAppName)

				// Wait for throttling to take effect (caching will expire by this time):
				waitForTabletThrottlingStatus(t, tab, sourceThrottlerAppName, throttlerStatusThrottled)
				waitForTabletThrottlingStatus(t, tab, targetThrottlerAppName, throttlerStatusNotThrottled)
			}
			insertMoreProductsForSourceThrottler(t)
			// To be fair to the test, we give the target time to apply the new changes. We
			// expect it to NOT get them in the first place, we expect the additional rows
			// to **not appear** in the materialized view.
			for _, tab := range customerTablets {
				waitForRowCountInTablet(t, tab, keyspace, workflow, 5)
				// Confirm that we updated the stats on the target tablets as expected.
				jsVal, err := getDebugVar(t, tab.Port, []string{"VReplicationThrottledCounts"})
				require.NoError(t, err)
				require.NotEqual(t, "{}", jsVal)
				// The JSON value looks like this: {"cproduct.4.tablet.vstreamer": 2}
				vstreamerThrottledCount := gjson.Get(jsVal, fmt.Sprintf(`%s\.*\.tablet\.vstreamer`, workflow)).Int()
				require.Greater(t, vstreamerThrottledCount, int64(0))
				// We only need to do this stat check once.
				val, err := getDebugVar(t, tab.Port, []string{"VReplicationThrottledCountTotal"})
				require.NoError(t, err)
				throttledCount, err := strconv.ParseInt(val, 10, 64)
				require.NoError(t, err)
				require.GreaterOrEqual(t, throttledCount, vstreamerThrottledCount)
			}
		})
		t.Run("unthrottle-app-product", func(t *testing.T) {
			// Unthrottle the vstreamer component, and expect the rows to show up.
			for _, tab := range productTablets {
				body, err := unthrottleApp(tab, sourceThrottlerAppName)
				assert.NoError(t, err)
				assert.Contains(t, body, sourceThrottlerAppName)
				// Give time for unthrottling to take effect and for targets to fetch data.
				waitForTabletThrottlingStatus(t, tab, sourceThrottlerAppName, throttlerStatusNotThrottled)
			}
			for _, tab := range customerTablets {
				waitForRowCountInTablet(t, tab, keyspace, workflow, 8)
			}
		})

		t.Run("throttle-app-customer", func(t *testing.T) {
			// Now, throttle vreplication on the target side (vplayer), and insert some
			// more rows.
			for _, tab := range customerTablets {
				body, err := throttleApp(tab, targetThrottlerAppName)
				assert.NoError(t, err)
				assert.Contains(t, body, targetThrottlerAppName)
				// Wait for throttling to take effect (caching will expire by this time):
				waitForTabletThrottlingStatus(t, tab, targetThrottlerAppName, throttlerStatusThrottled)
				waitForTabletThrottlingStatus(t, tab, sourceThrottlerAppName, throttlerStatusNotThrottled)
			}
			insertMoreProductsForTargetThrottler(t)
			// To be fair to the test, we give the target time to apply the new changes.
			// We expect it to NOT get them in the first place, we expect the additional
			// rows to **not appear** in the materialized view.
			for _, tab := range customerTablets {
				waitForRowCountInTablet(t, tab, keyspace, workflow, 8)
				// Confirm that we updated the stats on the target tablets as expected.
				jsVal, err := getDebugVar(t, tab.Port, []string{"VReplicationThrottledCounts"})
				require.NoError(t, err)
				require.NotEqual(t, "{}", jsVal)
				// The JSON value now looks like this: {"cproduct.4.tablet.vstreamer": 2, "cproduct.4.tablet.vplayer": 4}
				vplayerThrottledCount := gjson.Get(jsVal, fmt.Sprintf(`%s\.*\.tablet\.vplayer`, workflow)).Int()
				require.Greater(t, vplayerThrottledCount, int64(0))
			}
		})
		t.Run("unthrottle-app-customer", func(t *testing.T) {
			// unthrottle on target tablets, and expect the rows to show up
			for _, tab := range customerTablets {
				body, err := unthrottleApp(tab, targetThrottlerAppName)
				assert.NoError(t, err)
				assert.Contains(t, body, targetThrottlerAppName)
			}
			// give time for unthrottling to take effect and for target to fetch data
			for _, tab := range customerTablets {
				waitForTabletThrottlingStatus(t, tab, targetThrottlerAppName, throttlerStatusNotThrottled)
				waitForRowCountInTablet(t, tab, keyspace, workflow, 11)
			}
		})
	})
}

func materializeRollup(t *testing.T, useVtctldClient bool) {
	t.Run("materializeRollup", func(t *testing.T) {
		vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
		defer vtgateConn.Close()
		keyspace := "product"
		workflow := "rollup"
		applyVSchema(t, materializeSalesVSchema, keyspace)
		defaultCell := vc.Cells[vc.CellNames[0]]
		productTab := vc.Cells[defaultCell.Name].Keyspaces["product"].Shards["0"].Tablets["zone1-100"].Vttablet
		materialize(t, materializeRollupSpec, useVtctldClient)
		catchup(t, productTab, workflow, "Materialize")
		waitForRowCount(t, vtgateConn, "product", "rollup", 1)
		waitForQueryResult(t, vtgateConn, "product:0", "select rollupname, kount from rollup",
			`[[VARCHAR("total") INT32(2)]]`)
	})
}

func materializeSales(t *testing.T, useVtctldClient bool) {
	t.Run("materializeSales", func(t *testing.T) {
		vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
		defer vtgateConn.Close()
		keyspace := "product"
		applyVSchema(t, materializeSalesVSchema, keyspace)
		materialize(t, materializeSalesSpec, useVtctldClient)
		defaultCell := vc.Cells[vc.CellNames[0]]
		productTab := vc.Cells[defaultCell.Name].Keyspaces["product"].Shards["0"].Tablets["zone1-100"].Vttablet
		catchup(t, productTab, "sales", "Materialize")
		waitForRowCount(t, vtgateConn, "product", "sales", 2)
		waitForQueryResult(t, vtgateConn, "product:0", "select kount, amount from sales",
			`[[INT32(1) INT32(10)] [INT32(2) INT32(35)]]`)
	})
}

func materializeMerchantSales(t *testing.T, useVtctldClient bool) {
	t.Run("materializeMerchantSales", func(t *testing.T) {
		vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
		defer vtgateConn.Close()
		workflow := "msales"
		materialize(t, materializeMerchantSalesSpec, useVtctldClient)
		defaultCell := vc.Cells[vc.CellNames[0]]
		merchantTablets := vc.getVttabletsInKeyspace(t, defaultCell, merchantKeyspace, "primary")
		for _, tab := range merchantTablets {
			catchup(t, tab, workflow, "Materialize")
		}
		waitForRowCountInTablet(t, merchantTablets["zone1-400"], merchantKeyspace, "msales", 1)
		waitForRowCountInTablet(t, merchantTablets["zone1-500"], merchantKeyspace, "msales", 1)
		waitForRowCount(t, vtgateConn, merchantKeyspace, "msales", 2)
	})
}

func materializeMerchantOrders(t *testing.T, useVtctldClient bool) {
	t.Run("materializeMerchantOrders", func(t *testing.T) {
		vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
		defer vtgateConn.Close()
		workflow := "morders"
		keyspace := merchantKeyspace
		applyVSchema(t, merchantOrdersVSchema, keyspace)
		materialize(t, materializeMerchantOrdersSpec, useVtctldClient)
		defaultCell := vc.Cells[vc.CellNames[0]]
		merchantTablets := vc.getVttabletsInKeyspace(t, defaultCell, merchantKeyspace, "primary")
		for _, tab := range merchantTablets {
			catchup(t, tab, workflow, "Materialize")
		}
		waitForRowCountInTablet(t, merchantTablets["zone1-400"], merchantKeyspace, "morders", 2)
		waitForRowCountInTablet(t, merchantTablets["zone1-500"], merchantKeyspace, "morders", 1)
		waitForRowCount(t, vtgateConn, merchantKeyspace, "morders", 3)
	})
}

func checkVtgateHealth(t *testing.T, cell *Cell) {
	for _, vtgate := range cell.Vtgates {
		vtgateHealthURL := strings.Replace(vtgate.VerifyURL, "vars", "health", -1)
		if !checkHealth(t, vtgateHealthURL) {
			assert.Fail(t, "Vtgate not healthy: ", vtgateHealthURL)
		}
	}
}

func checkTabletHealth(t *testing.T, tablet *Tablet) {
	vttabletHealthURL := strings.Replace(tablet.Vttablet.VerifyURL, "debug/vars", "healthz", -1)
	if !checkHealth(t, vttabletHealthURL) {
		assert.Fail(t, "Vttablet not healthy: ", vttabletHealthURL)
	}
}

func iterateTablets(t *testing.T, cluster *VitessCluster, f func(t *testing.T, tablet *Tablet)) {
	for _, cell := range cluster.Cells {
		for _, ks := range cell.Keyspaces {
			for _, shard := range ks.Shards {
				for _, tablet := range shard.Tablets {
					f(t, tablet)
				}
			}
		}
	}
}

func iterateCells(t *testing.T, cluster *VitessCluster, f func(t *testing.T, cell *Cell)) {
	for _, cell := range cluster.Cells {
		f(t, cell)
	}
}

func verifyClusterHealth(t *testing.T, cluster *VitessCluster) {
	iterateCells(t, cluster, checkVtgateHealth)
	iterateTablets(t, cluster, checkTabletHealth)
}

const acceptableLagSeconds = 5

func waitForLowLag(t *testing.T, keyspace, workflow string) {
	if BypassLagCheck {
		return
	}
	var lagSeconds int64
	waitDuration := 500 * time.Millisecond
	duration := maxWait
	for duration > 0 {
		output, err := vc.VtctlClient.ExecuteCommandWithOutput("Workflow", fmt.Sprintf("%s.%s", keyspace, workflow), "Show")
		require.NoError(t, err)
		lagSeconds, err = jsonparser.GetInt([]byte(output), "MaxVReplicationTransactionLag")

		require.NoError(t, err)
		if lagSeconds <= acceptableLagSeconds {
			log.Infof("waitForLowLag acceptable for workflow %s, keyspace %s, current lag is %d", workflow, keyspace, lagSeconds)
			break
		} else {
			log.Infof("waitForLowLag too high for workflow %s, keyspace %s, current lag is %d", workflow, keyspace, lagSeconds)
		}
		time.Sleep(waitDuration)
		duration -= waitDuration
	}

	if duration <= 0 {
		t.Fatalf("waitForLowLag timed out for workflow %s, keyspace %s, current lag is %d", workflow, keyspace, lagSeconds)
	}
}

func catchup(t *testing.T, vttablet *cluster.VttabletProcess, workflow, info string) {
	vttablet.WaitForVReplicationToCatchup(t, workflow, fmt.Sprintf("vt_%s", vttablet.Keyspace), sidecarDBName, maxWait)
}

func moveTablesAction(t *testing.T, action, cell, workflow, sourceKs, targetKs, tables string, extraFlags ...string) {
	var err error
	args := []string{"MoveTables", "--workflow=" + workflow, "--target-keyspace=" + targetKs, action}
	switch strings.ToLower(action) {
	case strings.ToLower(workflowActionCreate):
		extraFlags = append(extraFlags, "--source-keyspace="+sourceKs, "--tables="+tables, "--cells="+cell, "--tablet-types=primary,replica,rdonly")
	case strings.ToLower(workflowActionSwitchTraffic):
		extraFlags = append(extraFlags, "--initialize-target-sequences")
	}
	args = append(args, extraFlags...)
	output, err := vc.VtctldClient.ExecuteCommandWithOutput(args...)
	if output != "" {
		fmt.Printf("Output of vtctldclient MoveTables %s for %s workflow:\n++++++\n%s\n--------\n",
			action, workflow, output)
	}
	if err != nil {
		t.Fatalf("MoveTables %s command failed with %+v\n", action, err)
	}
}
func moveTablesActionWithTabletTypes(t *testing.T, action, cell, workflow, sourceKs, targetKs, tables string, tabletTypes string, ignoreErrors bool) {
	if err := vc.VtctldClient.ExecuteCommand("MoveTables", "--workflow="+workflow, "--target-keyspace="+targetKs, action,
		"--source-keyspace="+sourceKs, "--tables="+tables, "--cells="+cell, "--tablet-types="+tabletTypes); err != nil {
		if !ignoreErrors {
			t.Fatalf("MoveTables %s command failed with %+v\n", action, err)
		}
	}
}

// reshardAction is a helper function to run the reshard command and
// action using vtctldclient.
func reshardAction(t *testing.T, action, workflow, keyspaceName, sourceShards, targetShards, cell, tabletTypes string, extraFlags ...string) {
	var err error
	args := []string{"Reshard", "--workflow=" + workflow, "--target-keyspace=" + keyspaceName, action}

	switch strings.ToLower(action) {
	case strings.ToLower(workflowActionCreate):
		if tabletTypes == "" {
			tabletTypes = "replica,rdonly,primary"
		}
		args = append(args, "--source-shards="+sourceShards, "--target-shards="+targetShards)
	}
	if cell != "" {
		args = append(args, "--cells="+cell)
	}
	if tabletTypes != "" {
		args = append(args, "--tablet-types="+tabletTypes)
	}
	args = append(args, extraFlags...)
	output, err := vc.VtctldClient.ExecuteCommandWithOutput(args...)
	if output != "" {
		log.Infof("Output of vtctldclient Reshard %s for %s workflow:\n++++++\n%s\n--------\n",
			action, workflow, output)
	}
	if err != nil {
		t.Fatalf("Reshard %s command failed with %+v\nOutput: %s", action, err, output)
	}
}

func applyVSchema(t *testing.T, vschema, keyspace string) {
	err := vc.VtctlClient.ExecuteCommand("ApplyVSchema", "--", "--vschema", vschema, keyspace)
	require.NoError(t, err)
}

func switchReadsDryRun(t *testing.T, workflowType, cells, ksWorkflow string, dryRunResults []string) {
	if workflowType != binlogdatapb.VReplicationWorkflowType_name[int32(binlogdatapb.VReplicationWorkflowType_MoveTables)] &&
		workflowType != binlogdatapb.VReplicationWorkflowType_name[int32(binlogdatapb.VReplicationWorkflowType_Reshard)] {
		require.FailNowf(t, "Invalid workflow type for SwitchTraffic, must be MoveTables or Reshard",
			"workflow type specified: %s", workflowType)
	}
	ensureCanSwitch(t, workflowType, cells, ksWorkflow)
	output, err := vc.VtctlClient.ExecuteCommandWithOutput(workflowType, "--", "--cells="+cells, "--tablet_types=rdonly,replica",
		"--dry_run", "SwitchTraffic", ksWorkflow)
	require.NoError(t, err, fmt.Sprintf("Switching Reads DryRun Error: %s: %s", err, output))
	if dryRunResults != nil {
		validateDryRunResults(t, output, dryRunResults)
	}
}

func ensureCanSwitch(t *testing.T, workflowType, cells, ksWorkflow string) {
	timer := time.NewTimer(defaultTimeout)
	defer timer.Stop()
	for {
		_, err := vc.VtctlClient.ExecuteCommandWithOutput(workflowType, "--", "--cells="+cells, "--dry_run", "SwitchTraffic", ksWorkflow)
		if err == nil {
			return
		}
		select {
		case <-timer.C:
			t.Fatalf("Did not become ready to switch traffic for %s before the timeout of %s", ksWorkflow, defaultTimeout)
		default:
			time.Sleep(defaultTick)
		}
	}
}

func switchReads(t *testing.T, workflowType, cells, ksWorkflow string, reverse bool) {
	if workflowType != binlogdatapb.VReplicationWorkflowType_MoveTables.String() &&
		workflowType != binlogdatapb.VReplicationWorkflowType_Reshard.String() {
		require.FailNowf(t, "Invalid workflow type for SwitchTraffic, must be MoveTables or Reshard",
			"workflow type specified: %s", workflowType)
	}
	var output string
	var err error
	command := "SwitchTraffic"
	if reverse {
		command = "ReverseTraffic"
	}
	ensureCanSwitch(t, workflowType, cells, ksWorkflow)
	output, err = vc.VtctlClient.ExecuteCommandWithOutput(workflowType, "--", "--cells="+cells, "--tablet_types=rdonly",
		command, ksWorkflow)
	require.NoError(t, err, fmt.Sprintf("%s Error: %s: %s", command, err, output))
	output, err = vc.VtctlClient.ExecuteCommandWithOutput(workflowType, "--", "--cells="+cells, "--tablet_types=replica",
		command, ksWorkflow)
	require.NoError(t, err, fmt.Sprintf("%s Error: %s: %s", command, err, output))
}

func switchWrites(t *testing.T, workflowType, ksWorkflow string, reverse bool) {
	if workflowType != binlogdatapb.VReplicationWorkflowType_MoveTables.String() &&
		workflowType != binlogdatapb.VReplicationWorkflowType_Reshard.String() {
		require.FailNowf(t, "Invalid workflow type for SwitchTraffic, must be MoveTables or Reshard",
			"workflow type specified: %s", workflowType)
	}
	command := "SwitchTraffic"
	if reverse {
		command = "ReverseTraffic"
	}
	const SwitchWritesTimeout = "91s" // max: 3 tablet picker 30s waits + 1
	ensureCanSwitch(t, workflowType, "", ksWorkflow)
	// Use vtctldclient for MoveTables SwitchTraffic ~ 50% of the time.
	if workflowType == binlogdatapb.VReplicationWorkflowType_MoveTables.String() && time.Now().Second()%2 == 0 {
		parts := strings.Split(ksWorkflow, ".")
		require.Equal(t, 2, len(parts))
		moveTablesAction(t, command, defaultCellName, parts[1], sourceKs, parts[0], "", "--timeout="+SwitchWritesTimeout, "--tablet-types=primary")
		return
	}
	output, err := vc.VtctlClient.ExecuteCommandWithOutput(workflowType, "--", "--tablet_types=primary",
		"--timeout="+SwitchWritesTimeout, "--initialize-target-sequences", command, ksWorkflow)
	if output != "" {
		fmt.Printf("Output of switching writes with vtctlclient for %s:\n++++++\n%s\n--------\n", ksWorkflow, output)
	}
	// printSwitchWritesExtraDebug is useful when debugging failures in Switch writes due to corner cases/races
	_ = printSwitchWritesExtraDebug
	require.NoError(t, err, fmt.Sprintf("Switch writes Error: %s: %s", err, output))
}

func switchWritesDryRun(t *testing.T, workflowType, ksWorkflow string, dryRunResults []string) {
	if workflowType != binlogdatapb.VReplicationWorkflowType_name[int32(binlogdatapb.VReplicationWorkflowType_MoveTables)] &&
		workflowType != binlogdatapb.VReplicationWorkflowType_name[int32(binlogdatapb.VReplicationWorkflowType_Reshard)] {
		require.FailNowf(t, "Invalid workflow type for SwitchTraffic, must be MoveTables or Reshard",
			"workflow type specified: %s", workflowType)
	}
	output, err := vc.VtctlClient.ExecuteCommandWithOutput(workflowType, "--", "--tablet_types=primary", "--dry_run",
		"SwitchTraffic", ksWorkflow)
	require.NoError(t, err, fmt.Sprintf("Switch writes DryRun Error: %s: %s", err, output))
	validateDryRunResults(t, output, dryRunResults)
}

// restartWorkflow confirms that a workflow can be successfully
// stopped and started.
func restartWorkflow(t *testing.T, ksWorkflow string) {
	keyspace, workflow, found := strings.Cut(ksWorkflow, ".")
	require.True(t, found, "unexpected ksWorkflow value: %s", ksWorkflow)
	err := vc.VtctldClient.ExecuteCommand("workflow", "--keyspace", keyspace, "stop", "--workflow", workflow)
	require.NoError(t, err, "failed to stop workflow: %v", err)
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Stopped.String())
	err = vc.VtctldClient.ExecuteCommand("workflow", "--keyspace", keyspace, "start", "--workflow", workflow)
	require.NoError(t, err, "failed to start workflow: %v", err)
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())
}

func printSwitchWritesExtraDebug(t *testing.T, ksWorkflow, msg string) {
	// Temporary code: print lots of info for debugging occasional flaky failures in customer reshard in CI for multicell test
	debug := true
	if debug {
		log.Infof("------------------- START Extra debug info %s Switch writes %s", msg, ksWorkflow)
		ksShards := []string{"product/0", "customer/-80", "customer/80-"}
		printShardPositions(vc, ksShards)
		defaultCell := vc.Cells[vc.CellNames[0]]
		custKs := vc.Cells[defaultCell.Name].Keyspaces["customer"]
		customerTab1 := custKs.Shards["-80"].Tablets["zone1-200"].Vttablet
		customerTab2 := custKs.Shards["80-"].Tablets["zone1-300"].Vttablet
		productKs := vc.Cells[defaultCell.Name].Keyspaces["product"]
		productTab := productKs.Shards["0"].Tablets["zone1-100"].Vttablet
		tabs := []*cluster.VttabletProcess{productTab, customerTab1, customerTab2}
		queries := []string{
			sqlparser.BuildParsedQuery("select  id, workflow, pos, stop_pos, cell, tablet_types, time_updated, transaction_timestamp, state, message from %s.vreplication", sidecarDBIdentifier).Query,
			sqlparser.BuildParsedQuery("select * from %s.copy_state", sidecarDBIdentifier).Query,
			sqlparser.BuildParsedQuery("select * from %s.resharding_journal", sidecarDBIdentifier).Query,
		}
		for _, tab := range tabs {
			for _, query := range queries {
				qr, err := tab.QueryTablet(query, "", false)
				require.NoError(t, err)
				log.Infof("\nTablet:%s.%s.%s.%d\nQuery: %s\n%+v\n",
					tab.Cell, tab.Keyspace, tab.Shard, tab.TabletUID, query, qr.Rows)
			}
		}
		log.Infof("------------------- END Extra debug info %s SwitchWrites %s", msg, ksWorkflow)
	}
}

// generateInnoDBRowHistory generates at least maxSourceTrxHistory rollback segment entries.
// This allows us to confirm two behaviors:
//  1. MoveTables blocks on starting its first copy phase until we rollback
//  2. All other workflows continue to work w/o issue with this MVCC history in place (not used yet)
//
// Returns a db connection used for the transaction which you can use for follow-up
// work, such as rolling it back directly or using the releaseInnoDBRowHistory call.
func generateInnoDBRowHistory(t *testing.T, sourceKS string, neededTrxHistory int64) *mysql.Conn {
	dbConn1 := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	dbConn2 := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	execQuery(t, dbConn1, "use "+sourceKS)
	execQuery(t, dbConn2, "use "+sourceKS)
	offset := int64(1000)
	limit := int64(neededTrxHistory * 100)
	insertStmt := strings.Builder{}
	for i := offset; i <= offset+limit; i++ {
		if i == offset {
			insertStmt.WriteString(fmt.Sprintf("insert into product (pid, description) values (%d, 'test')", i))
		} else {
			insertStmt.WriteString(fmt.Sprintf(", (%d, 'test')", i))
		}
	}
	execQuery(t, dbConn2, "start transaction")
	execQuery(t, dbConn2, "select count(*) from product")
	execQuery(t, dbConn1, insertStmt.String())
	execQuery(t, dbConn2, "rollback")
	execQuery(t, dbConn2, "start transaction")
	execQuery(t, dbConn2, "select count(*) from product")
	execQuery(t, dbConn1, fmt.Sprintf("delete from product where pid >= %d and pid <= %d", offset, offset+limit))
	return dbConn2
}

// waitForInnoDBRowHistory waits for the history list length to be greater than the
// expected length.
func waitForInnoDBHistoryLength(t *testing.T, tablet *cluster.VttabletProcess, expectedLength int64) {
	timer := time.NewTimer(defaultTimeout)
	defer timer.Stop()
	historyLen := int64(0)
	for {
		res, err := tablet.QueryTablet(historyLenQuery, tablet.Keyspace, false)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, 1, len(res.Rows))
		historyLen, err = res.Rows[0][0].ToInt64()
		require.NoError(t, err)
		if historyLen > expectedLength {
			return
		}
		select {
		case <-timer.C:
			t.Fatalf("Did not reach the expected InnoDB history length of %d before the timeout of %s; last seen value: %d", expectedLength, defaultTimeout, historyLen)
		default:
			time.Sleep(defaultTick)
		}
	}
}

func releaseInnoDBRowHistory(t *testing.T, dbConn *mysql.Conn) {
	execQuery(t, dbConn, "rollback")
}
