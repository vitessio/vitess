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
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/buger/jsonparser"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	throttlebase "vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer"
)

var (
	vc                     *VitessCluster
	vtgate                 *cluster.VtgateProcess
	defaultCell            *Cell
	vtgateConn             *mysql.Conn
	defaultRdonly          int
	defaultReplicas        int
	allCellNames           string
	sourceKsOpts           = make(map[string]string)
	targetKsOpts           = make(map[string]string)
	httpClient             = throttlebase.SetupHTTPClient(time.Second)
	sourceThrottlerAppName = "vstreamer"
	targetThrottlerAppName = "vreplication"
)

const (
	// for some tests we keep an open transaction during a SwitchWrites and commit it afterwards, to reproduce https://github.com/vitessio/vitess/issues/9400
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

func throttleResponse(tablet *cluster.VttabletProcess, path string) (resp *http.Response, respBody string, err error) {
	apiURL := fmt.Sprintf("http://%s:%d/%s", tablet.TabletHostname, tablet.Port, path)
	resp, err = httpClient.Get(apiURL)
	if err != nil {
		return resp, respBody, err
	}
	b, err := io.ReadAll(resp.Body)
	respBody = string(b)
	return resp, respBody, err
}

func throttleApp(tablet *cluster.VttabletProcess, app string) (*http.Response, string, error) {
	return throttleResponse(tablet, fmt.Sprintf("throttler/throttle-app?app=%s&duration=1h", app))
}

func unthrottleApp(tablet *cluster.VttabletProcess, app string) (*http.Response, string, error) {
	return throttleResponse(tablet, fmt.Sprintf("throttler/unthrottle-app?app=%s", app))
}

func throttlerCheckSelf(tablet *cluster.VttabletProcess, app string) (resp *http.Response, respBody string, err error) {
	apiURL := fmt.Sprintf("http://%s:%d/throttler/check-self?app=%s", tablet.TabletHostname, tablet.Port, app)
	resp, err = httpClient.Get(apiURL)
	if err != nil {
		return resp, respBody, err
	}
	b, err := io.ReadAll(resp.Body)
	respBody = string(b)
	return resp, respBody, err
}

func TestVreplicationCopyThrottling(t *testing.T) {
	workflow := "copy-throttling"
	cell := "zone1"
	table := "customer"
	shard := "0"
	vc = NewVitessCluster(t, "TestVreplicationCopyThrottling", []string{cell}, mainClusterConfig)
	defer vc.TearDown(t)
	defaultCell = vc.Cells[cell]
	// To test vstreamer source throttling for the MoveTables operation
	maxSourceTrxHistory := int64(5)
	extraVTTabletArgs = []string{
		// We rely on holding open transactions to generate innodb history so extend the timeout
		// to avoid flakiness when the CI is very slow.
		fmt.Sprintf("--queryserver-config-transaction-timeout=%d", int64(defaultTimeout.Seconds())*3),
		fmt.Sprintf("--vreplication_copy_phase_max_innodb_history_list_length=%d", maxSourceTrxHistory),
	}

	if _, err := vc.AddKeyspace(t, []*Cell{defaultCell}, sourceKs, shard, initialProductVSchema, initialProductSchema, 0, 0, 100, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := vc.AddKeyspace(t, []*Cell{defaultCell}, targetKs, shard, "", "", 0, 0, 200, nil); err != nil {
		t.Fatal(err)
	}
	vtgate = defaultCell.Vtgates[0]
	require.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", sourceKs, shard), 1)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", targetKs, shard), 1)

	// Confirm that the initial copy table phase does not proceed until the source tablet(s)
	// have an InnoDB History List length that is less than specified in the tablet's config.
	// We update rows in a table not part of the MoveTables operation so that we're not blocking
	// on the LOCK TABLE call but rather the InnoDB History List length.
	trxConn := generateInnoDBRowHistory(t, sourceKs, maxSourceTrxHistory)
	// History should have been generated on the source primary tablet
	waitForInnoDBHistoryLength(t, vc.getPrimaryTablet(t, sourceKs, shard), maxSourceTrxHistory)
	// We need to force primary tablet types as the history list has been increased on the source primary
	moveTablesWithTabletTypes(t, defaultCell.Name, workflow, sourceKs, targetKs, table, "primary")
	// Wait for the copy phase to start
	waitForWorkflowState(t, vc, fmt.Sprintf("%s.%s", targetKs, workflow), workflowStateCopying)
	// The initial copy phase should be blocking on the history list
	confirmWorkflowHasCopiedNoData(t, targetKs, workflow)
	releaseInnoDBRowHistory(t, trxConn)
	trxConn.Close()
}

func TestBasicVreplicationWorkflow(t *testing.T) {
	sourceKsOpts["DBTypeVersion"] = "mysql-5.7"
	targetKsOpts["DBTypeVersion"] = "mysql-5.7"
	testBasicVreplicationWorkflow(t)
}

func testBasicVreplicationWorkflow(t *testing.T) {
	defaultCellName := "zone1"
	allCells := []string{"zone1"}
	allCellNames = "zone1"
	vc = NewVitessCluster(t, "TestBasicVreplicationWorkflow", allCells, mainClusterConfig)

	require.NotNil(t, vc)
	// Keep the cluster processes minimal to deal with CI resource constraints
	defaultReplicas = 0
	defaultRdonly = 0
	defer func() { defaultReplicas = 1 }()

	defer vc.TearDown(t)

	defaultCell = vc.Cells[defaultCellName]
	vc.AddKeyspace(t, []*Cell{defaultCell}, "product", "0", initialProductVSchema, initialProductSchema, defaultReplicas, defaultRdonly, 100, sourceKsOpts)
	vtgate = defaultCell.Vtgates[0]
	require.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", "product", "0"), 1)

	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)
	insertInitialData(t)
	materializeRollup(t)
	shardCustomer(t, true, []*Cell{defaultCell}, defaultCellName, false)

	// the Lead and Lead-1 tables tested a specific case with binary sharding keys. Drop it now so that we don't
	// have to update the rest of the tests
	execVtgateQuery(t, vtgateConn, "customer", "drop table `Lead`,`Lead-1`")
	validateRollupReplicates(t)
	shardOrders(t)
	shardMerchant(t)

	materializeProduct(t)

	materializeMerchantOrders(t)
	materializeSales(t)
	materializeMerchantSales(t)

	reshardMerchant2to3SplitMerge(t)
	reshardMerchant3to1Merge(t)

	insertMoreCustomers(t, 16)
	reshardCustomer2to4Split(t, nil, "")
	expectNumberOfStreams(t, vtgateConn, "Customer2to4", "sales", "product:0", 4)
	reshardCustomer3to2SplitMerge(t)
	expectNumberOfStreams(t, vtgateConn, "Customer3to2", "sales", "product:0", 3)
	reshardCustomer3to1Merge(t)
	expectNumberOfStreams(t, vtgateConn, "Customer3to1", "sales", "product:0", 1)

	t.Run("Verify CopyState Is Optimized Afterwards", func(t *testing.T) {
		tabletMap := vc.getVttabletsInKeyspace(t, defaultCell, "customer", topodatapb.TabletType_PRIMARY.String())
		require.NotNil(t, tabletMap)
		require.Greater(t, len(tabletMap), 0)
		for _, tablet := range tabletMap {
			verifyCopyStateIsOptimized(t, tablet)
		}
	})
}

func TestV2WorkflowsAcrossDBVersions(t *testing.T) {
	sourceKsOpts["DBTypeVersion"] = "mysql-5.7"
	targetKsOpts["DBTypeVersion"] = "mysql-8.0"
	testBasicVreplicationWorkflow(t)
}

func TestMultiCellVreplicationWorkflow(t *testing.T) {
	cells := []string{"zone1", "zone2"}
	allCellNames = strings.Join(cells, ",")

	vc = NewVitessCluster(t, "TestMultiCellVreplicationWorkflow", cells, mainClusterConfig)
	require.NotNil(t, vc)
	defaultCellName := "zone1"
	defaultCell = vc.Cells[defaultCellName]

	defer vc.TearDown(t)

	cell1 := vc.Cells["zone1"]
	cell2 := vc.Cells["zone2"]
	vc.AddKeyspace(t, []*Cell{cell1, cell2}, "product", "0", initialProductVSchema, initialProductSchema, defaultReplicas, defaultRdonly, 100, sourceKsOpts)

	vtgate = cell1.Vtgates[0]
	require.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", "product", "0"), 1)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", "product", "0"), 2)

	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)
	insertInitialData(t)
	shardCustomer(t, true, []*Cell{cell1, cell2}, cell2.Name, true)

	// we tag along this test so as not to create the overhead of creating another cluster
	testVStreamCellFlag(t)
}

func TestVStreamFlushBinlog(t *testing.T) {
	defaultCellName := "zone1"
	allCells := []string{defaultCellName}
	allCellNames = defaultCellName
	workflow := "test_vstream_p2c"
	shard := "0"
	vc = NewVitessCluster(t, "TestVStreamBinlogFlush", allCells, mainClusterConfig)
	require.NotNil(t, vc)
	defer vc.TearDown(t)
	defaultCell = vc.Cells[defaultCellName]

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
	vtgate = defaultCell.Vtgates[0]
	require.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", sourceKs, shard), 1)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", targetKs, shard), 1)
	verifyClusterHealth(t, vc)

	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	sourceTab = vc.getPrimaryTablet(t, sourceKs, shard)

	insertInitialData(t)

	tables := "product,customer,merchant,orders"
	moveTables(t, defaultCellName, workflow, sourceKs, targetKs, tables)
	// Wait until we get through the copy phase...
	catchup(t, vc.getPrimaryTablet(t, targetKs, shard), workflow, "MoveTables")

	// So far, we should not have rotated any binlogs
	flushCount := int64(sourceTab.GetVars()["VStreamerFlushedBinlogs"].(float64))
	require.Equal(t, flushCount, int64(0), "VStreamerFlushedBinlogs should be 0")

	// Generate a lot of binlog event bytes
	targetBinlogSize := vstreamer.GetBinlogRotationThreshold() + 16
	vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	queryF := "insert into db_order_test (c_uuid, dbstuff, created_at) values ('%d', repeat('A', 65000), now())"
	for i := 100; i < 5000; i++ {
		res, err := vtgateConn.ExecuteFetch(fmt.Sprintf(queryF, i), -1, false)
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
			}

			ctx2, cancel := context.WithTimeout(ctx, 30*time.Second)
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
	mainClusterConfig.vreplicationCompressGTID = true
	defer func() {
		mainClusterConfig.vreplicationCompressGTID = false
	}()
	vc = NewVitessCluster(t, "TestCellAliasVreplicationWorkflow", cells, mainClusterConfig)
	require.NotNil(t, vc)
	allCellNames = "zone1,zone2"
	defaultCellName := "zone1"
	defaultCell = vc.Cells[defaultCellName]

	defer vc.TearDown(t)

	cell1 := vc.Cells["zone1"]
	cell2 := vc.Cells["zone2"]
	vc.AddKeyspace(t, []*Cell{cell1, cell2}, "product", "0", initialProductVSchema, initialProductSchema, defaultReplicas, defaultRdonly, 100, sourceKsOpts)

	// Add cell alias containing only zone2
	result, err := vc.VtctlClient.ExecuteCommandWithOutput("AddCellsAlias", "--", "--cells", "zone2", "alias")
	require.NoError(t, err, "command failed with output: %v", result)

	vtgate = cell1.Vtgates[0]
	require.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", "product", "0"), 1)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", "product", "0"), 2)

	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)

	insertInitialData(t)
	t.Run("VStreamFrom", func(t *testing.T) {
		testVStreamFrom(t, "product", 2)
	})
	shardCustomer(t, true, []*Cell{cell1, cell2}, "alias", false)
}

// testVStreamFrom confirms that the "vstream * from" endpoint is serving data
func testVStreamFrom(t *testing.T, table string, expectedRowCount int) {
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

func insertInitialData(t *testing.T) {
	t.Run("insertInitialData", func(t *testing.T) {
		log.Infof("Inserting initial data")
		lines, _ := os.ReadFile("unsharded_init_data.sql")
		execMultipleQueries(t, vtgateConn, "product:0", string(lines))
		execVtgateQuery(t, vtgateConn, "product:0", "insert into customer_seq(id, next_id, cache) values(0, 100, 100);")
		execVtgateQuery(t, vtgateConn, "product:0", "insert into order_seq(id, next_id, cache) values(0, 100, 100);")
		execVtgateQuery(t, vtgateConn, "product:0", "insert into customer_seq2(id, next_id, cache) values(0, 100, 100);")
		log.Infof("Done inserting initial data")

		waitForRowCount(t, vtgateConn, "product:0", "product", 2)
		waitForRowCount(t, vtgateConn, "product:0", "customer", 3)
		waitForQueryResult(t, vtgateConn, "product:0", "select * from merchant",
			`[[VARCHAR("Monoprice") VARCHAR("eléctronics")] [VARCHAR("newegg") VARCHAR("elec†ronics")]]`)
	})
}

// insertMoreCustomers creates additional customers.
// Note: this will only work when the customer sequence is in place.
func insertMoreCustomers(t *testing.T, numCustomers int) {
	sql := "insert into customer (name) values "
	i := 0
	for i < numCustomers {
		i++
		sql += fmt.Sprintf("('customer%d')", i)
		if i != numCustomers {
			sql += ","
		}
	}
	execVtgateQuery(t, vtgateConn, "customer", sql)
}

func insertMoreProducts(t *testing.T) {
	sql := "insert into product(pid, description) values(3, 'cpu'),(4, 'camera'),(5, 'mouse');"
	execVtgateQuery(t, vtgateConn, "product", sql)
}

func insertMoreProductsForSourceThrottler(t *testing.T) {
	sql := "insert into product(pid, description) values(103, 'new-cpu'),(104, 'new-camera'),(105, 'new-mouse');"
	execVtgateQuery(t, vtgateConn, "product", sql)
}

func insertMoreProductsForTargetThrottler(t *testing.T) {
	sql := "insert into product(pid, description) values(203, 'new-cpu'),(204, 'new-camera'),(205, 'new-mouse');"
	execVtgateQuery(t, vtgateConn, "product", sql)
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
		if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", "customer", "-80"), 1); err != nil {
			t.Fatal(err)
		}
		if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", "customer", "80-"), 1); err != nil {
			t.Fatal(err)
		}

		// Assume we are operating on first cell
		defaultCell := cells[0]
		custKs := vc.Cells[defaultCell.Name].Keyspaces["customer"]

		tables := "customer,Lead,Lead-1,db_order_test"
		moveTables(t, sourceCellOrAlias, workflow, sourceKs, targetKs, tables)

		customerTab1 := custKs.Shards["-80"].Tablets["zone1-200"].Vttablet
		customerTab2 := custKs.Shards["80-"].Tablets["zone1-300"].Vttablet
		productTab := vc.Cells[defaultCell.Name].Keyspaces["product"].Shards["0"].Tablets["zone1-100"].Vttablet

		// Wait to finish the copy phase for all tables
		catchup(t, customerTab1, workflow, "MoveTables")
		catchup(t, customerTab2, workflow, "MoveTables")

		// Confirm that the 0 scale decimal field, dec80, is replicated correctly
		dec80Replicated := false
		execVtgateQuery(t, vtgateConn, sourceKs, "update customer set dec80 = 0")
		waitForNoWorkflowLag(t, vc, targetKs, workflow)
		for _, shard := range []string{"-80", "80-"} {
			shardTarget := fmt.Sprintf("%s:%s", targetKs, shard)
			if res := execVtgateQuery(t, vtgateConn, shardTarget, "select cid from customer"); len(res.Rows) > 0 {
				waitForQueryResult(t, vtgateConn, shardTarget, "select distinct dec80 from customer", `[[DECIMAL(0)]]`)
				dec80Replicated = true
			}
		}
		require.Equal(t, true, dec80Replicated)

		query := "select cid from customer"
		require.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productTab, "product", query, query))
		insertQuery1 := "insert into customer(cid, name) values(1001, 'tempCustomer1')"
		matchInsertQuery1 := "insert into customer(cid, `name`) values (:vtg1, :vtg2)"
		require.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productTab, "product", insertQuery1, matchInsertQuery1))

		// confirm that the backticking of table names in the routing rules works
		tbls := []string{"Lead", "Lead-1"}
		for _, tbl := range tbls {
			output, err := osExec(t, "mysql", []string{"-u", "vtdba", "-P", fmt.Sprintf("%d", vc.ClusterConfig.vtgateMySQLPort),
				"--host=127.0.0.1", "-e", fmt.Sprintf("select * from `%s`", tbl)})
			if err != nil {
				require.FailNow(t, output)
			}
			execVtgateQuery(t, vtgateConn, "product", fmt.Sprintf("update `%s` set name='xyz'", tbl))
		}

		vdiff1(t, ksWorkflow, "")
		switchReadsDryRun(t, allCellNames, ksWorkflow, dryRunResultsReadCustomerShard)
		switchReads(t, allCellNames, ksWorkflow)
		require.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productTab, "customer", query, query))

		var commit func(t *testing.T)
		if withOpenTx {
			commit, _ = vc.startQuery(t, openTxQuery)
		}
		switchWritesDryRun(t, ksWorkflow, dryRunResultsSwitchWritesCustomerShard)
		switchWrites(t, ksWorkflow, false)
		checkThatVDiffFails(t, targetKs, workflow)

		if withOpenTx && commit != nil {
			commit(t)
		}

		catchup(t, productTab, workflow, "MoveTables")

		vdiff1(t, "product.p2c_reverse", "")
		if withOpenTx {
			execVtgateQuery(t, vtgateConn, "", deleteOpenTxQuery)
		}

		ksShards := []string{"product/0", "customer/-80", "customer/80-"}
		printShardPositions(vc, ksShards)
		insertQuery2 := "insert into customer(name, cid) values('tempCustomer2', 100)"
		matchInsertQuery2 := "insert into customer(`name`, cid) values (:vtg1, :_cid0)"
		require.False(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productTab, "customer", insertQuery2, matchInsertQuery2))

		insertQuery2 = "insert into customer(name, cid) values('tempCustomer3', 101)" //ID 101, hence due to reverse_bits in shard 80-
		require.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, customerTab2, "customer", insertQuery2, matchInsertQuery2))

		insertQuery2 = "insert into customer(name, cid) values('tempCustomer4', 102)" //ID 102, hence due to reverse_bits in shard -80
		require.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, customerTab1, "customer", insertQuery2, matchInsertQuery2))

		execVtgateQuery(t, vtgateConn, "customer", "update customer set meta = convert(x'7b7d' using utf8mb4) where cid = 1")
		reverseKsWorkflow := "product.p2c_reverse"
		if testReverse {
			//Reverse Replicate
			switchReads(t, allCellNames, reverseKsWorkflow)
			printShardPositions(vc, ksShards)
			switchWrites(t, reverseKsWorkflow, false)

			output, err := vc.VtctlClient.ExecuteCommandWithOutput("Workflow", ksWorkflow, "show")
			require.NoError(t, err)
			require.Contains(t, output, "'customer.reverse_bits'")
			require.Contains(t, output, "'customer.bmd5'")

			insertQuery1 = "insert into customer(cid, name) values(1002, 'tempCustomer5')"
			require.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productTab, "product", insertQuery1, matchInsertQuery1))
			// both inserts go into 80-, this tests the edge-case where a stream (-80) has no relevant new events after the previous switch
			insertQuery1 = "insert into customer(cid, name) values(1003, 'tempCustomer6')"
			require.False(t, validateThatQueryExecutesOnTablet(t, vtgateConn, customerTab1, "customer", insertQuery1, matchInsertQuery1))
			insertQuery1 = "insert into customer(cid, name) values(1004, 'tempCustomer7')"
			require.False(t, validateThatQueryExecutesOnTablet(t, vtgateConn, customerTab2, "customer", insertQuery1, matchInsertQuery1))

			//Go forward again
			switchReads(t, allCellNames, ksWorkflow)
			switchWrites(t, ksWorkflow, false)
			dropSourcesDryRun(t, ksWorkflow, false, dryRunResultsDropSourcesDropCustomerShard)
			dropSourcesDryRun(t, ksWorkflow, true, dryRunResultsDropSourcesRenameCustomerShard)

			var exists bool
			exists, err = checkIfDenyListExists(t, vc, "product:0", "customer")
			require.NoError(t, err, "Error getting denylist for customer:0")
			require.True(t, exists)
			dropSources(t, ksWorkflow)

			exists, err = checkIfDenyListExists(t, vc, "product:0", "customer")
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

			insertQuery2 = "insert into customer(name, cid) values('tempCustomer8', 103)" //ID 103, hence due to reverse_bits in shard 80-
			require.False(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productTab, "customer", insertQuery2, matchInsertQuery2))
			insertQuery2 = "insert into customer(name, cid) values('tempCustomer10', 104)" //ID 105, hence due to reverse_bits in shard -80
			require.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, customerTab1, "customer", insertQuery2, matchInsertQuery2))
			insertQuery2 = "insert into customer(name, cid) values('tempCustomer9', 105)" //ID 104, hence due to reverse_bits in shard 80-
			require.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, customerTab2, "customer", insertQuery2, matchInsertQuery2))

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
		waitForRowCount(t, vtgateConn, "product", "rollup", 1)
		waitForQueryResult(t, vtgateConn, "product:0", "select rollupname, kount from rollup",
			`[[VARCHAR("total") INT32(5)]]`)
	})
}

func reshardCustomer2to4Split(t *testing.T, cells []*Cell, sourceCellOrAlias string) {
	t.Run("reshardCustomer2to4Split", func(t *testing.T) {
		ksName := "customer"
		counts := map[string]int{"zone1-600": 4, "zone1-700": 5, "zone1-800": 6, "zone1-900": 5}
		reshard(t, ksName, "customer", "c2c4", "-80,80-", "-40,40-80,80-c0,c0-", 600, counts, nil, cells, sourceCellOrAlias)
		waitForRowCount(t, vtgateConn, ksName, "customer", 20)
		query := "insert into customer (name) values('yoko')"
		execVtgateQuery(t, vtgateConn, ksName, query)
		waitForRowCount(t, vtgateConn, ksName, "customer", 21)
	})
}

func reshardMerchant2to3SplitMerge(t *testing.T) {
	t.Run("reshardMerchant2to3SplitMerge", func(t *testing.T) {
		ksName := merchantKeyspace
		counts := map[string]int{"zone1-1600": 0, "zone1-1700": 2, "zone1-1800": 0}
		reshard(t, ksName, "merchant", "m2m3", "-80,80-", "-40,40-c0,c0-", 1600, counts, dryRunResultsSwitchWritesM2m3, nil, "")
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
		ksName := merchantKeyspace
		counts := map[string]int{"zone1-2000": 3}
		reshard(t, ksName, "merchant", "m3m1", "-40,40-c0,c0-", "0", 2000, counts, nil, nil, "")
		waitForRowCount(t, vtgateConn, ksName, "merchant", 3)
		query := "insert into merchant (mname, category) values('flipkart', 'electronics')"
		execVtgateQuery(t, vtgateConn, ksName, query)
		waitForRowCount(t, vtgateConn, ksName, "merchant", 4)
	})
}

func reshardCustomer3to2SplitMerge(t *testing.T) { //-40,40-80,80-c0 => merge/split, c0- stays the same  ending up with 3
	t.Run("reshardCustomer3to2SplitMerge", func(t *testing.T) {
		ksName := "customer"
		counts := map[string]int{"zone1-1000": 8, "zone1-1100": 8, "zone1-1200": 5}
		reshard(t, ksName, "customer", "c4c3", "-40,40-80,80-c0", "-60,60-c0", 1000, counts, nil, nil, "")
	})
}

func reshardCustomer3to1Merge(t *testing.T) { //to unsharded
	t.Run("reshardCustomer3to1Merge", func(t *testing.T) {
		ksName := "customer"
		counts := map[string]int{"zone1-1500": 21}
		reshard(t, ksName, "customer", "c3c1", "-60,60-c0,c0-", "0", 1500, counts, nil, nil, "")
	})
}

func reshard(t *testing.T, ksName string, tableName string, workflow string, sourceShards string, targetShards string, tabletIDBase int, counts map[string]int, dryRunResultSwitchWrites []string, cells []*Cell, sourceCellOrAlias string) {
	t.Run("reshard", func(t *testing.T) {
		if cells == nil {
			cells = []*Cell{defaultCell}
		}
		if sourceCellOrAlias == "" {
			sourceCellOrAlias = defaultCell.Name
		}
		ksWorkflow := ksName + "." + workflow
		keyspace := vc.Cells[defaultCell.Name].Keyspaces[ksName]
		require.NoError(t, vc.AddShards(t, cells, keyspace, targetShards, defaultReplicas, defaultRdonly, tabletIDBase, targetKsOpts))
		arrTargetShardNames := strings.Split(targetShards, ",")

		for _, shardName := range arrTargetShardNames {
			if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", ksName, shardName), 1); err != nil {
				t.Fatal(err)
			}
		}
		if err := vc.VtctlClient.ExecuteCommand("Reshard", "--", "--v1", "--cells="+sourceCellOrAlias, "--tablet_types=replica,primary", ksWorkflow, "--", sourceShards, targetShards); err != nil {
			t.Fatalf("Reshard command failed with %+v\n", err)
		}
		tablets := vc.getVttabletsInKeyspace(t, defaultCell, ksName, "primary")
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
		vdiff1(t, ksWorkflow, "")
		switchReads(t, allCellNames, ksWorkflow)
		if dryRunResultSwitchWrites != nil {
			switchWritesDryRun(t, ksWorkflow, dryRunResultSwitchWrites)
		}
		switchWrites(t, ksWorkflow, false)
		dropSources(t, ksWorkflow)
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
		workflow := "o2c"
		cell := defaultCell.Name
		sourceKs := "product"
		targetKs := "customer"
		tables := "orders"
		ksWorkflow := fmt.Sprintf("%s.%s", targetKs, workflow)
		applyVSchema(t, ordersVSchema, targetKs)
		moveTables(t, cell, workflow, sourceKs, targetKs, tables)

		custKs := vc.Cells[defaultCell.Name].Keyspaces["customer"]
		customerTab1 := custKs.Shards["-80"].Tablets["zone1-200"].Vttablet
		customerTab2 := custKs.Shards["80-"].Tablets["zone1-300"].Vttablet
		catchup(t, customerTab1, workflow, "MoveTables")
		catchup(t, customerTab2, workflow, "MoveTables")
		vdiff1(t, ksWorkflow, "")
		switchReads(t, allCellNames, ksWorkflow)
		switchWrites(t, ksWorkflow, false)
		dropSources(t, ksWorkflow)
		waitForRowCountInTablet(t, customerTab1, "customer", "orders", 1)
		waitForRowCountInTablet(t, customerTab2, "customer", "orders", 2)
		waitForRowCount(t, vtgateConn, "customer", "orders", 3)
	})
}

func checkThatVDiffFails(t *testing.T, keyspace, workflow string) {
	ksWorkflow := fmt.Sprintf("%s.%s", keyspace, workflow)
	t.Run("check that vdiff1 won't run", func(t2 *testing.T) {
		output, err := vc.VtctlClient.ExecuteCommandWithOutput("VDiff", ksWorkflow)
		require.Error(t, err)
		require.Contains(t, output, "invalid VDiff run")
	})
	t.Run("check that vdiff2 won't run", func(t2 *testing.T) {
		output, err := vc.VtctlClient.ExecuteCommandWithOutput("VDiff", "--", "--v2", ksWorkflow)
		require.Error(t, err)
		require.Contains(t, output, "invalid VDiff run")

	})
}

func shardMerchant(t *testing.T) {
	t.Run("shardMerchant", func(t *testing.T) {
		workflow := "p2m"
		cell := defaultCell.Name
		sourceKs := "product"
		targetKs := merchantKeyspace
		tables := "merchant"
		ksWorkflow := fmt.Sprintf("%s.%s", targetKs, workflow)
		if _, err := vc.AddKeyspace(t, []*Cell{defaultCell}, merchantKeyspace, "-80,80-", merchantVSchema, "", defaultReplicas, defaultRdonly, 400, targetKsOpts); err != nil {
			t.Fatal(err)
		}
		if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", merchantKeyspace, "-80"), 1); err != nil {
			t.Fatal(err)
		}
		if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", merchantKeyspace, "80-"), 1); err != nil {
			t.Fatal(err)
		}
		moveTables(t, cell, workflow, sourceKs, targetKs, tables)
		merchantKs := vc.Cells[defaultCell.Name].Keyspaces[merchantKeyspace]
		merchantTab1 := merchantKs.Shards["-80"].Tablets["zone1-400"].Vttablet
		merchantTab2 := merchantKs.Shards["80-"].Tablets["zone1-500"].Vttablet
		catchup(t, merchantTab1, workflow, "MoveTables")
		catchup(t, merchantTab2, workflow, "MoveTables")

		vdiff1(t, fmt.Sprintf("%s.%s", merchantKeyspace, workflow), "")
		switchReads(t, allCellNames, ksWorkflow)
		switchWrites(t, ksWorkflow, false)
		printRoutingRules(t, vc, "After merchant movetables")

		// confirm that the backticking of keyspaces in the routing rules works
		output, err := osExec(t, "mysql", []string{"-u", "vtdba", "-P", fmt.Sprintf("%d", vc.ClusterConfig.vtgateMySQLPort),
			fmt.Sprintf("--host=%s", vc.ClusterConfig.hostname), "-e", "select * from merchant"})
		if err != nil {
			require.FailNow(t, output)
		}
		dropSources(t, ksWorkflow)

		waitForRowCountInTablet(t, merchantTab1, merchantKeyspace, "merchant", 1)
		waitForRowCountInTablet(t, merchantTab2, merchantKeyspace, "merchant", 1)
		waitForRowCount(t, vtgateConn, merchantKeyspace, "merchant", 2)
	})
}

func materialize(t *testing.T, spec string) {
	t.Run("materialize", func(t *testing.T) {
		err := vc.VtctlClient.ExecuteCommand("Materialize", spec)
		require.NoError(t, err, "Materialize")
	})
}

func materializeProduct(t *testing.T) {
	t.Run("materializeProduct", func(t *testing.T) {
		// materializing from "product" keyspace to "customer" keyspace
		workflow := "cproduct"
		keyspace := "customer"
		applyVSchema(t, materializeProductVSchema, keyspace)
		materialize(t, materializeProductSpec)
		customerTablets := vc.getVttabletsInKeyspace(t, defaultCell, keyspace, "primary")
		for _, tab := range customerTablets {
			catchup(t, tab, workflow, "Materialize")
			waitForRowCountInTablet(t, tab, keyspace, workflow, 5)
		}

		productTablets := vc.getVttabletsInKeyspace(t, defaultCell, "product", "primary")
		t.Run("throttle-app-product", func(t *testing.T) {
			// Now, throttle the streamer on source tablets, insert some rows
			for _, tab := range productTablets {
				_, body, err := throttleApp(tab, sourceThrottlerAppName)
				assert.NoError(t, err)
				assert.Contains(t, body, sourceThrottlerAppName)

				// Wait for throttling to take effect (caching will expire by this time):
				waitForTabletThrottlingStatus(t, tab, sourceThrottlerAppName, throttlerStatusThrottled)
				waitForTabletThrottlingStatus(t, tab, targetThrottlerAppName, throttlerStatusNotThrottled)
			}
			insertMoreProductsForSourceThrottler(t)
			// To be fair to the test, we give the target time to apply the new changes. We expect it to NOT get them in the first place,
			// we expect the additional rows to **not appear** in the materialized view
			for _, tab := range customerTablets {
				waitForRowCountInTablet(t, tab, keyspace, workflow, 5)
			}
		})
		t.Run("unthrottle-app-product", func(t *testing.T) {
			// unthrottle on source tablets, and expect the rows to show up
			for _, tab := range productTablets {
				_, body, err := unthrottleApp(tab, sourceThrottlerAppName)
				assert.NoError(t, err)
				assert.Contains(t, body, sourceThrottlerAppName)
				// give time for unthrottling to take effect and for target to fetch data
				waitForTabletThrottlingStatus(t, tab, sourceThrottlerAppName, throttlerStatusNotThrottled)
			}
			for _, tab := range customerTablets {
				waitForRowCountInTablet(t, tab, keyspace, workflow, 8)
			}
		})

		t.Run("throttle-app-customer", func(t *testing.T) {
			// Now, throttle vreplication (vcopier/vapplier) on target tablets, and
			// insert some more rows.
			for _, tab := range customerTablets {
				_, body, err := throttleApp(tab, targetThrottlerAppName)
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
			}
		})
		t.Run("unthrottle-app-customer", func(t *testing.T) {
			// unthrottle on target tablets, and expect the rows to show up
			for _, tab := range customerTablets {
				_, body, err := unthrottleApp(tab, targetThrottlerAppName)
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

func materializeRollup(t *testing.T) {
	t.Run("materializeRollup", func(t *testing.T) {
		keyspace := "product"
		workflow := "rollup"
		applyVSchema(t, materializeSalesVSchema, keyspace)
		productTab := vc.Cells[defaultCell.Name].Keyspaces["product"].Shards["0"].Tablets["zone1-100"].Vttablet
		materialize(t, materializeRollupSpec)
		catchup(t, productTab, workflow, "Materialize")
		waitForRowCount(t, vtgateConn, "product", "rollup", 1)
		waitForQueryResult(t, vtgateConn, "product:0", "select rollupname, kount from rollup",
			`[[VARCHAR("total") INT32(2)]]`)
	})
}

func materializeSales(t *testing.T) {
	t.Run("materializeSales", func(t *testing.T) {
		keyspace := "product"
		applyVSchema(t, materializeSalesVSchema, keyspace)
		materialize(t, materializeSalesSpec)
		productTab := vc.Cells[defaultCell.Name].Keyspaces["product"].Shards["0"].Tablets["zone1-100"].Vttablet
		catchup(t, productTab, "sales", "Materialize")
		waitForRowCount(t, vtgateConn, "product", "sales", 2)
		waitForQueryResult(t, vtgateConn, "product:0", "select kount, amount from sales",
			`[[INT32(1) INT32(10)] [INT32(2) INT32(35)]]`)
	})
}

func materializeMerchantSales(t *testing.T) {
	t.Run("materializeMerchantSales", func(t *testing.T) {
		workflow := "msales"
		materialize(t, materializeMerchantSalesSpec)
		merchantTablets := vc.getVttabletsInKeyspace(t, defaultCell, merchantKeyspace, "primary")
		for _, tab := range merchantTablets {
			catchup(t, tab, workflow, "Materialize")
		}
		waitForRowCountInTablet(t, merchantTablets["zone1-400"], merchantKeyspace, "msales", 1)
		waitForRowCountInTablet(t, merchantTablets["zone1-500"], merchantKeyspace, "msales", 1)
		waitForRowCount(t, vtgateConn, merchantKeyspace, "msales", 2)
	})
}

func materializeMerchantOrders(t *testing.T) {
	t.Run("materializeMerchantOrders", func(t *testing.T) {
		workflow := "morders"
		keyspace := merchantKeyspace
		applyVSchema(t, merchantOrdersVSchema, keyspace)
		materialize(t, materializeMerchantOrdersSpec)
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
			assert.Failf(t, "Vtgate not healthy: ", vtgateHealthURL)
		}
	}
}

func checkTabletHealth(t *testing.T, tablet *Tablet) {
	vttabletHealthURL := strings.Replace(tablet.Vttablet.VerifyURL, "debug/vars", "healthz", -1)
	if !checkHealth(t, vttabletHealthURL) {
		assert.Failf(t, "Vttablet not healthy: ", vttabletHealthURL)
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
	vttablet.WaitForVReplicationToCatchup(t, workflow, fmt.Sprintf("vt_%s", vttablet.Keyspace), maxWait)
}

func moveTables(t *testing.T, cell, workflow, sourceKs, targetKs, tables string) {
	if err := vc.VtctlClient.ExecuteCommand("MoveTables", "--", "--v1", "--cells="+cell, "--workflow="+workflow,
		"--tablet_types="+"primary,replica,rdonly", sourceKs, targetKs, tables); err != nil {
		t.Fatalf("MoveTables command failed with %+v\n", err)
	}
}
func moveTablesWithTabletTypes(t *testing.T, cell, workflow, sourceKs, targetKs, tables string, tabletTypes string) {
	if err := vc.VtctlClient.ExecuteCommand("MoveTables", "--", "--v1", "--cells="+cell, "--workflow="+workflow,
		"--tablet_types="+tabletTypes, sourceKs, targetKs, tables); err != nil {
		t.Fatalf("MoveTables command failed with %+v\n", err)
	}
}
func applyVSchema(t *testing.T, vschema, keyspace string) {
	err := vc.VtctlClient.ExecuteCommand("ApplyVSchema", "--", "--vschema", vschema, keyspace)
	require.NoError(t, err)
}

func switchReadsDryRun(t *testing.T, cells, ksWorkflow string, dryRunResults []string) {
	output, err := vc.VtctlClient.ExecuteCommandWithOutput("SwitchReads", "--", "--cells="+cells, "--tablet_types=replica", "--dry_run", ksWorkflow)
	require.NoError(t, err, fmt.Sprintf("SwitchReads DryRun Error: %s: %s", err, output))
	validateDryRunResults(t, output, dryRunResults)
}

func switchReads(t *testing.T, cells, ksWorkflow string) {
	var output string
	var err error
	output, err = vc.VtctlClient.ExecuteCommandWithOutput("SwitchReads", "--", "--cells="+cells, "--tablet_types=rdonly", ksWorkflow)
	require.NoError(t, err, fmt.Sprintf("SwitchReads Error: %s: %s", err, output))
	output, err = vc.VtctlClient.ExecuteCommandWithOutput("SwitchReads", "--", "--cells="+cells, "--tablet_types=replica", ksWorkflow)
	require.NoError(t, err, fmt.Sprintf("SwitchReads Error: %s: %s", err, output))
}

func switchWritesDryRun(t *testing.T, ksWorkflow string, dryRunResults []string) {
	output, err := vc.VtctlClient.ExecuteCommandWithOutput("SwitchWrites", "--", "--dry_run", ksWorkflow)
	require.NoError(t, err, fmt.Sprintf("SwitchWrites DryRun Error: %s: %s", err, output))
	validateDryRunResults(t, output, dryRunResults)
}

func printSwitchWritesExtraDebug(t *testing.T, ksWorkflow, msg string) {
	// Temporary code: print lots of info for debugging occasional flaky failures in customer reshard in CI for multicell test
	debug := true
	if debug {
		log.Infof("------------------- START Extra debug info %s SwitchWrites %s", msg, ksWorkflow)
		ksShards := []string{"product/0", "customer/-80", "customer/80-"}
		printShardPositions(vc, ksShards)
		custKs := vc.Cells[defaultCell.Name].Keyspaces["customer"]
		customerTab1 := custKs.Shards["-80"].Tablets["zone1-200"].Vttablet
		customerTab2 := custKs.Shards["80-"].Tablets["zone1-300"].Vttablet
		productKs := vc.Cells[defaultCell.Name].Keyspaces["product"]
		productTab := productKs.Shards["0"].Tablets["zone1-100"].Vttablet
		tabs := []*cluster.VttabletProcess{productTab, customerTab1, customerTab2}
		queries := []string{
			"select  id, workflow, pos, stop_pos, cell, tablet_types, time_updated, transaction_timestamp, state, message from _vt.vreplication",
			"select * from _vt.copy_state",
			"select * from _vt.resharding_journal",
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

func switchWrites(t *testing.T, ksWorkflow string, reverse bool) {
	const SwitchWritesTimeout = "91s" // max: 3 tablet picker 30s waits + 1
	output, err := vc.VtctlClient.ExecuteCommandWithOutput("SwitchWrites", "--",
		"--filtered_replication_wait_time="+SwitchWritesTimeout, fmt.Sprintf("--reverse=%t", reverse), ksWorkflow)
	if output != "" {
		fmt.Printf("Output of SwitchWrites for %s:\n++++++\n%s\n--------\n", ksWorkflow, output)
	}
	//printSwitchWritesExtraDebug is useful when debugging failures in SwitchWrites due to corner cases/races
	_ = printSwitchWritesExtraDebug
	require.NoError(t, err, fmt.Sprintf("SwitchWrites Error: %s: %s", err, output))
}

func dropSourcesDryRun(t *testing.T, ksWorkflow string, renameTables bool, dryRunResults []string) {
	args := []string{"DropSources", "--", "--dry_run"}
	if renameTables {
		args = append(args, "--rename_tables")
	}
	args = append(args, ksWorkflow)
	output, err := vc.VtctlClient.ExecuteCommandWithOutput(args...)
	require.NoError(t, err, fmt.Sprintf("DropSources Error: %s: %s", err, output))
	validateDryRunResults(t, output, dryRunResults)
}

func dropSources(t *testing.T, ksWorkflow string) {
	output, err := vc.VtctlClient.ExecuteCommandWithOutput("DropSources", ksWorkflow)
	require.NoError(t, err, fmt.Sprintf("DropSources Error: %s: %s", err, output))
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
