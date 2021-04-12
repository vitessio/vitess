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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	throttlebase "vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/wrangler"
)

var (
	vc                     *VitessCluster
	vtgate                 *cluster.VtgateProcess
	defaultCell            *Cell
	vtgateConn             *mysql.Conn
	defaultRdonly          int
	defaultReplicas        int
	allCellNames           string
	httpClient             = throttlebase.SetupHTTPClient(time.Second)
	sourceThrottlerAppName = "vstreamer"
	targetThrottlerAppName = "vreplication"
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
	b, err := ioutil.ReadAll(resp.Body)
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
	b, err := ioutil.ReadAll(resp.Body)
	respBody = string(b)
	return resp, respBody, err
}

func TestBasicVreplicationWorkflow(t *testing.T) {
	defaultCellName := "zone1"
	allCells := []string{"zone1"}
	allCellNames = "zone1"
	vc = NewVitessCluster(t, "TestBasicVreplicationWorkflow", allCells, mainClusterConfig)

	require.NotNil(t, vc)
	defaultReplicas = 0 // because of CI resource constraints we can only run this test with master tablets
	defer func() { defaultReplicas = 1 }()

	defer vc.TearDown()

	defaultCell = vc.Cells[defaultCellName]
	vc.AddKeyspace(t, []*Cell{defaultCell}, "product", "0", initialProductVSchema, initialProductSchema, defaultReplicas, defaultRdonly, 100)
	vtgate = defaultCell.Vtgates[0]
	require.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "product", "0"), 1)

	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)
	insertInitialData(t)
	materializeRollup(t)

	shardCustomer(t, true, []*Cell{defaultCell}, defaultCellName)

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
}

func TestMultiCellVreplicationWorkflow(t *testing.T) {
	cells := []string{"zone1", "zone2"}
	allCellNames = "zone1,zone2"

	vc = NewVitessCluster(t, "TestBasicVreplicationWorkflow", cells, mainClusterConfig)
	require.NotNil(t, vc)
	defaultCellName := "zone1"
	defaultCell = vc.Cells[defaultCellName]

	defer vc.TearDown()

	cell1 := vc.Cells["zone1"]
	cell2 := vc.Cells["zone2"]
	vc.AddKeyspace(t, []*Cell{cell1, cell2}, "product", "0", initialProductVSchema, initialProductSchema, defaultReplicas, defaultRdonly, 100)

	vtgate = cell1.Vtgates[0]
	require.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "product", "0"), 1)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", "product", "0"), 2)

	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)
	insertInitialData(t)
	shardCustomer(t, true, []*Cell{cell1, cell2}, cell2.Name)
}

func TestCellAliasVreplicationWorkflow(t *testing.T) {
	cells := []string{"zone1", "zone2"}

	vc = NewVitessCluster(t, "TestBasicVreplicationWorkflow", cells, mainClusterConfig)
	require.NotNil(t, vc)
	allCellNames = "zone1,zone2"
	defaultCellName := "zone1"
	defaultCell = vc.Cells[defaultCellName]

	defer vc.TearDown()

	cell1 := vc.Cells["zone1"]
	cell2 := vc.Cells["zone2"]
	vc.AddKeyspace(t, []*Cell{cell1, cell2}, "product", "0", initialProductVSchema, initialProductSchema, defaultReplicas, defaultRdonly, 100)

	// Add cell alias containing only zone2
	result, err := vc.VtctlClient.ExecuteCommandWithOutput("AddCellsAlias", "-cells", "zone2", "alias")
	require.NoError(t, err, "command failed with output: %v", result)

	vtgate = cell1.Vtgates[0]
	require.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "product", "0"), 1)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", "product", "0"), 2)

	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)
	insertInitialData(t)
	shardCustomer(t, true, []*Cell{cell1, cell2}, "alias")
}

func insertInitialData(t *testing.T) {
	t.Run("insertInitialData", func(t *testing.T) {
		fmt.Printf("Inserting initial data\n")
		lines, _ := ioutil.ReadFile("unsharded_init_data.sql")
		execMultipleQueries(t, vtgateConn, "product:0", string(lines))
		execVtgateQuery(t, vtgateConn, "product:0", "insert into customer_seq(id, next_id, cache) values(0, 100, 100);")
		execVtgateQuery(t, vtgateConn, "product:0", "insert into order_seq(id, next_id, cache) values(0, 100, 100);")
		fmt.Printf("Done inserting initial data\n")

		validateCount(t, vtgateConn, "product:0", "product", 2)
		validateCount(t, vtgateConn, "product:0", "customer", 3)
		validateQuery(t, vtgateConn, "product:0", "select * from merchant",
			`[[VARCHAR("monoprice") VARCHAR("electronics")] [VARCHAR("newegg") VARCHAR("electronics")]]`)
	})
}

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

func shardCustomer(t *testing.T, testReverse bool, cells []*Cell, sourceCellOrAlias string) {
	t.Run("shardCustomer", func(t *testing.T) {
		workflow := "p2c"
		sourceKs := "product"
		targetKs := "customer"
		ksWorkflow := fmt.Sprintf("%s.%s", targetKs, workflow)
		if _, err := vc.AddKeyspace(t, cells, "customer", "-80,80-", customerVSchema, customerSchema, defaultReplicas, defaultRdonly, 200); err != nil {
			t.Fatal(err)
		}
		if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "customer", "-80"), 1); err != nil {
			t.Fatal(err)
		}
		if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "customer", "80-"), 1); err != nil {
			t.Fatal(err)
		}

		tables := "customer"
		moveTables(t, sourceCellOrAlias, workflow, sourceKs, targetKs, tables)

		// Assume we are operating on first cell
		defaultCell := cells[0]
		custKs := vc.Cells[defaultCell.Name].Keyspaces["customer"]
		customerTab1 := custKs.Shards["-80"].Tablets["zone1-200"].Vttablet
		customerTab2 := custKs.Shards["80-"].Tablets["zone1-300"].Vttablet

		catchup(t, customerTab1, workflow, "MoveTables")
		catchup(t, customerTab2, workflow, "MoveTables")

		productTab := vc.Cells[defaultCell.Name].Keyspaces["product"].Shards["0"].Tablets["zone1-100"].Vttablet
		query := "select * from customer"
		require.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productTab, "product", query, query))
		insertQuery1 := "insert into customer(cid, name) values(1001, 'tempCustomer1')"
		matchInsertQuery1 := "insert into customer(cid, `name`) values (:vtg1, :vtg2)"
		require.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productTab, "product", insertQuery1, matchInsertQuery1))
		vdiff(t, ksWorkflow, "")
		switchReadsDryRun(t, allCellNames, ksWorkflow, dryRunResultsReadCustomerShard)
		switchReads(t, allCellNames, ksWorkflow)
		require.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productTab, "customer", query, query))
		switchWritesDryRun(t, ksWorkflow, dryRunResultsSwitchWritesCustomerShard)
		switchWrites(t, ksWorkflow, false)
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
			exists, err := checkIfBlacklistExists(t, vc, "product:0", "customer")
			require.NoError(t, err, "Error getting blacklist for customer:0")
			require.True(t, exists)
			dropSources(t, ksWorkflow)

			exists, err = checkIfBlacklistExists(t, vc, "product:0", "customer")
			require.NoError(t, err, "Error getting blacklist for customer:0")
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
			validateCountInTablet(t, customerTab1, "customer", "customer", 1)
			validateCountInTablet(t, customerTab2, "customer", "customer", 2)
			validateCount(t, vtgateConn, "customer", "customer.customer", 3)

			query = "insert into customer (name, cid) values('george', 5)"
			execVtgateQuery(t, vtgateConn, "customer", query)
			validateCountInTablet(t, customerTab1, "customer", "customer", 1)
			validateCountInTablet(t, customerTab2, "customer", "customer", 3)
			validateCount(t, vtgateConn, "customer", "customer.customer", 4)
		}
	})
}

func validateRollupReplicates(t *testing.T) {
	t.Run("validateRollupReplicates", func(t *testing.T) {
		insertMoreProducts(t)
		time.Sleep(1 * time.Second)
		validateCount(t, vtgateConn, "product", "rollup", 1)
		validateQuery(t, vtgateConn, "product:0", "select rollupname, kount from rollup",
			`[[VARCHAR("total") INT32(5)]]`)
	})
}

func reshardCustomer2to4Split(t *testing.T, cells []*Cell, sourceCellOrAlias string) {
	t.Run("reshardCustomer2to4Split", func(t *testing.T) {
		ksName := "customer"
		counts := map[string]int{"zone1-600": 4, "zone1-700": 5, "zone1-800": 6, "zone1-900": 5}
		reshard(t, ksName, "customer", "c2c4", "-80,80-", "-40,40-80,80-c0,c0-", 600, counts, nil, cells, sourceCellOrAlias)
		validateCount(t, vtgateConn, ksName, "customer", 20)
		query := "insert into customer (name) values('yoko')"
		execVtgateQuery(t, vtgateConn, ksName, query)
		validateCount(t, vtgateConn, ksName, "customer", 21)
	})
}

func reshardMerchant2to3SplitMerge(t *testing.T) {
	t.Run("reshardMerchant2to3SplitMerge", func(t *testing.T) {
		ksName := "merchant"
		counts := map[string]int{"zone1-1600": 0, "zone1-1700": 2, "zone1-1800": 0}
		reshard(t, ksName, "merchant", "m2m3", "-80,80-", "-40,40-c0,c0-", 1600, counts, dryRunResultsSwitchWritesM2m3, nil, "")
		validateCount(t, vtgateConn, ksName, "merchant", 2)
		query := "insert into merchant (mname, category) values('amazon', 'electronics')"
		execVtgateQuery(t, vtgateConn, ksName, query)
		validateCount(t, vtgateConn, ksName, "merchant", 3)

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
			output, err = vc.VtctlClient.ExecuteCommandWithOutput("GetShard", "merchant:"+shard)
			if err != nil {
				t.Fatalf("GetShard merchant failed for: %s: %v", shard, err)
			}
			assert.NotContains(t, output, "node doesn't exist", "GetShard failed for valid shard merchant:"+shard)
			assert.Contains(t, output, "master_alias", "GetShard failed for valid shard merchant:"+shard)
		}

		for _, shard := range strings.Split("-40,40-c0,c0-", ",") {
			expectNumberOfStreams(t, vtgateConn, "reshardMerchant2to3SplitMerge", "m2m3", "merchant:"+shard, 0)
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
		ksName := "merchant"
		counts := map[string]int{"zone1-2000": 3}
		reshard(t, ksName, "merchant", "m3m1", "-40,40-c0,c0-", "0", 2000, counts, nil, nil, "")
		validateCount(t, vtgateConn, ksName, "merchant", 3)
		query := "insert into merchant (mname, category) values('flipkart', 'electronics')"
		execVtgateQuery(t, vtgateConn, ksName, query)
		validateCount(t, vtgateConn, ksName, "merchant", 4)
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
		require.NoError(t, vc.AddShards(t, cells, keyspace, targetShards, defaultReplicas, defaultRdonly, tabletIDBase))
		arrTargetShardNames := strings.Split(targetShards, ",")

		for _, shardName := range arrTargetShardNames {
			if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", ksName, shardName), 1); err != nil {
				t.Fatal(err)
			}
		}
		if err := vc.VtctlClient.ExecuteCommand("Reshard", "-cells="+sourceCellOrAlias, "-tablet_types=replica,master", ksWorkflow, sourceShards, targetShards); err != nil {
			t.Fatalf("Reshard command failed with %+v\n", err)
		}
		tablets := vc.getVttabletsInKeyspace(t, defaultCell, ksName, "master")
		targetShards = "," + targetShards + ","
		for _, tab := range tablets {
			if strings.Contains(targetShards, ","+tab.Shard+",") {
				fmt.Printf("Waiting for vrepl to catch up on %s since it IS a target shard\n", tab.Shard)
				catchup(t, tab, workflow, "Reshard")
			} else {
				fmt.Printf("Not waiting for vrepl to catch up on %s since it is NOT a target shard\n", tab.Shard)
				continue
			}
		}
		vdiff(t, ksWorkflow, "")
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
			validateCountInTablet(t, tablets[tabletName], ksName, tableName, count)
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
		vdiff(t, ksWorkflow, "")
		switchReads(t, allCellNames, ksWorkflow)
		switchWrites(t, ksWorkflow, false)
		dropSources(t, ksWorkflow)
		validateCountInTablet(t, customerTab1, "customer", "orders", 1)
		validateCountInTablet(t, customerTab2, "customer", "orders", 2)
		validateCount(t, vtgateConn, "customer", "orders", 3)
	})
}

func shardMerchant(t *testing.T) {
	t.Run("shardMerchant", func(t *testing.T) {
		workflow := "p2m"
		cell := defaultCell.Name
		sourceKs := "product"
		targetKs := "merchant"
		tables := "merchant"
		ksWorkflow := fmt.Sprintf("%s.%s", targetKs, workflow)
		if _, err := vc.AddKeyspace(t, []*Cell{defaultCell}, "merchant", "-80,80-", merchantVSchema, "", defaultReplicas, defaultRdonly, 400); err != nil {
			t.Fatal(err)
		}
		if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "merchant", "-80"), 1); err != nil {
			t.Fatal(err)
		}
		if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "merchant", "80-"), 1); err != nil {
			t.Fatal(err)
		}
		moveTables(t, cell, workflow, sourceKs, targetKs, tables)
		merchantKs := vc.Cells[defaultCell.Name].Keyspaces["merchant"]
		merchantTab1 := merchantKs.Shards["-80"].Tablets["zone1-400"].Vttablet
		merchantTab2 := merchantKs.Shards["80-"].Tablets["zone1-500"].Vttablet
		catchup(t, merchantTab1, workflow, "MoveTables")
		catchup(t, merchantTab2, workflow, "MoveTables")

		vdiff(t, "merchant.p2m", "")
		switchReads(t, allCellNames, ksWorkflow)
		switchWrites(t, ksWorkflow, false)
		dropSources(t, ksWorkflow)

		validateCountInTablet(t, merchantTab1, "merchant", "merchant", 1)
		validateCountInTablet(t, merchantTab2, "merchant", "merchant", 1)
		validateCount(t, vtgateConn, "merchant", "merchant", 2)
	})
}

func vdiff(t *testing.T, workflow, cells string) {
	t.Run("vdiff", func(t *testing.T) {
		output, err := vc.VtctlClient.ExecuteCommandWithOutput("VDiff", "-tablet_types=master", "-source_cell="+cells, "-format", "json", workflow)
		fmt.Printf("vdiff err: %+v, output: %+v\n", err, output)
		require.Nil(t, err)
		require.NotNil(t, output)
		diffReports := make([]*wrangler.DiffReport, 0)
		err = json.Unmarshal([]byte(output), &diffReports)
		require.Nil(t, err)
		if len(diffReports) < 1 {
			t.Fatal("VDiff did not return a valid json response " + output + "\n")
		}
		require.True(t, len(diffReports) > 0)
		for key, diffReport := range diffReports {
			if diffReport.ProcessedRows != diffReport.MatchingRows {
				require.Failf(t, "vdiff failed", "Table %d : %#v\n", key, diffReport)
			}
		}
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
		customerTablets := vc.getVttabletsInKeyspace(t, defaultCell, keyspace, "master")
		{
			for _, tab := range customerTablets {
				catchup(t, tab, workflow, "Materialize")
			}
			for _, tab := range customerTablets {
				validateCountInTablet(t, tab, keyspace, workflow, 5)
			}
		}

		productTablets := vc.getVttabletsInKeyspace(t, defaultCell, "product", "master")
		t.Run("throttle-app-product", func(t *testing.T) {
			// Now, throttle the streamer on source tablets, insert some rows
			for _, tab := range productTablets {
				_, body, err := throttleApp(tab, sourceThrottlerAppName)
				assert.NoError(t, err)
				assert.Contains(t, body, sourceThrottlerAppName)
			}
			// Wait for throttling to take effect (caching will expire by this time):
			time.Sleep(1 * time.Second)
			for _, tab := range productTablets {
				{
					_, body, err := throttlerCheckSelf(tab, sourceThrottlerAppName)
					assert.NoError(t, err)
					assert.Contains(t, body, "417")
				}
				{
					_, body, err := throttlerCheckSelf(tab, targetThrottlerAppName)
					assert.NoError(t, err)
					assert.Contains(t, body, "200")
				}
			}
			insertMoreProductsForSourceThrottler(t)
			// To be fair to the test, we give the target time to apply the new changes. We expect it to NOT get them in the first place,
			time.Sleep(1 * time.Second)
			// we expect the additional rows to **not appear** in the materialized view
			for _, tab := range customerTablets {
				validateCountInTablet(t, tab, keyspace, workflow, 5)
			}
		})
		t.Run("unthrottle-app-product", func(t *testing.T) {
			// unthrottle on source tablets, and expect the rows to show up
			for _, tab := range productTablets {
				_, body, err := unthrottleApp(tab, sourceThrottlerAppName)
				assert.NoError(t, err)
				assert.Contains(t, body, sourceThrottlerAppName)
			}
			// give time for unthrottling to take effect and for target to fetch data
			time.Sleep(3 * time.Second)
			for _, tab := range productTablets {
				{
					_, body, err := throttlerCheckSelf(tab, sourceThrottlerAppName)
					assert.NoError(t, err)
					assert.Contains(t, body, "200")
				}
			}
			for _, tab := range customerTablets {
				validateCountInTablet(t, tab, keyspace, workflow, 8)
			}
		})

		t.Run("throttle-app-customer", func(t *testing.T) {
			// Now, throttle the streamer on source tablets, insert some rows
			for _, tab := range customerTablets {
				_, body, err := throttleApp(tab, targetThrottlerAppName)
				assert.NoError(t, err)
				assert.Contains(t, body, targetThrottlerAppName)
			}
			// Wait for throttling to take effect (caching will expire by this time):
			time.Sleep(1 * time.Second)
			for _, tab := range customerTablets {
				{
					_, body, err := throttlerCheckSelf(tab, targetThrottlerAppName)
					assert.NoError(t, err)
					assert.Contains(t, body, "417")
				}
				{
					_, body, err := throttlerCheckSelf(tab, sourceThrottlerAppName)
					assert.NoError(t, err)
					assert.Contains(t, body, "200")
				}
			}
			insertMoreProductsForTargetThrottler(t)
			// To be fair to the test, we give the target time to apply the new changes. We expect it to NOT get them in the first place,
			time.Sleep(1 * time.Second)
			// we expect the additional rows to **not appear** in the materialized view
			for _, tab := range customerTablets {
				validateCountInTablet(t, tab, keyspace, workflow, 8)
			}
		})
		t.Run("unthrottle-app-customer", func(t *testing.T) {
			// unthrottle on source tablets, and expect the rows to show up
			for _, tab := range customerTablets {
				_, body, err := unthrottleApp(tab, targetThrottlerAppName)
				assert.NoError(t, err)
				assert.Contains(t, body, targetThrottlerAppName)
			}
			// give time for unthrottling to take effect and for target to fetch data
			time.Sleep(3 * time.Second)
			for _, tab := range customerTablets {
				{
					_, body, err := throttlerCheckSelf(tab, targetThrottlerAppName)
					assert.NoError(t, err)
					assert.Contains(t, body, "200")
				}
			}
			for _, tab := range customerTablets {
				validateCountInTablet(t, tab, keyspace, workflow, 11)
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
		validateCount(t, vtgateConn, "product", "rollup", 1)
		validateQuery(t, vtgateConn, "product:0", "select rollupname, kount from rollup",
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
		validateCount(t, vtgateConn, "product", "sales", 2)
		validateQuery(t, vtgateConn, "product:0", "select kount, amount from sales",
			`[[INT32(1) INT32(10)] [INT32(2) INT32(35)]]`)
	})
}

func materializeMerchantSales(t *testing.T) {
	t.Run("materializeMerchantSales", func(t *testing.T) {
		workflow := "msales"
		materialize(t, materializeMerchantSalesSpec)
		merchantTablets := vc.getVttabletsInKeyspace(t, defaultCell, "merchant", "master")
		for _, tab := range merchantTablets {
			catchup(t, tab, workflow, "Materialize")
		}
		validateCountInTablet(t, merchantTablets["zone1-400"], "merchant", "msales", 1)
		validateCountInTablet(t, merchantTablets["zone1-500"], "merchant", "msales", 1)
		validateCount(t, vtgateConn, "merchant", "msales", 2)
	})
}

func materializeMerchantOrders(t *testing.T) {
	t.Run("materializeMerchantOrders", func(t *testing.T) {
		workflow := "morders"
		keyspace := "merchant"
		applyVSchema(t, merchantOrdersVSchema, keyspace)
		materialize(t, materializeMerchantOrdersSpec)
		merchantTablets := vc.getVttabletsInKeyspace(t, defaultCell, "merchant", "master")
		for _, tab := range merchantTablets {
			catchup(t, tab, workflow, "Materialize")
		}
		validateCountInTablet(t, merchantTablets["zone1-400"], "merchant", "morders", 2)
		validateCountInTablet(t, merchantTablets["zone1-500"], "merchant", "morders", 1)
		validateCount(t, vtgateConn, "merchant", "morders", 3)
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

func catchup(t *testing.T, vttablet *cluster.VttabletProcess, workflow, info string) {
	const MaxWait = 10 * time.Second
	err := vc.WaitForVReplicationToCatchup(vttablet, workflow, fmt.Sprintf("vt_%s", vttablet.Keyspace), MaxWait)
	require.NoError(t, err, fmt.Sprintf("%s timed out for workflow %s on tablet %s.%s.%s", info, workflow, vttablet.Keyspace, vttablet.Shard, vttablet.Name))
}

func moveTables(t *testing.T, cell, workflow, sourceKs, targetKs, tables string) {
	if err := vc.VtctlClient.ExecuteCommand("MoveTables", "-cells="+cell, "-workflow="+workflow,
		"-tablet_types="+"master,replica,rdonly", sourceKs, targetKs, tables); err != nil {
		t.Fatalf("MoveTables command failed with %+v\n", err)
	}
}
func applyVSchema(t *testing.T, vschema, keyspace string) {
	err := vc.VtctlClient.ExecuteCommand("ApplyVSchema", "-vschema", vschema, keyspace)
	require.NoError(t, err)
}

func switchReadsDryRun(t *testing.T, cells, ksWorkflow string, dryRunResults []string) {
	output, err := vc.VtctlClient.ExecuteCommandWithOutput("SwitchReads", "-cells="+cells, "-tablet_type=replica", "-dry_run", ksWorkflow)
	require.NoError(t, err, fmt.Sprintf("SwitchReads DryRun Error: %s: %s", err, output))
	validateDryRunResults(t, output, dryRunResults)
}

func switchReads(t *testing.T, cells, ksWorkflow string) {
	var output string
	var err error
	output, err = vc.VtctlClient.ExecuteCommandWithOutput("SwitchReads", "-cells="+cells, "-tablet_type=rdonly", ksWorkflow)
	require.NoError(t, err, fmt.Sprintf("SwitchReads Error: %s: %s", err, output))
	output, err = vc.VtctlClient.ExecuteCommandWithOutput("SwitchReads", "-cells="+cells, "-tablet_type=replica", ksWorkflow)
	require.NoError(t, err, fmt.Sprintf("SwitchReads Error: %s: %s", err, output))
}

func switchWritesDryRun(t *testing.T, ksWorkflow string, dryRunResults []string) {
	output, err := vc.VtctlClient.ExecuteCommandWithOutput("SwitchWrites", "-dry_run", ksWorkflow)
	require.NoError(t, err, fmt.Sprintf("SwitchWrites DryRun Error: %s: %s", err, output))
	validateDryRunResults(t, output, dryRunResults)
}

func printSwitchWritesExtraDebug(t *testing.T, ksWorkflow, msg string) {
	// Temporary code: print lots of info for debugging occasional flaky failures in customer reshard in CI for multicell test
	debug := true
	if debug {
		fmt.Printf("------------------- START Extra debug info %s SwitchWrites %s\n", msg, ksWorkflow)
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
				fmt.Printf("\nTablet:%s.%s.%s.%d\nQuery: %s\n%+v\n\n",
					tab.Cell, tab.Keyspace, tab.Shard, tab.TabletUID, query, qr.Rows)
			}
		}
		fmt.Printf("------------------- END Extra debug info %s SwitchWrites %s\n", msg, ksWorkflow)
	}
}

func switchWrites(t *testing.T, ksWorkflow string, reverse bool) {
	const SwitchWritesTimeout = "91s" // max: 3 tablet picker 30s waits + 1
	output, err := vc.VtctlClient.ExecuteCommandWithOutput("SwitchWrites",
		"-filtered_replication_wait_time="+SwitchWritesTimeout, fmt.Sprintf("-reverse=%t", reverse), ksWorkflow)
	if output != "" {
		fmt.Printf("Output of SwitchWrites for %s:\n++++++\n%s\n--------\n", ksWorkflow, output)
	}
	//printSwitchWritesExtraDebug is useful when debugging failures in SwitchWrites due to corner cases/races
	_ = printSwitchWritesExtraDebug
	require.NoError(t, err, fmt.Sprintf("SwitchWrites Error: %s: %s", err, output))
}

func dropSourcesDryRun(t *testing.T, ksWorkflow string, renameTables bool, dryRunResults []string) {
	args := []string{"DropSources", "-dry_run"}
	if renameTables {
		args = append(args, "-rename_tables")
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
