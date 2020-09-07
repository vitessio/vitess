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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/wrangler"
)

var (
	vc              *VitessCluster
	vtgate          *cluster.VtgateProcess
	defaultCell     *Cell
	vtgateConn      *mysql.Conn
	defaultRdonly   int
	defaultReplicas int
	allCellNames    string
)

func init() {
	defaultRdonly = 0
	defaultReplicas = 1
}

func TestBasicVreplicationWorkflow(t *testing.T) {
	defaultCellName := "zone1"
	allCellNames = "zone1"
	vc = InitCluster(t, []string{defaultCellName})
	assert.NotNil(t, vc)

	defer vc.TearDown()

	defaultCell = vc.Cells[defaultCellName]
	vc.AddKeyspace(t, []*Cell{defaultCell}, "product", "0", initialProductVSchema, initialProductSchema, defaultReplicas, defaultRdonly, 100)
	vtgate = defaultCell.Vtgates[0]
	assert.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "product", "0"), 1)

	vtgateConn = getConnection(t, globalConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t)
	insertInitialData(t)
	shardCustomer(t, true, []*Cell{defaultCell}, defaultCellName)
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

	vc = InitCluster(t, cells)
	assert.NotNil(t, vc)
	defaultCellName := "zone1"
	defaultCell = vc.Cells[defaultCellName]

	defer vc.TearDown()

	cell1 := vc.Cells["zone1"]
	cell2 := vc.Cells["zone2"]
	vc.AddKeyspace(t, []*Cell{cell1, cell2}, "product", "0", initialProductVSchema, initialProductSchema, defaultReplicas, defaultRdonly, 100)

	vtgate = cell1.Vtgates[0]
	assert.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "product", "0"), 1)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", "product", "0"), 2)

	vtgateConn = getConnection(t, globalConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t)
	insertInitialData(t)
	shardCustomer(t, true, []*Cell{cell1, cell2}, cell2.Name)

	insertMoreCustomers(t, 16)
	reshardCustomer2to4Split(t, []*Cell{cell1, cell2}, cell2.Name)
	validateCount(t, vtgateConn, "customer:-40", "customer", 5)
	validateCount(t, vtgateConn, "customer:40-80", "customer", 5)
	validateCount(t, vtgateConn, "customer:80-c0", "customer", 6)
	validateCount(t, vtgateConn, "customer:c0-", "customer", 5)
}

func TestCellAliasVreplicationWorkflow(t *testing.T) {
	cells := []string{"zone1", "zone2"}

	vc = InitCluster(t, cells)
	assert.NotNil(t, vc)
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
	assert.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "product", "0"), 1)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", "product", "0"), 2)

	vtgateConn = getConnection(t, globalConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t)
	insertInitialData(t)
	shardCustomer(t, true, []*Cell{cell1, cell2}, "alias")

	insertMoreCustomers(t, 16)
	reshardCustomer2to4Split(t, []*Cell{cell1, cell2}, "alias")
	validateCount(t, vtgateConn, "customer:-40", "customer", 5)
	validateCount(t, vtgateConn, "customer:40-80", "customer", 5)
	validateCount(t, vtgateConn, "customer:80-c0", "customer", 6)
	validateCount(t, vtgateConn, "customer:c0-", "customer", 5)
}

func insertInitialData(t *testing.T) {
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

func shardCustomer(t *testing.T, testReverse bool, cells []*Cell, sourceCellOrAlias string) {
	if _, err := vc.AddKeyspace(t, cells, "customer", "-80,80-", customerVSchema, customerSchema, defaultReplicas, defaultRdonly, 200); err != nil {
		t.Fatal(err)
	}
	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "customer", "-80"), 1); err != nil {
		t.Fatal(err)
	}
	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "customer", "80-"), 1); err != nil {
		t.Fatal(err)
	}

	if err := vc.VtctlClient.ExecuteCommand("MoveTables", "-cells="+sourceCellOrAlias, "-workflow=p2c",
		"-tablet_types="+"replica,rdonly", "product", "customer", "customer"); err != nil {
		t.Fatalf("MoveTables command failed with %+v\n", err)
	}

	// Assume we are operating on first cell
	defaultCell := cells[0]

	customerTab1 := vc.Cells[defaultCell.Name].Keyspaces["customer"].Shards["-80"].Tablets["zone1-200"].Vttablet
	customerTab2 := vc.Cells[defaultCell.Name].Keyspaces["customer"].Shards["80-"].Tablets["zone1-300"].Vttablet

	if vc.WaitForVReplicationToCatchup(customerTab1, "p2c", "vt_customer", 1*time.Second) != nil {
		t.Fatal("MoveTables timed out for customer.p2c -80")

	}
	if vc.WaitForVReplicationToCatchup(customerTab2, "p2c", "vt_customer", 1*time.Second) != nil {
		t.Fatal("MoveTables timed out for customer.p2c 80-")
	}

	productTab := vc.Cells[defaultCell.Name].Keyspaces["product"].Shards["0"].Tablets["zone1-100"].Vttablet
	productTabReplica := vc.Cells[defaultCell.Name].Keyspaces["product"].Shards["0"].Tablets["zone1-101"].Vttablet
	query := "select * from customer"
	assert.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productTab, "product", query, query))
	insertQuery1 := "insert into customer(cid, name) values(1001, 'tempCustomer1')"
	matchInsertQuery1 := "insert into customer(cid, name) values (:vtg1, :vtg2)"
	assert.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productTab, "product", insertQuery1, matchInsertQuery1))
	vdiff(t, "customer.p2c")
	var output string
	var err error

	if output, err = vc.VtctlClient.ExecuteCommandWithOutput("SwitchReads", "-cells="+allCellNames, "-tablet_type=rdonly", "customer.p2c"); err != nil {
		t.Fatalf("SwitchReads error: %s\n", output)
	}
	want := dryRunResultsReadCustomerShard
	if output, err = vc.VtctlClient.ExecuteCommandWithOutput("SwitchReads", "-cells="+allCellNames, "-tablet_type=replica", "-dry_run", "customer.p2c"); err != nil {
		t.Fatalf("SwitchReads Dry Run error: %s\n", output)
	}
	validateDryRunResults(t, output, want)
	if output, err = vc.VtctlClient.ExecuteCommandWithOutput("SwitchReads", "-cells="+allCellNames, "-tablet_type=replica", "customer.p2c"); err != nil {
		t.Fatalf("SwitchReads error: %s\n", output)
	}

	assert.False(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productTabReplica, "customer", query, query))
	assert.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productTab, "customer", query, query))
	want = dryRunResultsSwitchWritesCustomerShard
	if output, err = vc.VtctlClient.ExecuteCommandWithOutput("SwitchWrites", "-dry_run", "customer.p2c"); err != nil {
		t.Fatalf("SwitchWrites error: %s\n", output)
	}
	validateDryRunResults(t, output, want)

	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("SwitchWrites", "customer.p2c"); err != nil {
		t.Fatalf("SwitchWrites error: %s\n", output)
	}
	insertQuery2 := "insert into customer(name) values('tempCustomer2')"
	matchInsertQuery2 := "insert into customer(name, cid) values (:vtg1, :_cid0)"
	assert.False(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productTab, "customer", insertQuery2, matchInsertQuery2))

	insertQuery2 = "insert into customer(name, cid) values('tempCustomer3', 101)" //ID 101, hence due to reverse_bits in shard 80-
	assert.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, customerTab2, "customer", insertQuery2, matchInsertQuery2))

	insertQuery2 = "insert into customer(name, cid) values('tempCustomer4', 102)" //ID 102, hence due to reverse_bits in shard -80
	assert.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, customerTab1, "customer", insertQuery2, matchInsertQuery2))

	if testReverse {
		//Reverse Replicate
		if output, err := vc.VtctlClient.ExecuteCommandWithOutput("SwitchReads", "-cells="+allCellNames, "-tablet_type=rdonly", "product.p2c_reverse"); err != nil {
			t.Fatalf("SwitchReads error: %s\n", output)
		}
		if output, err := vc.VtctlClient.ExecuteCommandWithOutput("SwitchReads", "-cells="+allCellNames, "-tablet_type=replica", "product.p2c_reverse"); err != nil {
			t.Fatalf("SwitchReads error: %s\n", output)
		}
		if output, err := vc.VtctlClient.ExecuteCommandWithOutput("SwitchWrites", "product.p2c_reverse"); err != nil {
			t.Fatalf("SwitchWrites error: %s\n", output)
		}
		insertQuery1 = "insert into customer(cid, name) values(1002, 'tempCustomer5')"
		assert.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productTab, "product", insertQuery1, matchInsertQuery1))
		insertQuery1 = "insert into customer(cid, name) values(1003, 'tempCustomer6')"
		assert.False(t, validateThatQueryExecutesOnTablet(t, vtgateConn, customerTab1, "customer", insertQuery1, matchInsertQuery1))
		insertQuery1 = "insert into customer(cid, name) values(1004, 'tempCustomer7')"
		assert.False(t, validateThatQueryExecutesOnTablet(t, vtgateConn, customerTab2, "customer", insertQuery1, matchInsertQuery1))

		//Go forward again
		if output, err := vc.VtctlClient.ExecuteCommandWithOutput("SwitchReads", "-cells="+allCellNames, "-tablet_type=rdonly", "customer.p2c"); err != nil {
			t.Fatalf("SwitchReads error: %s\n", output)
		}
		if output, err := vc.VtctlClient.ExecuteCommandWithOutput("SwitchReads", "-cells="+allCellNames, "-tablet_type=replica", "customer.p2c"); err != nil {
			t.Fatalf("SwitchReads error: %s\n", output)
		}
		if output, err := vc.VtctlClient.ExecuteCommandWithOutput("SwitchWrites", "customer.p2c"); err != nil {
			t.Fatalf("SwitchWrites error: %s\n", output)
		}
		want = dryRunResultsDropSourcesDropCustomerShard
		if output, err = vc.VtctlClient.ExecuteCommandWithOutput("DropSources", "-dry_run", "customer.p2c"); err != nil {
			t.Fatalf("DropSources dry run error: %s\n", output)
		}
		validateDryRunResults(t, output, want)

		want = dryRunResultsDropSourcesRenameCustomerShard
		if output, err = vc.VtctlClient.ExecuteCommandWithOutput("DropSources", "-dry_run", "-rename_tables", "customer.p2c"); err != nil {
			t.Fatalf("DropSources dry run with rename error: %s\n", output)
		}
		validateDryRunResults(t, output, want)

		var exists bool
		if exists, err = checkIfBlacklistExists(t, vc, "product:0", "customer"); err != nil {
			t.Fatal("Error getting blacklist for customer:0")
		}
		assert.True(t, exists)

		if output, err = vc.VtctlClient.ExecuteCommandWithOutput("DropSources", "customer.p2c"); err != nil {
			t.Fatalf("DropSources error: %s\n", output)
		}

		if exists, err = checkIfBlacklistExists(t, vc, "product:0", "customer"); err != nil {
			t.Fatal("Error getting blacklist for customer:0")
		}
		assert.False(t, exists)

		for _, shard := range strings.Split("-80,80-", ",") {
			expectNumberOfStreams(t, vtgateConn, "shardCustomerTargetStreams", "p2c", "customer:"+shard, 0)
		}

		expectNumberOfStreams(t, vtgateConn, "shardCustomerReverseStreams", "p2c_reverse", "product:0", 0)

		var found bool
		found, err = checkIfTableExists(t, vc, "zone1-100", "customer")
		assert.NoError(t, err, "Customer table not deleted from zone1-100")
		assert.False(t, found)

		found, err = checkIfTableExists(t, vc, "zone1-200", "customer")
		assert.NoError(t, err, "Customer table not deleted from zone1-200")
		assert.True(t, found)

		insertQuery2 = "insert into customer(name, cid) values('tempCustomer8', 103)" //ID 103, hence due to reverse_bits in shard 80-
		assert.False(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productTab, "customer", insertQuery2, matchInsertQuery2))
		insertQuery2 = "insert into customer(name, cid) values('tempCustomer10', 104)" //ID 105, hence due to reverse_bits in shard -80
		assert.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, customerTab1, "customer", insertQuery2, matchInsertQuery2))
		insertQuery2 = "insert into customer(name, cid) values('tempCustomer9', 105)" //ID 104, hence due to reverse_bits in shard 80-
		assert.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, customerTab2, "customer", insertQuery2, matchInsertQuery2))

		execVtgateQuery(t, vtgateConn, "customer", "delete from customer where name like 'tempCustomer%'")
		assert.Empty(t, validateCountInTablet(t, customerTab1, "customer", "customer", 1))
		assert.Empty(t, validateCountInTablet(t, customerTab2, "customer", "customer", 2))
		assert.Empty(t, validateCount(t, vtgateConn, "customer", "customer.customer", 3))

		query = "insert into customer (name, cid) values('george', 5)"
		execVtgateQuery(t, vtgateConn, "customer", query)
		assert.Empty(t, validateCountInTablet(t, customerTab1, "customer", "customer", 1))
		assert.Empty(t, validateCountInTablet(t, customerTab2, "customer", "customer", 3))
		assert.Empty(t, validateCount(t, vtgateConn, "customer", "customer.customer", 4))
	}
}

func reshardCustomer2to4Split(t *testing.T, cells []*Cell, sourceCellOrAlias string) {
	ksName := "customer"
	counts := map[string]int{"zone1-600": 4, "zone1-700": 5, "zone1-800": 6, "zone1-900": 5}
	reshard(t, ksName, "customer", "c2c4", "-80,80-", "-40,40-80,80-c0,c0-", 600, counts, nil, cells, sourceCellOrAlias)
	assert.Empty(t, validateCount(t, vtgateConn, ksName, "customer", 20))
	query := "insert into customer (name) values('yoko')"
	execVtgateQuery(t, vtgateConn, ksName, query)
	assert.Empty(t, validateCount(t, vtgateConn, ksName, "customer", 21))
}

func reshardMerchant2to3SplitMerge(t *testing.T) {
	ksName := "merchant"
	counts := map[string]int{"zone1-1600": 0, "zone1-1700": 2, "zone1-1800": 0}
	reshard(t, ksName, "merchant", "m2m3", "-80,80-", "-40,40-c0,c0-", 1600, counts, dryrunresultsswitchwritesM2m3, nil, "")
	assert.Empty(t, validateCount(t, vtgateConn, ksName, "merchant", 2))
	query := "insert into merchant (mname, category) values('amazon', 'electronics')"
	execVtgateQuery(t, vtgateConn, ksName, query)
	assert.Empty(t, validateCount(t, vtgateConn, ksName, "merchant", 3))

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
	assert.False(t, found)
	found, err = checkIfTableExists(t, vc, "zone1-1600", "merchant")
	assert.NoError(t, err, "Merchant table not found in zone1-1600")
	assert.True(t, found)

}

func reshardMerchant3to1Merge(t *testing.T) {
	ksName := "merchant"
	counts := map[string]int{"zone1-2000": 3}
	reshard(t, ksName, "merchant", "m3m1", "-40,40-c0,c0-", "0", 2000, counts, nil, nil, "")
	assert.Empty(t, validateCount(t, vtgateConn, ksName, "merchant", 3))
	query := "insert into merchant (mname, category) values('flipkart', 'electronics')"
	execVtgateQuery(t, vtgateConn, ksName, query)
	assert.Empty(t, validateCount(t, vtgateConn, ksName, "merchant", 4))
}

func reshardCustomer3to2SplitMerge(t *testing.T) { //-40,40-80,80-c0 => merge/split, c0- stays the same  ending up with 3
	ksName := "customer"
	counts := map[string]int{"zone1-1000": 7, "zone1-1100": 9, "zone1-1200": 5}
	reshard(t, ksName, "customer", "c4c3", "-40,40-80,80-c0", "-60,60-c0", 1000, counts, nil, nil, "")
}

func reshardCustomer3to1Merge(t *testing.T) { //to unsharded
	ksName := "customer"
	counts := map[string]int{"zone1-1500": 21}
	reshard(t, ksName, "customer", "c3c1", "-60,60-c0,c0-", "0", 1500, counts, nil, nil, "")
}

func reshard(t *testing.T, ksName string, tableName string, workflow string, sourceShards string, targetShards string, tabletIDBase int, counts map[string]int, dryRunResultswitchWrites []string, cells []*Cell, sourceCellOrAlias string) {
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
			if vc.WaitForVReplicationToCatchup(tab, workflow, "vt_"+ksName, 10*time.Second) != nil {
				t.Fatal("Reshard timed out")
			}
		} else {
			fmt.Printf("Not waiting for vrepl to catch up on %s since it is NOT a target shard\n", tab.Shard)
			continue
		}
	}
	vdiff(t, ksWorkflow)
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("SwitchReads", "-cells="+allCellNames, "-tablet_type=rdonly", ksWorkflow); err != nil {
		t.Fatalf("SwitchReads error: %s\n", output)
	}
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("SwitchReads", "-cells="+allCellNames, "-tablet_type=replica", ksWorkflow); err != nil {
		t.Fatalf("SwitchReads error: %s\n", output)
	}

	if dryRunResultswitchWrites != nil {
		var output string
		var err error
		if output, err = vc.VtctlClient.ExecuteCommandWithOutput("SwitchWrites", "-dry_run", ksWorkflow); err != nil {
			t.Fatalf("SwitchWrites dry run error: %s\n", output)
		}
		validateDryRunResults(t, output, dryRunResultswitchWrites)
	}
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("SwitchWrites", ksWorkflow); err != nil {
		t.Fatalf("SwitchWrites error: %s\n", output)
	}
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("DropSources", ksWorkflow); err != nil {
		t.Fatalf("DropSources error: %s\n", output)
	}

	for tabletName, count := range counts {
		if tablets[tabletName] == nil {
			continue
		}
		assert.Empty(t, validateCountInTablet(t, tablets[tabletName], ksName, tableName, count))
	}
}

func shardOrders(t *testing.T) {
	if err := vc.VtctlClient.ExecuteCommand("ApplyVSchema", "-vschema", ordersVSchema, "customer"); err != nil {
		t.Fatal(err)
	}
	if err := vc.VtctlClient.ExecuteCommand("MoveTables", "-cells="+defaultCell.Name, "-workflow=o2c",
		"-tablet_types="+"replica,rdonly", "product", "customer", "orders"); err != nil {
		t.Fatal(err)
	}
	customerTab1 := vc.Cells[defaultCell.Name].Keyspaces["customer"].Shards["-80"].Tablets["zone1-200"].Vttablet
	customerTab2 := vc.Cells[defaultCell.Name].Keyspaces["customer"].Shards["80-"].Tablets["zone1-300"].Vttablet
	if vc.WaitForVReplicationToCatchup(customerTab1, "o2c", "vt_customer", 1*time.Second) != nil {
		assert.Fail(t, "MoveTables timed out for customer.o2c -80")

	}
	if vc.WaitForVReplicationToCatchup(customerTab2, "o2c", "vt_customer", 1*time.Second) != nil {
		assert.Fail(t, "MoveTables timed out for customer.o2c 80-")
	}

	vdiff(t, "customer.o2c")
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("SwitchReads", "-cells="+allCellNames, "-tablet_type=rdonly", "customer.o2c"); err != nil {
		t.Fatalf("SwitchReads error: %s\n", output)
	}
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("SwitchReads", "-cells="+allCellNames, "-tablet_type=replica", "customer.o2c"); err != nil {
		t.Fatalf("SwitchReads error: %s\n", output)
	}
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("SwitchWrites", "customer.o2c"); err != nil {
		t.Fatalf("SwitchWrites error: %s\n", output)
	}
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("DropSources", "customer.o2c"); err != nil {
		t.Fatalf("DropSources error: %s\n", output)
	}

	assert.Empty(t, validateCountInTablet(t, customerTab1, "customer", "orders", 1))
	assert.Empty(t, validateCountInTablet(t, customerTab2, "customer", "orders", 2))
	assert.Empty(t, validateCount(t, vtgateConn, "customer", "orders", 3))
}

func shardMerchant(t *testing.T) {
	if _, err := vc.AddKeyspace(t, []*Cell{defaultCell}, "merchant", "-80,80-", merchantVSchema, "", defaultReplicas, defaultRdonly, 400); err != nil {
		t.Fatal(err)
	}
	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "merchant", "-80"), 1); err != nil {
		t.Fatal(err)
	}
	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "merchant", "80-"), 1); err != nil {
		t.Fatal(err)
	}
	if err := vc.VtctlClient.ExecuteCommand("MoveTables", "-cells="+defaultCell.Name, "-workflow=p2m",
		"-tablet_types="+"replica,rdonly", "product", "merchant", "merchant"); err != nil {
		t.Fatal(err)
	}

	merchantTab1 := vc.Cells[defaultCell.Name].Keyspaces["merchant"].Shards["-80"].Tablets["zone1-400"].Vttablet
	merchantTab2 := vc.Cells[defaultCell.Name].Keyspaces["merchant"].Shards["80-"].Tablets["zone1-500"].Vttablet
	if vc.WaitForVReplicationToCatchup(merchantTab1, "p2m", "vt_merchant", 1*time.Second) != nil {
		t.Fatal("MoveTables timed out for merchant.p2m -80")

	}
	if vc.WaitForVReplicationToCatchup(merchantTab2, "p2m", "vt_merchant", 1*time.Second) != nil {
		t.Fatal("MoveTables timed out for merchant.p2m 80-")
	}

	vdiff(t, "merchant.p2m")
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("SwitchReads", "-cells="+allCellNames, "-tablet_type=rdonly", "merchant.p2m"); err != nil {
		t.Fatalf("SwitchReads error: %s\n", output)
	}
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("SwitchReads", "-cells="+allCellNames, "-tablet_type=replica", "merchant.p2m"); err != nil {
		t.Fatalf("SwitchReads error: %s\n", output)
	}
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("SwitchWrites", "merchant.p2m"); err != nil {
		t.Fatalf("SwitchWrites error: %s\n", output)
	}
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("DropSources", "merchant.p2m"); err != nil {
		t.Fatalf("DropSources error: %s\n", output)
	}

	assert.Empty(t, validateCountInTablet(t, merchantTab1, "merchant", "merchant", 1))
	assert.Empty(t, validateCountInTablet(t, merchantTab2, "merchant", "merchant", 1))
	assert.Empty(t, validateCount(t, vtgateConn, "merchant", "merchant", 2))

}

func vdiff(t *testing.T, workflow string) {
	output, err := vc.VtctlClient.ExecuteCommandWithOutput("VDiff", "-format", "json", workflow)
	fmt.Printf("vdiff err: %+v, output: %+v\n", err, output)
	assert.Nil(t, err)
	assert.NotNil(t, output)
	diffReports := make([]*wrangler.DiffReport, 0)
	err = json.Unmarshal([]byte(output), &diffReports)
	assert.Nil(t, err)
	//fmt.Printf("Number of diffReports is %d\n", len(diffReports))
	if len(diffReports) < 1 {
		t.Fatal("VDiff did not return a valid json response " + output + "\n")
	}
	assert.True(t, len(diffReports) > 0)
	for key, diffReport := range diffReports {
		if diffReport.ProcessedRows != diffReport.MatchingRows {
			t.Errorf("vdiff error for %d : %#v\n", key, diffReport)
		}
	}
}

func materializeProduct(t *testing.T) {
	workflow := "cproduct"
	if err := vc.VtctlClient.ExecuteCommand("ApplyVSchema", "-vschema", materializeProductVSchema, "customer"); err != nil {
		t.Fatal(err)
	}
	if err := vc.VtctlClient.ExecuteCommand("Materialize", materializeProductSpec); err != nil {
		t.Fatal(err)
	}
	customerTablets := vc.getVttabletsInKeyspace(t, defaultCell, "customer", "master")
	for _, tab := range customerTablets {
		if vc.WaitForVReplicationToCatchup(tab, workflow, "vt_customer", 3*time.Second) != nil {
			t.Fatal("Materialize timed out")
		}
	}
	for _, tab := range customerTablets {
		assert.Empty(t, validateCountInTablet(t, tab, "customer", "cproduct", 2))
	}
}

func materializeSales(t *testing.T) {
	if err := vc.VtctlClient.ExecuteCommand("ApplyVSchema", "-vschema", materializeSalesVSchema, "product"); err != nil {
		t.Fatal(err)
	}
	if err := vc.VtctlClient.ExecuteCommand("Materialize", materializeSalesSpec); err != nil {
		t.Fatal(err)
	}
	productTab := vc.Cells[defaultCell.Name].Keyspaces["product"].Shards["0"].Tablets["zone1-100"].Vttablet
	if vc.WaitForVReplicationToCatchup(productTab, "sales", "vt_product", 3*time.Second) != nil {
		assert.Fail(t, "Materialize timed out for product.sales")
	}
	assert.Empty(t, validateCount(t, vtgateConn, "product", "sales", 2))
	assert.Empty(t, validateQuery(t, vtgateConn, "product:0", "select kount, amount from sales",
		`[[INT32(1) INT32(10)] [INT32(2) INT32(35)]]`))
}

func materializeMerchantSales(t *testing.T) {
	workflow := "msales"
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("Materialize", materializeMerchantSalesSpec); err != nil {
		fmt.Printf("Materialize MerchantSales error is %+v", output)
		t.Fatal(err)
	}
	merchantTablets := vc.getVttabletsInKeyspace(t, defaultCell, "merchant", "master")
	for _, tab := range merchantTablets {
		if vc.WaitForVReplicationToCatchup(tab, workflow, "vt_merchant", 1*time.Second) != nil {
			t.Fatal("Materialize timed out")
		}
	}
	assert.Empty(t, validateCountInTablet(t, merchantTablets["zone1-400"], "merchant", "msales", 1))
	assert.Empty(t, validateCountInTablet(t, merchantTablets["zone1-500"], "merchant", "msales", 1))
	assert.Empty(t, validateCount(t, vtgateConn, "merchant", "msales", 2))
}

func materializeMerchantOrders(t *testing.T) {
	workflow := "morders"
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("ApplyVSchema", "-vschema", merchantOrdersVSchema, "merchant"); err != nil {
		fmt.Printf("ApplyVSchema error: %+v", output)
		t.Fatal(err)
	}
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("Materialize", materializeMerchantOrdersSpec); err != nil {
		fmt.Printf("MerchantOrders error is %+v", output)
		t.Fatal(err)
	}
	merchantTablets := vc.getVttabletsInKeyspace(t, defaultCell, "merchant", "master")
	for _, tab := range merchantTablets {
		if vc.WaitForVReplicationToCatchup(tab, workflow, "vt_merchant", 1*time.Second) != nil {
			t.Fatal("Materialize timed out")
		}
	}
	assert.Empty(t, validateCountInTablet(t, merchantTablets["zone1-400"], "merchant", "morders", 2))
	assert.Empty(t, validateCountInTablet(t, merchantTablets["zone1-500"], "merchant", "morders", 1))
	assert.Empty(t, validateCount(t, vtgateConn, "merchant", "morders", 3))
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

func iterateTablets(t *testing.T, f func(t *testing.T, tablet *Tablet)) {
	for _, cell := range vc.Cells {
		for _, ks := range cell.Keyspaces {
			for _, shard := range ks.Shards {
				for _, tablet := range shard.Tablets {
					f(t, tablet)
				}
			}
		}
	}
}

func iterateCells(t *testing.T, f func(t *testing.T, cell *Cell)) {
	for _, cell := range vc.Cells {
		f(t, cell)
	}
}

// Should check health of key components vtgate, vtctld, tablets, look for etcd keys
func verifyClusterHealth(t *testing.T) {
	iterateCells(t, checkVtgateHealth)
	iterateTablets(t, checkTabletHealth)
}
