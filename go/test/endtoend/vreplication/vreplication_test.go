package vreplication

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/wrangler"
)

var (
	vc              *VitessCluster
	vtgate          *cluster.VtgateProcess
	cell            *Cell
	vtgateConn      *mysql.Conn
	defaultRdonly   int
	defaultReplicas int
)

func init() {
	defaultRdonly = 0
	defaultReplicas = 1
}

func TestBasicVreplicationWorkflow(t *testing.T) {

	cellName := "zone1"

	vc = InitCluster(t, cellName)
	assert.NotNil(t, vc)

	if true {
		defer vc.TearDown()
	}
	cell = vc.Cells[cellName]

	vc.AddKeyspace(t, cell, "product", "0", initialProductVSchema, initialProductSchema, defaultReplicas, defaultRdonly, 100)

	vtgate = cell.Vtgates[0]
	assert.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "product", "0"), 1)

	vtgateConn = getConnection(t, globalConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t)
	insertInitialData(t)
	shardCustomer(t, true)
	shardOrders(t)

	shardMerchant(t)
	materializeProduct(t)
	materializeMerchantOrders(t)
	materializeSales(t)
	materializeMerchantSales(t)

	reshardMerchant2to3SplitMerge(t)
	reshardMerchant3to1Merge(t)

	insertMoreCustomers(t, 16)
	reshardCustomer2to4Split(t)
	expectNumberOfStreams(t, "Customer2to4", "sales", "product:0", 4)
	reshardCustomer3to2SplitMerge(t)
	expectNumberOfStreams(t, "Customer3to2", "sales", "product:0", 3)
	reshardCustomer3to1Merge(t)
	expectNumberOfStreams(t, "Customer3to1", "sales", "product:0", 1)
}

func expectNumberOfStreams(t *testing.T, name string, workflow string, database string, want int) {
	query := fmt.Sprintf("select count(*) from _vt.vreplication where workflow='%s';", workflow)
	result := validateQuery(t, vtgateConn, database, query, fmt.Sprintf(`[[INT64(%d)]]`, want))
	if result != "" {
		t.Fatalf("Sales streams not migrated after 2to4: %s\n", result)
	}
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
		i += 1
		sql += fmt.Sprintf("('customer%d')", i)
		if i != numCustomers {
			sql += ","
		}
	}
	execVtgateQuery(t, vtgateConn, "customer", sql)
}

func shardCustomer(t *testing.T, testReverse bool) {
	if _, err := vc.AddKeyspace(t, cell, "customer", "-80,80-", customerVSchema, customerSchema, defaultReplicas, defaultRdonly, 200); err != nil {
		t.Fatal(err)
	}
	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "customer", "-80"), 1); err != nil {
		t.Fatal(err)
	}
	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "customer", "80-"), 1); err != nil {
		t.Fatal(err)
	}

	if err := vc.VtctlClient.ExecuteCommand("Migrate", "-cell="+cell.Name, "-workflow=p2c",
		"-tablet_types="+"replica,rdonly", "product", "customer", "customer"); err != nil {
		t.Fatalf("Migrate command failed with %+v\n", err)
	}

	customerTab1 := vc.Cells[cell.Name].Keyspaces["customer"].Shards["-80"].Tablets["zone1-200"].Vttablet
	customerTab2 := vc.Cells[cell.Name].Keyspaces["customer"].Shards["80-"].Tablets["zone1-300"].Vttablet

	if vc.WaitForVReplicationToCatchup(customerTab1, "p2c", "vt_customer", 1*time.Second) != nil {
		t.Fatal("Migrate timed out for customer.p2c -80")

	}
	if vc.WaitForVReplicationToCatchup(customerTab2, "p2c", "vt_customer", 1*time.Second) != nil {
		t.Fatal("Migrate timed out for customer.p2c 80-")
	}

	productTab := vc.Cells[cell.Name].Keyspaces["product"].Shards["0"].Tablets["zone1-100"].Vttablet
	productTabReplica := vc.Cells[cell.Name].Keyspaces["product"].Shards["0"].Tablets["zone1-101"].Vttablet
	query := "select * from customer"
	assert.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productTab, "product", query, query))
	insertQuery1 := "insert into customer(cid, name) values(1001, 'tempCustomer1')"
	matchInsertQuery1 := "insert into customer(cid, name) values (:vtg1, :vtg2)"
	assert.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productTab, "product", insertQuery1, matchInsertQuery1))
	vdiff(t, "customer.p2c")
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("MigrateReads", "-cells="+cell.Name, "-tablet_type=rdonly", "customer.p2c"); err != nil {
		t.Fatalf("MigrateReads error: %s\n", output)
	}
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("MigrateReads", "-cells="+cell.Name, "-tablet_type=replica", "customer.p2c"); err != nil {
		t.Fatalf("MigrateReads error: %s\n", output)
	}

	assert.False(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productTabReplica, "customer", query, query))
	assert.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productTab, "customer", query, query))
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("MigrateWrites", "customer.p2c"); err != nil {
		t.Fatalf("MigrateWrites error: %s\n", output)
	}
	insertQuery2 := "insert into customer(name) values('tempCustomer2')"
	matchInsertQuery2 := "insert into customer(name, cid) values (:vtg1, :_cid0)"
	assert.False(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productTab, "customer", insertQuery2, matchInsertQuery2))
	insertQuery2 = "insert into customer(name) values('tempCustomer3')" //ID 101, hence due to reverse_bits in shard 80-
	assert.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, customerTab2, "customer", insertQuery2, matchInsertQuery2))
	insertQuery2 = "insert into customer(name) values('tempCustomer4')" //ID 102, hence due to reverse_bits in shard -80
	assert.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, customerTab1, "customer", insertQuery2, matchInsertQuery2))

	if testReverse {
		//Reverse Replicate
		if output, err := vc.VtctlClient.ExecuteCommandWithOutput("MigrateReads", "-cells="+cell.Name, "-tablet_type=rdonly", "product.p2c_reverse"); err != nil {
			t.Fatalf("MigrateReads error: %s\n", output)
		}
		if output, err := vc.VtctlClient.ExecuteCommandWithOutput("MigrateReads", "-cells="+cell.Name, "-tablet_type=replica", "product.p2c_reverse"); err != nil {
			t.Fatalf("MigrateReads error: %s\n", output)
		}
		if output, err := vc.VtctlClient.ExecuteCommandWithOutput("MigrateWrites", "product.p2c_reverse"); err != nil {
			t.Fatalf("MigrateWrites error: %s\n", output)
		}
		insertQuery1 = "insert into customer(cid, name) values(1002, 'tempCustomer5')"
		assert.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productTab, "product", insertQuery1, matchInsertQuery1))
		insertQuery1 = "insert into customer(cid, name) values(1003, 'tempCustomer6')"
		assert.False(t, validateThatQueryExecutesOnTablet(t, vtgateConn, customerTab1, "customer", insertQuery1, matchInsertQuery1))
		insertQuery1 = "insert into customer(cid, name) values(1004, 'tempCustomer7')"
		assert.False(t, validateThatQueryExecutesOnTablet(t, vtgateConn, customerTab2, "customer", insertQuery1, matchInsertQuery1))

		//Go forward again
		if output, err := vc.VtctlClient.ExecuteCommandWithOutput("MigrateReads", "-cells="+cell.Name, "-tablet_type=rdonly", "customer.p2c"); err != nil {
			t.Fatalf("MigrateReads error: %s\n", output)
		}
		if output, err := vc.VtctlClient.ExecuteCommandWithOutput("MigrateReads", "-cells="+cell.Name, "-tablet_type=replica", "customer.p2c"); err != nil {
			t.Fatalf("MigrateReads error: %s\n", output)
		}
		if output, err := vc.VtctlClient.ExecuteCommandWithOutput("MigrateWrites", "customer.p2c"); err != nil {
			t.Fatalf("MigrateWrites error: %s\n", output)
		}
		insertQuery2 = "insert into customer(name) values('tempCustomer8')" //ID 103, hence due to reverse_bits in shard 80-
		assert.False(t, validateThatQueryExecutesOnTablet(t, vtgateConn, productTab, "customer", insertQuery2, matchInsertQuery2))
		insertQuery2 = "insert into customer(name) values('tempCustomer9')" //ID 104, hence due to reverse_bits in shard 80-
		assert.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, customerTab1, "customer", insertQuery2, matchInsertQuery2))
		insertQuery2 = "insert into customer(name) values('tempCustomer10')" //ID 105, hence due to reverse_bits in shard -80
		assert.True(t, validateThatQueryExecutesOnTablet(t, vtgateConn, customerTab2, "customer", insertQuery2, matchInsertQuery2))

		execVtgateQuery(t, vtgateConn, "customer", "delete from customer where name like 'tempCustomer%'")
		assert.Empty(t, validateCountInTablet(t, customerTab1, "customer", "customer", 1))
		assert.Empty(t, validateCountInTablet(t, customerTab2, "customer", "customer", 2))
		assert.Empty(t, validateCount(t, vtgateConn, "customer", "customer.customer", 3))

		query = "insert into customer (name) values('george')"
		execVtgateQuery(t, vtgateConn, "customer", query)
		assert.Empty(t, validateCountInTablet(t, customerTab1, "customer", "customer", 2))
		assert.Empty(t, validateCountInTablet(t, customerTab2, "customer", "customer", 2))
		assert.Empty(t, validateCount(t, vtgateConn, "customer", "customer.customer", 4))
	}
	removeTabletControls(t, "product/0")
}

func reshardCustomer2to4Split(t *testing.T) {
	ksName := "customer"
	counts := map[string]int{"zone1-600": 4, "zone1-700": 6, "zone1-800": 5, "zone1-900": 5}
	reshard(t, ksName, "customer", "c2c4", "-80,80-", "-40,40-80,80-c0,c0-", 600, counts)
	assert.Empty(t, validateCount(t, vtgateConn, ksName, "customer", 20))
	query := "insert into customer (name) values('yoko')"
	execVtgateQuery(t, vtgateConn, ksName, query)
	assert.Empty(t, validateCount(t, vtgateConn, ksName, "customer", 21))
}

func reshardMerchant2to3SplitMerge(t *testing.T) {
	ksName := "merchant"
	counts := map[string]int{"zone1-1600": 0, "zone1-1700": 2, "zone1-1800": 0}
	reshard(t, ksName, "merchant", "m2m3", "-80,80-", "-40,40-c0,c0-", 1600, counts)
	assert.Empty(t, validateCount(t, vtgateConn, ksName, "merchant", 2))
	query := "insert into merchant (mname, category) values('amazon', 'electronics')"
	execVtgateQuery(t, vtgateConn, ksName, query)
	assert.Empty(t, validateCount(t, vtgateConn, ksName, "merchant", 3))
}

func reshardMerchant3to1Merge(t *testing.T) {
	ksName := "merchant"
	counts := map[string]int{"zone1-2000": 3}
	reshard(t, ksName, "merchant", "m3m1", "-40,40-c0,c0-", "0", 2000, counts)
	assert.Empty(t, validateCount(t, vtgateConn, ksName, "merchant", 3))
	query := "insert into merchant (mname, category) values('flipkart', 'electronics')"
	execVtgateQuery(t, vtgateConn, ksName, query)
	assert.Empty(t, validateCount(t, vtgateConn, ksName, "merchant", 4))
}

func reshardCustomer3to2SplitMerge(t *testing.T) { //-40,40-80,80-c0 => merge/split, c0- stays the same  ending up with 3
	ksName := "customer"
	counts := map[string]int{"zone1-600": 5, "zone1-700": 6, "zone1-800": 5, "zone1-900": 5}
	reshard(t, ksName, "customer", "c4c3", "-40,40-80,80-c0", "-60,60-c0", 1000, counts)
}

func reshardCustomer3to1Merge(t *testing.T) { //to unsharded
	ksName := "customer"
	counts := map[string]int{"zone1-1500": 21}
	reshard(t, ksName, "customer", "c3c1", "-60,60-c0,c0-", "0", 1500, counts)
}

func reshard(t *testing.T, ksName string, tableName string, workflow string, sourceShards string, targetShards string, tabletIDBase int, counts map[string]int) {
	ksWorkflow := ksName + "." + workflow
	keyspace := vc.Cells[cell.Name].Keyspaces[ksName]
	if err := vc.AddShards(t, cell, keyspace, targetShards, defaultReplicas, defaultRdonly, tabletIDBase); err != nil {
		t.Fatalf(err.Error())
	}
	arrShardNames := strings.Split(targetShards, ",")

	for _, shardName := range arrShardNames {
		if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", ksName, shardName), 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := vc.VtctlClient.ExecuteCommand("Reshard", ksWorkflow, sourceShards, targetShards); err != nil {
		t.Fatalf("Reshard command failed with %+v\n", err)
	}
	tablets := vc.getVttabletsInKeyspace(t, cell, ksName, "master")
	targetShards = "," + targetShards + ","
	for _, tab := range tablets {
		if strings.Index(targetShards, ","+tab.Shard+",") >= 0 {
			fmt.Printf("Waiting for vrepl to catch up on %s since it IS a target shard\n", tab.Shard)
			if vc.WaitForVReplicationToCatchup(tab, workflow, "vt_"+ksName, 3*time.Second) != nil {
				t.Fatal("Migrate timed out")
			}
		} else {
			fmt.Printf("Not waiting for vrepl to catch up on %s since it is NOT a target shard\n", tab.Shard)
			continue
		}
	}
	vdiff(t, ksWorkflow)
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("MigrateReads", "-cells="+cell.Name, "-tablet_type=rdonly", ksWorkflow); err != nil {
		t.Fatalf("MigrateReads error: %s\n", output)
	}
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("MigrateReads", "-cells="+cell.Name, "-tablet_type=replica", ksWorkflow); err != nil {
		t.Fatalf("MigrateReads error: %s\n", output)
	}
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("MigrateWrites", ksWorkflow); err != nil {
		t.Fatalf("MigrateWrites error: %s\n", output)
	}
	if counts != nil {
		for tabletName, count := range counts {
			if tablets[tabletName] == nil {
				continue
			}
			assert.Empty(t, validateCountInTablet(t, tablets[tabletName], ksName, tableName, count))
		}
	}
}

func shardOrders(t *testing.T) {
	if err := vc.VtctlClient.ExecuteCommand("ApplyVSchema", "-vschema", ordersVSchema, "customer"); err != nil {
		t.Fatal(err)
	}
	if err := vc.VtctlClient.ExecuteCommand("Migrate", "-cell="+cell.Name, "-workflow=o2c",
		"-tablet_types="+"replica,rdonly", "product", "customer", "orders"); err != nil {
		t.Fatal(err)
	}
	customerTab1 := vc.Cells[cell.Name].Keyspaces["customer"].Shards["-80"].Tablets["zone1-200"].Vttablet
	customerTab2 := vc.Cells[cell.Name].Keyspaces["customer"].Shards["80-"].Tablets["zone1-300"].Vttablet
	if vc.WaitForVReplicationToCatchup(customerTab1, "o2c", "vt_customer", 1*time.Second) != nil {
		assert.Fail(t, "Migrate timed out for customer.o2c -80")

	}
	if vc.WaitForVReplicationToCatchup(customerTab2, "o2c", "vt_customer", 1*time.Second) != nil {
		assert.Fail(t, "Migrate timed out for customer.o2c 80-")
	}

	vdiff(t, "customer.o2c")
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("MigrateReads", "-cells="+cell.Name, "-tablet_type=rdonly", "customer.o2c"); err != nil {
		t.Fatalf("MigrateReads error: %s\n", output)
	}
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("MigrateReads", "-cells="+cell.Name, "-tablet_type=replica", "customer.o2c"); err != nil {
		t.Fatalf("MigrateReads error: %s\n", output)
	}
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("MigrateWrites", "customer.o2c"); err != nil {
		t.Fatalf("MigrateWrites error: %s\n", output)
	}

	assert.Empty(t, validateCountInTablet(t, customerTab1, "customer", "orders", 1))
	assert.Empty(t, validateCountInTablet(t, customerTab2, "customer", "orders", 2))
	assert.Empty(t, validateCount(t, vtgateConn, "customer", "orders", 3))
	removeTabletControls(t, "product/0")
}

func shardMerchant(t *testing.T) {
	if _, err := vc.AddKeyspace(t, cell, "merchant", "-80,80-", merchantVSchema, "", defaultReplicas, defaultRdonly, 400); err != nil {
		t.Fatal(err)
	}
	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "merchant", "-80"), 1); err != nil {
		t.Fatal(err)
	}
	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "merchant", "80-"), 1); err != nil {
		t.Fatal(err)
	}
	if err := vc.VtctlClient.ExecuteCommand("Migrate", "-cell="+cell.Name, "-workflow=p2m",
		"-tablet_types="+"replica,rdonly", "product", "merchant", "merchant"); err != nil {
		t.Fatal(err)
	}

	merchantTab1 := vc.Cells[cell.Name].Keyspaces["merchant"].Shards["-80"].Tablets["zone1-400"].Vttablet
	merchantTab2 := vc.Cells[cell.Name].Keyspaces["merchant"].Shards["80-"].Tablets["zone1-500"].Vttablet
	if vc.WaitForVReplicationToCatchup(merchantTab1, "p2m", "vt_merchant", 1*time.Second) != nil {
		t.Fatal("Migrate timed out for merchant.p2m -80")

	}
	if vc.WaitForVReplicationToCatchup(merchantTab2, "p2m", "vt_merchant", 1*time.Second) != nil {
		t.Fatal("Migrate timed out for merchant.p2m 80-")
	}

	vdiff(t, "merchant.p2m")
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("MigrateReads", "-cells="+cell.Name, "-tablet_type=rdonly", "merchant.p2m"); err != nil {
		t.Fatalf("MigrateReads error: %s\n", output)
	}
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("MigrateReads", "-cells="+cell.Name, "-tablet_type=replica", "merchant.p2m"); err != nil {
		t.Fatalf("MigrateReads error: %s\n", output)
	}
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("MigrateWrites", "merchant.p2m"); err != nil {
		t.Fatalf("MigrateWrites error: %s\n", output)
	}

	assert.Empty(t, validateCountInTablet(t, merchantTab1, "merchant", "merchant", 1))
	assert.Empty(t, validateCountInTablet(t, merchantTab2, "merchant", "merchant", 1))
	assert.Empty(t, validateCount(t, vtgateConn, "merchant", "merchant", 2))

	removeTabletControls(t, "product/0")
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
			fmt.Errorf("vdiff error for %d : %#v\n", key, diffReport)
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
	customerTablets := vc.getVttabletsInKeyspace(t, cell, "customer", "master")
	for _, tab := range customerTablets {
		if vc.WaitForVReplicationToCatchup(tab, workflow, "vt_customer", 3*time.Second) != nil {
			t.Fatal("Migrate timed out")
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
	productTab := vc.Cells[cell.Name].Keyspaces["product"].Shards["0"].Tablets["zone1-100"].Vttablet
	if vc.WaitForVReplicationToCatchup(productTab, "sales", "vt_product", 3*time.Second) != nil {
		assert.Fail(t, "Migrate timed out for product.sales -80")

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
	merchantTablets := vc.getVttabletsInKeyspace(t, cell, "merchant", "master")
	for _, tab := range merchantTablets {
		if vc.WaitForVReplicationToCatchup(tab, workflow, "vt_merchant", 1*time.Second) != nil {
			t.Fatal("Migrate timed out")
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
	merchantTablets := vc.getVttabletsInKeyspace(t, cell, "merchant", "master")
	for _, tab := range merchantTablets {
		if vc.WaitForVReplicationToCatchup(tab, workflow, "vt_merchant", 1*time.Second) != nil {
			t.Fatal("Migrate timed out")
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

func iterateShards(t *testing.T, f func(t *testing.T, shard *Shard)) {
	for _, cell := range vc.Cells {
		for _, ks := range cell.Keyspaces {
			for _, shard := range ks.Shards {
				f(t, shard)
			}
		}
	}
}

func iterateKeyspaces(t *testing.T, f func(t *testing.T, keyspace *Keyspace)) {
	for _, cell := range vc.Cells {
		for _, ks := range cell.Keyspaces {
			f(t, ks)
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

func removeTabletControls(t *testing.T, source string) {
	tabletTypes := []string{"rdonly", "replica", "master"}
	for _, tabletType := range tabletTypes {
		if err := vc.VtctlClient.ExecuteCommand("SetShardTabletControl", "--remove", source, tabletType); err != nil {
			t.Fatal(err)
		}
	}
}
