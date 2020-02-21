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

	if false { //TODO for testing: remove before commit
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
	shardCustomer(t)
	shardOrders(t)
	reshardCustomerAndOrdersManyToMany(t)
	shardMerchant(t)
	materializeProduct(t)
	materializeMerchantOrders(t)
	materializeSales(t)
	//reshardOrdersToMerchant(t)  //TODO
	validateAll(t)

	/*
		split, merge, split/merge
		Reshard 1->many, many->many (reshard into -40,40-), many->one
		Migrate 1->many, many->1, many->many (orders to merchant new vindex)
		create lookup vindex, no owner, by order id
		order table: order id,
	*/
	//vdiff one for table, one for shard (introduce corruption)
	//TODO test reverse replication for each
	//TODO once dry run is implemented, test dry-run for each workflow and reverse replication

}

func insertInitialData(t *testing.T) {
	fmt.Printf("Inserting initial data\n")
	lines, _ := ioutil.ReadFile("unsharded_init_data.sql")
	execMultipleQueries(t, vtgateConn, "product:0", string(lines))
	execVtgateQuery(t, vtgateConn, "product:0", "insert into customer_seq(id, next_id, cache) values(0, 100, 100);")
	fmt.Printf("Done inserting initial data\n")

	validateCount(t, vtgateConn, "product:0", "product", 2)
	validateCount(t, vtgateConn, "product:0", "customer", 3)
	validateQuery(t, vtgateConn, "product:0", "select * from merchant",
		`[[VARCHAR("monoprice") VARCHAR("electronics")] [VARCHAR("newegg") VARCHAR("electronics")]]`)
}

func shardCustomer(t *testing.T) {
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

	if vc.WaitForMigrateToComplete(customerTab1, "p2c", "vt_customer", 1*time.Second) != nil {
		t.Fatal("Migrate timed out for customer.p2c -80")

	}
	if vc.WaitForMigrateToComplete(customerTab2, "p2c", "vt_customer", 1*time.Second) != nil {
		t.Fatal("Migrate timed out for customer.p2c 80-")
	}

	vdiff(t, "customer.p2c")
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("MigrateReads", "-cells="+cell.Name, "-tablet_type=rdonly", "customer.p2c"); err != nil {
		t.Fatalf("MigrateReads error: %s\n", output)
	}
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("MigrateReads", "-cells="+cell.Name, "-tablet_type=replica", "customer.p2c"); err != nil {
		t.Fatalf("MigrateReads error: %s\n", output)
	}
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("MigrateWrites", "customer.p2c"); err != nil {
		t.Fatalf("MigrateWrites error: %s\n", output)
	}

	assert.Empty(t, validateCountInTablet(t, customerTab1, "customer", "customer", 1))
	assert.Empty(t, validateCountInTablet(t, customerTab2, "customer", "customer", 2))
	assert.Empty(t, validateCount(t, vtgateConn, "customer", "customer.customer", 3))
	//assert.Empty(t, validateCount(t, vtgateConn, "product:0", "customer", 3)) //TODO how do we validate blacklisted tables?

	query := "insert into customer.customer (name) values('george')"
	execVtgateQuery(t, vtgateConn, "customer", query) //creates with id 100, so in -80 because of reverse_bits
	assert.Empty(t, validateCountInTablet(t, customerTab1, "customer", "customer", 2))
	assert.Empty(t, validateCount(t, vtgateConn, "customer", "customer.customer", 4))
	//TODO verify with Sugu why this is needed. If this line is not present shardMerchant errors out with conflicting blacklisted tables error
	removeTabletControls(t, "product/0")
}

func reshardCustomerAndOrdersManyToMany(t *testing.T) {

}

func shardOrders(t *testing.T) {
	if err := vc.VtctlClient.ExecuteCommand("ApplyVSchema", "-vschema", ordersVSchema, "customer"); err != nil {
		t.Fatal(err)
	}
	if err := vc.VtctlClient.ExecuteCommand("RebuildKeyspaceGraph", "customer"); err != nil {
		t.Fatal(err)
	}
	if err := vc.VtctlClient.ExecuteCommand("Migrate", "-cell="+cell.Name, "-workflow=o2c",
		"-tablet_types="+"replica,rdonly", "product", "customer", "orders"); err != nil {
		t.Fatal(err)
	}
	customerTab1 := vc.Cells[cell.Name].Keyspaces["customer"].Shards["-80"].Tablets["zone1-200"].Vttablet
	customerTab2 := vc.Cells[cell.Name].Keyspaces["customer"].Shards["80-"].Tablets["zone1-300"].Vttablet
	if vc.WaitForMigrateToComplete(customerTab1, "o2c", "vt_customer", 1*time.Second) != nil {
		assert.Fail(t, "Migrate timed out for customer.o2c -80")

	}
	if vc.WaitForMigrateToComplete(customerTab2, "o2c", "vt_customer", 1*time.Second) != nil {
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
	if vc.WaitForMigrateToComplete(merchantTab1, "p2m", "vt_merchant", 1*time.Second) != nil {
		t.Fatal("Migrate timed out for merchant.p2m -80")

	}
	if vc.WaitForMigrateToComplete(merchantTab2, "p2m", "vt_merchant", 1*time.Second) != nil {
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
	output, err := vc.VtctlClient.ExecuteCommandWithOutput("VDiff", "-tablet_types", "master,replica,rdonly", "-format", "json", workflow)
	fmt.Printf("vdiff err: %+v, output: %+v\n", err, output)
	assert.Nil(t, err)
	assert.NotNil(t, output)
	diffReports := make(map[string]*wrangler.DiffReport)
	err = json.Unmarshal([]byte(output), &diffReports)
	assert.NotNil(t, err)
	assert.True(t, len(diffReports) > 0)
	for key, diffReport := range diffReports {
		if diffReport.ProcessedRows != diffReport.MatchingRows {
			fmt.Errorf("vdiff error for %s : %#v\n", key, diffReport)
		}
	}
}

func materializeProduct(t *testing.T) {
	if err := vc.VtctlClient.ExecuteCommand("ApplyVSchema", "-vschema", materializeProductVSchema, "customer"); err != nil {
		t.Fatal(err)
	}
	if err := vc.VtctlClient.ExecuteCommand("RebuildKeyspaceGraph", "customer"); err != nil {
		t.Fatal(err)
	}
	if err := vc.VtctlClient.ExecuteCommand("Materialize", materializeProductSpec); err != nil {
		t.Fatal(err)
	}
	customerTab1 := vc.Cells[cell.Name].Keyspaces["customer"].Shards["-80"].Tablets["zone1-200"].Vttablet
	customerTab2 := vc.Cells[cell.Name].Keyspaces["customer"].Shards["80-"].Tablets["zone1-300"].Vttablet
	if vc.WaitForMigrateToComplete(customerTab1, "cproduct", "vt_customer", 1*time.Second) != nil {
		assert.Fail(t, "Migrate timed out for customer.cproduct -80")

	}
	if vc.WaitForMigrateToComplete(customerTab2, "cproduct", "vt_customer", 1*time.Second) != nil {
		assert.Fail(t, "Migrate timed out for customer.cproduct 80-")
	}
	time.Sleep(2 * time.Second) //WaitForMigrate is not working here, TODO need to remove time.sleep with what?
	assert.Empty(t, validateCountInTablet(t, customerTab1, "customer", "cproduct", 2))
	assert.Empty(t, validateCountInTablet(t, customerTab2, "customer", "cproduct", 2))
}

func materializeSales(t *testing.T) {
	if err := vc.VtctlClient.ExecuteCommand("ApplyVSchema", "-vschema", materializeSalesVSchema, "product"); err != nil {
		t.Fatal(err)
	}
	if err := vc.VtctlClient.ExecuteCommand("RebuildKeyspaceGraph", "product"); err != nil {
		t.Fatal(err)
	}
	if err := vc.VtctlClient.ExecuteCommand("Materialize", materializeSalesSpec); err != nil {
		t.Fatal(err)
	}
	productTab := vc.Cells[cell.Name].Keyspaces["product"].Shards["0"].Tablets["zone1-100"].Vttablet
	if vc.WaitForMigrateToComplete(productTab, "sales", "vt_product", 2*time.Second) != nil {
		assert.Fail(t, "Migrate timed out for product.sales -80")

	}
	time.Sleep(2 * time.Second) //WaitForMigrate is not working here, TODO need to remove time.sleep with what?
	assert.Empty(t, validateCount(t, vtgateConn, "product", "sales", 2))
	validateQuery(t, vtgateConn, "product:0", "select kount, amount from sales",
		`[[INT64(1) INT64(10)] [INT64(2) INT64(35)]]`)

}

func materializeMerchantOrders(t *testing.T) {
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("ApplyVSchema", "-vschema", merchantOrdersVSchema, "merchant"); err != nil {
		fmt.Printf("ApplyVSchema error: %+v", output)
		t.Fatal(err)
	}
	if err := vc.VtctlClient.ExecuteCommand("RebuildKeyspaceGraph", "merchant"); err != nil {
		t.Fatal(err)
	}
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("Materialize", materializeMerchantOrdersSpec); err != nil {
		fmt.Printf("MerchantOrders error is %+v", output)
		t.Fatal(err)
	}
	merchantTab1 := vc.Cells[cell.Name].Keyspaces["merchant"].Shards["-80"].Tablets["zone1-400"].Vttablet
	merchantTab2 := vc.Cells[cell.Name].Keyspaces["merchant"].Shards["80-"].Tablets["zone1-500"].Vttablet
	if vc.WaitForMigrateToComplete(merchantTab1, "morders", "vt_merchant", 1*time.Second) != nil {
		assert.Fail(t, "Migrate timed out for merchant.morders -80")

	}
	if vc.WaitForMigrateToComplete(merchantTab2, "morders", "vt_merchant", 1*time.Second) != nil {
		assert.Fail(t, "Migrate timed out for merchant.morders 80-")
	}
	time.Sleep(2 * time.Second) //WaitForMigrate is not working here, TODO need to remove time.sleep with what?
	assert.Empty(t, validateCount(t, vtgateConn, "merchant", "morders", 3))
}

func reshardOrders(t *testing.T) {

}

//run various queries to ensure that nothing is broken due to ensuing migrates
func validateAll(t *testing.T) {

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
