package vreplication

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/wrangler"

	//"vitess.io/vitess/go/vt/wrangler"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
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
	shardMerchant(t)

	/*
		split, merge, split/merge
		Reshard 1->many, many->many (reshard into -40,40-), many->one
		Migrate 1->many, many->1, many->many (orders to merchant new vindex)

	*/

	/*
		Create Customer keyspace
		Migrate Customer to unsharded
		Reshard Customer
		Migrate Orders to sharded customer
		Create Merchant keyspace


	*/
	//shardCustomerTable(t)
	//moveOrders
	/*
		shardMerchant(t)
		materializeProduct(t)
		materializeSales(t)
		reshardOrders(t)

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
	_, _ = vc.AddKeyspace(t, cell, "customer", "-80,80-", customerVSchema, customerSchema, defaultReplicas, defaultRdonly, 200)
	_ = vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "customer", "-80"), 1)
	_ = vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "customer", "80-"), 1)

	_ = vc.VtctlClient.ExecuteCommand("Migrate", "-cell="+cell.Name, "-workflow=p2c",
		"-tablet_types="+"replica,rdonly", "product", "customer", "customer")
	customerTab1 := vc.Cells[cell.Name].Keyspaces["customer"].Shards["-80"].Tablets["zone1-200"].Vttablet
	customerTab2 := vc.Cells[cell.Name].Keyspaces["customer"].Shards["80-"].Tablets["zone1-300"].Vttablet
	if vc.WaitForMigrateToComplete(customerTab1, "p2c", "vt_customer", 1*time.Second) != nil {
		assert.Fail(t, "Migrate timed out for customer.p2c -80")

	}
	if vc.WaitForMigrateToComplete(customerTab2, "p2c", "vt_customer", 1*time.Second) != nil {
		assert.Fail(t, "Migrate timed out for customer.p2c 80-")
	}

	_ = vc.VtctlClient.ExecuteCommand("MigrateReads", "-cells="+cell.Name, "-tablet_types=rdonly", "customer.p2c")
	_ = vc.VtctlClient.ExecuteCommand("MigrateReads", "-cells="+cell.Name, "-tablet_types=replica", "customer.p2c")
	_ = vc.VtctlClient.ExecuteCommand("MigrateWrites", "customer.p2c")
	vdiff(t, "customer.p2c")

	assert.Empty(t, validateCountInTablet(t, customerTab1, "customer", "customer", 1))
	assert.Empty(t, validateCountInTablet(t, customerTab2, "customer", "customer", 2))
	assert.Empty(t, validateCount(t, vtgateConn, "customer", "customer", 3))
	assert.Empty(t, validateCount(t, vtgateConn, "product:0", "customer", 3))
}

func shardMerchant(t *testing.T) {
	vc.AddKeyspace(t, cell, "merchant", "-80,80-", merchantVSchema, "", defaultReplicas, defaultRdonly, 400)
	_ = vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "merchant", "-80"), 1)
	_ = vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "merchant", "80-"), 1)

	vc.VtctlClient.ExecuteCommand("Migrate", "-cell="+cell.Name, "-workflow=p2m",
		"-tablet_types="+"replica,rdonly", "product", "merchant", "merchant")

	merchantTab1 := vc.Cells[cell.Name].Keyspaces["merchant"].Shards["-80"].Tablets["zone1-400"].Vttablet
	merchantTab2 := vc.Cells[cell.Name].Keyspaces["merchant"].Shards["80-"].Tablets["zone1-500"].Vttablet
	if vc.WaitForMigrateToComplete(merchantTab1, "p2m", "vt_merchant", 1*time.Second) != nil {
		assert.Fail(t, "Migrate timed out for merchant.p2m -80")

	}
	if vc.WaitForMigrateToComplete(merchantTab2, "p2m", "vt_merchant", 1*time.Second) != nil {
		assert.Fail(t, "Migrate timed out for merchant.p2m 80-")
	}

	vc.VtctlClient.ExecuteCommand("MigrateReads", "-cells="+cell.Name, "-tablet_types=rdonly", "merchant.p2m")
	vc.VtctlClient.ExecuteCommand("MigrateReads", "-cells="+cell.Name, "-tablet_types=readonly", "merchant.p2m")
	vc.VtctlClient.ExecuteCommand("MigrateWrites", "merchant.p2m")
	vdiff(t, "merchant.p2m")

	assert.Empty(t, validateCountInTablet(t, merchantTab1, "merchant", "merchant", 1))
	assert.Empty(t, validateCountInTablet(t, merchantTab2, "merchant", "merchant", 1))
	assert.Empty(t, validateCount(t, vtgateConn, "merchant", "merchant", 2))
}

func vdiff(t *testing.T, workflow string) {
	output, err := vc.VtctlClient.ExecuteCommandWithOutput("VDiff", "-tablet_types", "master,replica,rdonly", "-format", "json", workflow)

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

}

func materializeSales(t *testing.T) {

}

func reshardOrders(t *testing.T) {

}

func checkVtgateHealth(t *testing.T, cell *Cell) {
	for _, vtgate := range cell.Vtgates {
		vtgateHealthURL := strings.Replace(vtgate.VerifyURL, "vars", "health", -1)
		if !checkHealth(t, vtgateHealthURL) {
			assert.Failf(t, "Vtgate not healthy: ", vtgateHealthURL)
		} else {
			//fmt.Printf("Vtgate is healthy %s\n", vtgateHealthURL)
		}
	}
}

func checkTabletHealth(t *testing.T, tablet *Tablet) {
	vttabletHealthURL := strings.Replace(tablet.Vttablet.VerifyURL, "debug/vars", "healthz", -1)
	if !checkHealth(t, vttabletHealthURL) {
		assert.Failf(t, "Vttablet not healthy: ", vttabletHealthURL)
	} else {
		//fmt.Printf("Vttablet is healthy %s\n", vttabletHealthURL)
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
