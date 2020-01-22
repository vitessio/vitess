package vreplication

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	vc     *VitessCluster
	vtgate *cluster.VtgateProcess
	cell   *Cell
	conn   *mysql.Conn
)

func tmpSetupEnv() {
	// This TEMPORARY HACK is required because GoLand is not using my env variables and defining them in Run/Edit Configurations did not work :(

	err := os.Setenv("PATH", "/home/rohit/vitess-ps/dist/etcd/etcd-v3.3.10-linux-amd64:/home/rohit/vitess-ps/bin:/home/rohit/vitess-ps/dist/etcd/etcd-v3.3.10-linux-amd64:/home/rohit/vitess-ps/bin:/usr/local/go/bin:/home/rohit/vitess/dist/etcd/etcd-v3.3.10-linux-amd64:/home/rohit/vitess/bin:/home/rohit/vitess-ps/dist/etcd/etcd-v3.3.10-linux-amd64:/home/rohit/vitess-ps/bin:/usr/local/go/bin:/home/rohit/vitess/dist/etcd/etcd-v3.3.10-linux-amd64:/home/rohit/vitess/bin:/home/rohit/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin")
	if err != nil {
		fmt.Printf("err %v\n", err)
	}
	err = os.Setenv("VTDATAROOT", "/home/rohit/vtdataroot")
	if err != nil {
		fmt.Printf("err %v\n", err)
	}
	err = os.Setenv("VTROOT", "/home/rohit/vitess-ps/")
	if err != nil {
		fmt.Printf("err %v\n", err)
	}
}

func TestBasicVreplicationWorkflow(t *testing.T) {
	// TODO remove this before final commit
	tmpSetupEnv()
	cellName := "zone1"

	vc = InitCluster(t, cellName)
	assert.NotNil(t, vc)
	if false { //TODO for testing: remove before commit
		defer vc.TearDown()
	}
	cell = vc.Cells[cellName]

	vc.AddKeyspace(t, cell, "product", "0", initialProductVSchema, initialProductSchema, 1, 1, 100)

	vtgate = cell.Vtgates[0]
	assert.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "product", "0"), 1)

	conn = getConnection(t)
	defer conn.Close()

	insertInitialData(t)
	shardCustomerTable(t)

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
	execMultipleQueries(t, conn, "product:0", string(lines))
	execVtgateQuery(t, conn, "product:0", "insert into customer_seq(id, next_id, cache) values(0, 100, 100);")
	fmt.Printf("Done inserting initial data\n")

	qr := execQuery(t, conn, "select count(*) from product")
	assert.NotNil(t, qr)
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(2)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = execQuery(t, conn, "select * from merchant")
	assert.NotNil(t, qr)
	if got, want := fmt.Sprintf("%v", qr.Rows),
		`[[VARCHAR("monoprice") VARCHAR("electronics")] [VARCHAR("newegg") VARCHAR("electronics")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
}

func shardCustomerTable(t *testing.T) {
	vc.AddKeyspace(t, cell, "customer", "0", customerVSchema, customerSchema, 1, 1, 200)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "customer", "0"), 1)
	//return
	//execVtgateQuery(t, conn, "customer", "insert into customer_seq(id, next_id, cache) values(0, 100, 100);")

	vc.VtctlClient.ExecuteCommand("Migrate", "-cell="+cell.Name, "-workflow=p2c",
		"-tablet_types="+"replica,rdonly", "product", "customer", "customer")
	return
	vc.VtctlClient.ExecuteCommand("MigrateReads", "-cells="+cell.Name, "-tablet_types=rdonly", "customer.p2c")
	vc.VtctlClient.ExecuteCommand("MigrateReads", "-cells="+cell.Name, "-tablet_types=readonly", "customer.p2c")
	vc.VtctlClient.ExecuteCommand("MigrateWrites", "customer.p2c")
	//TODO Verify

}

func shardMerchant(t *testing.T) {
	vc.AddKeyspace(t, cell, "merchant", "-80,80-", merchantVSchema, "", 0, 0, 300)
	vc.VtctlClient.ExecuteCommand("Migrate", "-cell="+cell.Name, "-workflow=p2m",
		"-tablet_types="+"replica,rdonly", "product", "merchant", "merchant")
	vc.VtctlClient.ExecuteCommand("MigrateReads", "-cells="+cell.Name, "-tablet_types=rdonly", "merchant.p2m")
	vc.VtctlClient.ExecuteCommand("MigrateReads", "-cells="+cell.Name, "-tablet_types=readonly", "merchant.p2m")
	vc.VtctlClient.ExecuteCommand("MigrateWrites", "merchant.p2m")
	//TODO Verify

}

func materializeProduct(t *testing.T) {

}

func materializeSales(t *testing.T) {

}

func reshardOrders(t *testing.T) {

}
