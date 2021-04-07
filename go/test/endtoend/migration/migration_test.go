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

package migration

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        *mysql.ConnParams
	cell            = "test"
	keyspaces       = make(map[string]*cluster.Keyspace)

	legacyProduct = cluster.Keyspace{
		Name: "product",
		SchemaSQL: `
create table product(pid bigint, description varbinary(128), primary key(pid));
`,
	}
	legacyProductData = `
insert into vt_product.product(pid, description) values(1, 'keyboard'), (2, 'monitor');
`

	legacyCustomer = cluster.Keyspace{
		Name: "customer",
		SchemaSQL: `
create table customer(cid bigint, name varbinary(128),	primary key(cid));
create table orders(oid bigint, cid bigint, pid bigint, mname varchar(128), price bigint, primary key(oid));
`,
	}
	legacyCustomerData = `
insert into vt_customer.customer(cid, name) values(1, 'john'), (2, 'paul'), (3, 'ringo');
insert into vt_customer.orders(oid, cid, mname, pid, price) values(1, 1, 'monoprice', 1, 10), (2, 1, 'newegg', 2, 15);
`

	commerce = cluster.Keyspace{
		Name: "commerce",
		SchemaSQL: `
create table product(pid bigint, description varbinary(128), primary key(pid));
create table customer(cid bigint, name varbinary(128),	primary key(cid));
create table orders(oid bigint, cid bigint, pid bigint, mname varchar(128), price bigint, primary key(oid));
`,
		VSchema: `{
  "tables": {
		"product": {},
		"customer": {},
		"orders": {}
	}
}`,
	}

	connFormat = `externalConnections:
  product:
    socket: %s
    dbName: vt_product
    app:
      user: vt_app
    dba:
      user: vt_dba
  customer:
    flavor: FilePos
    socket: %s
    dbName: vt_customer
    app:
      user: vt_app
    dba:
      user: vt_dba
`
)

/*
TestMigration demonstrates how to setup vreplication to import data from multiple external
mysql sources.
We use vitess to bring up two databases that we'll treat as external. We'll directly access
the mysql instances instead of going through any vitess layer.
The product database contains a product table.
The customer database contains a customer and an orders table.
We create a new "commerce" keyspace, which will be the target. The commerce keyspace will
take a yaml config that defines these external sources. it will look like this:

externalConnections:
  product:
    socket: /home/sougou/dev/src/vitess.io/vitess/vtdataroot/vtroot_15201/vt_0000000622/mysql.sock
    dbName: vt_product
    app:
      user: vt_app
    dba:
      user: vt_dba
  customer:
    flavor: FilePos
    socket: /home/sougou/dev/src/vitess.io/vitess/vtdataroot/vtroot_15201/vt_0000000620/mysql.sock
    dbName: vt_customer
    app:
      user: vt_app
    dba:
      user: vt_dba

We then execute the following vreplication inserts to initiate the import. The test uses
three streams although only two are required. This is to show that there can exist multiple
streams from the same source. The main difference between an external source vs a vitess
source is that the source proto contains an "external_mysql" field instead of keyspace and shard.
That field is the key into the externalConnections section of the input yaml.

VReplicationExec: insert into _vt.vreplication (workflow, db_name, source, pos, max_tps, max_replication_lag, tablet_types, time_updated, transaction_timestamp, state) values('product', 'vt_commerce', 'filter:<rules:<match:\"product\" > > external_mysql:\"product\" ', '', 9999, 9999, 'master', 0, 0, 'Running')
VReplicationExec: insert into _vt.vreplication (workflow, db_name, source, pos, max_tps, max_replication_lag, tablet_types, time_updated, transaction_timestamp, state) values('customer', 'vt_commerce', 'filter:<rules:<match:\"customer\" > > external_mysql:\"customer\" ', '', 9999, 9999, 'master', 0, 0, 'Running')
VReplicationExec: insert into _vt.vreplication (workflow, db_name, source, pos, max_tps, max_replication_lag, tablet_types, time_updated, transaction_timestamp, state) values('orders', 'vt_commerce', 'filter:<rules:<match:\"orders\" > > external_mysql:\"customer\" ', '', 9999, 9999, 'master', 0, 0, 'Running')
*/
func TestMigration(t *testing.T) {
	yamlFile := startCluster(t)
	defer clusterInstance.Teardown()

	tabletConfig := func(vt *cluster.VttabletProcess) {
		vt.ExtraArgs = append(vt.ExtraArgs, "-tablet_config", yamlFile)
	}
	createKeyspace(t, commerce, []string{"0"}, tabletConfig)
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", "commerce")
	require.NoError(t, err)

	err = clusterInstance.StartVtgate()
	require.NoError(t, err)

	migrate(t, "product", "commerce", []string{"product"})
	migrate(t, "customer", "commerce", []string{"customer"})
	migrate(t, "customer", "commerce", []string{"orders"})
	vttablet := keyspaces["commerce"].Shards[0].Vttablets[0].VttabletProcess
	waitForVReplicationToCatchup(t, vttablet, 1*time.Second)

	testcases := []struct {
		query  string
		result *sqltypes.Result
	}{{
		query: "select * from product",
		result: sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"pid|description",
			"int64|varbinary"),
			"1|keyboard",
			"2|monitor",
		),
	}, {
		query: "select * from customer",
		result: sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"cid|name",
			"int64|varbinary"),
			"1|john",
			"2|paul",
			"3|ringo",
		),
	}, {
		query: "select * from orders",
		result: sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"oid|cid|mname|pid|price",
			"int64|int64|int64|varchar|int64"),
			"1|1|1|monoprice|10",
			"2|1|2|newegg|15",
		),
	}}

	vtParams = &mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(context.Background(), vtParams)
	require.NoError(t, err)
	defer conn.Close()

	execQuery(t, conn, "use `commerce`")
	for _, tcase := range testcases {
		result := execQuery(t, conn, tcase.query)
		// nil out the fields because they're too detailed.
		result.Fields = nil
		tcase.result.Fields = nil
		assert.Equal(t, tcase.result, result, tcase.query)
	}
}

func migrate(t *testing.T, fromdb, toks string, tables []string) {
	bls := &binlogdatapb.BinlogSource{
		ExternalMysql: fromdb,
		Filter:        &binlogdatapb.Filter{},
	}
	for _, table := range tables {
		bls.Filter.Rules = append(bls.Filter.Rules, &binlogdatapb.Rule{Match: table})
	}
	val := sqltypes.NewVarBinary(fmt.Sprintf("%v", bls))
	var sqlEscaped bytes.Buffer
	val.EncodeSQL(&sqlEscaped)
	query := fmt.Sprintf("insert into _vt.vreplication "+
		"(workflow, db_name, source, pos, max_tps, max_replication_lag, tablet_types, time_updated, transaction_timestamp, state) values"+
		"('%s', '%s', %s, '', 9999, 9999, 'master', 0, 0, 'Running')", tables[0], "vt_"+toks, sqlEscaped.String())
	fmt.Printf("VReplicationExec: %s\n", query)
	vttablet := keyspaces[toks].Shards[0].Vttablets[0].VttabletProcess
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("VReplicationExec", vttablet.TabletPath, query)
	require.NoError(t, err)
}

func startCluster(t *testing.T) string {
	clusterInstance = cluster.NewCluster(cell, "localhost")

	err := clusterInstance.StartTopo()
	if err != nil {
		t.Fatal(err)
	}

	createKeyspace(t, legacyCustomer, []string{"0"})
	createKeyspace(t, legacyProduct, []string{"0"})

	productSocket := path.Join(keyspaces["product"].Shards[0].Vttablets[0].VttabletProcess.Directory, "mysql.sock")
	customerSocket := path.Join(keyspaces["customer"].Shards[0].Vttablets[0].VttabletProcess.Directory, "mysql.sock")

	populate(t, productSocket, legacyProductData)
	populate(t, customerSocket, legacyCustomerData)

	buf := &bytes.Buffer{}
	fmt.Fprintf(buf, "externalConnections:\n")
	tabletConfig := fmt.Sprintf(connFormat, productSocket, customerSocket)
	fmt.Printf("tablet_config:\n%s\n", tabletConfig)
	yamlFile := path.Join(clusterInstance.TmpDirectory, "external.yaml")
	err = ioutil.WriteFile(yamlFile, []byte(tabletConfig), 0644)
	require.NoError(t, err)
	return yamlFile
}

func createKeyspace(t *testing.T, ks cluster.Keyspace, shards []string, customizers ...interface{}) {
	t.Helper()

	err := clusterInstance.StartKeyspace(ks, shards, 1, false, customizers...)
	require.NoError(t, err)
	keyspaces[ks.Name] = &clusterInstance.Keyspaces[len(clusterInstance.Keyspaces)-1]
}

func populate(t *testing.T, socket, sql string) {
	t.Helper()

	params := &mysql.ConnParams{
		UnixSocket: socket,
		Uname:      "vt_app",
	}
	conn, err := mysql.Connect(context.Background(), params)
	require.NoError(t, err)
	defer conn.Close()
	lines := strings.Split(sql, "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		execQuery(t, conn, line)
	}
}

// waitForVReplicationToCatchup: logic copied from go/test/endtoend/vreplication/cluster.go
func waitForVReplicationToCatchup(t *testing.T, vttablet *cluster.VttabletProcess, duration time.Duration) {
	queries := []string{
		`select count(*) from _vt.vreplication where pos = ''`,
		"select count(*) from information_schema.tables where table_schema='_vt' and table_name='copy_state' limit 1;",
		`select count(*) from _vt.copy_state`,
	}
	results := []string{"[INT64(0)]", "[INT64(1)]", "[INT64(0)]"}
	for ind, query := range queries {
		waitDuration := 100 * time.Millisecond
		for {
			qr, err := vttablet.QueryTablet(query, "", false)
			if err != nil {
				t.Fatal(err)
			}
			if len(qr.Rows) > 0 && fmt.Sprintf("%v", qr.Rows[0]) == string(results[ind]) {
				break
			}
			time.Sleep(waitDuration)
			duration -= waitDuration
			if duration <= 0 {
				t.Fatalf("waitForVReplicationToCatchup timed out, query: %v, result: %v", query, qr)
			}
		}
	}
}

func execQuery(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 10000, true)
	require.Nil(t, err)
	return qr
}
