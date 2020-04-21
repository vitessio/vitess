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
	"context"
	"fmt"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        *mysql.ConnParams
	cell            = "test"
	keyspaces       = make(map[string]*cluster.Keyspace)

	legacyMerchant = cluster.Keyspace{
		Name: "merchant",
		SchemaSQL: `
create table merchant(mname varchar(128), category varchar(128), primary key(mname));
create table customer(cid int, name varbinary(128),	primary key(cid));
create table orders(oid int, cid int, pid int, mname varchar(128), price int, primary key(oid));
`,
	}
	legacyMerchantData = `
insert into merchant(mname, category) values('monoprice', 'electronics'), ('newegg', 'electronics');
insert into customer(cid, name) values(1, 'john'), (2, 'paul'), (3, 'ringo'), (4, 'led');
insert into orders(oid, cid, mname, pid, price) values(1, 1, 'monoprice', 1, 10), (2, 1, 'newegg', 2, 15), (3, 4, 'monoprice', 2, 20);
`

	legacyProduct = cluster.Keyspace{
		Name: "product",
		SchemaSQL: `
create table product(pid int, description varbinary(128), primary key(pid));
`,
	}
	legacyProductData = `
insert into product(pid, description) values(1, 'keyboard'), (2, 'monitor');
`

	commerce = cluster.Keyspace{
		Name: "commerce",
		SchemaSQL: `
create table product(pid int, description varbinary(128), primary key(pid));
create table merchant(mname varchar(128), category varchar(128), primary key(mname));
`,
		VSchema: `{
  "tables": {
		"product": {},
		"merchant": {}
	}
}`,
	}

	customer = cluster.Keyspace{
		Name: "customer",
		SchemaSQL: `
create table customer(cid int, name varbinary(128),	primary key(cid));
create table orders(oid int, cid int, pid int, mname varchar(128), price int, primary key(oid));
`,
		VSchema: `
{
	"sharded": true,
	"vindexes": {
		"reverse_bits": {
			"type": "reverse_bits"
		}
	},
	"tables": {
		"customer": {
			"column_vindexes": [{
				"column": "cid",
				"name": "reverse_bits"
			}]
		},
		"orders": {
			"column_vindexes": [{
				"column": "cid",
				"name": "reverse_bits"
			}],
			"auto_increment": {
				"column": "cid",
				"sequence": "customer_seq"
			}
		}
	}
}`,
	}
)

func TestMigration(t *testing.T) {
	startCluster(t)
	defer clusterInstance.Teardown()

	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("ListAllTablets")
	require.NoError(t, err)
	fmt.Printf("result:\n%v", result)
	fmt.Printf("Commerce vttablet: %v\n", keyspaces[commerce.Name].Shards[0].Vttablets[0].VttabletProcess)
	fmt.Printf("Commerce mysqlctl: %v\n", keyspaces[commerce.Name].Shards[0].Vttablets[0].MysqlctlProcess)

	buf := &strings.Builder{}
	fmt.Fprintf(buf, "externalConnections:\n")
	buildConnYaml(buf, keyspaces[legacyProduct.Name])
	buildConnYaml(buf, keyspaces[legacyMerchant.Name])
	fmt.Printf("%s\n", buf.String())
}

func startCluster(t *testing.T) {
	clusterInstance = cluster.NewCluster(cell, "localhost")

	err := clusterInstance.StartTopo()
	if err != nil {
		t.Fatal(err)
	}

	createKeyspace(t, legacyMerchant, []string{"0"})
	createKeyspace(t, legacyProduct, []string{"0"})
	createKeyspace(t, commerce, []string{"0"})
	createKeyspace(t, customer, []string{"-80", "80-"})

	err = clusterInstance.StartVtgate()
	require.NoError(t, err)
	vtParams = &mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: clusterInstance.VtgateMySQLPort,
	}

	populate(t, "merchant:0", legacyMerchantData)
	populate(t, "product:0", legacyProductData)
}

func createKeyspace(t *testing.T, ks cluster.Keyspace, shards []string) {
	t.Helper()

	err := clusterInstance.StartKeyspace(ks, []string{"0"}, 1, false)
	require.NoError(t, err)
	keyspaces[ks.Name] = &clusterInstance.Keyspaces[len(clusterInstance.Keyspaces)-1]
}

func populate(t *testing.T, target, sql string) {
	t.Helper()

	fmt.Printf("Populating: %v\n", target)
	conn, err := mysql.Connect(context.Background(), vtParams)
	require.NoError(t, err)
	execQuery(t, conn, fmt.Sprintf("use `%v`", target))
	lines := strings.Split(sql, "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		execQuery(t, conn, line)
	}
}

func buildConnYaml(buf *strings.Builder, ks *cluster.Keyspace) {
	connFormat := `  %s:
    socket: %s
    dbName: vt_%s
    app:
      user: vt_app
    dba:
      user: vt_dba
`
	vttablet := ks.Shards[0].Vttablets[0].VttabletProcess
	fmt.Fprintf(buf, connFormat, ks.Name, path.Join(vttablet.Directory, "mysql.sock"), ks.Name)
}

func execQuery(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 10000, true)
	require.Nil(t, err)
	return qr
}
