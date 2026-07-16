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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vitesst"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

var (
	clusterInstance *vitesst.Cluster
	vtParams        mysql.ConnParams
	cell            = "test"

	legacyProduct = vitesst.WithKeyspace("product").
			WithReplicas(1).
			WithSchema(`
create table product(pid bigint, description varbinary(128), primary key(pid));
`)
	legacyProductData = `
insert into vt_product.product(pid, description) values(1, 'keyboard'), (2, 'monitor');
`

	legacyCustomer = vitesst.WithKeyspace("customer").
			WithReplicas(1).
			WithSchema(`
create table customer(cid bigint, name varbinary(128),	primary key(cid));
create table orders(oid bigint, cid bigint, pid bigint, mname varchar(128), price bigint, primary key(oid));
`)
	legacyCustomerData = `
insert into vt_customer.customer(cid, name) values(1, 'john'), (2, 'paul'), (3, 'ringo');
insert into vt_customer.orders(oid, cid, mname, pid, price) values(1, 1, 'monoprice', 1, 10), (2, 1, 'newegg', 2, 15);
`

	commerceSchema = `
create table product(pid bigint, description varbinary(128), primary key(pid));
create table customer(cid bigint, name varbinary(128),	primary key(cid));
create table orders(oid bigint, cid bigint, pid bigint, mname varchar(128), price bigint, primary key(oid));
`
	commerceVSchema = `{
  "tables": {
		"product": {},
		"customer": {},
		"orders": {}
	}
}`

	// externalUsers grants the users named in the externalConnections config
	// access from the other containers on the cluster network.
	externalUsers = `
CREATE USER 'vt_app'@'%';
GRANT ALL ON *.* TO 'vt_app'@'%';
CREATE USER 'vt_filtered'@'%';
GRANT ALL ON *.* TO 'vt_filtered'@'%';
CREATE USER 'vt_allprivs'@'%';
GRANT ALL ON *.* TO 'vt_allprivs'@'%';
`

	// tabletConfigPath is where the externalConnections config is placed in the
	// commerce tablet containers.
	tabletConfigPath = "/vt/files/external.yaml"

	// externalMySQLPort is the port mysqld listens on inside every tablet
	// container.
	externalMySQLPort = 3306

	connFormat = `externalConnections:
  product:
    host: %s
    port: %d
    dbName: vt_product
    app:
      user: vt_app
    dba:
      user: vt_dba
    filtered:
      user: vt_filtered
    allprivs:
      user: vt_allprivs
  customer:
    flavor: FilePos
    host: %s
    port: %d
    dbName: vt_customer
    app:
      user: vt_app
    dba:
      user: vt_dba
    filtered:
      user: vt_filtered
    allprivs:
      user: vt_allprivs
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
	  host: vttablet-102
	  port: 3306
	  dbName: vt_product
	  app:
	    user: vt_app
	  dba:
	    user: vt_dba
	customer:
	  flavor: FilePos
	  host: vttablet-100
	  port: 3306
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
*/
func TestMigration(t *testing.T) {
	ctx := t.Context()
	tabletConfig := startCluster(ctx, t)

	commerce := vitesst.WithKeyspace("commerce").
		WithReplicas(1).
		WithSchema(commerceSchema).
		WithVSchema(commerceVSchema).
		WithTabletSpec(func(spec *vitesst.TabletSpec) {
			spec.Files = append(spec.Files, vitesst.ContainerFile{
				Content:       []byte(tabletConfig),
				ContainerPath: tabletConfigPath,
			})
			spec.ExtraArgs = append(spec.ExtraArgs, "--tablet-config", tabletConfigPath)
		})
	_, err := clusterInstance.AddKeyspace(t, ctx, commerce)
	require.NoError(t, err)

	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "RebuildKeyspaceGraph", "commerce")
	require.NoError(t, err)

	migrate(ctx, t, "product", "commerce", []string{"product"})
	migrate(ctx, t, "customer", "commerce", []string{"customer"})
	migrate(ctx, t, "customer", "commerce", []string{"orders"})
	vttablet := clusterInstance.Keyspace("commerce").Shards()[0].Primary()
	startStreams(ctx, t, vttablet)
	waitForVReplicationToCatchup(ctx, t, vttablet, 30*time.Second)

	// vtgate starts once the streams are running, so it waits for the
	// restarted primary of the commerce keyspace to be healthy.
	_, err = clusterInstance.AddVTGate(t, ctx)
	require.NoError(t, err)

	testcases := []struct {
		query  string
		result *sqltypes.Result
	}{{
		query: "select * from product",
		result: sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"pid|description",
				"int64|varbinary",
			),
			"1|keyboard",
			"2|monitor",
		),
	}, {
		query: "select * from customer",
		result: sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"cid|name",
				"int64|varbinary",
			),
			"1|john",
			"2|paul",
			"3|ringo",
		),
	}, {
		query: "select * from orders",
		result: sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"oid|cid|mname|pid|price",
				"int64|int64|int64|varchar|int64",
			),
			"1|1|1|monoprice|10",
			"2|1|2|newegg|15",
		),
	}}

	vtParams = clusterInstance.VTParams(ctx, "")
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	execQuery(t, conn, "use `commerce`")
	for _, tcase := range testcases {
		result := execQuery(t, conn, tcase.query)
		// nil out the fields because they're too detailed, and the status
		// flags because they carry the session's autocommit bit.
		result.Fields = nil
		result.StatusFlags = 0
		tcase.result.Fields = nil
		assert.Equal(t, tcase.result, result, tcase.query)
	}
}

func migrate(ctx context.Context, t *testing.T, fromdb, toks string, tables []string) {
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
		"(workflow, db_name, source, pos, max_tps, max_replication_lag, tablet_types, time_updated, transaction_timestamp, state, options) values"+
		"('%s', '%s', %s, '', 9999, 9999, 'primary', 0, 0, 'Running', '{}')", tables[0], "vt_"+toks, sqlEscaped.String())
	fmt.Printf("VReplication insert: %s\n", query)
	vttablet := clusterInstance.Keyspace(toks).Shards()[0].Primary().Alias()
	err := clusterInstance.Vtctld().ExecuteCommand(ctx, "ExecuteFetchAsDBA", vttablet, query)
	require.NoError(t, err)
}

// startStreams restarts vttablet so that its vreplication engine picks up the
// rows inserted into _vt.vreplication. The engine loads its streams when it
// opens, which happens once the tablet becomes the primary, so rows written
// straight into the table afterwards are only seen by the next open.
func startStreams(ctx context.Context, t *testing.T, vttablet *vitesst.Tablet) {
	t.Helper()

	require.NoError(t, vttablet.StopVttablet(ctx))
	require.NoError(t, vttablet.StartVttablet(ctx))
	require.NoError(t, vttablet.WaitForTabletType(ctx, 60*time.Second, "primary"))
}

func startCluster(ctx context.Context, t *testing.T) string {
	cluster, err := vitesst.NewCluster(t,
		vitesst.WithCells(cell),
		vitesst.WithoutVTGate(),
		vitesst.WithInitDBSQLExtra(externalUsers),
		legacyCustomer,
		legacyProduct,
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(t, ctx)
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(ctx)
		if err := cleanup(cleanupCtx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})
	require.NoError(t, err)
	clusterInstance = cluster

	product := cluster.Keyspace("product").Shards()[0].Primary()
	customer := cluster.Keyspace("customer").Shards()[0].Primary()

	populate(ctx, t, product, legacyProductData)
	populate(ctx, t, customer, legacyCustomerData)

	tabletConfig := fmt.Sprintf(connFormat, product.Name(), externalMySQLPort, customer.Name(), externalMySQLPort)
	fmt.Printf("tablet_config:\n%s\n", tabletConfig)
	return tabletConfig
}

func populate(ctx context.Context, t *testing.T, tablet *vitesst.Tablet, sql string) {
	t.Helper()

	lines := strings.SplitSeq(sql, "\n")
	for line := range lines {
		if line == "" {
			continue
		}
		_, err := tablet.QueryTabletWithDB(ctx, line, "")
		require.NoError(t, err)
	}
}

// waitForVReplicationToCatchup: logic copied from go/test/endtoend/vreplication/cluster.go
func waitForVReplicationToCatchup(ctx context.Context, t *testing.T, vttablet *vitesst.Tablet, duration time.Duration) {
	queries := []string{
		`select count(*) from _vt.vreplication where pos = ''`,
		"select count(*) from information_schema.tables where table_schema='_vt' and table_name='copy_state' limit 1;",
		`select count(*) from _vt.copy_state`,
	}
	results := []string{"[INT64(0)]", "[INT64(1)]", "[INT64(0)]"}
	for ind, query := range queries {
		waitDuration := 100 * time.Millisecond
		for {
			qr, err := vttablet.QueryTabletWithDB(ctx, query, "")
			require.NoError(t, err)
			if len(qr.Rows) > 0 && fmt.Sprintf("%v", qr.Rows[0]) == string(results[ind]) {
				break
			}
			time.Sleep(waitDuration)
			duration -= waitDuration
			if duration <= 0 {
				require.Failf(t, "vreplication catchup timeout", "waitForVReplicationToCatchup timed out, query: %v, result: %v", query, qr)
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
