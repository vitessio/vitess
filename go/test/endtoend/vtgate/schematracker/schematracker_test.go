/*
Copyright 2021 The Vitess Authors.

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

package schematracker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	hostname     = "localhost"
	keyspaceName = "ks"
	cell         = "zone1"
	sqlSchema    = `
		create table main (
			id bigint,
			val varchar(128),
			primary key(id)
		) Engine=InnoDB;
`
)

func TestNewUnshardedTable(t *testing.T) {
	defer cluster.PanicHandler(t)
	var err error

	// initialize our cluster
	clusterInstance := cluster.NewCluster(cell, hostname)
	defer clusterInstance.Teardown()

	// Start topo server
	err = clusterInstance.StartTopo()
	require.NoError(t, err)

	// create keyspace
	keyspace := &cluster.Keyspace{
		Name:      keyspaceName,
		SchemaSQL: sqlSchema,
	}

	// enabling and setting the schema reload time to one second
	clusterInstance.VtTabletExtraArgs = []string{"-queryserver-config-schema-change-signal", "-queryserver-config-schema-change-signal-interval", "1"}
	err = clusterInstance.StartUnshardedKeyspace(*keyspace, 1, false)
	require.NoError(t, err)

	// Start vtgate with the schema_change_signal flag
	clusterInstance.VtGateExtraArgs = []string{"-schema_change_signal"}
	err = clusterInstance.StartVtgate()
	require.NoError(t, err)

	// create a sql connection
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: clusterInstance.VtgateMySQLPort,
	})
	require.NoError(t, err)
	defer conn.Close()

	// ensuring our initial table "main" is in the schema
	qr := exec(t, conn, "SHOW VSCHEMA TABLES")
	got := fmt.Sprintf("%v", qr.Rows)
	want := `[[VARCHAR("dual")] [VARCHAR("main")]]`
	require.Equal(t, want, got)

	// create a new table which is not part of the VSchema
	exec(t, conn, `create table new_table_tracked(id bigint, name varchar(100), primary key(id)) Engine=InnoDB`)

	// waiting for the vttablet's schema_reload interval to kick in
	time.Sleep(2 * time.Second)

	// ensuring our new table is in the schema
	qr = exec(t, conn, "SHOW VSCHEMA TABLES")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[[VARCHAR("dual")] [VARCHAR("main")] [VARCHAR("new_table_tracked")]]`
	assert.Equal(t, want, got)
}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.NoError(t, err, "for query: "+query)
	return qr
}
