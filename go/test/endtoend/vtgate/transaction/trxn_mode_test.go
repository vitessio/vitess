/*
Copyright 2019 The Vitess Authors.

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

package transaction

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	keyspaceName    = "ks"
	cell            = "zone1"
	hostname        = "localhost"
	sqlSchema       = `
	create table twopc_user (
		user_id bigint,
		name varchar(128),
		primary key (user_id)
	) Engine=InnoDB;

	create table twopc_lookup (
		name varchar(128),
		id bigint,
		primary key (id)
	) Engine=InnoDB;`

	vSchema = `
	{	
		"sharded":true,
		"vindexes": {
			"hash_index": {
				"type": "hash"
			},
			"twopc_lookup_vdx": {
				"type": "lookup_hash_unique",
				"params": {
				  "table": "twopc_lookup",
				  "from": "name",
				  "to": "id",
				  "autocommit": "true"
				},
				"owner": "twopc_user"
			}
		},	
		"tables": {
			"twopc_user":{
				"column_vindexes": [
					{
						"column": "user_id",
						"name": "hash_index"
					},
					{
						"column": "name",
						"name": "twopc_lookup_vdx"
					}
				]
			},
			"twopc_lookup": {
				"column_vindexes": [
					{
						"column": "id",
						"name": "hash_index"
					}
				]
			}
		}
	}
	`
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitcode, err := func() (int, error) {
		clusterInstance = &cluster.LocalProcessCluster{Cell: cell, Hostname: hostname}
		defer clusterInstance.Teardown()

		// Reserve vtGate port in order to pass it to vtTablet
		clusterInstance.VtgateGrpcPort = clusterInstance.GetAndReservePort()
		// Set extra tablet args for twopc
		clusterInstance.VtTabletExtraArgs = []string{
			"-twopc_enable",
			"-twopc_coordinator_address", fmt.Sprintf("localhost:%d", clusterInstance.VtgateGrpcPort),
			"-twopc_abandon_age", "3600",
		}

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
			VSchema:   vSchema,
		}
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 1, false); err != nil {
			return 1, err
		}

		// Starting Vtgate in SINGLE transaction mode
		clusterInstance.VtGateExtraArgs = []string{"-transaction_mode", "SINGLE"}
		if err := clusterInstance.StartVtgate(); err != nil {
			return 1, err
		}
		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}

		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}
}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	if err != nil {
		t.Fatal(err)
	}
	return qr
}

// TestTransactionModes tests trasactions using twopc mode
func TestTransactionModes(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Insert targeted to multiple tables should fail as Transaction mode is SINGLE
	exec(t, conn, "begin")
	exec(t, conn, "insert into twopc_user(user_id, name) values(1,'john')")
	_, err = conn.ExecuteFetch("insert into twopc_user(user_id, name) values(6,'vick')", 1000, false)
	exec(t, conn, "rollback")
	want := "multi-db transaction attempted"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("multi-db insert: %v, must contain %s", err, want)
	}

	// Enable TWOPC transaction mode
	clusterInstance.VtGateExtraArgs = []string{"-transaction_mode", "TWOPC"}
	// Restart VtGate
	if err = clusterInstance.ReStartVtgate(); err != nil {
		t.Errorf("Fail to re-start vtgate with new config:  %v", err)
	}

	// Make a new mysql connection to vtGate
	vtParams = mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn2, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()

	// Insert targeted to multiple db should PASS with TWOPC trx mode
	exec(t, conn2, "begin")
	exec(t, conn2, "insert into twopc_user(user_id, name) values(3,'mark')")
	exec(t, conn2, "insert into twopc_user(user_id, name) values(4,'doug')")
	exec(t, conn2, "insert into twopc_lookup(name, id) values('Tim',7)")
	exec(t, conn2, "commit")

	// Verify the values are present
	qr := exec(t, conn2, "select user_id from twopc_user where name='mark'")
	got := fmt.Sprintf("%v", qr.Rows)
	want = `[[INT64(3)]]`
	assert.Equal(t, want, got)

	qr = exec(t, conn2, "select name from twopc_lookup where id=3")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[[VARCHAR("mark")]]`
	assert.Equal(t, want, got)

	// DELETE from multiple tables using TWOPC transaction mode
	exec(t, conn2, "begin")
	exec(t, conn2, "delete from twopc_user where user_id = 3")
	exec(t, conn2, "delete from twopc_lookup where id = 3")
	exec(t, conn2, "commit")

	// VERIFY that values are deleted
	qr = exec(t, conn2, "select user_id from twopc_user where user_id=3")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[]`
	assert.Equal(t, want, got)

	qr = exec(t, conn2, "select name from twopc_lookup where id=3")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[]`
	assert.Equal(t, want, got)
}
