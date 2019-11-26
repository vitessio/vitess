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

package sequence

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	keyspaceName    = "ks"
	cell            = "zone1"
	hostname        = "localhost"
	sqlSchema       = `
	create table sequence_test(
		id bigint,
		val varchar(16),
		primary key(id)
	)Engine=InnoDB;

	create table sequence_test_seq (
		id int default 0, 
		next_id bigint default null, 
		cache bigint default null, 
		primary key(id)
	) comment 'vitess_sequence' Engine=InnoDB;
	`

	vSchema = `
		{	
			"sharded":false,
			"vindexes": {
				"hash_index": {
					"type": "hash"
				}
			},	
			"tables": {
				"sequence_test":{
					"auto_increment":{
						"column" : "id",
						"sequence" : "sequence_test_seq"
					},
					"column_vindexes": [
						{
							"column": "id",
							"name": "hash_index"
						}
					]
				},
				"sequence_test_seq": {
					"type":   "sequence"
				}
			}
		}
	`
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		clusterInstance =  cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
			VSchema:   vSchema,
		}
		if err := clusterInstance.StartUnshardedKeyspace(*keyspace, 1, false); err != nil {
			return 1
		}

		// Start vtgate
		if err := clusterInstance.StartVtgate(); err != nil {
			return 1
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	if err != nil {
		t.Fatal(err)
	}
	return qr
}

func TestSeq(t *testing.T) {
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	//Initialize seq table
	exec(t, conn, "insert into sequence_test_seq(id, next_id, cache) values(0,1,10)")

	//Insert 4 values in the main table
	exec(t, conn, "insert into sequence_test(val) values('a'), ('b') ,('c'), ('d')")

	// Test select calls to main table and verify expected id.
	qr := exec(t, conn, "select id, val  from sequence_test where id=4")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(4) VARCHAR("d")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// Test next available seq id from cache
	qr = exec(t, conn, "select next 1 values from sequence_test_seq")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(5)]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//Test next_id from seq table which should be the increased by cache value(id+cache)
	qr = exec(t, conn, "select next_id from sequence_test_seq")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(11)]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// Test insert with no auto-inc
	exec(t, conn, "insert into sequence_test(id, val) values(6, 'f')")
	qr = exec(t, conn, "select * from sequence_test")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("a")] [INT64(2) VARCHAR("b")] [INT64(3) VARCHAR("c")] [INT64(4) VARCHAR("d")] [INT64(6) VARCHAR("f")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//Next insert will fail as we have corrupted the sequence
	exec(t, conn, "begin")
	_, err = conn.ExecuteFetch("insert into sequence_test(val) values('g')", 1000, false)
	exec(t, conn, "rollback")
	want := "Duplicate entry"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("wrong insert: %v, must contain %s", err, want)
	}

}
