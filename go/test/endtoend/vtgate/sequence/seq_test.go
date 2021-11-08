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

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance    *cluster.LocalProcessCluster
	cell               = "zone1"
	hostname           = "localhost"
	unshardedKs        = "uks"
	unshardedSQLSchema = `
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

CREATE TABLE id_seq ( id INT, next_id BIGINT, cache BIGINT, PRIMARY KEY(id)) comment 'vitess_sequence';

INSERT INTO id_seq (id, next_id, cache) values (0, 1, 1000);

	`

	unshardedVSchema = `
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
				},
                "id_seq": {
                    "type": "sequence"
                 }
			}
		}
`

	shardedKeyspaceName = `sks`

	shardedSQLSchema = `
CREATE TABLE ` + "`dotted.tablename`" + ` (
    id BIGINT NOT NULL,
    c1 DOUBLE NOT NULL,
    c2 BIGINT,
    PRIMARY KEY (id),
    UNIQUE KEY (c1, c2)
);

CREATE TABLE lookup_vindex (
    c1 DOUBLE NOT NULL,
    c2 BIGINT,
    keyspace_id BLOB,
    UNIQUE KEY (c1, c2)
);

CREATE TABLE allDefaults (
    id bigint NOT NULL,
    foo varchar(255),
    primary key (id)
);`

	shardedVSchema = `
		{
		  "sharded": true,
		  "vindexes": {
			"lookup_vindex": {
			  "type": "consistent_lookup",
			  "params": {
				"from": "c1,c2",
				"table": "lookup_vindex",
				"to": "keyspace_id"
			  },
			  "owner": "dotted.tablename"
			},
			"hash": {
			  "type": "hash"
			}
		  },
		  "tables": {
			"dotted.tablename": {
			  "columnVindexes": [
				{
				  "column": "id",
				  "name": "hash"
				},
				{
				  "name": "lookup_vindex",
				  "columns": [ "c1", "c2" ]
				}
			  ],
			  "autoIncrement": {
				"column": "id",
				"sequence": "id_seq"
			  }
			},			
			"allDefaults": {
			  "columnVindexes": [
				{
				  "column": "id",
				  "name": "hash"
				}
              ],
			  "autoIncrement": {
				"column": "id",
				"sequence": "id_seq"
			  }
			},
			"lookup_vindex": {
			  "columnVindexes": [
				{
				  "column": "c1",
				  "name": "hash"
				}
			  ]
			}
		  }
		}`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1
		}

		// Start keyspace
		uKeyspace := &cluster.Keyspace{
			Name:      unshardedKs,
			SchemaSQL: unshardedSQLSchema,
			VSchema:   unshardedVSchema,
		}
		if err := clusterInstance.StartUnshardedKeyspace(*uKeyspace, 1, false); err != nil {
			return 1
		}

		sKeyspace := &cluster.Keyspace{
			Name:      shardedKeyspaceName,
			SchemaSQL: shardedSQLSchema,
			VSchema:   shardedVSchema,
		}
		if err := clusterInstance.StartKeyspace(*sKeyspace, []string{"-80", "80-"}, 1, false); err != nil {
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
	require.Nil(t, err)
	return qr
}

func TestSeq(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
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

func TestDotTableSeq(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host:   "localhost",
		Port:   clusterInstance.VtgateMySQLPort,
		DbName: shardedKeyspaceName,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.ExecuteFetch("insert into `dotted.tablename` (c1,c2) values (10,10)", 1000, true)
	require.NoError(t, err)

	_, err = conn.ExecuteFetch("insert into `dotted.tablename` (c1,c2) values (10,10)", 1000, true)
	require.Error(t, err)
	mysqlErr := err.(*mysql.SQLError)
	assert.Equal(t, 1062, mysqlErr.Num)
	assert.Equal(t, "23000", mysqlErr.State)
	assert.Contains(t, mysqlErr.Message, "Duplicate entry")
}

func TestInsertAllDefaults(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host:   "localhost",
		Port:   clusterInstance.VtgateMySQLPort,
		DbName: shardedKeyspaceName,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// inserting into a table that has default values for all columns works well
	exec(t, conn, `insert into allDefaults () values ()`)
	result := exec(t, conn, `select * from uks.id_seq`)
	assert.Equal(t, 1, len(result.Rows))

	// inserting into a table that does not have default values for all columns fails
	_, err = conn.ExecuteFetch("insert into lookup_vindex () values ()", 0, false)
	require.Error(t, err)
}
