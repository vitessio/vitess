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

package concurrentdml

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/sqltypes"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	cell            = "zone1"
	hostname        = "localhost"
	unsKs           = "commerce"
	unsSchema       = `
CREATE TABLE t1_seq (
    id INT, 
    next_id BIGINT, 
    cache BIGINT, 
    PRIMARY KEY(id)
) comment 'vitess_sequence';

INSERT INTO t1_seq (id, next_id, cache) values(0, 1, 1000);
`

	unsVSchema = `
{
  "sharded": false,
  "tables": {}
}
`
	sKs     = "customer"
	sSchema = `
CREATE TABLE t1 (
    c1 BIGINT NOT NULL,
    c2 BIGINT NOT NULL,
    c3 BIGINT,
    PRIMARY KEY (c1),
    UNIQUE KEY (c2),
    UNIQUE KEY (c3)
) ENGINE=Innodb;

CREATE TABLE lookup_t1 (
    c2 BIGINT NOT NULL,
    keyspace_id BINARY(8),
    primary key (c2)
);

CREATE TABLE lookup_t2 (
    c3 BIGINT NOT NULL,
    keyspace_id BINARY(8),
    primary key (c3)
);
`

	sVSchema = `
{
    "sharded": true,
    "vindexes": {
        "xxhash": {
            "type": "xxhash"
        },
        "lookup_c2": {
            "type": "consistent_lookup_unique",
            "params": {
                "table": "lookup_t1",
                "from": "c2",
                "to": "keyspace_id",
                "ignore_nulls": "true"
            },
            "owner": "t1"
        },
        "lookup_c3": {
            "type": "consistent_lookup_unique",
            "params": {
                "table": "lookup_t2",
                "from": "c3",
                "to": "keyspace_id",
                "ignore_nulls": "true"
            },
            "owner": "t1"
        }
    },
    "tables": {
        "t1": {
            "columnVindexes": [
                {
                    "column": "c1",
                    "name": "xxhash"
                },
                {
                    "column": "c2",
                    "name": "lookup_c2"
                },
                {
                    "column": "c3",
                    "name": "lookup_c3"
                }
            ],
            "columns": [
                {
                    "name": "c1",
                    "type": "INT64"
                },
                {
                    "name": "c2",
                    "type": "INT64"
                },
                {
                    "name": "c3",
                    "type": "INT64"
                }
            ],
            "autoIncrement": {
                "column": "c1",
                "sequence": "commerce.t1_seq"
            },
            "columnListAuthoritative": true
        },
        "lookup_t1": {
            "columnVindexes": [
                {
                    "column": "c2",
                    "name": "xxhash"
                }
            ]
        },
        "lookup_t2": {
            "columnVindexes": [
                {
                    "column": "c3",
                    "name": "xxhash"
                }
            ]
        }
    }
}
`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()
		//defer time.Sleep(10 * time.Minute)

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1
		}

		// Start keyspace
		uKeyspace := &cluster.Keyspace{
			Name:      unsKs,
			SchemaSQL: unsSchema,
			VSchema:   unsVSchema,
		}
		if err := clusterInstance.StartUnshardedKeyspace(*uKeyspace, 1, false); err != nil {
			return 1
		}

		sKeyspace := &cluster.Keyspace{
			Name:      sKs,
			SchemaSQL: sSchema,
			VSchema:   sVSchema,
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

func TestConcurrentUpdateInsertLookupVindex(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn1, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn1.Close()

	conn2, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn2.Close()

	conn3, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn3.Close()

	/*
		t1
		300 100 300
		300 100 400 - in flight

		lookup_c2
		100 300

		lookup_c3
		300 300
		400 300 - in flight

		conn1
			- pre - insert lookup_c3, update lock lookup_c2, insert lookup_c2 (handDup)
			- normal - update_t1, share lock lookup_c2
			- post - delete lookup_c3

		conn2
			- pre - insert lookup_c2 (handDup)
		, lookup_c3
			- normal - lock share lookup_c2
			- post

	*/
	exec(t, conn1, `insert into t1 values (300,100,300)`)
	exec(t, conn1, `start transaction`)
	exec(t, conn1, `UPDATE t1 SET c3 = 400 WHERE c2 = 100`)

	go func() {
		execAssertError(t, conn2, `insert into t1 values (400,100,400);`, `Duplicate entry '100' for key`)
	}()

	time.Sleep(2 * time.Second)
	qr := exec(t, conn1, `insert ignore into t1 values (200,100,200)`)
	require.Zero(t, qr.RowsAffected)

	exec(t, conn1, `commit`)

	qr = exec(t, conn3, `select * from t1 order by c1`)
	assert.Equal(t, fmt.Sprintf("%v", qr.Rows), `[[INT64(300) INT64(100) INT64(400)]]`)
}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.NoError(t, err)
	return qr
}

func execAssertError(t *testing.T, conn *mysql.Conn, query string, errorString string) {
	t.Helper()
	_, err := conn.ExecuteFetch(query, 1000, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), errorString)
}
