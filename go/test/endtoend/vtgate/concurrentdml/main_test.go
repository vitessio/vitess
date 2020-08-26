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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
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

func TestInsertIgnoreOnLookupUniqueVindex(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	exec(t, conn, `delete from t1 where c1 = 300`)
	exec(t, conn, `insert into t1 values (300,100,300)`)
	qr1 := exec(t, conn, `select c2.keyspace_id, c3.keyspace_id from lookup_t1 c2, lookup_t2 c3`)

	qr := exec(t, conn, `insert ignore into t1 values (200,100,200)`)
	assert.Zero(t, qr.RowsAffected)

	qr = exec(t, conn, `select * from t1 order by c1`)
	assert.Equal(t, fmt.Sprintf("%v", qr.Rows), `[[INT64(300) INT64(100) INT64(300)]]`)

	qr2 := exec(t, conn, `select c2.keyspace_id, c3.keyspace_id from lookup_t1 c2, lookup_t2 c3`)
	// To ensure lookup vindex is not updated.
	assert.Equal(t, qr1.Rows, qr2.Rows, "")
}

func TestOpenTxBlocksInSerial(t *testing.T) {
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

	exec(t, conn1, `delete from t1 where c1 = 300`)
	exec(t, conn1, `insert into t1 values (300,100,300)`)
	exec(t, conn1, `begin`)
	exec(t, conn1, `UPDATE t1 SET c3 = 400 WHERE c2 = 100`)

	// This will wait for innodb_lock_wait_timeout timeout pf 20 seconds to kick in.
	execAssertError(t, conn2, `insert into t1 values (400,100,400)`, `Lock wait timeout exceeded`)

	qr := exec(t, conn1, `insert ignore into t1 values (200,100,200)`)
	assert.Zero(t, qr.RowsAffected)
	exec(t, conn1, `commit`)

	qr = exec(t, conn1, `select * from t1 order by c1`)
	assert.Equal(t, fmt.Sprintf("%v", qr.Rows), `[[INT64(300) INT64(100) INT64(400)]]`)
}

func TestOpenTxBlocksInConcurrent(t *testing.T) {
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

	exec(t, conn1, `delete from t1 where c1 = 300`)
	exec(t, conn1, `insert into t1 values (300,100,300)`)
	exec(t, conn1, `begin`)
	exec(t, conn1, `UPDATE t1 SET c3 = 400 WHERE c2 = 100`)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		// This will wait for other transaction to complete to through the duplicate key error.
		execAssertError(t, conn2, `insert into t1 values (400,100,400)`, `Duplicate entry '100' for key`)
		wg.Done()
	}()

	time.Sleep(3 * time.Second)
	qr := exec(t, conn1, `insert ignore into t1 values (200,100,200)`)
	assert.Zero(t, qr.RowsAffected)
	exec(t, conn1, `commit`)

	qr = exec(t, conn1, `select * from t1 order by c1`)
	assert.Equal(t, fmt.Sprintf("%v", qr.Rows), `[[INT64(300) INT64(100) INT64(400)]]`)
	wg.Wait()
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
	assert.Contains(t, err.Error(), errorString)
}
