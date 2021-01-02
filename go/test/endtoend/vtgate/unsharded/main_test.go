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

package unsharded

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
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
	KeyspaceName    = "customer"
	SchemaSQL       = `
CREATE TABLE t1 (
    c1 BIGINT NOT NULL,
    c2 BIGINT NOT NULL,
    c3 BIGINT,
    c4 varchar(100),
    PRIMARY KEY (c1),
    UNIQUE KEY (c2),
    UNIQUE KEY (c3),
    UNIQUE KEY (c4)
) ENGINE=Innodb;

CREATE TABLE allDefaults (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255)
) ENGINE=Innodb;`
	VSchema = `
{
    "sharded": false,
    "tables": {
        "t1": {
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
                },
                {
                    "name": "c4",
                    "type": "VARCHAR"
                }
            ]
        },
        "allDefaults": {
            "columns": [
                {
                    "name": "id",
                    "type": "INT64"
                },
                {
                    "name": "name",
                    "type": "VARCHAR"
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

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1
		}

		// Start keyspace
		Keyspace := &cluster.Keyspace{
			Name:      KeyspaceName,
			SchemaSQL: SchemaSQL,
			VSchema:   VSchema,
		}
		if err := clusterInstance.StartUnshardedKeyspace(*Keyspace, 0, false); err != nil {
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

func TestSelectIntoAndLoadFrom(t *testing.T) {
	// Test is skipped because it requires secure-file-priv variable to be set to not NULL or empty.
	t.Skip()
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	defer exec(t, conn, `delete from t1`)
	exec(t, conn, `insert into t1(c1, c2, c3, c4) values (300,100,300,'abc')`)
	res := exec(t, conn, `select @@secure_file_priv;`)
	directory := res.Rows[0][0].ToString()
	query := `select * from t1 into outfile '` + directory + `x.txt'`
	exec(t, conn, query)
	defer os.Remove(directory + `x.txt`)
	query = `load data infile '` + directory + `x.txt' into table t1`
	execAssertError(t, conn, query, "Duplicate entry '300' for key 'PRIMARY'")
	exec(t, conn, `delete from t1`)
	exec(t, conn, query)
	assertMatches(t, conn, `select c1,c2,c3 from t1`, `[[INT64(300) INT64(100) INT64(300)]]`)
	query = `select * from t1 into dumpfile '` + directory + `x1.txt'`
	exec(t, conn, query)
	defer os.Remove(directory + `x1.txt`)
	query = `select * from t1 into outfile '` + directory + `x2.txt' Fields terminated by ';' optionally enclosed by '"' escaped by '\t' lines terminated by '\n'`
	exec(t, conn, query)
	defer os.Remove(directory + `x2.txt`)
	query = `load data infile '` + directory + `x2.txt' replace into table t1 Fields terminated by ';' optionally enclosed by '"' escaped by '\t' lines terminated by '\n'`
	exec(t, conn, query)
	assertMatches(t, conn, `select c1,c2,c3 from t1`, `[[INT64(300) INT64(100) INT64(300)]]`)
}

func TestEmptyStatement(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()
	defer exec(t, conn, `delete from t1`)
	execAssertError(t, conn, " \t;", "Query was empty")
	execMulti(t, conn, `insert into t1(c1, c2, c3, c4) values (300,100,300,'abc'); ;; insert into t1(c1, c2, c3, c4) values (301,101,301,'abcd');;`)
	assertMatches(t, conn, `select c1,c2,c3 from t1`, `[[INT64(300) INT64(100) INT64(300)] [INT64(301) INT64(101) INT64(301)]]`)
}

func TestInsertAllDefaults(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	exec(t, conn, `insert into allDefaults () values ()`)
	assertMatches(t, conn, `select * from allDefaults`, "[[INT64(1) NULL]]")
}

func TestCreateViewUnsharded(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	defer exec(t, conn, `delete from t1`)
	// Test that create view works and the output is as expected
	_, err = conn.ExecuteFetch(`create view v1 as select * from t1`, 1000, true)
	require.NoError(t, err)
	exec(t, conn, `insert into t1(c1, c2, c3, c4) values (300,100,300,'abc'),(30,10,30,'ac'),(3,0,3,'a')`)
	assertMatches(t, conn, "select * from v1", `[[INT64(3) INT64(0) INT64(3) VARCHAR("a")] [INT64(30) INT64(10) INT64(30) VARCHAR("ac")] [INT64(300) INT64(100) INT64(300) VARCHAR("abc")]]`)
}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.NoError(t, err)
	return qr
}

func execMulti(t *testing.T, conn *mysql.Conn, query string) []*sqltypes.Result {
	t.Helper()
	var res []*sqltypes.Result
	qr, more, err := conn.ExecuteFetchMulti(query, 1000, true)
	res = append(res, qr)
	require.NoError(t, err)
	for more == true {
		qr, more, _, err = conn.ReadQueryResult(1000, true)
		require.NoError(t, err)
		res = append(res, qr)
	}
	return res
}

func execAssertError(t *testing.T, conn *mysql.Conn, query string, errorString string) {
	t.Helper()
	_, err := conn.ExecuteFetch(query, 1000, true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), errorString)
}

func assertMatches(t *testing.T, conn *mysql.Conn, query, expected string) {
	t.Helper()
	qr := exec(t, conn, query)
	got := fmt.Sprintf("%v", qr.Rows)
	diff := cmp.Diff(expected, got)
	if diff != "" {
		t.Errorf("Query: %s (-want +got):\n%s", query, diff)
	}
}
