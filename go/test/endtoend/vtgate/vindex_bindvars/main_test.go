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

package vtgate

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gotest.tools/assert"

	"vitess.io/vitess/go/sqltypes"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	KeyspaceName    = "ks"
	Cell            = "test"
	SchemaSQL       = `CREATE TABLE t1 (
    id BIGINT NOT NULL,
    field BIGINT NOT NULL,
    field2 BIGINT,
    PRIMARY KEY (id)
) ENGINE=Innodb;


CREATE TABLE lookup1 (
    field BIGINT NOT NULL,
    keyspace_id binary(8),
    UNIQUE KEY (field)
) ENGINE=Innodb;

CREATE TABLE lookup2 (
    field2 BIGINT NOT NULL,
    keyspace_id binary(8),
    UNIQUE KEY (field2)
) ENGINE=Innodb;
`

	VSchema = `
{
    "sharded": true,
    "vindexes": {
        "hash": {
            "type": "hash"
        },
        "lookup1": {
            "type": "consistent_lookup",
            "params": {
                "table": "lookup1",
                "from": "field",
                "to": "keyspace_id",
		"ignore_nulls": "true"
            },
            "owner": "t1"
        },
        "lookup2": {
            "type": "consistent_lookup",
            "params": {
                "table": "lookup2",
                "from": "field2",
                "to": "keyspace_id",
		"ignore_nulls": "true"
            },
            "owner": "t1"
        }
    },
    "tables": {
        "t1": {
            "column_vindexes": [
                {
                    "column": "id",
                    "name": "hash"
                },
                {
                    "column": "field",
                    "name": "lookup1"
                },
                {
                    "column": "field2",
                    "name": "lookup2"
                }
            ]
        },
        "lookup1": {
            "column_vindexes": [
                {
                    "column": "field",
                    "name": "hash"
                }
            ]
        },
        "lookup2": {
            "column_vindexes": [
                {
                    "column": "field2",
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
		clusterInstance = cluster.NewCluster(Cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      KeyspaceName,
			SchemaSQL: SchemaSQL,
			VSchema:   VSchema,
		}
		err = clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 1, true)
		if err != nil {
			return 1
		}

		// Start vtgate
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}
		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}
		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestVindexBindVarOverlap(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	exec(t, conn, "INSERT INTO t1 (id, field, field2) VALUES "+
		"(0,1,2), "+
		"(1,2,3), "+
		"(2,3,4), "+
		"(3,4,5), "+
		"(4,5,6), "+
		"(5,6,7), "+
		"(6,7,8), "+
		"(7,8,9), "+
		"(8,9,10), "+
		"(9,10,11), "+
		"(10,11,12), "+
		"(11,12,13), "+
		"(12,13,14), "+
		"(13,14,15), "+
		"(14,15,16), "+
		"(15,16,17), "+
		"(16,17,18), "+
		"(17,18,19), "+
		"(18,19,20), "+
		"(19,20,21), "+
		"(20,21,22)")
	result := exec(t, conn, "select id, field, field2 from t1 order by id")

	expected :=
		"[[INT64(0) INT64(1) INT64(2)] " +
			"[INT64(1) INT64(2) INT64(3)] " +
			"[INT64(2) INT64(3) INT64(4)] " +
			"[INT64(3) INT64(4) INT64(5)] " +
			"[INT64(4) INT64(5) INT64(6)] " +
			"[INT64(5) INT64(6) INT64(7)] " +
			"[INT64(6) INT64(7) INT64(8)] " +
			"[INT64(7) INT64(8) INT64(9)] " +
			"[INT64(8) INT64(9) INT64(10)] " +
			"[INT64(9) INT64(10) INT64(11)] " +
			"[INT64(10) INT64(11) INT64(12)] " +
			"[INT64(11) INT64(12) INT64(13)] " +
			"[INT64(12) INT64(13) INT64(14)] " +
			"[INT64(13) INT64(14) INT64(15)] " +
			"[INT64(14) INT64(15) INT64(16)] " +
			"[INT64(15) INT64(16) INT64(17)] " +
			"[INT64(16) INT64(17) INT64(18)] " +
			"[INT64(17) INT64(18) INT64(19)] " +
			"[INT64(18) INT64(19) INT64(20)] " +
			"[INT64(19) INT64(20) INT64(21)] " +
			"[INT64(20) INT64(21) INT64(22)]]"
	assert.Equal(t, expected, fmt.Sprintf("%v", result.Rows))
}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.NoError(t, err)
	return qr
}
