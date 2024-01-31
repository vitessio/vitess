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

	"vitess.io/vitess/go/test/endtoend/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
    field3 BIGINT,
    field4 BIGINT,
    field5 BIGINT,
    field6 BIGINT,
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

CREATE TABLE lookup3 (
    field3 BIGINT NOT NULL,
    keyspace_id binary(8),
    UNIQUE KEY (field3)
) ENGINE=Innodb;

CREATE TABLE lookup4 (
    field4 BIGINT NOT NULL,
    keyspace_id binary(8),
    UNIQUE KEY (field4)
) ENGINE=Innodb;

CREATE TABLE lookup5 (
    field5 BIGINT NOT NULL,
    keyspace_id binary(8),
    UNIQUE KEY (field5)
) ENGINE=Innodb;

CREATE TABLE lookup6 (
    field6 BIGINT NOT NULL,
    keyspace_id binary(8),
    UNIQUE KEY (field6)
) ENGINE=Innodb;

CREATE TABLE thex (
    id VARBINARY(64) NOT NULL,
    field BIGINT NOT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB;
`

	VSchema = `
{
    "sharded": true,
    "vindexes": {
        "hash": {
            "type": "hash"
        },
        "binary_vdx": {
            "type": "binary"
        },
        "binary_md5_vdx": {
            "type": "binary_md5"
        },
        "xxhash_vdx": {
            "type": "xxhash"
        },
        "numeric_vdx": {
            "type": "numeric"
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
        },
        "lookup3": {
            "type": "lookup",
            "params": {
                "from": "field3",
                "no_verify": "true",
                "table": "lookup3",
                "to": "keyspace_id"
            },
            "owner": "t1"
        },
        "lookup4": {
            "type": "lookup",
            "params": {
                "from": "field4",
                "read_lock": "exclusive",
                "table": "lookup4",
                "to": "keyspace_id"
            },
            "owner": "t1"
        },
        "lookup5": {
            "type": "lookup",
            "params": {
                "from": "field5",
                "read_lock": "shared",
                "table": "lookup5",
                "to": "keyspace_id"
            },
            "owner": "t1"
        },
        "lookup6": {
            "type": "lookup",
            "params": {
                "from": "field6",
                "read_lock": "none",
                "table": "lookup6",
                "to": "keyspace_id"
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
                },
                {
                    "column": "field3",
                    "name": "lookup3"
                },
                {
                    "column": "field4",
                    "name": "lookup4"
                },
                {
                    "column": "field5",
                    "name": "lookup5"
                },
                {
                    "column": "field6",
                    "name": "lookup6"
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
        },
        "lookup3": {
            "column_vindexes": [
                {
                    "column": "field3",
                    "name": "binary_md5_vdx"
                }
            ]
        },
        "lookup4": {
            "column_vindexes": [
                {
                    "column": "field4",
                    "name": "binary_md5_vdx"
                }
            ]
        },
        "lookup5": {
            "column_vindexes": [
                {
                    "column": "field5",
                    "name": "binary_md5_vdx"
                }
            ]
        },
        "lookup6": {
            "column_vindexes": [
                {
                    "column": "field6",
                    "name": "binary_md5_vdx"
                }
            ]
        },
        "thex": {
            "column_vindexes": [
                {
                    "column": "id",
                    "name": "binary_vdx"
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
		err = clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 0, false)
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

func TestVindexHexTypes(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "INSERT INTO thex (id, field) VALUES "+
		"(0x01,1), "+
		"(x'a5',2), "+
		"(0x48656c6c6f20476f7068657221,3), "+
		"(x'c26caa1a5eb94096d29a1bec',4)")
	result := utils.Exec(t, conn, "select id, field from thex order by id")

	expected :=
		"[[VARBINARY(\"\\x01\") INT64(1)] " +
			"[VARBINARY(\"Hello Gopher!\") INT64(3)] " +
			"[VARBINARY(\"\\xa5\") INT64(2)] " +
			"[VARBINARY(\"\\xc2l\\xaa\\x1a^\\xb9@\\x96Қ\\x1b\\xec\") INT64(4)]]"
	assert.Equal(t, expected, fmt.Sprintf("%v", result.Rows))
}

func TestVindexBindVarOverlap(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "INSERT INTO t1 (id, field, field2, field3, field4, field5, field6) VALUES "+
		"(0,1,2,3,4,5,6), "+
		"(1,2,3,4,5,6,7), "+
		"(2,3,4,5,6,7,8), "+
		"(3,4,5,6,7,8,9), "+
		"(4,5,6,7,8,9,10), "+
		"(5,6,7,8,9,10,11), "+
		"(6,7,8,9,10,11,12), "+
		"(7,8,9,10,11,12,13), "+
		"(8,9,10,11,12,13,14), "+
		"(9,10,11,12,13,14,15), "+
		"(10,11,12,13,14,15,16), "+
		"(11,12,13,14,15,16,17), "+
		"(12,13,14,15,16,17,18), "+
		"(13,14,15,16,17,18,19), "+
		"(14,15,16,17,18,19,20), "+
		"(15,16,17,18,19,20,21), "+
		"(16,17,18,19,20,21,22), "+
		"(17,18,19,20,21,22,23), "+
		"(18,19,20,21,22,23,24), "+
		"(19,20,21,22,23,24,25), "+
		"(20,21,22,23,24,25,26)")
	result := utils.Exec(t, conn, "select id, field, field2, field3, field4, field5, field6 from t1 order by id")

	expected :=
		"[[INT64(0) INT64(1) INT64(2) INT64(3) INT64(4) INT64(5) INT64(6)] " +
			"[INT64(1) INT64(2) INT64(3) INT64(4) INT64(5) INT64(6) INT64(7)] " +
			"[INT64(2) INT64(3) INT64(4) INT64(5) INT64(6) INT64(7) INT64(8)] " +
			"[INT64(3) INT64(4) INT64(5) INT64(6) INT64(7) INT64(8) INT64(9)] " +
			"[INT64(4) INT64(5) INT64(6) INT64(7) INT64(8) INT64(9) INT64(10)] " +
			"[INT64(5) INT64(6) INT64(7) INT64(8) INT64(9) INT64(10) INT64(11)] " +
			"[INT64(6) INT64(7) INT64(8) INT64(9) INT64(10) INT64(11) INT64(12)] " +
			"[INT64(7) INT64(8) INT64(9) INT64(10) INT64(11) INT64(12) INT64(13)] " +
			"[INT64(8) INT64(9) INT64(10) INT64(11) INT64(12) INT64(13) INT64(14)] " +
			"[INT64(9) INT64(10) INT64(11) INT64(12) INT64(13) INT64(14) INT64(15)] " +
			"[INT64(10) INT64(11) INT64(12) INT64(13) INT64(14) INT64(15) INT64(16)] " +
			"[INT64(11) INT64(12) INT64(13) INT64(14) INT64(15) INT64(16) INT64(17)] " +
			"[INT64(12) INT64(13) INT64(14) INT64(15) INT64(16) INT64(17) INT64(18)] " +
			"[INT64(13) INT64(14) INT64(15) INT64(16) INT64(17) INT64(18) INT64(19)] " +
			"[INT64(14) INT64(15) INT64(16) INT64(17) INT64(18) INT64(19) INT64(20)] " +
			"[INT64(15) INT64(16) INT64(17) INT64(18) INT64(19) INT64(20) INT64(21)] " +
			"[INT64(16) INT64(17) INT64(18) INT64(19) INT64(20) INT64(21) INT64(22)] " +
			"[INT64(17) INT64(18) INT64(19) INT64(20) INT64(21) INT64(22) INT64(23)] " +
			"[INT64(18) INT64(19) INT64(20) INT64(21) INT64(22) INT64(23) INT64(24)] " +
			"[INT64(19) INT64(20) INT64(21) INT64(22) INT64(23) INT64(24) INT64(25)] " +
			"[INT64(20) INT64(21) INT64(22) INT64(23) INT64(24) INT64(25) INT64(26)]]"
	assert.Equal(t, expected, fmt.Sprintf("%v", result.Rows))
}
