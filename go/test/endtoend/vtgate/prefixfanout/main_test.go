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
package prefixfanout

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
	cell            = "zone1"
	hostname        = "localhost"

	sKs     = "cfc_testing"
	sSchema = `
CREATE TABLE t1 (
c1 VARCHAR(20) NOT NULL,
c2 varchar(40) NOT NULL,
PRIMARY KEY (c1)
) ENGINE=Innodb;
`
	sVSchema = `
{
    "sharded": true,
    "vindexes": {
        "cfc": {
          "type": "cfc"
		}
	},
    "tables": {
        "t1": {
            "column_vindexes": [
                {
                    "column": "c1",
                    "name": "cfc"
                }
			],
			"columns": [
				{
					"name": "c2",
					"type": "VARCHAR"
				}
			]
		}
    }
}`

	sKsMD5     = `cfc_testing_md5`
	sSchemaMD5 = `
CREATE TABLE t2 (
c1 VARCHAR(20) NOT NULL,
c2 varchar(40) NOT NULL,
PRIMARY KEY (c1)
) ENGINE=Innodb;`

	sVSchemaMD5 = `
{
    "sharded": true,
    "vindexes": {
   	"cfc_md5": {
		  "type": "cfc",
		  "params": {
			  "hash": "md5",
			  "offsets": "[2]"
		  }
		}
	},
    "tables": {
      	"t2": {
            "column_vindexes": [
                {
                    "column": "c1",
                    "name": "cfc_md5"
                }
			],
			"columns": [
				{
					"name": "c2",
					"type": "VARCHAR"
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
		sKeyspace := &cluster.Keyspace{
			Name:      sKs,
			SchemaSQL: sSchema,
			VSchema:   sVSchema,
		}
		// cfc_testing
		if err := clusterInstance.StartKeyspace(*sKeyspace, []string{"-41", "41-4180", "4180-42", "42-"}, 0, false); err != nil {
			return 1
		}
		// cfc_testing_md5
		if err := clusterInstance.StartKeyspace(
			cluster.Keyspace{
				Name:      sKsMD5,
				SchemaSQL: sSchemaMD5,
				VSchema:   sVSchemaMD5,
			}, []string{"-c2", "c2-c20a80", "c20a80-d0", "d0-"}, 0, false); err != nil {
			return 1
		}

		// Start vtgate
		// This waits for the vtgate process to be healthy
		if err := clusterInstance.StartVtgate(); err != nil {
			return 1
		}

		// Wait for the cluster to be running and healthy
		if err := clusterInstance.WaitForTabletsToHealthyInVtgate(); err != nil {
			return 1
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestCFCPrefixQueryNoHash(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "delete from t1")
	defer utils.Exec(t, conn, "delete from t1")
	// prepare the sentinel rows, i.e. every shard stores a row begins with letter A.
	// hex ascii code of 'A' is 41. For a given primary key, e.g. 'AA' here, it should
	// only legally belong to a single shard. We insert into all shards with different
	// `c2` value so that we can test if a query fans out to all or not. Based on the
	// following shard layout only "41-4180", "4180-42" should serve the rows staring with 'A'.
	shards := []string{"-41", "41-4180", "4180-42", "42-"}
	for i, s := range shards {
		utils.Exec(t, conn, fmt.Sprintf("use `%s:%s`", sKs, s))
		utils.Exec(t, conn, fmt.Sprintf("insert into t1 values('AA', 'shard-%d')", i))
	}
	utils.Exec(t, conn, "use cfc_testing")
	qr := utils.Exec(t, conn, "select c2 from t1 where c1 like 'A%' order by c2")
	assert.Equal(t, 2, len(qr.Rows))
	// should only target a subset of shards serving rows starting with 'A'.
	assert.EqualValues(t, `[[VARCHAR("shard-1")] [VARCHAR("shard-2")]]`, fmt.Sprintf("%v", qr.Rows))
	// should only target a subset of shards serving rows starting with 'AA',
	// the shards to which 'AA' maps to.
	qr = utils.Exec(t, conn, "select c2 from t1 where c1 like 'AA'")
	assert.Equal(t, 1, len(qr.Rows))
	assert.EqualValues(t, `[[VARCHAR("shard-1")]]`, fmt.Sprintf("%v", qr.Rows))
	// fan out to all when there is no prefix
	qr = utils.Exec(t, conn, "select c2 from t1 where c1 like '%A' order by c2")
	assert.Equal(t, 4, len(qr.Rows))
	fmt.Printf("%v", qr.Rows)
	for i, r := range qr.Rows {
		assert.Equal(t, fmt.Sprintf("shard-%d", i), r[0].ToString())
	}
}

func TestCFCPrefixQueryWithHash(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "delete from t2")
	defer utils.Exec(t, conn, "delete from t2")

	shards := []string{"-c2", "c2-c20a80", "c20a80-d0", "d0-"}
	// same idea of sentinel rows as above. Even though each row legally belongs to
	// only one shard, we insert into all shards with different info to test our fan out.
	for i, s := range shards {
		utils.Exec(t, conn, fmt.Sprintf("use `%s:%s`", sKsMD5, s))
		utils.Exec(t, conn, fmt.Sprintf("insert into t2 values('12AX', 'shard-%d')", i))
		utils.Exec(t, conn, fmt.Sprintf("insert into t2 values('12BX', 'shard-%d')", i))
		utils.Exec(t, conn, fmt.Sprintf("insert into t2 values('27CX', 'shard-%d')", i))
	}

	utils.Exec(t, conn, fmt.Sprintf("use `%s`", sKsMD5))
	// The prefix is ('12', 'A')
	// md5('12') -> c20ad4d76fe97759aa27a0c99bff6710
	// md5('A') -> 7fc56270e7a70fa81a5935b72eacbe29
	// so keyspace id is c20a7f, which means shards "c2-c20a80"
	qr := utils.Exec(t, conn, "select c2 from t2 where c1 like '12A%' order by c2")
	assert.Equal(t, 1, len(qr.Rows))
	assert.Equal(t, `[[VARCHAR("shard-1")]]`, fmt.Sprintf("%v", qr.Rows))
	// The prefix is ('12')
	// md5('12') -> c20ad4d76fe97759aa27a0c99bff6710 so the corresponding
	// so keyspace id is c20a, which means shards "c2-c20a80", "c20a80-d0"
	qr = utils.Exec(t, conn, "select c2 from t2 where c1 like '12%' order by c2")
	assert.Equal(t, 4, len(qr.Rows))
	assert.Equal(t, `[[VARCHAR("shard-1")] [VARCHAR("shard-1")] [VARCHAR("shard-2")] [VARCHAR("shard-2")]]`, fmt.Sprintf("%v", qr.Rows))
	// in vschema the prefix length is defined as 2 bytes however only 1 byte
	// is provided here so the query fans out to all.
	qr = utils.Exec(t, conn, "select c2 from t2 where c1 like '2%' order by c2")
	assert.Equal(t, 4, len(qr.Rows))
	assert.Equal(t, `[[VARCHAR("shard-0")] [VARCHAR("shard-1")] [VARCHAR("shard-2")] [VARCHAR("shard-3")]]`, fmt.Sprintf("%v", qr.Rows))
}

func TestCFCInsert(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "delete from t1")
	defer utils.Exec(t, conn, "delete from t1")

	utils.Exec(t, conn, "insert into t1 (c1, c2) values ('AAA', 'BBB')")
	qr := utils.Exec(t, conn, "select c2 from t1 where c1 like 'A%'")
	assert.Equal(t, 1, len(qr.Rows))
	shards := []string{"-41", "4180-42", "42-"}
	for _, s := range shards {
		utils.Exec(t, conn, fmt.Sprintf("use `cfc_testing:%s`", s))
		qr = utils.Exec(t, conn, "select * from t1")
		assert.Equal(t, 0, len(qr.Rows))
	}
	// 'AAA' belongs to 41-4180
	utils.Exec(t, conn, "use `cfc_testing:41-4180`")
	qr = utils.Exec(t, conn, "select c2 from t1")
	assert.Equal(t, 1, len(qr.Rows))
	assert.Equal(t, `[[VARCHAR("BBB")]]`, fmt.Sprintf("%v", qr.Rows))
}
