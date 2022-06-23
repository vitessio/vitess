/*
Copyright 2022 The Vitess Authors.

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

package vtexplain

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"os"
	"testing"

	"vitess.io/vitess/go/sqltypes"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	shardedKs       = "ks"

	shardedKsShards = []string{"-40", "40-80", "80-c0", "c0-"}
	Cell            = "test"
	//go:embed schema.sql
	shardedSchemaSQL string

	//go:embed vschema.json
	shardedVSchema string
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
		sKs := &cluster.Keyspace{
			Name:      shardedKs,
			SchemaSQL: shardedSchemaSQL,
			VSchema:   shardedVSchema,
		}

		err = clusterInstance.StartKeyspace(*sKs, shardedKsShards, 0, false)
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

func TestVtGateVtExplain(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	wantQr := sqltypes.MakeTestResult(sqltypes.MakeTestFields("#|keyspace|shard|query", "int32|varchar|varchar|varchar"),
		"1|ks|-40|begin",
		"2|ks|-40|insert into lookup(lookup, id, keyspace_id) values (:_lookup_0, :id_0, :keyspace_id_0),(:_lookup_1, :id_1, :keyspace_id_1) on duplicate key update lookup = values(lookup), id = values(id), keyspace_id = values(keyspace_id)",
		"3|ks|40-80|begin",
		"4|ks|40-80|insert into lookup(lookup, id, keyspace_id) values (:_lookup_2, :id_2, :keyspace_id_2) on duplicate key update lookup = values(lookup), id = values(id), keyspace_id = values(keyspace_id)",
		"5|ks|-40|commit",
		"6|ks|40-80|commit",
		"7|ks|-40|begin",
		"8|ks|-40|insert into lookup_unique(lookup_unique, keyspace_id) values (:_lookup_unique_0, :keyspace_id_0),(:_lookup_unique_1, :keyspace_id_1)",
		"9|ks|40-80|begin",
		"10|ks|40-80|insert into lookup_unique(lookup_unique, keyspace_id) values (:_lookup_unique_2, :keyspace_id_2)",
		"11|ks|-40|commit",
		"12|ks|40-80|commit",
		"13|ks|-40|begin",
		"14|ks|-40|insert into `user`(id, lookup, lookup_unique) values (:_id_0, :_lookup_0, :_lookup_unique_0),(:_id_1, :_lookup_1, :_lookup_unique_1)",
		"15|ks|40-80|begin",
		"16|ks|40-80|insert into `user`(id, lookup, lookup_unique) values (:_id_2, :_lookup_2, :_lookup_unique_2)",
		"17|ks|-40|commit",
		"18|ks|40-80|commit",
	)
	utils.AssertMatchesNoOrder(t, conn, `explain format=vtexplain insert into user (id,lookup,lookup_unique) values (1,'apa','apa'),(2,'apa','bandar'),(3,'monkey','monkey')`, fmt.Sprintf("%v", wantQr.Rows))

	wantQr = sqltypes.MakeTestResult(sqltypes.MakeTestFields("#|keyspace|shard|query", "int32|varchar|varchar|varchar"),
		"1|ks|-40|select lookup, keyspace_id from lookup where lookup in ::__vals",
		"2|ks|40-80|select id from `user` where lookup = :_lookup_0")
	utils.AssertMatches(t, conn, `explain format=vtexplain select id from user where lookup = "apa"`, fmt.Sprintf("%v", wantQr.Rows))
}
