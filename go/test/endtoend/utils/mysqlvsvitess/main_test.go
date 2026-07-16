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

package mysqlvsvitess

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
)

var (
	clusterInstance *vitesst.Cluster
	vtParams        mysql.ConnParams
	mysqlParams     mysql.ConnParams
	keyspaceName    = "ks"
	schemaSQL       = `create table t1(
		id1 bigint,
		id2 bigint,
		id3 bigint,
		primary key(id1)
	) Engine=InnoDB;`

	vschema = `
{
  "sharded": true,
  "vindexes": {
    "hash": {
      "type": "hash"
    }
  },
  "tables": {
	"t1": {
      "column_vindexes": [
        {
          "column": "id1",
          "name": "hash"
        }
      ]
    }
  }
}`
)

func setupCluster(t testing.TB) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(t,
		vitesst.WithKeyspace(keyspaceName).
			WithShardNames("-80", "80-").
			WithSchema(schemaSQL).
			WithVSchema(vschema),
		vitesst.WithVTGateArgs("--schema-change-signal", "--enable-system-settings=true"),
		vitesst.WithVTTabletArgs("--queryserver-config-schema-change-signal"),
	)
	require.NoError(t, err)
	cleanup, err := cluster.Start(t, ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, cleanup(context.WithoutCancel(ctx)))
	})

	clusterInstance = cluster
	vtParams = cluster.VTParams(ctx, "")

	conn, closer, err := vitesst.NewMySQL(t, ctx, cluster, keyspaceName, schemaSQL)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		require.NoError(t, closer(cleanupCtx))
	})
	mysqlParams = conn
}

func TestCreateMySQL(t *testing.T) {
	setupCluster(t)
	ctx := t.Context()
	mysqlConn, err := mysql.Connect(ctx, &mysqlParams)
	require.NoError(t, err)
	defer mysqlConn.Close()

	vtConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer vtConn.Close()

	vitesst.ExecCompareMySQL(t, vtConn, mysqlConn, "insert into t1(id1, id2, id3) values (1, 1, 1), (2, 2, 2), (3, 3, 3)")
	vitesst.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, "select * from t1;", `[[INT64(1) INT64(1) INT64(1)] [INT64(2) INT64(2) INT64(2)] [INT64(3) INT64(3) INT64(3)]]`)
	vitesst.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, "select * from t1 order by id1 desc;", `[[INT64(3) INT64(3) INT64(3)] [INT64(2) INT64(2) INT64(2)] [INT64(1) INT64(1) INT64(1)]]`)
}
