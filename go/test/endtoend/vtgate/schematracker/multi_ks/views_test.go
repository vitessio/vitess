/*
Copyright 2025 The Vitess Authors.

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

package multiks

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	shardedKs       = "sks"
	unshardedKs     = "uks"
	cell            = "zone1"

	//go:embed sschema.sql
	shardedSchema string

	//go:embed uschema.sql
	unshardedSchema string

	//go:embed svschema.json
	shardedVSchema string

	//go:embed uvschema.json
	unshardedVSchema string
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// enable views tracking
		clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs, "--enable-views")
		clusterInstance.VtTabletExtraArgs = append(clusterInstance.VtTabletExtraArgs, "--queryserver-enable-views")

		// Start sharded keyspace
		cell := clusterInstance.Cell
		sks := cluster.Keyspace{
			Name:      shardedKs,
			SchemaSQL: shardedSchema,
			VSchema:   shardedVSchema,
		}

		err = clusterInstance.StartKeyspace(sks, []string{"-80", "80-"}, 0, false, cell)
		if err != nil {
			return 1
		}

		// Start unsharded keyspace
		uks := cluster.Keyspace{
			Name:      unshardedKs,
			SchemaSQL: unshardedSchema,
			VSchema:   unshardedVSchema,
		}

		err = clusterInstance.StartUnshardedKeyspace(uks, 0, false, cell)
		if err != nil {
			return 1
		}

		// Start vtgate
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}

		err = clusterInstance.WaitForVTGateAndVTTablets(1 * time.Minute)
		if err != nil {
			fmt.Println(err)
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

// TestViewQueries validates that view tracking works as expected with qualified and unqualified views.
func TestViewQueries(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	viewExists := func(t *testing.T, ksMap map[string]any) bool {
		// wait for the view definition to change
		views := ksMap["views"]
		viewsMap := views.(map[string]any)
		_, ok := viewsMap["cv3"]
		return ok
	}

	utils.WaitForVschemaCondition(t, clusterInstance.VtgateProcess, shardedKs, viewExists, "view cv3 not found")

	// Insert some data
	insertData(t, conn)

	tcases := []struct {
		name      string
		sql       string
		defaultKs string
		expRes    string
		expErr    string
	}{{
		name:      "unique unsharded view - qualified",
		defaultKs: "@primary",
		sql:       "select * from uks.uv1 order by id",
		expRes:    "[[INT64(1) INT64(1010)] [INT64(2) INT64(4020)]]",
	}, {
		name:      "unique sharded view - qualified",
		defaultKs: "@primary",
		sql:       "select * from sks.sv1 order by id",
		expRes:    "[[INT64(1) INT64(1100)] [INT64(2) INT64(4200)]]",
	}, {
		name:      "unique unsharded view - non qualified",
		defaultKs: "@primary",
		sql:       "select * from uv1 order by id",
		expRes:    "[[INT64(1) INT64(1010)] [INT64(2) INT64(4020)]]",
	}, {
		name:      "unique sharded view - non qualified",
		defaultKs: "@primary",
		sql:       "select * from sv1 order by id",
		expRes:    "[[INT64(1) INT64(1100)] [INT64(2) INT64(4200)]]",
	}, {
		name:      "unique unsharded view - non qualified - default unsharded keyspace",
		defaultKs: "uks",
		sql:       "select * from uv1 order by id",
		expRes:    "[[INT64(1) INT64(1010)] [INT64(2) INT64(4020)]]",
	}, {
		name:      "unique sharded view - non qualified - default unsharded keyspace- should error",
		defaultKs: "uks",
		sql:       "select * from sv1 order by id",
		expErr:    "(errno 1146) (sqlstate 42S02)",
	}, {
		name:      "unique unsharded view - non qualified - default sharded keyspace - should error",
		defaultKs: "sks",
		sql:       "select * from uv1 order by id",
		expErr:    "table uv1 not found",
	}, {
		name:      "unique sharded view - non qualified - default sharded keyspace",
		defaultKs: "sks",
		sql:       "select * from sv1 order by id",
		expRes:    "[[INT64(1) INT64(1100)] [INT64(2) INT64(4200)]]",
	}, {
		name:      "unique sharded view - qualified - default sharded keyspace",
		defaultKs: "sks",
		sql:       "select * from sks.sv1 order by id",
		expRes:    "[[INT64(1) INT64(1100)] [INT64(2) INT64(4200)]]",
	}, {
		name:      "vschema provided - unsharded view - non qualified",
		defaultKs: "@primary",
		sql:       "select * from cv1 order by id",
		expRes:    "[[INT64(1) INT64(990)] [INT64(2) INT64(3980)]]",
	}, {
		name:      "vschema provided - sharded view - non qualified",
		defaultKs: "@primary",
		sql:       "select id, mcol from cv2 order by id",
		expRes:    "[[INT64(1) INT64(100000)] [INT64(2) INT64(800000)]]",
	}, {
		name:      "non unique view - non qualified - should error",
		defaultKs: "@primary",
		sql:       "select * from cv3 order by id",
		expErr:    "ambiguous table reference: cv3",
	}, {
		name:      "non unique view - unsharded qualified",
		defaultKs: "@primary",
		sql:       "select * from uks.cv3 order by id",
		expRes:    "[[INT64(1) DECIMAL(100.0000)] [INT64(2) DECIMAL(200.0000)]]",
	}, {
		name:      "non unique view - sharded qualified",
		defaultKs: "@primary",
		sql:       "select * from sks.cv3 order by id",
		expRes:    "[[INT64(1) DECIMAL(10.0000)] [INT64(2) DECIMAL(20.0000)]]",
	}, {
		name:      "non unique view - non qualified - default unsharded keyspace",
		defaultKs: "uks",
		sql:       "select * from cv3 order by id",
		expRes:    "[[INT64(1) DECIMAL(100.0000)] [INT64(2) DECIMAL(200.0000)]]",
	}, {
		name:      "non unique view - non qualified - default sharded keyspace",
		defaultKs: "sks",
		sql:       "select * from cv3 order by id",
		expRes:    "[[INT64(1) DECIMAL(10.0000)] [INT64(2) DECIMAL(20.0000)]]",
	}}

	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			utils.Exec(t, conn, "use "+tc.defaultKs)
			qr, err := utils.ExecAllowError(t, conn, tc.sql)
			if tc.expErr != "" {
				require.ErrorContains(t, err, tc.expErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expRes, fmt.Sprintf("%v", qr.Rows))
		})
	}
}

func insertData(t *testing.T, conn *mysql.Conn) {
	t.Helper()

	// unsharded
	utils.Exec(t, conn, "insert into uks.t(id, col) values(1, 1000)")
	utils.Exec(t, conn, "insert into uks.t(id, col) values(2, 4000)")
	utils.Exec(t, conn, "insert into u(id, col) values(1, 10)")
	utils.Exec(t, conn, "insert into u(id, col) values(2, 20)")

	// sharded
	utils.Exec(t, conn, "insert into sks.t(id, col) values(1, 1000)")
	utils.Exec(t, conn, "insert into sks.t(id, col) values(2, 4000)")
	utils.Exec(t, conn, "insert into s(id, col) values(1, 100)")
	utils.Exec(t, conn, "insert into s(id, col) values(2, 200)")
}
