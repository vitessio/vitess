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
	"os"
	"testing"

	_ "embed"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/vtgate/utils"
)

var (
	clusterInstance       *cluster.LocalProcessCluster
	vtParams              mysql.ConnParams
	keyspaceShardedName   = "sharded"
	keyspaceUnshardedName = "unsharded"
	Cell                  = "test"

	//go:embed unsharded/schema.sql
	unshardedSchemaSQL string

	//go:embed unsharded/vschema.json
	unshardedVSchema string

	//go:embed sharded/schema.sql
	shardedSchemaSQL string

	//go:embed sharded/vschema.json
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

		// Start unsharded keyspace
		keyspaceUnsharded := &cluster.Keyspace{
			Name:      keyspaceUnshardedName,
			SchemaSQL: unshardedSchemaSQL,
			VSchema:   unshardedVSchema,
		}

		if err := clusterInstance.StartUnshardedKeyspace(*keyspaceUnsharded, 0, false); err != nil {
			return 1
		}

		// Start sharded keyspace
		keyspaceSharded := &cluster.Keyspace{
			Name:      keyspaceShardedName,
			SchemaSQL: shardedSchemaSQL,
			VSchema:   shardedVSchema,
		}
		err = clusterInstance.StartKeyspace(*keyspaceSharded, []string{"-80", "80-"}, 0, false)
		if err != nil {
			return 1
		}

		clusterInstance.VtGateExtraArgs = []string{"-enable_system_settings=true"}

		// Start vtgate
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}

		vtParams = mysql.ConnParams{
			Host:   clusterInstance.Hostname,
			Port:   clusterInstance.VtgateMySQLPort,
			DbName: keyspaceShardedName,
		}
		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestInsertingWithLookupIndexInsideTransaction(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	// Change the sql mode without _actually_ changing it
	utils.Exec(t, conn, "set session sql_mode = CONCAT(@@sql_mode, ',', @@sql_mode)")

	utils.Exec(t, conn, "BEGIN")
	utils.Exec(t, conn, "SAVEPOINT sp_1")
	utils.Exec(t, conn, "insert into t1 (lookup_id) values (2921)")
	utils.Exec(t, conn, "RELEASE SAVEPOINT sp_1")
	utils.Exec(t, conn, "COMMIT")
}
