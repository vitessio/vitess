/*
Copyright 2023 The Vitess Authors.

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

package foreignkey

import (
	"context"
	_ "embed"
	"flag"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/utils"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	shardedKs       = "ks"
	Cell            = "test"

	//go:embed sharded_schema.sql
	shardedSchemaSQL string

	//go:embed sharded_vschema.json
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

		err = clusterInstance.StartKeyspace(*sKs, []string{"-80", "80-"}, 0, false)
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

func start(t *testing.T) (*mysql.Conn, func()) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)

	deleteAll := func() {
		_ = utils.Exec(t, conn, "use `ks/-80`")
		tables := []string{"t3", "t2", "t1", "multicol_tbl2", "multicol_tbl1"}
		for _, table := range tables {
			_ = utils.Exec(t, conn, "delete from "+table)
		}
		_ = utils.Exec(t, conn, "use `ks/80-`")
		for _, table := range tables {
			_ = utils.Exec(t, conn, "delete from "+table)
		}
		_ = utils.Exec(t, conn, "use `ks`")
	}

	deleteAll()

	return conn, func() {
		deleteAll()
		conn.Close()
		cluster.PanicHandler(t)
	}
}
