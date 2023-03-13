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
	KeyspaceName    = "ks"
	Cell            = "test"

	//go:embed schema.sql
	SchemaSQL string

	//go:embed vschema.json
	VSchema string

	routingRules = `
{"rules": [
  {
    "from_table": "ks.t1000",
	"to_tables": ["ks.t1"]
  }
]}
`
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
		clusterInstance.VtGateExtraArgs = []string{"--schema_change_signal"}
		clusterInstance.VtTabletExtraArgs = []string{"--queryserver-config-schema-change-signal", "--queryserver-config-schema-change-signal-interval", "0.1", "--queryserver-config-max-result-size", "100", "--queryserver-config-terse-errors"}
		err = clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 0, false)
		if err != nil {
			return 1
		}

		err = clusterInstance.VtctlclientProcess.ApplyRoutingRules(routingRules)
		if err != nil {
			return 1
		}

		_, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("RebuildVSchemaGraph")
		if err != nil {
			return 1
		}

		clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs, "--enable_system_settings=true")
		// Start vtgate
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}

		vtParams = clusterInstance.GetVTParams(KeyspaceName)

		return m.Run()
	}()
	os.Exit(exitCode)
}

func start(t *testing.T) (*mysql.Conn, func()) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)

	deleteAll := func() {
		utils.Exec(t, conn, "use ks")
		tables := []string{"t1", "t2", "vstream_test", "t3", "t4", "t6", "t7_xxhash", "t7_xxhash_idx", "t7_fk", "t8", "t9", "t9_id_to_keyspace_id_idx", "t10", "t10_id_to_keyspace_id_idx", "t1_id2_idx", "t2_id4_idx", "t3_id7_idx", "t4_id2_idx", "t5_null_vindex", "t6_id2_idx"}
		for _, table := range tables {
			_, _ = utils.ExecAllowError(t, conn, "delete from "+table)
		}
		utils.Exec(t, conn, "set workload = oltp")
	}

	deleteAll()

	return conn, func() {
		deleteAll()
		conn.Close()
		cluster.PanicHandler(t)
	}
}
