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

package cli

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttest"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

func TestVtclient(t *testing.T) {
	// Build the config for vttest.
	var cfg vttest.Config
	cfg.Topology = &vttestpb.VTTestTopology{
		Keyspaces: []*vttestpb.Keyspace{
			{
				Name: "test_keyspace",
				Shards: []*vttestpb.Shard{
					{
						Name: "0",
					},
				},
			},
		},
	}
	schema := `CREATE TABLE table1 (
	  id BIGINT(20) UNSIGNED NOT NULL,
	  i INT NOT NULL,
	  PRIMARY KEY (id)
	) ENGINE=InnoDB`
	vschema := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"hash": {
				Type: "hash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"table1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{
					{
						Column: "id",
						Name:   "hash",
					},
				},
			},
		},
	}
	require.NoError(t, cfg.InitSchemas("test_keyspace", schema, vschema))
	defer os.RemoveAll(cfg.SchemaDir)
	cluster := vttest.LocalCluster{
		Config: cfg,
	}
	require.NoError(t, cluster.Setup())
	defer cluster.TearDown()

	vtgateAddr := fmt.Sprintf("localhost:%v", cluster.Env.PortForProtocol("vtcombo", "grpc"))
	queries := []struct {
		args         []string
		rowsAffected int64
		errMsg       string
	}{
		{
			args: []string{"SELECT * FROM table1"},
		},
		{
			args: []string{
				"--target", "@primary", "--bind-variables", `[ 1, 100 ]`,
				"INSERT INTO table1 (id, i) VALUES (:v1, :v2)",
			},
			rowsAffected: 1,
		},
		{
			args: []string{
				"--target", "@primary",
				"UPDATE table1 SET i = (i + 1)",
			},
			rowsAffected: 1,
		},
		{
			args: []string{
				"--target", "@primary",
				"SELECT * FROM table1",
			},
			rowsAffected: 1,
		},
		{
			args: []string{
				"--target", "@primary", "--bind-variables", `[ 1 ]`,
				"DELETE FROM table1 WHERE id = :v1",
			},
			rowsAffected: 1,
		},
		{
			args: []string{
				"--target", "@primary",
				"SELECT * FROM table1",
			},
			rowsAffected: 0,
		},
		{
			args:   []string{"SELECT * FROM nonexistent"},
			errMsg: "table nonexistent not found",
		},
	}

	// initialize the vtclient flags before running any commands
	InitializeFlags()
	for _, q := range queries {
		// Run main function directly and not as external process. To achieve this,
		// overwrite os.Args which is used by pflag.Parse().
		args := []string{"--server", vtgateAddr}
		args = append(args, q.args...)

		err := Main.ParseFlags(args)
		require.NoError(t, err)

		Main.SetContext(t.Context())
		results, err := _run(Main, args)
		if q.errMsg != "" {
			require.ErrorContainsf(t, err, q.errMsg, "vtclient %v returned wrong error", os.Args[1:])
			return
		}

		require.NoErrorf(t, err, "vtclient %v failed", args[1:])
		require.Equalf(t, q.rowsAffected, results.rowsAffected, "wrong rows affected for query: %v", os.Args[1:])
	}
}
