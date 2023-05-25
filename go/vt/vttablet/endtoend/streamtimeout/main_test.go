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

/*
All tests in this package come with toxiproxy in front of the MySQL server
*/
package streamtimeout

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttest"

	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

var (
	cluster vttest.LocalCluster
	config  *tabletenv.TabletConfig
)

func TestMain(m *testing.M) {
	flag.Parse() // Do not remove this comment, import into google3 depends on it
	tabletenv.Init()

	exitCode := func() int {
		// Launch MySQL.
		// We need a Keyspace in the topology, so the DbName is set.
		// We need a Shard too, so the database 'vttest' is created.
		cfg := vttest.Config{
			Topology: &vttestpb.VTTestTopology{
				Keyspaces: []*vttestpb.Keyspace{
					{
						Name: "vttest",
						Shards: []*vttestpb.Shard{
							{
								Name:           "0",
								DbNameOverride: "vttest",
							},
						},
					},
				},
			},
			OnlyMySQL: true,
			Charset:   "utf8mb4_general_ci",
		}

		env, err := vttest.NewLocalTestEnv("", 0)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			return 1
		}
		env.EnableToxiproxy = true
		cluster = vttest.LocalCluster{
			Config: cfg,
			Env:    env,
		}
		if err := cluster.Setup(); err != nil {
			fmt.Fprintf(os.Stderr, "could not launch mysql: %v\n", err)
			return 1
		}
		defer cluster.TearDown()

		connParams := cluster.MySQLConnParams()
		connAppDebugParams := cluster.MySQLAppDebugConnParams()
		config = tabletenv.NewDefaultConfig()
		_ = config.SignalSchemaChangeReloadIntervalSeconds.Set("2100ms")
		config.SchemaChangeReloadTimeout = 10 * time.Second
		config.SignalWhenSchemaChange = true
		err = framework.StartCustomServer(connParams, connAppDebugParams, cluster.DbName(), config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			return 1
		}
		defer framework.StopServer()
		return m.Run()
	}()
	os.Exit(exitCode)
}
