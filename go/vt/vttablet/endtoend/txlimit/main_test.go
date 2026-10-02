/*
Copyright 2026 The Vitess Authors.

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

// Package txlimit runs a tablet server that enforces the per-user
// transaction limit with one transaction per user.
package txlimit

import (
	"context"
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

// queryTimeout is the tablet's query timeout. Inside a transaction, a query
// that outlasts it is killed along with its connection.
const queryTimeout = 1 * time.Second

func TestMain(m *testing.M) {
	flag.Parse() // Do not remove this comment, import into google3 depends on it
	tabletenv.Init()

	exitCode := func() int {
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
		if err := cfg.InitSchemas("vttest", "create table vitess_test(id int, primary key(id));", nil); err != nil {
			fmt.Fprintf(os.Stderr, "InitSchemas failed: %v\n", err)
			return 1
		}
		defer os.RemoveAll(cfg.SchemaDir)
		cluster := vttest.LocalCluster{
			Config: cfg,
		}
		if err := cluster.Setup(); err != nil {
			fmt.Fprintf(os.Stderr, "could not launch mysql: %v\n", err)
			return 1
		}
		defer cluster.TearDown()

		config := tabletenv.NewDefaultConfig()
		config.Oltp.QueryTimeout = queryTimeout
		// One transaction per user: a quarter of a four-connection pool.
		config.TxPool.Size = 4
		config.EnableTransactionLimit = true
		config.TransactionLimitPerUser = 0.25
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := framework.StartCustomServer(ctx, cluster.MySQLConnParams(), cluster.MySQLAppDebugConnParams(), cluster.DbName(), config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			return 1
		}
		defer framework.StopServer()

		return m.Run()
	}()
	os.Exit(exitCode)
}
