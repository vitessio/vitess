/*
Copyright 2024 The Vitess Authors.

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

package connecttcp

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"

	"vitess.io/vitess/go/mysql"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttest"
)

var (
	connParams         mysql.ConnParams
	connAppDebugParams mysql.ConnParams
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
		if err := cfg.InitSchemas("vttest", testSchema, nil); err != nil {
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

		if err := allowConnectOnTCP(cluster); err != nil {
			fmt.Fprintf(os.Stderr, "failed to allow tcp priviliges: %v", err)
			return 1
		}

		connParams = cluster.MySQLTCPConnParams()
		connAppDebugParams = cluster.MySQLAppDebugConnParams()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		config := tabletenv.NewDefaultConfig()
		config.TwoPCEnable = true
		config.TwoPCAbandonAge = 1

		if err := framework.StartCustomServer(ctx, connParams, connAppDebugParams, cluster.DbName(), config); err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			return 1
		}
		defer framework.StopServer()

		return m.Run()
	}()
	os.Exit(exitCode)
}

func allowConnectOnTCP(cluster vttest.LocalCluster) error {
	connParams = cluster.MySQLConnParams()
	conn, err := mysql.Connect(context.Background(), &connParams)
	if err != nil {
		return err
	}
	if _, err = conn.ExecuteFetch("UPDATE mysql.user SET Host = '%' WHERE User = 'vt_dba';", 0, false); err != nil {
		return err
	}
	if _, err = conn.ExecuteFetch("FLUSH PRIVILEGES;", 0, false); err != nil {
		return err
	}
	conn.Close()
	return nil
}

var testSchema = `create table vitess_test(intval int primary key);`
