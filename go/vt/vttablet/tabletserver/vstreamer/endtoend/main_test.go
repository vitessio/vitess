/*
Copyright 2017 Google Inc.

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

package vstreamer

import (
	"flag"
	"fmt"
	"os"
	"path"
	"testing"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttest"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
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
			ExtraMyCnf: []string{path.Join(os.Getenv("VTTOP"), "config/mycnf/rbr.cnf")},
			OnlyMySQL:  true,
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

		keyspaceName := "vttest"
		ts, err := initTopo(keyspaceName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "topo init failed: %v\n", err)
			return 1
		}
		config := tabletenv.DefaultQsConfig
		config.EnableAutoCommit = true

		connParams = cluster.MySQLConnParams()
		connAppDebugParams = cluster.MySQLAppDebugConnParams()
		if err := framework.StartFullServer(ts, config, connParams, connAppDebugParams, cluster.DbName(), keyspaceName, "cell1-100"); err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			return 1
		}
		defer framework.StopServer()

		return m.Run()
	}()
	os.Exit(exitCode)
}

func initTopo(keyspaceName string) (*topo.Server, error) {
	ctx := context.Background()
	logger := logutil.NewConsoleLogger()

	cells := []string{"cell1"}
	ts := memorytopo.NewServer(cells...)

	if err := ts.CreateKeyspace(ctx, keyspaceName, &topodatapb.Keyspace{}); err != nil {
		return nil, fmt.Errorf("CreateKeyspace failed: %v", err)
	}

	ks := &vschemapb.Keyspace{
		Sharded: true,
	}
	if err := ts.SaveVSchema(ctx, keyspaceName, ks); err != nil {
		return nil, fmt.Errorf("SaveVSchema failed: %v", err)
	}
	if err := topotools.RebuildVSchema(ctx, logger, ts, cells); err != nil {
		return nil, fmt.Errorf("RebuildVSchema failed: %v", err)
	}
	return ts, nil
}

var testSchema = `create table stream1(id int, val varbinary(128), primary key(id));
create table stream2(id int, val varbinary(128), primary key(id));`
