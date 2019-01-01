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
	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttest"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

var (
	engine             *Engine
	mysqld             *mysqlctl.Mysqld
	connParams         mysql.ConnParams
	connAppDebugParams mysql.ConnParams
	topoServ           *topo.Server
	keyspaceName       = "vttest"
	cells              = []string{"cell1"}
)

type checker struct{}

var _ = connpool.MySQLChecker(checker{})

func (checker) CheckMySQL() {}

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
		defer os.RemoveAll(cfg.SchemaDir)
		cluster := vttest.LocalCluster{
			Config: cfg,
		}
		if err := cluster.Setup(); err != nil {
			fmt.Fprintf(os.Stderr, "could not launch mysql: %v\n", err)
			return 1
		}
		defer cluster.TearDown()

		// initTopo initializes topoServ.
		if err := initEngine(&cluster); err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			return 1
		}
		defer engine.Close()

		return m.Run()
	}()
	os.Exit(exitCode)
}

func initEngine(cluster *vttest.LocalCluster) error {
	if err := initTopo(); err != nil {
		return err
	}

	se := schema.NewEngine(checker{}, tabletenv.DefaultQsConfig)
	srvTopoServer := srvtopo.NewResilientServer(topoServ, "TestTopo")
	engine = NewEngine(srvTopoServer, se)

	dbcfgs := dbconfigs.NewTestDBConfigs(cluster.MySQLConnParams(), cluster.MySQLAppDebugConnParams(), cluster.DbName())
	mysqld = mysqlctl.NewMysqld(dbcfgs)
	se.InitDBConfig(dbcfgs)
	engine.InitDBConfig(dbcfgs)

	engine.Open(keyspaceName, cells[0])
	return nil
}

func initTopo() error {
	ctx := context.Background()

	topoServ = memorytopo.NewServer(cells...)
	if err := topoServ.CreateKeyspace(ctx, keyspaceName, &topodatapb.Keyspace{}); err != nil {
		return err
	}
	// The first vschema should not be empty. Leads to Node not found error.
	// TODO(sougou): need to fix the bug.
	return setVSchema(`{"sharded": true}`)
}

func setVSchema(vs string) error {
	ctx := context.Background()
	logger := logutil.NewConsoleLogger()
	var kspb vschemapb.Keyspace
	if err := json2.Unmarshal([]byte(vs), &kspb); err != nil {
		return fmt.Errorf("Unmarshal failed: %v", err)
	}
	if err := topoServ.SaveVSchema(ctx, keyspaceName, &kspb); err != nil {
		return fmt.Errorf("SaveVSchema failed: %v", err)
	}
	if err := topotools.RebuildVSchema(ctx, logger, topoServ, cells); err != nil {
		return fmt.Errorf("RebuildVSchema failed: %v", err)
	}
	return nil
}
