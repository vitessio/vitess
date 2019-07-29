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

// Package testenv supplies test functions for testing vstreamer.
package testenv

import (
	"context"
	"fmt"
	"os"
	"path"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttest"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

// Env contains all the env vars for a test against a mysql instance.
type Env struct {
	cluster *vttest.LocalCluster

	KeyspaceName string
	ShardName    string
	Cells        []string

	TopoServ     *topo.Server
	SrvTopo      srvtopo.Server
	Dbcfgs       *dbconfigs.DBConfigs
	Mysqld       *mysqlctl.Mysqld
	SchemaEngine *schema.Engine
}

type checker struct{}

var _ = connpool.MySQLChecker(checker{})

func (checker) CheckMySQL() {}

// Init initializes an Env.
func Init() (*Env, error) {
	te := &Env{
		KeyspaceName: "vttest",
		ShardName:    "0",
		Cells:        []string{"cell1"},
	}

	ctx := context.Background()
	te.TopoServ = memorytopo.NewServer(te.Cells...)
	if err := te.TopoServ.CreateKeyspace(ctx, te.KeyspaceName, &topodatapb.Keyspace{}); err != nil {
		return nil, err
	}
	if err := te.TopoServ.CreateShard(ctx, te.KeyspaceName, te.ShardName); err != nil {
		panic(err)
	}
	te.SrvTopo = srvtopo.NewResilientServer(te.TopoServ, "TestTopo")

	cfg := vttest.Config{
		Topology: &vttestpb.VTTestTopology{
			Keyspaces: []*vttestpb.Keyspace{
				{
					Name: te.KeyspaceName,
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
	te.cluster = &vttest.LocalCluster{
		Config: cfg,
	}
	if err := te.cluster.Setup(); err != nil {
		os.RemoveAll(te.cluster.Config.SchemaDir)
		return nil, fmt.Errorf("could not launch mysql: %v", err)
	}

	te.Dbcfgs = dbconfigs.NewTestDBConfigs(te.cluster.MySQLConnParams(), te.cluster.MySQLAppDebugConnParams(), te.cluster.DbName())
	var err error
	te.Mysqld, err = mysqlctl.NewMysqld(te.Dbcfgs)
	if err != nil {
		return nil, err
	}
	te.SchemaEngine = schema.NewEngine(checker{}, tabletenv.DefaultQsConfig)
	te.SchemaEngine.InitDBConfig(te.Dbcfgs)

	// The first vschema should not be empty. Leads to Node not found error.
	// TODO(sougou): need to fix the bug.
	if err := te.SetVSchema(`{"sharded": true}`); err != nil {
		te.Close()
		return nil, err
	}

	return te, nil
}

// Close tears down TestEnv.
func (te *Env) Close() {
	te.SchemaEngine.Close()
	te.Mysqld.Close()
	te.cluster.TearDown()
	os.RemoveAll(te.cluster.Config.SchemaDir)
}

// SetVSchema sets the vschema for the test keyspace.
func (te *Env) SetVSchema(vs string) error {
	ctx := context.Background()
	var kspb vschemapb.Keyspace
	if err := json2.Unmarshal([]byte(vs), &kspb); err != nil {
		return err
	}
	if err := te.TopoServ.SaveVSchema(ctx, te.KeyspaceName, &kspb); err != nil {
		return err
	}
	return te.TopoServ.RebuildSrvVSchema(ctx, te.Cells)
}
