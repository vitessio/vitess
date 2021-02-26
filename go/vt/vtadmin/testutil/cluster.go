/*
Copyright 2021 The Vitess Authors.

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

package testutil

import (
	"database/sql"
	"fmt"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vitessdriver"
	"vitess.io/vitess/go/vt/vtadmin/cluster"
	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery/fakediscovery"
	vtadminvtctldclient "vitess.io/vitess/go/vt/vtadmin/vtctldclient"
	"vitess.io/vitess/go/vt/vtadmin/vtsql"
	"vitess.io/vitess/go/vt/vtadmin/vtsql/fakevtsql"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// Dbcfg is a test utility for controlling the behavior of the cluster's DB
// at the package sql level.
type Dbcfg struct {
	ShouldErr bool
}

// BuildCluster is a shared helper for building a cluster that contains the given tablets and
// talking to the given vtctld server. dbconfigs contains an optional config
// for controlling the behavior of the cluster's DB at the package sql level.
func BuildCluster(i int, vtctldClient vtctldclient.VtctldClient, tablets []*vtadminpb.Tablet, dbconfigs map[string]*Dbcfg) *cluster.Cluster {
	disco := fakediscovery.New()
	disco.AddTaggedGates(nil, &vtadminpb.VTGate{Hostname: fmt.Sprintf("cluster%d-gate", i)})
	disco.AddTaggedVtctlds(nil, &vtadminpb.Vtctld{Hostname: "doesn't matter"})

	cluster := &cluster.Cluster{
		ID:        fmt.Sprintf("c%d", i),
		Name:      fmt.Sprintf("cluster%d", i),
		Discovery: disco,
	}

	dbconfig, ok := dbconfigs[cluster.ID]
	if !ok {
		dbconfig = &Dbcfg{ShouldErr: false}
	}

	db := vtsql.New(&vtsql.Config{
		Cluster:   cluster.ToProto(),
		Discovery: disco,
	})
	db.DialFunc = func(cfg vitessdriver.Configuration) (*sql.DB, error) {
		return sql.OpenDB(&fakevtsql.Connector{Tablets: tablets, ShouldErr: dbconfig.ShouldErr}), nil
	}

	vtctld := vtadminvtctldclient.New(&vtadminvtctldclient.Config{
		Cluster:   cluster.ToProto(),
		Discovery: disco,
	})
	vtctld.DialFunc = func(addr string, ff grpcclient.FailFast, opts ...grpc.DialOption) (vtctldclient.VtctldClient, error) {
		return vtctldClient, nil
	}

	cluster.DB = db
	cluster.Vtctld = vtctld

	return cluster
}
