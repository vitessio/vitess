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
	"sync"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vitessdriver"
	"vitess.io/vitess/go/vt/vtadmin/cluster"
	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery"
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

// TestClusterConfig controls the way that a cluster.Cluster object is
// constructed for testing vtadmin code.
type TestClusterConfig struct {
	// Cluster provides the protobuf-based version of the cluster info. It is
	// to set the ID and Name of the resulting cluster.Cluster, as well as to
	// name a single, phony, vtgate entry in the cluster's discovery service.
	Cluster *vtadminpb.Cluster
	// VtctldClient provides the vtctldclient.VtctldClient implementation the
	// cluster's vtctld proxy will use. Most unit tests will use an instance of
	// the VtctldClient type provided by this package in order to mock out the
	// vtctld layer.
	VtctldClient vtctldclient.VtctldClient
	// Tablets provides the set of tablets reachable by this cluster's vtsql.DB.
	// Tablets are copied, and then mutated to have their Cluster field set to
	// match the Cluster provided by this TestClusterConfig, so mutations are
	// transparent to the caller.
	Tablets []*vtadminpb.Tablet
	// DBConfig controls the behavior of the cluster's vtsql.DB.
	DBConfig Dbcfg
}

const discoveryTestImplName = "vtadmin.testutil"

var (
	m         sync.Mutex
	testdisco discovery.Discovery
)

func init() {
	discovery.Register(discoveryTestImplName, func(cluster *vtadminpb.Cluster, flags *pflag.FlagSet, args []string) (discovery.Discovery, error) {
		return testdisco, nil
	})
}

// BuildCluster is a shared helper for building a cluster based on the given
// test configuration.
func BuildCluster(t testing.TB, cfg TestClusterConfig) *cluster.Cluster {
	t.Helper()

	disco := fakediscovery.New()
	disco.AddTaggedGates(nil, &vtadminpb.VTGate{Hostname: fmt.Sprintf("%s-%s-gate", cfg.Cluster.Name, cfg.Cluster.Id)})
	disco.AddTaggedVtctlds(nil, &vtadminpb.Vtctld{Hostname: "doesn't matter"})

	clusterConf := cluster.Config{
		ID:            cfg.Cluster.Id,
		Name:          cfg.Cluster.Name,
		DiscoveryImpl: discoveryTestImplName,
	}

	m.Lock()
	testdisco = disco
	c, err := cluster.New(clusterConf)
	m.Unlock()

	require.NoError(t, err, "failed to create cluster from configs %+v %+v", clusterConf, cfg)

	tablets := make([]*vtadminpb.Tablet, len(cfg.Tablets))
	for i, t := range cfg.Tablets {
		tablet := &vtadminpb.Tablet{
			Cluster: cfg.Cluster,
			Tablet:  t.Tablet,
			State:   t.State,
		}

		tablets[i] = tablet
	}

	db := c.DB.(*vtsql.VTGateProxy)
	db.DialFunc = func(_ vitessdriver.Configuration) (*sql.DB, error) {
		return sql.OpenDB(&fakevtsql.Connector{Tablets: tablets, ShouldErr: cfg.DBConfig.ShouldErr}), nil
	}

	vtctld := c.Vtctld.(*vtadminvtctldclient.ClientProxy)
	vtctld.DialFunc = func(addr string, ff grpcclient.FailFast, opts ...grpc.DialOption) (vtctldclient.VtctldClient, error) {
		return cfg.VtctldClient, nil
	}

	return c
}

// BuildClusters is a helper for building multiple clusters from a slice of
// TestClusterConfigs.
func BuildClusters(t testing.TB, cfgs ...TestClusterConfig) []*cluster.Cluster {
	clusters := make([]*cluster.Cluster, len(cfgs))

	for i, cfg := range cfgs {
		clusters[i] = BuildCluster(t, cfg)
	}

	return clusters
}
