/*
Copyright 2020 The Vitess Authors.

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

package cluster

import (
	"fmt"

	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery"
	"vitess.io/vitess/go/vt/vtadmin/vtctldclient"
	"vitess.io/vitess/go/vt/vtadmin/vtsql"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// Cluster is the self-contained unit of services required for vtadmin to talk
// to a vitess cluster. This consists of a discovery service, a database
// connection, and a vtctl client.
type Cluster struct {
	ID        string
	Name      string
	Discovery discovery.Discovery

	DB     vtsql.DB
	Vtctld vtctldclient.Proxy

	// These fields are kept to power debug endpoints.
	// (TODO|@amason): Figure out if these are needed or if there's a way to
	// push down to the credentials / vtsql.
	// vtgateCredentialsPath string
}

// New creates a new Cluster from a Config.
func New(cfg Config) (*Cluster, error) {
	cluster := &Cluster{
		ID:   cfg.ID,
		Name: cfg.Name,
	}

	discoargs := buildPFlagSlice(cfg.DiscoveryFlagsByImpl[cfg.DiscoveryImpl])

	disco, err := discovery.New(cfg.DiscoveryImpl, cluster.ToProto(), discoargs)
	if err != nil {
		return nil, fmt.Errorf("error while creating discovery impl (%s): %w", cfg.DiscoveryImpl, err)
	}

	cluster.Discovery = disco

	protocluster := cluster.ToProto()

	vtsqlargs := buildPFlagSlice(cfg.VtSQLFlags)

	vtsqlCfg, err := vtsql.Parse(protocluster, disco, vtsqlargs)
	if err != nil {
		return nil, fmt.Errorf("error while creating vtsql connection config: %w", err)
	}

	vtctldargs := buildPFlagSlice(cfg.VtctldFlags)

	vtctldCfg, err := vtctldclient.Parse(protocluster, disco, vtctldargs)
	if err != nil {
		return nil, fmt.Errorf("error while creating vtctldclient proxy config: %w", err)
	}

	cluster.DB = vtsql.New(vtsqlCfg)
	cluster.Vtctld = vtctldclient.New(vtctldCfg)

	return cluster, nil
}

// ToProto returns a value-copy protobuf equivalent of the cluster.
func (c Cluster) ToProto() *vtadminpb.Cluster {
	return &vtadminpb.Cluster{
		Id:   c.ID,
		Name: c.Name,
	}
}

func buildPFlagSlice(flags map[string]string) []string {
	args := make([]string, 0, len(flags))
	for k, v := range flags {
		// The k=v syntax is needed to account for negating boolean flags.
		args = append(args, "--"+k+"="+v)
	}

	return args
}
