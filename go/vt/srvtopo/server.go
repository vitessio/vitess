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

/*
Package srvtopo contains a set of helper methods and classes to
use the topology service in a serving environment.
*/
package srvtopo

import (
	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/topo"
)

// Server is a subset of the topo.Server API that only contains
// the serving graph read-only calls used by clients to resolve
// serving addresses, and to get VSchema.
type Server interface {
	// GetTopoServer returns the full topo.Server instance
	GetTopoServer() *topo.Server

	// GetSrvKeyspaceNames returns the list of keyspaces served in
	// the provided cell.
	GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error)

	// GetSrvKeyspace returns the SrvKeyspace for a cell/keyspace.
	GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error)

	// WatchSrvVSchema starts watching the SrvVSchema object for
	// the provided cell.  It will call the callback when
	// a new value or an error occurs.
	WatchSrvVSchema(ctx context.Context, cell string, callback func(*vschemapb.SrvVSchema, error))
}

// NewKeyspaceFilteringServer constructs a new server based on the provided
// implementation that prevents the specified keyspaces from being exposed
// to consumers of the new Server.
func NewKeyspaceFilteringServer(underlying Server, selectedKeyspaces []string) Server {
	keyspaces := map[string]bool{}
	for _, ks := range selectedKeyspaces {
		keyspaces[ks] = true
	}

	log.Infof("Keyspace filtering enabled, selecting %v", selectedKeyspaces)

	return keyspaceFilteringServer{
		server:         underlying,
		selectKeyspace: keyspaces,
	}
}

type keyspaceFilteringServer struct {
	server         Server
	selectKeyspace map[string]bool
}

func (ksf keyspaceFilteringServer) GetTopoServer() *topo.Server {
	return ksf.server.GetTopoServer()
}

func (ksf keyspaceFilteringServer) GetSrvKeyspaceNames(
	ctx context.Context,
	cell string,
) ([]string, error) {
	keyspaces, err := ksf.server.GetSrvKeyspaceNames(ctx, cell)
	ret := make([]string, 0, len(keyspaces))
	for _, ks := range keyspaces {
		if ksf.selectKeyspace[ks] {
			ret = append(ret, ks)
		}
	}
	return ret, err
}

func (ksf keyspaceFilteringServer) GetSrvKeyspace(
	ctx context.Context,
	cell,
	keyspace string,
) (*topodatapb.SrvKeyspace, error) {
	if !ksf.selectKeyspace[keyspace] {
		return nil, topo.NewError(topo.NoNode, keyspace)
	}

	return ksf.server.GetSrvKeyspace(ctx, cell, keyspace)
}

func (ksf keyspaceFilteringServer) WatchSrvVSchema(
	ctx context.Context,
	cell string,
	callback func(*vschemapb.SrvVSchema, error),
) {
	filteringCallback := func(schema *vschemapb.SrvVSchema, err error) {
		for ks := range schema.Keyspaces {
			if !ksf.selectKeyspace[ks] {
				delete(schema.Keyspaces, ks)
			}
		}

		callback(schema, err)
	}

	ksf.server.WatchSrvVSchema(ctx, cell, filteringCallback)
}
