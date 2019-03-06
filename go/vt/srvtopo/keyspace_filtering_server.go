/*
Copyright 2018 The Vitess Authors.

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

package srvtopo

import (
	"fmt"

	"golang.org/x/net/context"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/topo"
)

var (
	// ErrNilUnderlyingServer is returned when attempting to create a new keyspace
	// filtering server if a nil underlying server implementation is provided.
	ErrNilUnderlyingServer = fmt.Errorf("Unable to construct filtering server without an underlying server")

	// ErrTopoServerNotAvailable is returned if a caller tries to access the
	// topo.Server supporting this srvtopo.Server.
	ErrTopoServerNotAvailable = fmt.Errorf("Cannot access underlying topology server when keyspace filtering is enabled")
)

// NewKeyspaceFilteringServer constructs a new server based on the provided
// implementation that prevents the specified keyspaces from being exposed
// to consumers of the new Server.
//
// A filtering server will not allow access to the topo.Server to prevent
// updates that may corrupt the global VSchema keyspace.
func NewKeyspaceFilteringServer(underlying Server, selectedKeyspaces []string) (Server, error) {
	if underlying == nil {
		return nil, ErrNilUnderlyingServer
	}

	keyspaces := map[string]bool{}
	for _, ks := range selectedKeyspaces {
		keyspaces[ks] = true
	}

	return keyspaceFilteringServer{
		server:          underlying,
		selectKeyspaces: keyspaces,
	}, nil
}

type keyspaceFilteringServer struct {
	server          Server
	selectKeyspaces map[string]bool
}

// GetTopoServer returns an error; filtering srvtopo.Server consumers may not
// access the underlying topo.Server.
func (ksf keyspaceFilteringServer) GetTopoServer() (*topo.Server, error) {
	return nil, ErrTopoServerNotAvailable
}

func (ksf keyspaceFilteringServer) GetSrvKeyspaceNames(
	ctx context.Context,
	cell string,
) ([]string, error) {
	keyspaces, err := ksf.server.GetSrvKeyspaceNames(ctx, cell)
	ret := make([]string, 0, len(keyspaces))
	for _, ks := range keyspaces {
		if ksf.selectKeyspaces[ks] {
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
	if !ksf.selectKeyspaces[keyspace] {
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
		if schema != nil {
			for ks := range schema.Keyspaces {
				if !ksf.selectKeyspaces[ks] {
					delete(schema.Keyspaces, ks)
				}
			}
		}

		callback(schema, err)
	}

	ksf.server.WatchSrvVSchema(ctx, cell, filteringCallback)
}
