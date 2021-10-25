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

package srvtopo

import (
	"context"
	"fmt"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/topo"
)

// NewReadOnlyServer wraps the topo server passed by the provided implementation
// and prevents any changes from being made to it.
func NewReadOnlyServer(underlying Server) (Server, error) {
	ros := readOnlyServer{underlying: underlying}

	if underlying == nil {
		return ros, ErrNilUnderlyingServer
	}

	topoServer, err := underlying.GetTopoServer()
	if err != nil || topoServer == nil {
		return ros, topo.NewError(topo.NoImplementation, fmt.Sprintf("could not get underlying topo server: %v", err))
	}

	err = topoServer.SetReadOnly(true)
	if err != nil {
		return ros, topo.NewError(topo.NoReadOnlyImplementation, fmt.Sprintf("could not provide read-only topo server: %v", err))
	}

	return ros, nil
}

// ReadOnlyServer wraps an underlying Server to ensure the topo server access is read-only
type readOnlyServer struct {
	underlying Server
}

// GetTopoServer returns a read-only topo server
func (ros readOnlyServer) GetTopoServer() (*topo.Server, error) {
	topoServer, err := ros.underlying.GetTopoServer()
	if err != nil || topoServer == nil {
		return nil, topo.NewError(topo.NoImplementation, fmt.Sprintf("could not get underlying topo server: %v", err))
	}

	// The topo server should already be read-only, from the constructor, but as an extra
	// safety precaution and guardrail let's ensure that it is before handing it out
	err = topoServer.SetReadOnly(true)
	if err != nil {
		return nil, topo.NewError(topo.NoReadOnlyImplementation, fmt.Sprintf("could not provide read-only topo server: %v", err))
	}
	readOnly, err := topoServer.IsReadOnly()
	if err != nil || !readOnly {
		return nil, topo.NewError(topo.NoReadOnlyImplementation, fmt.Sprintf("could not provide read-only topo server: %v", err))
	}

	return topoServer, err
}

// GetSrvKeyspaceNames returns the list of keyspaces served in the provided cell from the underlying server
func (ros readOnlyServer) GetSrvKeyspaceNames(ctx context.Context, cell string, staleOK bool) ([]string, error) {
	return ros.underlying.GetSrvKeyspaceNames(ctx, cell, staleOK)
}

// GetSrvKeyspace returns the SrvKeyspace for a cell/keyspace from the underlying server
func (ros readOnlyServer) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	return ros.underlying.GetSrvKeyspace(ctx, cell, keyspace)
}

func (ros readOnlyServer) WatchSrvKeyspace(ctx context.Context, cell, keyspace string, callback func(*topodatapb.SrvKeyspace, error) bool) {
	ros.underlying.WatchSrvKeyspace(ctx, cell, keyspace, callback)
}

// WatchSrvVSchema starts watching the SrvVSchema object for the provided cell.  It will call the callback when
// a new value or an error occurs.
func (ros readOnlyServer) WatchSrvVSchema(ctx context.Context, cell string, callback func(*vschemapb.SrvVSchema, error) bool) {
	ros.underlying.WatchSrvVSchema(ctx, cell, callback)
}
