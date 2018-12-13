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

package srvtopotest

import (
	"golang.org/x/net/context"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/topo"
)

// PassthroughSrvTopoServer is a bare implementation of srvtopo.Server for use in tests
type PassthroughSrvTopoServer struct {
	TopoServer      *topo.Server
	TopoServerError error

	SrvKeyspaceNames      []string
	SrvKeyspaceNamesError error

	SrvKeyspace      *topodatapb.SrvKeyspace
	SrvKeyspaceError error

	WatchedSrvVSchema      *vschemapb.SrvVSchema
	WatchedSrvVSchemaError error
}

// NewPassthroughSrvTopoServer returns a new, unconfigured test PassthroughSrvTopoServer
func NewPassthroughSrvTopoServer() *PassthroughSrvTopoServer {
	return &PassthroughSrvTopoServer{}
}

// GetTopoServer implements srvtopo.Server
func (srv *PassthroughSrvTopoServer) GetTopoServer() (*topo.Server, error) {
	return srv.TopoServer, srv.TopoServerError
}

// GetSrvKeyspaceNames implements srvtopo.Server
func (srv *PassthroughSrvTopoServer) GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error) {
	return srv.SrvKeyspaceNames, srv.SrvKeyspaceNamesError
}

// GetSrvKeyspace implements srvtopo.Server
func (srv *PassthroughSrvTopoServer) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	return srv.SrvKeyspace, srv.SrvKeyspaceError
}

// WatchSrvVSchema implements srvtopo.Server
func (srv *PassthroughSrvTopoServer) WatchSrvVSchema(ctx context.Context, cell string, callback func(*vschemapb.SrvVSchema, error)) {
	callback(srv.WatchedSrvVSchema, srv.WatchedSrvVSchemaError)
}
