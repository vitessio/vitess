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

package zk2topo

import (
	"fmt"
	"path"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
)

// This file contains the serving graph management code of zktopo.Server.
// Eventually, this code will move to go/vt/topo.

// GetSrvKeyspaceNames is part of the topo.Server interface.
func (zs *Server) GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error) {
	children, err := zs.ListDir(ctx, cell, keyspacesPath)
	switch err {
	case nil:
		return children, nil
	case topo.ErrNoNode:
		return nil, nil
	default:
		return nil, err
	}
}

// UpdateSrvKeyspace is part of the topo.Server interface.
func (zs *Server) UpdateSrvKeyspace(ctx context.Context, cell, keyspace string, srvKeyspace *topodatapb.SrvKeyspace) error {
	zkPath := path.Join(keyspacesPath, keyspace, topo.SrvKeyspaceFile)
	data, err := proto.Marshal(srvKeyspace)
	if err != nil {
		return err
	}
	_, err = zs.Update(ctx, cell, zkPath, data, nil)
	return err
}

// DeleteSrvKeyspace is part of the topo.Server interface.
func (zs *Server) DeleteSrvKeyspace(ctx context.Context, cell, keyspace string) error {
	zkPath := path.Join(keyspacesPath, keyspace, topo.SrvKeyspaceFile)
	return zs.Delete(ctx, cell, zkPath, nil)
}

// GetSrvKeyspace is part of the topo.Server interface.
func (zs *Server) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	zkPath := path.Join(keyspacesPath, keyspace, topo.SrvKeyspaceFile)
	data, _, err := zs.Get(ctx, cell, zkPath)
	if err != nil {
		return nil, err
	}
	srvKeyspace := &topodatapb.SrvKeyspace{}
	if err := proto.Unmarshal(data, srvKeyspace); err != nil {
		return nil, fmt.Errorf("SrvKeyspace unmarshal failed: %v %v", data, err)
	}
	return srvKeyspace, nil
}

// UpdateSrvVSchema is part of the topo.Server interface
func (zs *Server) UpdateSrvVSchema(ctx context.Context, cell string, srvVSchema *vschemapb.SrvVSchema) error {
	zkPath := topo.SrvVSchemaFile
	data, err := proto.Marshal(srvVSchema)
	if err != nil {
		return err
	}
	_, err = zs.Update(ctx, cell, zkPath, data, nil)
	return err
}

// GetSrvVSchema is part of the topo.Server interface
func (zs *Server) GetSrvVSchema(ctx context.Context, cell string) (*vschemapb.SrvVSchema, error) {
	zkPath := topo.SrvVSchemaFile
	data, _, err := zs.Get(ctx, cell, zkPath)
	if err != nil {
		return nil, err
	}
	srvVSchema := &vschemapb.SrvVSchema{}
	if err := proto.Unmarshal(data, srvVSchema); err != nil {
		return nil, fmt.Errorf("SrvVSchema unmarshal failed: %v %v", data, err)
	}
	return srvVSchema, nil
}
