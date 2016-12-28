// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcd2topo

import (
	"fmt"
	"path"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
)

// GetSrvKeyspaceNames implements topo.Server.
func (s *Server) GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error) {
	children, err := s.ListDir(ctx, cell, keyspacesPath)
	switch err {
	case nil:
		return children, nil
	case topo.ErrNoNode:
		return nil, nil
	default:
		return nil, err
	}
}

// UpdateSrvKeyspace implements topo.Server.
func (s *Server) UpdateSrvKeyspace(ctx context.Context, cell, keyspace string, srvKeyspace *topodatapb.SrvKeyspace) error {
	nodePath := path.Join(keyspacesPath, keyspace, topo.SrvKeyspaceFile)
	data, err := proto.Marshal(srvKeyspace)
	if err != nil {
		return err
	}
	_, err = s.Update(ctx, cell, nodePath, data, nil)
	return err
}

// DeleteSrvKeyspace implements topo.Server.
func (s *Server) DeleteSrvKeyspace(ctx context.Context, cell, keyspace string) error {
	nodePath := path.Join(keyspacesPath, keyspace, topo.SrvKeyspaceFile)
	return s.Delete(ctx, cell, nodePath, nil)
}

// GetSrvKeyspace implements topo.Server.
func (s *Server) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	nodePath := path.Join(keyspacesPath, keyspace, topo.SrvKeyspaceFile)
	data, _, err := s.Get(ctx, cell, nodePath)
	if err != nil {
		return nil, err
	}
	srvKeyspace := &topodatapb.SrvKeyspace{}
	if err := proto.Unmarshal(data, srvKeyspace); err != nil {
		return nil, fmt.Errorf("SrvKeyspace unmarshal failed: %v %v", data, err)
	}
	return srvKeyspace, nil
}

// UpdateSrvVSchema implements topo.Server.
func (s *Server) UpdateSrvVSchema(ctx context.Context, cell string, srvVSchema *vschemapb.SrvVSchema) error {
	nodePath := topo.SrvVSchemaFile
	data, err := proto.Marshal(srvVSchema)
	if err != nil {
		return err
	}
	_, err = s.Update(ctx, cell, nodePath, data, nil)
	return err
}

// GetSrvVSchema implements topo.Server.
func (s *Server) GetSrvVSchema(ctx context.Context, cell string) (*vschemapb.SrvVSchema, error) {
	nodePath := topo.SrvVSchemaFile
	data, _, err := s.Get(ctx, cell, nodePath)
	if err != nil {
		return nil, err
	}
	srvVSchema := &vschemapb.SrvVSchema{}
	if err := proto.Unmarshal(data, srvVSchema); err != nil {
		return nil, fmt.Errorf("SrvVSchema unmarshal failed: %v %v", data, err)
	}
	return srvVSchema, nil
}
