// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"encoding/json"
	"fmt"
	"path"
	"sort"

	zookeeper "github.com/samuel/go-zookeeper/zk"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/zk"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
)

// This file contains the serving graph management code of zktopo.Server.

func zkPathForCell(cell string) string {
	return fmt.Sprintf("/zk/%v/vt", cell)
}

func zkPathForSrvKeyspaces(cell string) string {
	return path.Join(zkPathForCell(cell), "ns")
}

func zkPathForSrvKeyspace(cell, keyspace string) string {
	return path.Join(zkPathForSrvKeyspaces(cell), keyspace)
}

func zkPathForSrvVSchema(cell string) string {
	return path.Join(zkPathForCell(cell), "vschema")
}

// GetSrvKeyspaceNames is part of the topo.Server interface
func (zkts *Server) GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error) {
	children, _, err := zkts.zconn.Children(zkPathForSrvKeyspaces(cell))
	switch err {
	case nil:
		sort.Strings(children)
		return children, nil
	case zookeeper.ErrNoNode:
		return nil, nil
	default:
		return nil, convertError(err)
	}
}

// UpdateSrvKeyspace is part of the topo.Server interface
func (zkts *Server) UpdateSrvKeyspace(ctx context.Context, cell, keyspace string, srvKeyspace *topodatapb.SrvKeyspace) error {
	path := zkPathForSrvKeyspace(cell, keyspace)
	data, err := json.MarshalIndent(srvKeyspace, "", "  ")
	if err != nil {
		return err
	}
	_, err = zkts.zconn.Set(path, data, -1)
	if err == zookeeper.ErrNoNode {
		_, err = zk.CreateRecursive(zkts.zconn, path, data, 0, zookeeper.WorldACL(zookeeper.PermAll))
	}
	return convertError(err)
}

// DeleteSrvKeyspace is part of the topo.Server interface
func (zkts *Server) DeleteSrvKeyspace(ctx context.Context, cell, keyspace string) error {
	path := zkPathForSrvKeyspace(cell, keyspace)
	err := zkts.zconn.Delete(path, -1)
	if err != nil {
		return convertError(err)
	}
	return nil
}

// GetSrvKeyspace is part of the topo.Server interface
func (zkts *Server) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	path := zkPathForSrvKeyspace(cell, keyspace)
	data, _, err := zkts.zconn.Get(path)
	if err != nil {
		return nil, convertError(err)
	}
	if len(data) == 0 {
		return nil, topo.ErrNoNode
	}
	srvKeyspace := &topodatapb.SrvKeyspace{}
	if err := json.Unmarshal(data, srvKeyspace); err != nil {
		return nil, fmt.Errorf("SrvKeyspace unmarshal failed: %v %v", data, err)
	}
	return srvKeyspace, nil
}

// UpdateSrvVSchema is part of the topo.Server interface
func (zkts *Server) UpdateSrvVSchema(ctx context.Context, cell string, srvVSchema *vschemapb.SrvVSchema) error {
	path := zkPathForSrvVSchema(cell)
	data, err := json.MarshalIndent(srvVSchema, "", "  ")
	if err != nil {
		return err
	}
	_, err = zkts.zconn.Set(path, data, -1)
	if err == zookeeper.ErrNoNode {
		_, err = zk.CreateRecursive(zkts.zconn, path, data, 0, zookeeper.WorldACL(zookeeper.PermAll))
	}
	return convertError(err)
}

// GetSrvVSchema is part of the topo.Server interface
func (zkts *Server) GetSrvVSchema(ctx context.Context, cell string) (*vschemapb.SrvVSchema, error) {
	path := zkPathForSrvVSchema(cell)
	data, _, err := zkts.zconn.Get(path)
	if err != nil {
		return nil, convertError(err)
	}
	if len(data) == 0 {
		return nil, topo.ErrNoNode
	}
	srvVSchema := &vschemapb.SrvVSchema{}
	if err := json.Unmarshal(data, srvVSchema); err != nil {
		return nil, fmt.Errorf("SrvVSchema unmarshal failed: %v %v", data, err)
	}
	return srvVSchema, nil
}
