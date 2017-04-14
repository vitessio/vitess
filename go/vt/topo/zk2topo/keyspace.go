// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk2topo

import (
	"fmt"
	"path"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// This file contains the Keyspace management code for zktopo.Server.
// Eventually this will be moved to the go/vt/topo package.

// CreateKeyspace is part of the topo.Server interface
func (zs *Server) CreateKeyspace(ctx context.Context, keyspace string, value *topodatapb.Keyspace) error {
	data, err := proto.Marshal(value)
	if err != nil {
		return err
	}

	keyspacePath := path.Join(keyspacesPath, keyspace, topo.KeyspaceFile)
	_, err = zs.Create(ctx, topo.GlobalCell, keyspacePath, data)
	return err
}

// UpdateKeyspace is part of the topo.Server interface
func (zs *Server) UpdateKeyspace(ctx context.Context, keyspace string, value *topodatapb.Keyspace, existingVersion int64) (int64, error) {
	data, err := proto.Marshal(value)
	if err != nil {
		return -1, err
	}

	keyspacePath := path.Join(keyspacesPath, keyspace, topo.KeyspaceFile)
	version, err := zs.Update(ctx, topo.GlobalCell, keyspacePath, data, ZKVersion(existingVersion))
	if err != nil {
		return -1, err
	}
	return int64(version.(ZKVersion)), nil
}

// DeleteKeyspace is part of the topo.Server interface.
func (zs *Server) DeleteKeyspace(ctx context.Context, keyspace string) error {
	keyspacePath := path.Join(keyspacesPath, keyspace, topo.KeyspaceFile)
	return zs.Delete(ctx, topo.GlobalCell, keyspacePath, nil)
}

// GetKeyspace is part of the topo.Server interface
func (zs *Server) GetKeyspace(ctx context.Context, keyspace string) (*topodatapb.Keyspace, int64, error) {
	keyspacePath := path.Join(keyspacesPath, keyspace, topo.KeyspaceFile)
	data, version, err := zs.Get(ctx, topo.GlobalCell, keyspacePath)
	if err != nil {
		return nil, 0, err
	}

	k := &topodatapb.Keyspace{}
	if err = proto.Unmarshal(data, k); err != nil {
		return nil, 0, fmt.Errorf("bad keyspace data %v", err)
	}

	return k, int64(version.(ZKVersion)), nil
}

// GetKeyspaces is part of the topo.Server interface
func (zs *Server) GetKeyspaces(ctx context.Context) ([]string, error) {
	children, err := zs.ListDir(ctx, topo.GlobalCell, keyspacesPath)
	switch err {
	case nil:
		return children, nil
	case topo.ErrNoNode:
		return nil, nil
	default:
		return nil, err
	}
}
