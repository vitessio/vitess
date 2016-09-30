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
)

/*
This file contains the Keyspace management code for zktopo.Server
*/

const (
	// GlobalKeyspacesPath is the path used to store global
	// information in ZK. Exported for tests.
	GlobalKeyspacesPath = "/zk/global/vt/keyspaces"
)

// CreateKeyspace is part of the topo.Server interface
func (zkts *Server) CreateKeyspace(ctx context.Context, keyspace string, value *topodatapb.Keyspace) error {
	keyspacePath := path.Join(GlobalKeyspacesPath, keyspace)
	pathList := []string{
		keyspacePath,
		path.Join(keyspacePath, "action"),
		path.Join(keyspacePath, "actionlog"),
		path.Join(keyspacePath, "shards"),
	}

	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}

	alreadyExists := false
	for i, zkPath := range pathList {
		var c []byte
		if i == 0 {
			c = data
		}
		_, err := zk.CreateRecursive(zkts.zconn, zkPath, c, 0, zookeeper.WorldACL(zookeeper.PermAll))
		switch err {
		case nil:
			// nothing here
		case zookeeper.ErrNodeExists:
			alreadyExists = true
		default:
			return convertError(err)
		}
	}
	if alreadyExists {
		return topo.ErrNodeExists
	}
	return nil
}

// UpdateKeyspace is part of the topo.Server interface
func (zkts *Server) UpdateKeyspace(ctx context.Context, keyspace string, value *topodatapb.Keyspace, existingVersion int64) (int64, error) {
	keyspacePath := path.Join(GlobalKeyspacesPath, keyspace)
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return -1, err
	}
	stat, err := zkts.zconn.Set(keyspacePath, data, int32(existingVersion))
	if err != nil {
		return -1, convertError(err)
	}

	return int64(stat.Version), nil
}

// DeleteKeyspace is part of the topo.Server interface.
func (zkts *Server) DeleteKeyspace(ctx context.Context, keyspace string) error {
	keyspacePath := path.Join(GlobalKeyspacesPath, keyspace)
	err := zk.DeleteRecursive(zkts.zconn, keyspacePath, -1)
	if err != nil {
		return convertError(err)
	}
	return nil
}

// GetKeyspace is part of the topo.Server interface
func (zkts *Server) GetKeyspace(ctx context.Context, keyspace string) (*topodatapb.Keyspace, int64, error) {
	keyspacePath := path.Join(GlobalKeyspacesPath, keyspace)
	data, stat, err := zkts.zconn.Get(keyspacePath)
	if err != nil {
		return nil, 0, convertError(err)
	}

	k := &topodatapb.Keyspace{}
	if err = json.Unmarshal(data, k); err != nil {
		return nil, 0, fmt.Errorf("bad keyspace data %v", err)
	}

	return k, int64(stat.Version), nil
}

// GetKeyspaces is part of the topo.Server interface
func (zkts *Server) GetKeyspaces(ctx context.Context) ([]string, error) {
	children, _, err := zkts.zconn.Children(GlobalKeyspacesPath)
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
