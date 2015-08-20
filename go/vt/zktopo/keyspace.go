// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"encoding/json"
	"fmt"
	"path"
	"sort"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/zk"
	"golang.org/x/net/context"
	"launchpad.net/gozk/zookeeper"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

/*
This file contains the Keyspace management code for zktopo.Server
*/

const (
	globalKeyspacesPath = "/zk/global/vt/keyspaces"
)

// CreateKeyspace is part of the topo.Server interface
func (zkts *Server) CreateKeyspace(ctx context.Context, keyspace string, value *pb.Keyspace) error {
	keyspacePath := path.Join(globalKeyspacesPath, keyspace)
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
		c := ""
		if i == 0 {
			c = string(data)
		}
		_, err := zk.CreateRecursive(zkts.zconn, zkPath, c, 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
		if err != nil {
			if zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
				alreadyExists = true
			} else {
				return fmt.Errorf("error creating keyspace: %v %v", zkPath, err)
			}
		}
	}
	if alreadyExists {
		return topo.ErrNodeExists
	}
	return nil
}

// UpdateKeyspace is part of the topo.Server interface
func (zkts *Server) UpdateKeyspace(ctx context.Context, keyspace string, value *pb.Keyspace, existingVersion int64) (int64, error) {
	keyspacePath := path.Join(globalKeyspacesPath, keyspace)
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return -1, err
	}
	stat, err := zkts.zconn.Set(keyspacePath, string(data), int(existingVersion))
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return -1, err
	}

	return int64(stat.Version()), nil
}

// DeleteKeyspace is part of the topo.Server interface.
func (zkts *Server) DeleteKeyspace(ctx context.Context, keyspace string) error {
	keyspacePath := path.Join(globalKeyspacesPath, keyspace)
	err := zk.DeleteRecursive(zkts.zconn, keyspacePath, -1)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return err
	}
	return nil
}

// GetKeyspace is part of the topo.Server interface
func (zkts *Server) GetKeyspace(ctx context.Context, keyspace string) (*pb.Keyspace, int64, error) {
	keyspacePath := path.Join(globalKeyspacesPath, keyspace)
	data, stat, err := zkts.zconn.Get(keyspacePath)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, 0, err
	}

	k := &pb.Keyspace{}
	if err = json.Unmarshal([]byte(data), k); err != nil {
		return nil, 0, fmt.Errorf("bad keyspace data %v", err)
	}

	return k, int64(stat.Version()), nil
}

// GetKeyspaces is part of the topo.Server interface
func (zkts *Server) GetKeyspaces(ctx context.Context) ([]string, error) {
	children, _, err := zkts.zconn.Children(globalKeyspacesPath)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			return nil, nil
		}
		return nil, err
	}

	sort.Strings(children)
	return children, nil
}

// DeleteKeyspaceShards is part of the topo.Server interface
func (zkts *Server) DeleteKeyspaceShards(ctx context.Context, keyspace string) error {
	shardsPath := path.Join(globalKeyspacesPath, keyspace, "shards")
	if err := zk.DeleteRecursive(zkts.zconn, shardsPath, -1); err != nil && !zookeeper.IsError(err, zookeeper.ZNONODE) {
		return err
	}
	return nil
}
