// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"fmt"
	"path"
	"sort"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

/*
This file contains the Keyspace management code for zktopo.Server
*/

const (
	globalKeyspacesPath = "/zk/global/vt/keyspaces"
)

func (zkts *Server) CreateKeyspace(keyspace string) error {
	keyspacePath := path.Join(globalKeyspacesPath, keyspace)
	pathList := []string{
		keyspacePath,
		path.Join(keyspacePath, "action"),
		path.Join(keyspacePath, "actionlog"),
		path.Join(keyspacePath, "shards"),
	}

	alreadyExists := false
	for _, zkPath := range pathList {
		_, err := zk.CreateRecursive(zkts.zconn, zkPath, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
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

func (zkts *Server) GetKeyspaces() ([]string, error) {
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

func (zkts *Server) DeleteKeyspaceShards(keyspace string) error {
	shardsPath := path.Join(globalKeyspacesPath, keyspace, "shards")
	if err := zk.DeleteRecursive(zkts.zconn, shardsPath, -1); err != nil && !zookeeper.IsError(err, zookeeper.ZNONODE) {
		return err
	}
	return nil
}
