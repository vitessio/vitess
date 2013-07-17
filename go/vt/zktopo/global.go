// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/topo"
	"code.google.com/p/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

/*
This file contains the global (not per-cell) methods of zktopo.Server
*/
const (
	globalKeyspacesPath = "/zk/global/vt/keyspaces"
)

//
// Cell management
//

func (zkts *Server) GetKnownCells() ([]string, error) {
	cellsWithGlobal := zk.ZkKnownCells(false)
	cells := make([]string, 0, len(cellsWithGlobal))
	for _, cell := range cellsWithGlobal {
		if cell != "global" {
			cells = append(cells, cell)
		}
	}
	return cells, nil
}

//
// Keyspace management
//

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

//
// Shard Management
//

func (zkts *Server) CreateShard(keyspace, shard string) error {
	shardPath := path.Join(globalKeyspacesPath, keyspace, "shards", shard)
	pathList := []string{
		shardPath,
		path.Join(shardPath, "action"),
		path.Join(shardPath, "actionlog"),
	}

	alreadyExists := false
	for i, zkPath := range pathList {
		c := ""
		if i == 0 {
			s := topo.Shard{}
			c = s.Json()
		}
		_, err := zk.CreateRecursive(zkts.zconn, zkPath, c, 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
		if err != nil {
			if zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
				alreadyExists = true
			} else {
				return fmt.Errorf("error creating shard: %v %v", zkPath, err)
			}
		}
	}
	if alreadyExists {
		return topo.ErrNodeExists
	}
	return nil
}

func (zkts *Server) UpdateShard(si *topo.ShardInfo) error {
	shardPath := path.Join(globalKeyspacesPath, si.Keyspace(), "shards", si.ShardName())
	_, err := zkts.zconn.Set(shardPath, si.Json(), -1)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
	}
	return err
}

func (zkts *Server) ValidateShard(keyspace, shard string) error {
	shardPath := path.Join(globalKeyspacesPath, keyspace, "shards", shard)
	zkPaths := []string{
		path.Join(shardPath, "action"),
		path.Join(shardPath, "actionlog"),
	}
	for _, zkPath := range zkPaths {
		_, _, err := zkts.zconn.Get(zkPath)
		if err != nil {
			return err
		}
	}
	return nil
}

func (zkts *Server) GetShard(keyspace, shard string) (*topo.ShardInfo, error) {
	shardPath := path.Join(globalKeyspacesPath, keyspace, "shards", shard)
	data, _, err := zkts.zconn.Get(shardPath)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, err
	}

	shardInfo, err := topo.NewShardInfo(keyspace, shard, data)
	if err != nil {
		return nil, err
	}
	return shardInfo, nil
}

func (zkts *Server) GetShardNames(keyspace string) ([]string, error) {
	shardsPath := path.Join(globalKeyspacesPath, keyspace, "shards")
	children, _, err := zkts.zconn.Children(shardsPath)
	if err != nil {
		return nil, err
	}

	sort.Strings(children)
	return children, nil
}

//
// Replication graph management
//

func (zkts *Server) GetReplicationPaths(keyspace, shard, repPath string) ([]topo.TabletAlias, error) {
	replicationPath := path.Join(globalKeyspacesPath, keyspace, "shards", shard, repPath)
	children, _, err := zkts.zconn.Children(replicationPath)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, err
	}

	result := make([]topo.TabletAlias, 0, len(children))
	for _, child := range children {
		// 'action' and 'actionlog' can only be present at the toplevel
		if (repPath == "" || repPath == "/") && (child == "action" || child == "actionlog") {
			continue
		}
		alias, err := topo.ParseTabletAliasString(child)
		if err != nil {
			return nil, err
		}
		result = append(result, alias)
	}
	return result, nil
}

func (zkts *Server) CreateReplicationPath(keyspace, shard, repPath string) error {
	replicationPath := path.Join(globalKeyspacesPath, keyspace, "shards", shard, repPath)
	_, err := zk.CreateRecursive(zkts.zconn, replicationPath, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
			err = topo.ErrNodeExists
		}
		return err
	}
	return nil
}

func (zkts *Server) DeleteReplicationPath(keyspace, shard, repPath string) error {
	replicationPath := path.Join(globalKeyspacesPath, keyspace, "shards", shard, repPath)
	err := zkts.zconn.Delete(replicationPath, -1)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		} else if zookeeper.IsError(err, zookeeper.ZNOTEMPTY) {
			err = topo.ErrNotEmpty
		}
		return err
	}
	return nil
}

//
// Lock management
//

// lockForAction creates the action node in zookeeper, waits for the
// queue lock, displays a nice error message if it cant get it
func (zkts *Server) lockForAction(actionDir, contents string, timeout time.Duration, interrupted chan struct{}) (string, error) {
	// create the action path
	actionPath, err := zkts.zconn.Create(actionDir, contents, zookeeper.SEQUENCE, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		return "", err
	}

	err = zk.ObtainQueueLock(zkts.zconn, actionPath, timeout, interrupted)
	if err != nil {
		var errToReturn error
		switch err {
		case zk.ErrInterrupted:
			errToReturn = topo.ErrInterrupted
		case zk.ErrTimeout:
			errToReturn = topo.ErrTimeout
		default:
			errToReturn = fmt.Errorf("failed to obtain action lock: %v %v", actionPath, err)
		}

		// Regardless of the reason, try to cleanup.
		relog.Warning("Failed to obtain action lock: %v", err)
		zkts.zconn.Delete(actionPath, -1)

		// Show the other actions in the directory
		dir := path.Dir(actionPath)
		children, _, err := zkts.zconn.Children(dir)
		if err != nil {
			relog.Warning("Failed to get children of %v: %v", dir, err)
			return "", errToReturn
		}

		if len(children) == 0 {
			relog.Warning("No other action running, you may just try again now.")
			return "", errToReturn
		}

		childPath := path.Join(dir, children[0])
		data, _, err := zkts.zconn.Get(childPath)
		if err != nil {
			relog.Warning("Failed to get first action node %v (may have just ended): %v", childPath, err)
			return "", errToReturn
		}

		relog.Warning("------ Most likely blocking action: %v\n%v", childPath, data)
		return "", errToReturn
	}

	return actionPath, nil
}

func (zkts *Server) unlockForAction(lockPath, results string) error {
	// Write the data to the actionlog
	actionLogPath := strings.Replace(lockPath, "/action/", "/actionlog/", 1)
	if _, err := zk.CreateRecursive(zkts.zconn, actionLogPath, results, 0, zookeeper.WorldACL(zookeeper.PERM_ALL)); err != nil {
		return err
	}

	// and delete the action
	return zk.DeleteRecursive(zkts.zconn, lockPath, -1)
}

func (zkts *Server) LockKeyspaceForAction(keyspace, contents string, timeout time.Duration, interrupted chan struct{}) (string, error) {
	// Action paths end in a trailing slash to that when we create
	// sequential nodes, they are created as children, not siblings.
	actionDir := path.Join(globalKeyspacesPath, keyspace, "action") + "/"
	return zkts.lockForAction(actionDir, contents, timeout, interrupted)
}

func (zkts *Server) UnlockKeyspaceForAction(keyspace, lockPath, results string) error {
	return zkts.unlockForAction(lockPath, results)
}

func (zkts *Server) LockShardForAction(keyspace, shard, contents string, timeout time.Duration, interrupted chan struct{}) (string, error) {
	// Action paths end in a trailing slash to that when we create
	// sequential nodes, they are created as children, not siblings.
	actionDir := path.Join(globalKeyspacesPath, keyspace, "shards", shard, "action") + "/"
	return zkts.lockForAction(actionDir, contents, timeout, interrupted)
}

func (zkts *Server) UnlockShardForAction(keyspace, shard, lockPath, results string) error {
	return zkts.unlockForAction(lockPath, results)
}
