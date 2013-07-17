// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"expvar"
	"fmt"
	"path"
	"sort"
	"time"

	"code.google.com/p/vitess/go/vt/topo"
	"code.google.com/p/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

// Server is the zookeeper topo.Server implementation.
type Server struct {
	zconn zk.Conn
}

func (zkts *Server) Close() {
	zkts.zconn.Close()
}

func (zkts *Server) GetZConn() zk.Conn {
	return zkts.zconn
}

// NewServer can be used to create a custom Server
// (for tests for instance) but it cannot change the globally
// registered one.
func NewServer(zconn zk.Conn) *Server {
	return &Server{zconn: zconn}
}

func init() {
	zconn := zk.NewMetaConn(false)
	expvar.Publish("ZkMetaConn", zconn)
	topo.RegisterServer("zookeeper", NewServer(zconn))
}

//
// These helper methods are for ZK specific things
//

func (zkts *Server) ShardActionPath(keyspace, shard string) string {
	return "/zk/global/vt/keyspaces/" + keyspace + "/shards/" + shard + "/action"
}

// PurgeActions removes all queued actions, leaving the action node
// itself in place.
//
// This inherently breaks the locking mechanism of the action queue,
// so this is a rare cleanup action, not a normal part of the flow.
//
// This can be used for tablets, shards and keyspaces.
func (zkts *Server) PurgeActions(zkActionPath string, canBePurged func(data string) bool) error {
	if path.Base(zkActionPath) != "action" {
		return fmt.Errorf("not action path: %v", zkActionPath)
	}

	children, _, err := zkts.zconn.Children(zkActionPath)
	if err != nil {
		return err
	}

	sort.Strings(children)
	// Purge newer items first so the action queues don't try to process something.
	for i := len(children) - 1; i >= 0; i-- {
		actionPath := path.Join(zkActionPath, children[i])
		data, _, err := zkts.zconn.Get(actionPath)
		if err != nil && !zookeeper.IsError(err, zookeeper.ZNONODE) {
			return fmt.Errorf("PurgeActions(%v) err: %v", zkActionPath, err)
		}
		if !canBePurged(data) {
			continue
		}

		err = zk.DeleteRecursive(zkts.zconn, actionPath, -1)
		if err != nil && !zookeeper.IsError(err, zookeeper.ZNONODE) {
			return fmt.Errorf("PurgeActions(%v) err: %v", zkActionPath, err)
		}
	}
	return nil
}

// StaleActions returns a list of queued actions that have been
// sitting for more than some amount of time.
func (zkts *Server) StaleActions(zkActionPath string, maxStaleness time.Duration, isStale func(data string) bool) ([]string, error) {
	if path.Base(zkActionPath) != "action" {
		return nil, fmt.Errorf("not action path: %v", zkActionPath)
	}

	children, _, err := zkts.zconn.Children(zkActionPath)
	if err != nil {
		return nil, err
	}

	staleActions := make([]string, 0, 16)
	// Purge newer items first so the action queues don't try to process something.
	sort.Strings(children)
	for i := 0; i < len(children); i++ {
		actionPath := path.Join(zkActionPath, children[i])
		data, stat, err := zkts.zconn.Get(actionPath)
		if err != nil && !zookeeper.IsError(err, zookeeper.ZNONODE) {
			return nil, fmt.Errorf("stale action err: %v", err)
		}
		if stat == nil || time.Since(stat.MTime()) <= maxStaleness {
			continue
		}
		if isStale(data) {
			staleActions = append(staleActions, data)
		}
	}
	return staleActions, nil
}

// PruneActionLogs prunes old actionlog entries. Returns how many
// entries were purged (even if there was an error).
//
// There is a chance some processes might still be waiting for action
// results, but it is very very small.
func (zkts *Server) PruneActionLogs(zkActionLogPath string, keepCount int) (prunedCount int, err error) {
	if path.Base(zkActionLogPath) != "actionlog" {
		return 0, fmt.Errorf("not actionlog path: %v", zkActionLogPath)
	}

	// get sorted list of children
	children, _, err := zkts.zconn.Children(zkActionLogPath)
	if err != nil {
		return 0, err
	}
	sort.Strings(children)

	// see if nothing to do
	if len(children) <= keepCount {
		return 0, nil
	}

	for i := 0; i < len(children)-keepCount; i++ {
		actionPath := path.Join(zkActionLogPath, children[i])
		err = zk.DeleteRecursive(zkts.zconn, actionPath, -1)
		if err != nil {
			return prunedCount, fmt.Errorf("purge action err: %v", err)
		}
		prunedCount++
	}
	return prunedCount, nil
}
