// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"fmt"
	"path"
	"sort"

	zookeeper "github.com/samuel/go-zookeeper/zk"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/zk"
)

// Server is the zookeeper topo.Impl implementation.
type Server struct {
	zconn zk.Conn
}

// newServer creates a Server.
func newServer() *Server {
	return &Server{
		zconn: zk.NewMetaConn(),
	}
}

// Close is part of topo.Server interface.
func (zkts *Server) Close() {
	zkts.zconn.Close()
}

// GetZConn returns the zookeeper connection for this Server.
func (zkts *Server) GetZConn() zk.Conn {
	return zkts.zconn
}

//
// These helper methods are for ZK specific things
//

// PurgeActions removes all queued actions, leaving the action node
// itself in place.
//
// This inherently breaks the locking mechanism of the action queue,
// so this is a rare cleanup action, not a normal part of the flow.
//
// This can be used for tablets, shards and keyspaces.
func (zkts *Server) PurgeActions(zkActionPath string, canBePurged func(data []byte) bool) error {
	if path.Base(zkActionPath) != "action" {
		return fmt.Errorf("not action path: %v", zkActionPath)
	}

	children, _, err := zkts.zconn.Children(zkActionPath)
	if err != nil {
		return convertError(err)
	}

	sort.Strings(children)
	// Purge newer items first so the action queues don't try to process something.
	for i := len(children) - 1; i >= 0; i-- {
		actionPath := path.Join(zkActionPath, children[i])
		data, _, err := zkts.zconn.Get(actionPath)
		if err != nil && err != zookeeper.ErrNoNode {
			return fmt.Errorf("PurgeActions(%v) err: %v", zkActionPath, err)
		}
		if !canBePurged(data) {
			continue
		}

		err = zk.DeleteRecursive(zkts.zconn, actionPath, -1)
		if err != nil && err != zookeeper.ErrNoNode {
			return fmt.Errorf("PurgeActions(%v) err: %v", zkActionPath, err)
		}
	}
	return nil
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
		return 0, convertError(err)
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

func init() {
	topo.RegisterFactory("zookeeper", func(serverAddr, root string) (topo.Impl, error) {
		return newServer(), nil
	})
}
