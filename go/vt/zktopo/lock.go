// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"fmt"
	"path"
	"strings"
	"time"

	log "github.com/golang/glog"
	zookeeper "github.com/samuel/go-zookeeper/zk"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/zk"
	"golang.org/x/net/context"
)

/*
This file contains the lock management code for zktopo.Server
*/

// lockForAction creates the action node in zookeeper, waits for the
// queue lock, displays a nice error message if it cant get it
func (zkts *Server) lockForAction(ctx context.Context, actionDir, contents string) (string, error) {
	// create the action path
	actionPath, err := zkts.zconn.Create(actionDir, []byte(contents), zookeeper.FlagSequence|zookeeper.FlagEphemeral, zookeeper.WorldACL(zk.PermFile))
	if err != nil {
		return "", convertError(err)
	}

	// get the timeout from the context
	var timeout time.Duration
	deadline, ok := ctx.Deadline()
	if !ok {
		// enforce a default timeout
		timeout = 30 * time.Second
	} else {
		timeout = deadline.Sub(time.Now())
	}

	// get the interrupted channel from context or don't interrupt
	interrupted := ctx.Done()
	if interrupted == nil {
		interrupted = make(chan struct{})
	}

	err = zk.ObtainQueueLock(zkts.zconn, actionPath, timeout, interrupted)
	if err != nil {
		var errToReturn error
		switch err {
		case zk.ErrTimeout:
			errToReturn = topo.ErrTimeout
		case zk.ErrInterrupted:
			// the context failed, get the error from it
			if ctx.Err() == context.DeadlineExceeded {
				errToReturn = topo.ErrTimeout
			} else {
				errToReturn = topo.ErrInterrupted
			}
		default:
			errToReturn = fmt.Errorf("failed to obtain action lock: %v %v", actionPath, err)
		}

		// Regardless of the reason, try to cleanup.
		log.Warningf("Failed to obtain action lock: %v", err)
		zkts.zconn.Delete(actionPath, -1)

		// Show the other actions in the directory
		dir := path.Dir(actionPath)
		children, _, err := zkts.zconn.Children(dir)
		if err != nil {
			log.Warningf("Failed to get children of %v: %v", dir, err)
			return "", errToReturn
		}

		if len(children) == 0 {
			log.Warningf("No other action running, you may just try again now.")
			return "", errToReturn
		}

		childPath := path.Join(dir, children[0])
		data, _, err := zkts.zconn.Get(childPath)
		if err != nil {
			log.Warningf("Failed to get first action node %v (may have just ended): %v", childPath, err)
			return "", errToReturn
		}

		log.Warningf("------ Most likely blocking action: %v\n%v", childPath, data)
		return "", errToReturn
	}

	return actionPath, nil
}

func (zkts *Server) unlockForAction(lockPath, results string) error {
	// Write the data to the actionlog
	actionLogPath := strings.Replace(lockPath, "/action/", "/actionlog/", 1)
	if _, err := zk.CreateRecursive(zkts.zconn, actionLogPath, []byte(results), 0, zookeeper.WorldACL(zookeeper.PermAll)); err != nil {
		log.Warningf("Cannot create actionlog path %v (check the permissions with 'zk stat'), will keep the lock, use 'zk rm' to clear the lock", actionLogPath)
		return convertError(err)
	}

	// and delete the action
	return zk.DeleteRecursive(zkts.zconn, lockPath, -1)
}

// LockKeyspaceForAction is part of topo.Server interface
func (zkts *Server) LockKeyspaceForAction(ctx context.Context, keyspace, contents string) (string, error) {
	// Action paths end in a trailing slash to that when we create
	// sequential nodes, they are created as children, not siblings.
	actionDir := path.Join(GlobalKeyspacesPath, keyspace, "action") + "/"
	return zkts.lockForAction(ctx, actionDir, contents)
}

// UnlockKeyspaceForAction is part of topo.Server interface
func (zkts *Server) UnlockKeyspaceForAction(ctx context.Context, keyspace, lockPath, results string) error {
	return zkts.unlockForAction(lockPath, results)
}

// LockShardForAction is part of topo.Server interface
func (zkts *Server) LockShardForAction(ctx context.Context, keyspace, shard, contents string) (string, error) {
	// Action paths end in a trailing slash to that when we create
	// sequential nodes, they are created as children, not siblings.
	actionDir := path.Join(GlobalKeyspacesPath, keyspace, "shards", shard, "action") + "/"
	return zkts.lockForAction(ctx, actionDir, contents)
}

// UnlockShardForAction is part of topo.Server interface
func (zkts *Server) UnlockShardForAction(ctx context.Context, keyspace, shard, lockPath, results string) error {
	return zkts.unlockForAction(lockPath, results)
}
