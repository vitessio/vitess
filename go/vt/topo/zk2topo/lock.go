// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk2topo

import (
	"fmt"
	"path"

	log "github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

// This file contains the lock management code for zktopo.Server.

// lockForAction creates the locks node in zookeeper, waits for the
// queue lock, displays a nice error message if it cant get it.
func (zs *Server) lockForAction(ctx context.Context, locksDir, contents string) (string, error) {
	conn, root, err := zs.connForCell(ctx, topo.GlobalCell)
	if err != nil {
		return "", err
	}

	// Lock paths end in a trailing slash to that when we create
	// sequential nodes, they are created as children, not siblings.
	locksDir = path.Join(root, locksDir) + "/"

	// Create the locks path, possibly creating the parent.
	locksPath, err := CreateRecursive(ctx, conn, locksDir, []byte(contents), zk.FlagSequence|zk.FlagEphemeral, zk.WorldACL(PermFile), 1)
	if err != nil {
		return "", convertError(err)
	}

	err = obtainQueueLock(ctx, conn, locksPath)
	if err != nil {
		var errToReturn error
		switch err {
		case context.DeadlineExceeded:
			errToReturn = topo.ErrTimeout
		case context.Canceled:
			errToReturn = topo.ErrInterrupted
		default:
			errToReturn = fmt.Errorf("failed to obtain action lock: %v %v", locksPath, err)
		}

		// Regardless of the reason, try to cleanup.
		log.Warningf("Failed to obtain action lock: %v", err)
		conn.Delete(ctx, locksPath, -1)

		// Show the other locks in the directory
		dir := path.Dir(locksPath)
		children, _, err := conn.Children(ctx, dir)
		if err != nil {
			log.Warningf("Failed to get children of %v: %v", dir, err)
			return "", errToReturn
		}

		if len(children) == 0 {
			log.Warningf("No other locks present, you may just try again now.")
			return "", errToReturn
		}

		childPath := path.Join(dir, children[0])
		data, _, err := conn.Get(ctx, childPath)
		if err != nil {
			log.Warningf("Failed to get first locks node %v (may have just ended): %v", childPath, err)
			return "", errToReturn
		}

		log.Warningf("------ Most likely blocking lock: %v\n%v", childPath, string(data))
		return "", errToReturn
	}

	// Remove the root prefix from the file. So when we delete it,
	// it's a relative file.
	locksPath = locksPath[len(root):]
	return locksPath, nil
}

func (zs *Server) unlockForAction(ctx context.Context, lockPath, results string) error {
	// Just delete the file.
	return zs.Delete(ctx, topo.GlobalCell, lockPath, nil)
}

// LockKeyspaceForAction is part of topo.Server interface
func (zs *Server) LockKeyspaceForAction(ctx context.Context, keyspace, contents string) (string, error) {
	locksDir := path.Join(keyspacesPath, keyspace, locksPath)
	return zs.lockForAction(ctx, locksDir, contents)
}

// UnlockKeyspaceForAction is part of topo.Server interface
func (zs *Server) UnlockKeyspaceForAction(ctx context.Context, keyspace, lockPath, results string) error {
	return zs.unlockForAction(ctx, lockPath, results)
}

// LockShardForAction is part of topo.Server interface
func (zs *Server) LockShardForAction(ctx context.Context, keyspace, shard, contents string) (string, error) {
	locksDir := path.Join(keyspacesPath, keyspace, shardsPath, shard, locksPath)
	return zs.lockForAction(ctx, locksDir, contents)
}

// UnlockShardForAction is part of topo.Server interface
func (zs *Server) UnlockShardForAction(ctx context.Context, keyspace, shard, lockPath, results string) error {
	return zs.unlockForAction(ctx, lockPath, results)
}
