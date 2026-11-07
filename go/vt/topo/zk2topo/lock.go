/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package zk2topo

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/z-division/go-zookeeper/zk"

	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

// This file contains the lock management code for zktopo.Server.

// zkLockDescriptor implements topo.LockDescriptor.
type zkLockDescriptor struct {
	zs       *Server
	nodePath string
}

// Lock is part of the topo.Conn interface.
func (zs *Server) Lock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	return zs.lock(ctx, dirPath, contents, true /*wait*/)
}

// LockWithTTL is part of the topo.Conn interface. It behaves the same as Lock
// as TTLs are not supported in Zookeeper.
func (zs *Server) LockWithTTL(ctx context.Context, dirPath, contents string, _ time.Duration) (topo.LockDescriptor, error) {
	return zs.lock(ctx, dirPath, contents, true /*wait*/)
}

// LockName is part of the topo.Conn interface.
func (zs *Server) LockName(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	return zs.lock(ctx, dirPath, contents, true /*wait*/)
}

// TryLock is part of the topo.Conn interface.
// Like the non-blocking variant of the ZooKeeper lock recipe, it creates
// its lock node and gives up if it is not the lowest one, so two callers
// cannot both see the lock as free and then block on each other.
func (zs *Server) TryLock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	// We list the directory first to make sure it exists, as lock()
	// creates any missing parent directories.
	if _, err := zs.ListDir(ctx, dirPath, false /*full*/); err != nil {
		// We need to return the right error codes, like
		// topo.ErrNoNode and topo.ErrInterrupted, and the
		// easiest way to do this is to return convertError(err).
		// It may lose some of the context, if this is an issue,
		// maybe logging the error would work here.
		return nil, convertError(err, dirPath)
	}

	return zs.lock(ctx, dirPath, contents, false /*wait*/)
}

// lock is used by Lock(), LockWithTTL(), LockName() and TryLock().
// If wait is false, it returns a topo.NodeExists error instead of
// waiting for older lock nodes to go away.
func (zs *Server) lock(ctx context.Context, dirPath, contents string, wait bool) (topo.LockDescriptor, error) {
	// Lock paths end in a trailing slash so that when we create
	// sequential nodes, they are created as children, not siblings.
	locksDir := path.Join(zs.root, dirPath, locksPath) + "/"

	// Create the lock path, creating the parents as needed.
	nodePath, err := CreateRecursive(ctx, zs.conn, locksDir, []byte(contents), zk.FlagSequence|zk.FlagEphemeral, zk.WorldACL(PermFile), -1)
	if err != nil {
		return nil, convertError(err, locksDir)
	}

	err = obtainQueueLock(ctx, zs.conn, nodePath, wait)
	if err == errQueueLockHeld {
		// Someone else holds the lock. Remove our node so it does
		// not block the callers queued behind it.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), baseTimeout)
		defer cancel()
		if err := zs.conn.Delete(cleanupCtx, nodePath, -1); err != nil {
			log.Warn(fmt.Sprintf("Failed to cleanup unsuccessful lock path %s: %v", nodePath, err))
		}
		return nil, topo.NewError(topo.NodeExists, "lock already exists at path "+dirPath)
	}
	if err != nil {
		var errToReturn error
		switch err {
		case context.DeadlineExceeded:
			errToReturn = topo.NewError(topo.Timeout, nodePath)
		case context.Canceled:
			errToReturn = topo.NewError(topo.Interrupted, nodePath)
		default:
			errToReturn = vterrors.Wrapf(err, "failed to obtain lock: %v", nodePath)
		}

		// Regardless of the reason, try to cleanup.
		log.Warn(fmt.Sprintf("Failed to obtain lock: %v", err))

		cleanupCtx, cancel := context.WithTimeout(context.Background(), baseTimeout)
		defer cancel()

		if err := zs.conn.Delete(cleanupCtx, nodePath, -1); err != nil {
			log.Warn(fmt.Sprintf("Failed to cleanup unsuccessful lock path %s: %v", nodePath, err))
		}

		// Show the other locks in the directory
		dir := path.Dir(nodePath)
		children, _, err := zs.conn.Children(cleanupCtx, dir)
		if err != nil {
			log.Warn(fmt.Sprintf("Failed to get children of %v: %v", dir, err))
			return nil, errToReturn
		}

		if len(children) == 0 {
			log.Warn("No other locks present, you may just try again now.")
			return nil, errToReturn
		}

		childPath := path.Join(dir, children[0])
		data, _, err := zs.conn.Get(cleanupCtx, childPath)
		if err != nil {
			log.Warn(fmt.Sprintf("Failed to get first locks node %v (may have just ended): %v", childPath, err))
			return nil, errToReturn
		}

		log.Warn(fmt.Sprintf("------ Most likely blocking lock: %v\n%v", childPath, string(data)))
		return nil, errToReturn
	}

	// Remove the root prefix from the file. So when we delete it,
	// it's a relative file.
	nodePath = nodePath[len(zs.root):]
	return &zkLockDescriptor{
		zs:       zs,
		nodePath: nodePath,
	}, nil
}

// Check is part of the topo.LockDescriptor interface.
func (ld *zkLockDescriptor) Check(ctx context.Context) error {
	// TODO(alainjobart): check the connection has not been interrupted.
	// We'd lose the ephemeral node in case of a session loss.
	return nil
}

// Unlock is part of the topo.LockDescriptor interface.
func (ld *zkLockDescriptor) Unlock(ctx context.Context) error {
	return ld.zs.Delete(ctx, ld.nodePath, nil)
}
