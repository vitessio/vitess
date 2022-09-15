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
	"fmt"
	"path"
	"regexp"

	"context"

	"github.com/z-division/go-zookeeper/zk"

	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

// This file contains the lock management code for zktopo.Server.

var nodeUnderLockPath = regexp.MustCompile(fmt.Sprintf("%s/.+", locksPath))

// zkLockDescriptor implements topo.LockDescriptor.
type zkLockDescriptor struct {
	zs       *Server
	nodePath string
}

// Lock is part of the topo.Conn interface.
func (zs *Server) Lock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	return zs.lock(ctx, dirPath, contents)
}

// TryLock is part of the topo.Conn interface.
// TryLock provides exactly same functionality as 'Lock', the only difference is
// it tires its best to be unblocking call. Unblocking is the best effort though.
// If there is already lock exists for dirPath then TryLock
// unlike Lock will return immediately with error 'lock already exists'.
func (zs *Server) TryLock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	// We list all the entries under dirPath
	entries, err := zs.List(ctx, dirPath)
	if err != nil {
		// We need to return the right error codes, like
		// topo.ErrNoNode and topo.ErrInterrupted, and the
		// easiest way to do this is to return convertError(err).
		// It may lose some of the context, if this is an issue,
		// maybe logging the error would work here.
		return nil, convertError(err, dirPath)
	}

	// if there is a folder '/locks' with some entries in it then we can assume that keyspace already have a lock
	// throw error in this case
	for _, e := range entries {
		path := string(e.Key[:])
		if nodeUnderLockPath.MatchString(path) {
			return nil, topo.NewError(topo.NodeExists, fmt.Sprintf("lock already exists at path %s", dirPath))
		}

		// TODO: instead of list should I call listDir and assume /locks directory only exists if there is a lock
		// TODO: Should we check if all the children under lock is ephemeral
	}

	// everything is good lets acquire lock.
	return zs.lock(ctx, dirPath, contents)
}

// Lock is part of the topo.Conn interface.
func (zs *Server) lock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	// Lock paths end in a trailing slash so that when we create
	// sequential nodes, they are created as children, not siblings.
	locksDir := path.Join(zs.root, dirPath, locksPath) + "/"

	// Create the locks path, possibly creating the parent.
	nodePath, err := CreateRecursive(ctx, zs.conn, locksDir, []byte(contents), zk.FlagSequence|zk.FlagEphemeral, zk.WorldACL(PermFile), 1)
	if err != nil {
		return nil, convertError(err, locksDir)
	}

	err = obtainQueueLock(ctx, zs.conn, nodePath)
	if err != nil {
		var errToReturn error
		switch err {
		case context.DeadlineExceeded:
			errToReturn = topo.NewError(topo.Timeout, nodePath)
		case context.Canceled:
			errToReturn = topo.NewError(topo.Interrupted, nodePath)
		default:
			errToReturn = vterrors.Wrapf(err, "failed to obtain action lock: %v", nodePath)
		}

		// Regardless of the reason, try to cleanup.
		log.Warningf("Failed to obtain action lock: %v", err)

		if err := zs.conn.Delete(ctx, nodePath, -1); err != nil {
			log.Warningf("Failed to close connection :%v", err)
		}

		// Show the other locks in the directory
		dir := path.Dir(nodePath)
		children, _, err := zs.conn.Children(ctx, dir)
		if err != nil {
			log.Warningf("Failed to get children of %v: %v", dir, err)
			return nil, errToReturn
		}

		if len(children) == 0 {
			log.Warningf("No other locks present, you may just try again now.")
			return nil, errToReturn
		}

		childPath := path.Join(dir, children[0])
		data, _, err := zs.conn.Get(ctx, childPath)
		if err != nil {
			log.Warningf("Failed to get first locks node %v (may have just ended): %v", childPath, err)
			return nil, errToReturn
		}

		log.Warningf("------ Most likely blocking lock: %v\n%v", childPath, string(data))
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
