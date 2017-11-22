/*
Copyright 2017 Google Inc.

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

	log "github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

// This file contains the lock management code for zktopo.Server.

// zsLockDescriptor implements topo.LockDescriptor.
type zsLockDescriptor struct {
	zs       *Server
	cell     string
	lockPath string
}

// Lock is part of the topo.Backend interface.
func (zs *Server) Lock(ctx context.Context, cell string, dirPath string) (topo.LockDescriptor, error) {
	conn, root, err := zs.connForCell(ctx, cell)
	if err != nil {
		return nil, err
	}

	// Lock paths end in a trailing slash to that when we create
	// sequential nodes, they are created as children, not siblings.
	locksDir := path.Join(root, dirPath, locksPath) + "/"

	// Create the locks path, possibly creating the parent.
	lockPath, err := CreateRecursive(ctx, conn, locksDir, []byte("lock"), zk.FlagSequence|zk.FlagEphemeral, zk.WorldACL(PermFile), 1)
	if err != nil {
		return nil, convertError(err)
	}

	err = obtainQueueLock(ctx, conn, lockPath)
	if err != nil {
		var errToReturn error
		switch err {
		case context.DeadlineExceeded:
			errToReturn = topo.ErrTimeout
		case context.Canceled:
			errToReturn = topo.ErrInterrupted
		default:
			errToReturn = fmt.Errorf("failed to obtain action lock: %v %v", lockPath, err)
		}

		// Regardless of the reason, try to cleanup.
		log.Warningf("Failed to obtain action lock: %v", err)
		conn.Delete(ctx, lockPath, -1)

		// Show the other locks in the directory
		dir := path.Dir(lockPath)
		children, _, err := conn.Children(ctx, dir)
		if err != nil {
			log.Warningf("Failed to get children of %v: %v", dir, err)
			return nil, errToReturn
		}

		if len(children) == 0 {
			log.Warningf("No other locks present, you may just try again now.")
			return nil, errToReturn
		}

		childPath := path.Join(dir, children[0])
		data, _, err := conn.Get(ctx, childPath)
		if err != nil {
			log.Warningf("Failed to get first locks node %v (may have just ended): %v", childPath, err)
			return nil, errToReturn
		}

		log.Warningf("------ Most likely blocking lock: %v\n%v", childPath, string(data))
		return nil, errToReturn
	}

	// Remove the root prefix from the file. So when we delete it,
	// it's a relative file.
	lockPath = lockPath[len(root):]
	return &zsLockDescriptor{
		zs:       zs,
		cell:     cell,
		lockPath: lockPath,
	}, nil
}

// Unlock is part of the topo.LockDescriptor interface.
func (ld *zsLockDescriptor) Unlock(ctx context.Context) error {
	return ld.zs.Delete(ctx, ld.cell, ld.lockPath, nil)
}
