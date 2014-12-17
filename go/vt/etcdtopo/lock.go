// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcdtopo

import (
	"fmt"
	"path"
	"strconv"

	"github.com/coreos/go-etcd/etcd"
	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

const (
	lockFilename = "_Lock"

	// We can't use "" as the magic value for an un-held lock, since the etcd
	// client library doesn't support CAS with an empty prevValue.
	openLockContents = "open"
)

func initLockFile(client Client, dirPath string) error {
	_, err := client.Set(path.Join(dirPath, lockFilename), openLockContents, 0)
	return convertError(err)
}

// lock implements a simple distributed mutex lock on a directory in etcd.
// There used to be a lock module in etcd, and there will be again someday, but
// currently (as of v0.4.x) it has been removed due to lack of maintenance.
//
// See: https://github.com/coreos/etcd/blob/v0.4.6/Documentation/modules.md
//
// TODO(enisoc): Use etcd lock module if/when it exists.
//
// If mustExist is true, then before locking a directory, the file "_Lock" must
// already exist. This allows rejection of lock attempts on directories that
// don't exist yet, since otherwise etcd would automatically create parent
// directories. That means any directory that might be locked with mustExist
// should have a _Lock file created with initLockFile() as soon as the directory
// is created.
func lock(ctx context.Context, client Client, dirPath, contents string, mustExist bool) (string, error) {
	lockPath := path.Join(dirPath, lockFilename)
	var lockHeldErr error
	if mustExist {
		lockHeldErr = topo.ErrBadVersion
	} else {
		lockHeldErr = topo.ErrNodeExists
	}

	for {
		// Check ctx.Done before the each attempt, so the entire function is a no-op
		// if it's called with a Done context.
		select {
		case <-ctx.Done():
			return "", convertError(ctx.Err())
		default:
		}

		var resp *etcd.Response
		var err error
		if mustExist {
			// CAS will fail if the lock file isn't empty.
			resp, err = client.CompareAndSwap(lockPath, contents, 0, /* ttl */
				openLockContents /* prevValue */, 0 /* prevIndex */)
		} else {
			// Create will fail if the lock file already exists.
			resp, err = client.Create(lockPath, contents, 0 /* ttl */)
		}
		if err == nil {
			if resp.Node == nil {
				return "", ErrBadResponse
			}

			// We got the lock. The index of the lock file can be used to
			// verify during unlock() that we only delete our own lock.
			// Add the index at the end of the lockPath to form the actionPath.
			lockID := strconv.FormatUint(resp.Node.ModifiedIndex, 10)
			return path.Join(lockPath, lockID), nil
		}

		// If it fails for any reason other than lockHeldErr
		// (meaning the lock is already held), then just give up.
		if err = convertError(err); err != lockHeldErr {
			return "", err
		}

		// The lock is already being held.
		// Wait for the lock file to be deleted, then try again.
		if err = waitForLock(ctx, client, lockPath, resp.EtcdIndex+1, mustExist); err != nil {
			return "", err
		}
	}
}

// unlock releases a lock acquired by lock() on the given directory.
// The string returned by lock() should be passed as the actionPath.
//
// mustExist specifies whether the lock was acquired with mustExist.
func unlock(client Client, dirPath, actionPath string, mustExist bool) error {
	lockID := path.Base(actionPath)
	lockPath := path.Join(dirPath, lockFilename)

	// Sanity check.
	if checkPath := path.Join(lockPath, lockID); checkPath != actionPath {
		return fmt.Errorf("unlock: actionPath doesn't match directory being unlocked: %q != %q", actionPath, checkPath)
	}

	// Delete the node only if it belongs to us (the index matches).
	prevIndex, err := strconv.ParseUint(lockID, 10, 64)
	if err != nil {
		return fmt.Errorf("unlock: can't parse lock ID (%v) in actionPath (%v): %v", lockID, actionPath, err)
	}
	if mustExist {
		_, err = client.CompareAndSwap(lockPath, "" /* value */, 0, /* ttl */
			"" /* prevValue */, prevIndex)
	} else {
		_, err = client.CompareAndDelete(lockPath, "" /* prevValue */, prevIndex)
	}
	if err != nil {
		return convertError(err)
	}

	return nil
}

// waitForLock will start a watch on the lockPath and return nil iff the watch
// returns an event saying the file was deleted. The waitIndex should be one
// plus the index at which you last found that the lock was held, to ensure that
// no delete actions are missed.
//
// mustExist specifies whether lock() was called with mustExist.
func waitForLock(ctx context.Context, client Client, lockPath string, waitIndex uint64, mustExist bool) error {
	watch := make(chan *etcd.Response)
	stop := make(chan bool)
	defer close(stop)

	// Watch() will loop indefinitely, sending all updates starting at waitIndex
	// to the watch chan until stop is closed, or an error occurs.
	watchErr := make(chan error, 1)
	go func() {
		_, err := client.Watch(lockPath, waitIndex, false /* recursive */, watch, stop)
		watchErr <- err
	}()

	for {
		select {
		case <-ctx.Done():
			return convertError(ctx.Err())
		case err := <-watchErr:
			return convertError(err)
		case resp := <-watch:
			if mustExist {
				if resp.Node != nil && resp.Node.Value == "" {
					return nil
				}
			} else {
				if resp.Action == "delete" {
					return nil
				}
			}
		}
	}
}

// LockSrvShardForAction implements topo.Server.
func (s *Server) LockSrvShardForAction(ctx context.Context, cellName, keyspace, shard, contents string) (string, error) {
	cell, err := s.getCell(cellName)
	if err != nil {
		return "", err
	}

	return lock(ctx, cell.Client, srvShardDirPath(keyspace, shard), contents,
		false /* mustExist */)
}

// UnlockSrvShardForAction implements topo.Server.
func (s *Server) UnlockSrvShardForAction(cellName, keyspace, shard, actionPath, results string) error {
	log.Infof("results of %v: %v", actionPath, results)

	cell, err := s.getCell(cellName)
	if err != nil {
		return err
	}

	return unlock(cell.Client, srvShardDirPath(keyspace, shard), actionPath,
		false /* mustExist */)
}

// LockKeyspaceForAction implements topo.Server.
func (s *Server) LockKeyspaceForAction(ctx context.Context, keyspace, contents string) (string, error) {
	return lock(ctx, s.getGlobal(), keyspaceDirPath(keyspace), contents,
		true /* mustExist */)
}

// UnlockKeyspaceForAction implements topo.Server.
func (s *Server) UnlockKeyspaceForAction(keyspace, actionPath, results string) error {
	log.Infof("results of %v: %v", actionPath, results)

	return unlock(s.getGlobal(), keyspaceDirPath(keyspace), actionPath,
		true /* mustExist */)
}

// LockShardForAction implements topo.Server.
func (s *Server) LockShardForAction(ctx context.Context, keyspace, shard, contents string) (string, error) {
	return lock(ctx, s.getGlobal(), shardDirPath(keyspace, shard), contents,
		true /* mustExist */)
}

// UnlockShardForAction implements topo.Server.
func (s *Server) UnlockShardForAction(keyspace, shard, actionPath, results string) error {
	log.Infof("results of %v: %v", actionPath, results)

	return unlock(s.getGlobal(), shardDirPath(keyspace, shard), actionPath,
		true /* mustExist */)
}
