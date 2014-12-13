// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcdtopo

import (
	"fmt"
	"path"
	"strconv"
	"time"

	"code.google.com/p/go.net/context"
	"github.com/coreos/go-etcd/etcd"
	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/topo"
)

const lockFilename = "_Lock"

// lock implements a simple distributed mutex lock on a directory in etcd.
// There used to be a lock module in etcd, and there will be again someday, but
// currently (as of v0.4.x) it has been removed due to lack of maintenance.
//
// See: https://github.com/coreos/etcd/blob/v0.4.6/Documentation/modules.md
//
// TODO(enisoc): Use etcd lock module if/when it exists.
func lock(ctx context.Context, client *etcd.Client, dirPath, contents string) (string, error) {
	lockPath := path.Join(dirPath, lockFilename)

	for {
		// Check ctx.Done before the each attempt, so the entire function is a no-op
		// if it's called with a Done context.
		select {
		case <-ctx.Done():
			return "", convertError(ctx.Err())
		default:
		}

		// Create will fail if the lock file already exists.
		resp, err := client.Create(lockPath, contents, 0 /* ttl */)
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

		// If it fails for any reason other than "node already exists"
		// (meaning the lock is already held), then just give up.
		if err = convertError(err); err != topo.ErrNodeExists {
			return "", err
		}

		// The lock is already being held.
		// Wait for the lock file to be deleted, then try again.
		if err = waitForLock(ctx, client, lockPath, resp.EtcdIndex+1); err != nil {
			return "", err
		}
	}
}

// unlock releases a lock acquired by lock() on the given directory.
// The string returned by lock() should be passed as the actionPath.
func unlock(client *etcd.Client, dirPath, actionPath string) error {
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
	_, err = client.CompareAndDelete(lockPath, "" /* prevValue */, prevIndex)
	if err != nil {
		return convertError(err)
	}

	return nil
}

// waitForLock will start a watch on the lockPath and return nil iff the watch
// returns an event saying the file was deleted. The waitIndex should be one
// plus the index at which you last found that the lock was held, to ensure that
// no delete actions are missed.
func waitForLock(ctx context.Context, client *etcd.Client, lockPath string, waitIndex uint64) error {
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
			if resp.Action == "delete" {
				return nil
			}
		}
	}
}

// topoContext applies the timeout and interrupted parameters used in the
// topo.Server API onto a Context. It returns the new context and a channel that
// the caller must close to free up resources associated with waiting on the
// passed-in interrupted channel. For example:
//
//   ctx, done := topoContext(context.TODO(), timeout, interrupted)
//   defer close(done)
//
// This will be unnecessary when topo.Server uses Context natively.
func topoContext(ctx context.Context, timeout time.Duration, interrupted chan struct{}) (context.Context, chan struct{}) {
	var cancel context.CancelFunc
	if timeout == 0 {
		ctx, cancel = context.WithCancel(ctx)
	} else {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	}
	done := make(chan struct{})

	go func() {
		select {
		case <-interrupted:
		case <-done:
		}
		cancel()
	}()

	return ctx, done
}

// LockSrvShardForAction implements topo.Server.
func (s *Server) LockSrvShardForAction(cellName, keyspace, shard, contents string, timeout time.Duration, interrupted chan struct{}) (string, error) {
	ctx, done := topoContext(context.TODO(), timeout, interrupted)
	defer close(done)

	cell, err := s.getCell(cellName)
	if err != nil {
		return "", err
	}

	return lock(ctx, cell.Client, srvShardDirPath(keyspace, shard), contents)
}

// UnlockSrvShardForAction implements topo.Server.
func (s *Server) UnlockSrvShardForAction(cellName, keyspace, shard, actionPath, results string) error {
	log.Infof("results of %v: %v", actionPath, results)

	cell, err := s.getCell(cellName)
	if err != nil {
		return err
	}

	return unlock(cell.Client, srvShardDirPath(keyspace, shard), actionPath)
}

// LockKeyspaceForAction implements topo.Server.
func (s *Server) LockKeyspaceForAction(keyspace, contents string, timeout time.Duration, interrupted chan struct{}) (string, error) {
	ctx, done := topoContext(context.TODO(), timeout, interrupted)
	defer close(done)

	return lock(ctx, s.getGlobal(), keyspaceDirPath(keyspace), contents)
}

// UnlockKeyspaceForAction implements topo.Server.
func (s *Server) UnlockKeyspaceForAction(keyspace, actionPath, results string) error {
	log.Infof("results of %v: %v", actionPath, results)

	return unlock(s.getGlobal(), keyspaceDirPath(keyspace), actionPath)
}

// LockShardForAction implements topo.Server.
func (s *Server) LockShardForAction(keyspace, shard, contents string, timeout time.Duration, interrupted chan struct{}) (string, error) {
	ctx, done := topoContext(context.TODO(), timeout, interrupted)
	defer close(done)

	return lock(ctx, s.getGlobal(), shardDirPath(keyspace, shard), contents)
}

// UnlockShardForAction implements topo.Server.
func (s *Server) UnlockShardForAction(keyspace, shard, actionPath, results string) error {
	log.Infof("results of %v: %v", actionPath, results)

	return unlock(s.getGlobal(), shardDirPath(keyspace, shard), actionPath)
}
