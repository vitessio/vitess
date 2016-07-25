// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcdtopo

import (
	"flag"
	"fmt"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/coreos/go-etcd/etcd"
	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

const lockFilename = "_Lock"

var (
	lockTTL       = flag.Duration("etcd_lock_ttl", 30*time.Second, "TTL for etcd locks to be released if heartbeat stops")
	lockHeartbeat = flag.Int("etcd_lock_heartbeat", 3, "number of times per lock TTL period to send keep-alive heartbeat")
)

// lockManager remembers currently held locks.
// Adding a lock starts a goroutine to send keep-alive heartbeats.
// We use etcd's TTL feature, which will expire (delete) the lock file
// if we fail to refresh it by resetting the TTL. This prevents locks
// from being orphaned if a process dies while holding the lock.
// Removing a lock stops the heartbeat goroutine and releases the lock.
type lockManager struct {
	sync.Mutex

	nextID uint64

	// locks is a map from lock ID to cancel func for that lock.
	locks map[uint64]func() error
}

var locks = &lockManager{locks: make(map[uint64]func() error)}

func (lm *lockManager) add(client Client, node *etcd.Node) (uint64, chan error) {
	stop := make(chan struct{})
	done := make(chan error)

	lm.Lock()
	id := lm.nextID
	lm.nextID++
	lm.locks[id] = func() error {
		close(stop)
		return <-done
	}
	lm.Unlock()

	lockPath := node.Key
	contents := node.Value
	version := node.ModifiedIndex

	// Start heartbeat goroutine for this lock.
	go func() {
		// Perform heartbeat at some fraction of the TTL period.
		ttl := uint64(*lockTTL / time.Second)
		period := *lockTTL / time.Duration(*lockHeartbeat)
		timer := time.NewTimer(period)
		defer timer.Stop()

		for {
			select {
			case <-stop:
				// Release the lock.
				_, err := client.CompareAndDelete(lockPath, "" /* prevValue */, version)
				done <- convertError(err)
				return
			case <-timer.C:
				// Refresh lock TTL.
				resp, err := client.CompareAndSwap(lockPath, contents, ttl, "" /* prevValue */, version)
				if err != nil {
					// We lost the lock.
					done <- convertError(err)
					return
				}
				// Save the new ModifiedIndex so our next CompareAndSwap
				// can use it to ensure we still own the lock.
				version = resp.Node.ModifiedIndex
				timer.Reset(period)
			}
		}
	}()

	return id, done
}

func (lm *lockManager) remove(id uint64) error {
	lm.Lock()
	cancel, ok := lm.locks[id]
	delete(lm.locks, id)
	lm.Unlock()

	if !ok {
		return fmt.Errorf("lockID doesn't exist: %v", id)
	}
	return cancel()
}

// lock implements a simple distributed mutex lock on a directory in etcd.
// There used to be a lock module in etcd, and there will be again someday, but
// currently (as of v0.4.x) it has been removed due to lack of maintenance.
//
// See: https://github.com/coreos/etcd/blob/v0.4.6/Documentation/modules.md
//
// TODO(enisoc): Use etcd lock module if/when it exists.
//
// If mustExist is true, then lock attempts on directories that don't exist yet
// will be rejected. This requires an extra round-trip to check the directory.
// Otherwise, etcd would automatically create parent directories and the lock
// would succeed regardless of whether the parent existed before.
//
// When mustExist is true, there is a race condition if a directory is deleted
// between the existence check and the creation of the lock. If that happens,
// the lock will recreate the directory. We accept this possibility for now
// because the main purpose of mustExist is to fail-fast when trying to lock an
// entity (e.g. a keyspace) that never existed. It's not a goal of this feature
// to ensure that lock attempts are correctly rejected at the precise moment
// when an existing keyspace is deleted.
//
// The benefit of the above trade-off is that implementing lock TTL and
// heartbeat becomes much simpler. We can use etcd's node TTL to delete the lock
// file if we fail to refresh it with a heartbeat.
func lock(ctx context.Context, client Client, dirPath, contents string, mustExist bool) (string, error) {
	lockPath := path.Join(dirPath, lockFilename)
	var resp *etcd.Response
	var err error

	for {
		// Check ctx.Done before the each attempt, so the entire function is a no-op
		// if it's called with a Done context.
		select {
		case <-ctx.Done():
			return "", convertError(ctx.Err())
		default:
		}

		if mustExist {
			// Verify that the parent directory exists.
			if _, err = client.Get(dirPath, false /* sort */, false /* recursive */); err != nil {
				return "", topo.ErrNoNode
			}
		}

		// Create will fail if the lock file already exists.
		resp, err = client.Create(lockPath, contents, uint64(*lockTTL/time.Second))
		if err == nil {
			if resp.Node == nil {
				return "", ErrBadResponse
			}

			// We got the lock. Start a heartbeat goroutine.
			lockID, _ := locks.add(client, resp.Node)

			// Make an actionPath by appending the lockID.
			return fmt.Sprintf("%v/%v", dirPath, lockID), nil
		}

		// If it fails for any reason other than ErrNodeExists
		// (meaning the lock is already held), then just give up.
		if topoErr := convertError(err); topoErr != topo.ErrNodeExists {
			return "", topoErr
		}
		etcdErr, ok := err.(*etcd.EtcdError)
		if !ok {
			return "", fmt.Errorf("error from etcd client has wrong type: got %#v, want %T", err, etcdErr)
		}

		// The lock is already being held.
		// Wait for the lock file to be deleted, then try again.
		if err = waitForLock(ctx, client, lockPath, etcdErr.Index+1); err != nil {
			return "", err
		}
	}
}

// unlock releases a lock acquired by lock() on the given directory.
// The string returned by lock() should be passed as the actionPath.
func unlock(dirPath, actionPath string) error {
	lockIDStr := path.Base(actionPath)

	// Sanity check.
	if checkPath := path.Join(dirPath, lockIDStr); checkPath != actionPath {
		return fmt.Errorf("unlock: actionPath doesn't match directory being unlocked: %q != %q", actionPath, checkPath)
	}

	// Delete the node only if it belongs to us (the index matches).
	lockID, err := strconv.ParseUint(lockIDStr, 10, 64)
	if err != nil {
		return fmt.Errorf("unlock: can't parse lock ID (%v) in actionPath (%v): %v", lockID, actionPath, err)
	}
	return locks.remove(lockID)
}

// waitForLock will start a watch on the lockPath and return nil iff the watch
// returns an event saying the file was deleted. The waitIndex should be one
// plus the index at which you last found that the lock was held, to ensure that
// no delete actions are missed.
func waitForLock(ctx context.Context, client Client, lockPath string, waitIndex uint64) error {
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
			if resp.Action == "compareAndDelete" || resp.Action == "expire" {
				return nil
			}
		}
	}
}

// LockKeyspaceForAction implements topo.Server.
func (s *Server) LockKeyspaceForAction(ctx context.Context, keyspace, contents string) (string, error) {
	return lock(ctx, s.getGlobal(), keyspaceDirPath(keyspace), contents,
		true /* mustExist */)
}

// UnlockKeyspaceForAction implements topo.Server.
func (s *Server) UnlockKeyspaceForAction(ctx context.Context, keyspace, actionPath, results string) error {
	log.Infof("results of %v: %v", actionPath, results)
	return unlock(keyspaceDirPath(keyspace), actionPath)
}

// LockShardForAction implements topo.Server.
func (s *Server) LockShardForAction(ctx context.Context, keyspace, shard, contents string) (string, error) {
	return lock(ctx, s.getGlobal(), shardDirPath(keyspace, shard), contents,
		true /* mustExist */)
}

// UnlockShardForAction implements topo.Server.
func (s *Server) UnlockShardForAction(ctx context.Context, keyspace, shard, actionPath, results string) error {
	log.Infof("results of %v: %v", actionPath, results)
	return unlock(shardDirPath(keyspace, shard), actionPath)
}
