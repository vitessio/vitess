// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcd2topo

import (
	"flag"
	"fmt"
	"path"
	"strconv"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

var (
	leaseTTL = flag.Int("topo_etcd_lease_ttl", 30, "Lease TTL for locks and master election. The client will use KeepAlive to keep the lease going.")
)

// newUniqueEphemeralKV creates a new file in the provided directory.
// It is linked to the Lease.
// Errors returned are converted to topo errors.
func (s *Server) newUniqueEphemeralKV(ctx context.Context, leaseID clientv3.LeaseID, nodePath string, contents string) (string, int64, error) {
	// Use the lease ID as the file name, so it's guaranteed unique.
	newKey := fmt.Sprintf("%v/%v", nodePath, leaseID)

	// Only create a new file if it doesn't exist already
	// (version = 0), to avoid two processes using the
	// same file name. Since we use the lease ID, this should never happen.
	txnresp, err := s.global.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(newKey), "=", 0)).
		Then(clientv3.OpPut(newKey, contents, clientv3.WithLease(leaseID))).
		Commit()
	if err != nil {
		if err == context.Canceled || err == context.DeadlineExceeded {
			// Our context was canceled as we were sending
			// a creation request. We don't know if it
			// succeeded or not. In any case, let's try to
			// delete the node, so we don't leave an orphan
			// node behind for *leaseTTL time.
			s.global.cli.Delete(context.Background(), newKey)
		}
		return "", 0, convertError(err)
	}
	if !txnresp.Succeeded {
		// The key already exists, that should not happen.
		return "", 0, ErrBadResponse
	}
	// The key was created.
	return newKey, txnresp.Header.Revision, nil
}

// waitOnLastRev waits on all revisions of the files in the provided
// directory that have revisions smaller than the provided revision.
// It returns true only if there is no more other older files.
func (s *Server) waitOnLastRev(ctx context.Context, nodePath string, revision int64) (bool, error) {
	// Get the keys that are blocking us, if any.
	opts := append(clientv3.WithLastRev(), clientv3.WithMaxModRev(revision-1))
	lastKey, err := s.global.cli.Get(ctx, nodePath+"/", opts...)
	if err != nil {
		return false, convertError(err)
	}
	if len(lastKey.Kvs) == 0 {
		// No older key, we're done waiting.
		return true, nil
	}

	// Wait for release on blocking key. Cancel the watch when we
	// exit this function.
	key := string(lastKey.Kvs[0].Key)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wc := s.global.cli.Watch(ctx, key, clientv3.WithRev(revision))
	if wc == nil {
		return false, fmt.Errorf("Watch failed")
	}

	select {
	case <-ctx.Done():
		return false, convertError(ctx.Err())
	case wresp := <-wc:
		for _, ev := range wresp.Events {
			if ev.Type == mvccpb.DELETE {
				// There might still be older keys,
				// but not this one.
				return false, nil
			}
		}
	}

	// The Watch stopped, we're not sure if there are more items.
	return false, nil
}

func (s *Server) lock(ctx context.Context, nodePath string, contents string) (string, error) {
	// Get a lease, set its KeepAlive.
	lease, err := s.global.cli.Grant(ctx, int64(*leaseTTL))
	if err != nil {
		return "", convertError(err)
	}
	leaseKA, err := s.global.cli.KeepAlive(ctx, lease.ID)
	if err != nil {
		return "", convertError(err)
	}
	go func() {
		for range leaseKA {
		}
	}()

	// Create an ephemeral node in the locks directory.
	_, revision, err := s.newUniqueEphemeralKV(ctx, lease.ID, nodePath, contents)
	if err != nil {
		return "", err
	}
	key := path.Join(nodePath, fmt.Sprintf("%v", lease.ID))

	// Wait until all older nodes in the locks directory are gone.
	for {
		done, err := s.waitOnLastRev(ctx, nodePath, revision)
		if err != nil {
			// We had an error waiting on the last node.
			// Revoke our lease, this will delete the file.
			if _, rerr := s.global.cli.Revoke(context.Background(), lease.ID); rerr != nil {
				log.Warningf("Revoke(%d) failed, may have left %v behind: %v", lease.ID, key, rerr)
			}
			return "", err
		}
		if done {
			// No more older nodes, we're it!
			return key, nil
		}
	}
}

// unlock releases a lock acquired by lock() on the given directory.
// The string returned by lock() should be passed as the actionPath.
func (s *Server) unlock(ctx context.Context, dirPath, actionPath string) error {
	leaseIDStr := path.Base(actionPath)

	// Sanity check.
	if checkPath := path.Join(dirPath, leaseIDStr); checkPath != actionPath {
		return fmt.Errorf("unlock: actionPath doesn't match directory being unlocked: %q != %q", actionPath, checkPath)
	}

	i, err := strconv.ParseInt(leaseIDStr, 10, 64)
	if err != nil {
		return fmt.Errorf("unlock: invalid leaseID %v: %v", leaseIDStr, err)
	}
	leaseID := clientv3.LeaseID(i)

	// Revoke the lease, will delete the node.
	_, err = s.global.cli.Revoke(ctx, leaseID)
	if err != nil {
		return convertError(err)
	}
	return nil
}

// LockKeyspaceForAction implements topo.Server.
func (s *Server) LockKeyspaceForAction(ctx context.Context, keyspace, contents string) (string, error) {
	// Check the keyspace exists first.
	keyspacePath := path.Join(keyspacesPath, keyspace, topo.KeyspaceFile)
	_, _, err := s.Get(ctx, topo.GlobalCell, keyspacePath)
	if err != nil {
		return "", err
	}

	return s.lock(ctx, path.Join(s.global.root, keyspacesPath, keyspace, locksPath), contents)
}

// UnlockKeyspaceForAction implements topo.Server.
func (s *Server) UnlockKeyspaceForAction(ctx context.Context, keyspace, actionPath, results string) error {
	log.Infof("results of %v: %v", actionPath, results)
	return s.unlock(ctx, path.Join(s.global.root, keyspacesPath, keyspace, locksPath), actionPath)
}

// LockShardForAction implements topo.Server.
func (s *Server) LockShardForAction(ctx context.Context, keyspace, shard, contents string) (string, error) {
	shardPath := path.Join(keyspacesPath, keyspace, shardsPath, shard, topo.ShardFile)
	_, _, err := s.Get(ctx, topo.GlobalCell, shardPath)
	if err != nil {
		return "", err
	}

	return s.lock(ctx, path.Join(s.global.root, keyspacesPath, keyspace, shardsPath, shard, locksPath), contents)
}

// UnlockShardForAction implements topo.Server.
func (s *Server) UnlockShardForAction(ctx context.Context, keyspace, shard, actionPath, results string) error {
	log.Infof("results of %v: %v", actionPath, results)
	return s.unlock(ctx, path.Join(s.global.root, keyspacesPath, keyspace, shardsPath, shard, locksPath), actionPath)
}
