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

package etcd2topo

import (
	"context"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/spf13/pflag"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vterrors"
)

var leaseTTL = 30 // This is the default used for all non-named locks

func init() {
	for _, cmd := range topo.FlagBinaries {
		servenv.OnParseFor(cmd, registerEtcd2TopoLockFlags)
	}
}

func registerEtcd2TopoLockFlags(fs *pflag.FlagSet) {
	utils.SetFlagIntVar(fs, &leaseTTL, "topo-etcd-lease-ttl", leaseTTL, "Lease TTL for locks and leader election. The client will use KeepAlive to keep the lease going.")
}

// newUniqueEphemeralKV creates a new file in the provided directory.
// It is linked to the Lease.
// Errors returned are converted to topo errors.
func (s *Server) newUniqueEphemeralKV(ctx context.Context, cli *clientv3.Client, leaseID clientv3.LeaseID, nodePath string, contents string) (string, int64, error) {
	// Use the lease ID as the file name, so it's guaranteed unique.
	newKey := fmt.Sprintf("%v/%v", nodePath, leaseID)

	// Only create a new file if it doesn't exist already
	// (version = 0), to avoid two processes using the
	// same file name. Since we use the lease ID, this should never happen.
	txnresp, err := cli.Txn(ctx).
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

			deleteCtx, deleteCancel := context.WithTimeout(context.WithoutCancel(ctx), topo.RemoteOperationTimeout)
			defer deleteCancel()
			if _, err := cli.Delete(deleteCtx, newKey); err != nil {
				log.Error(fmt.Sprintf("cli.Delete(%v) failed: %v", newKey, err))
			}
		}
		return "", 0, convertError(err, newKey)
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
func (s *Server) waitOnLastRev(ctx context.Context, cli *clientv3.Client, nodePath string, revision int64) (bool, error) {
	// Get the keys that are blocking us, if any.
	opts := append(clientv3.WithLastRev(), clientv3.WithMaxModRev(revision-1))
	lastKey, err := cli.Get(ctx, nodePath+"/", opts...)
	if err != nil {
		return false, convertError(err, nodePath)
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
	wc := cli.Watch(ctx, key, clientv3.WithRev(revision))
	if wc == nil {
		return false, vterrors.Errorf(vtrpc.Code_INTERNAL, "Watch failed")
	}

	select {
	case <-ctx.Done():
		return false, convertError(ctx.Err(), nodePath)
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

// etcdLockDescriptor implements topo.LockDescriptor.
type etcdLockDescriptor struct {
	s       *Server
	leaseID clientv3.LeaseID
	// stopKeepAlive ends the lease's KeepAlive.
	stopKeepAlive context.CancelFunc
}

// TryLock is part of the topo.Conn interface.
func (s *Server) TryLock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	// We list all the entries under dirPath
	entries, err := s.ListDir(ctx, dirPath, true)
	if err != nil {
		// We need to return the right error codes, like
		// topo.ErrNoNode and topo.ErrInterrupted, and the
		// easiest way to do this is to return convertError(err).
		// It may lose some of the context, if this is an issue,
		// maybe logging the error would work here.
		return nil, convertError(err, dirPath)
	}

	// If there is a folder '/locks' with some entries in it then we can assume that someone else already has a lock.
	// Throw error in this case
	for _, e := range entries {
		if e.Name == locksPath && e.Type == topo.TypeDirectory && e.Ephemeral {
			return nil, topo.NewError(topo.NodeExists, "lock already exists at path "+dirPath)
		}
	}

	// everything is good let's acquire the lock.
	return s.lock(ctx, dirPath, contents, leaseTTL, true)
}

// Lock is part of the topo.Conn interface.
func (s *Server) Lock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	// We list the directory first to make sure it exists.
	if _, err := s.ListDir(ctx, dirPath, false /*full*/); err != nil {
		// We need to return the right error codes, like
		// topo.ErrNoNode and topo.ErrInterrupted, and the
		// easiest way to do this is to return convertError(err).
		// It may lose some of the context, if this is an issue,
		// maybe logging the error would work here.
		return nil, convertError(err, dirPath)
	}

	return s.lock(ctx, dirPath, contents, leaseTTL, true)
}

// LockWithTTL is part of the topo.Conn interface.
func (s *Server) LockWithTTL(ctx context.Context, dirPath, contents string, ttl time.Duration) (topo.LockDescriptor, error) {
	// We list the directory first to make sure it exists.
	if _, err := s.ListDir(ctx, dirPath, false /*full*/); err != nil {
		// We need to return the right error codes, like
		// topo.ErrNoNode and topo.ErrInterrupted, and the
		// easiest way to do this is to return convertError(err).
		// It may lose some of the context, if this is an issue,
		// maybe logging the error would work here.
		return nil, convertError(err, dirPath)
	}

	return s.lock(ctx, dirPath, contents, int(ttl.Seconds()), false)
}

// LockName is part of the topo.Conn interface.
func (s *Server) LockName(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	return s.lock(ctx, dirPath, contents, int(topo.NamedLockTTL.Seconds()), false)
}

// lock is used by both Lock() and primary election. With untilUnlock, the
// lease's KeepAlive runs until Unlock. Otherwise it runs only as long as ctx, so
// that a lock with an explicit TTL expires ttl after its last check.
func (s *Server) lock(ctx context.Context, nodePath, contents string, ttl int, untilUnlock bool) (topo.LockDescriptor, error) {
	nodePath = path.Join(s.root, nodePath, locksPath)

	// Get a lease, set its KeepAlive.
	lease, err := s.cli.Grant(ctx, int64(ttl))
	if err != nil {
		return nil, convertError(err, nodePath)
	}
	// topo.Server bounds ctx to the acquisition, so a KeepAlive that runs until
	// Unlock must outlive it.
	kaParent := ctx
	if untilUnlock {
		kaParent = context.WithoutCancel(ctx)
	}
	kaCtx, stopKeepAlive := context.WithCancel(kaParent)
	leaseKA, err := s.cli.KeepAlive(kaCtx, lease.ID)
	if err != nil {
		stopKeepAlive()
		return nil, convertError(err, nodePath)
	}
	if untilUnlock {
		go s.keepAlive(kaCtx, s.cli, lease.ID, nodePath, leaseKA)
	} else {
		go func() {
			// Drain the lease keepAlive channel, we're not
			// interested in its contents.
			for range leaseKA {
			}
		}()
	}

	// Create an ephemeral node in the locks directory.
	key, revision, err := s.newUniqueEphemeralKV(ctx, s.cli, lease.ID, nodePath, contents)
	if err != nil {
		stopKeepAlive()
		return nil, err
	}

	// Wait until all older nodes in the locks directory are gone.
	for {
		done, err := s.waitOnLastRev(ctx, s.cli, nodePath, revision)
		if err != nil {
			// We had an error waiting on the last node.
			// Revoke our lease, this will delete the file.
			stopKeepAlive()
			revokeCtx, revokeCancel := context.WithTimeout(context.WithoutCancel(ctx), topo.RemoteOperationTimeout)
			if _, rerr := s.cli.Revoke(revokeCtx, lease.ID); rerr != nil {
				log.Warn(fmt.Sprintf("Revoke(%d) failed, may have left %v behind: %v", lease.ID, key, rerr))
			}
			revokeCancel()
			return nil, err
		}
		if done {
			// No more older nodes, we're it!
			return &etcdLockDescriptor{
				s:             s,
				leaseID:       lease.ID,
				stopKeepAlive: stopKeepAlive,
			}, nil
		}
	}
}

// keepAliveRetryDelay is how long keepAlive waits after a failed attempt to
// renew a lease whose KeepAlive the etcd client gave up on.
const keepAliveRetryDelay = time.Second

// keepAlive drains leaseKA until ctx ends. The etcd client closes leaseKA when
// it gets no response for a whole TTL, for example during an etcd outage, but
// the lease can outlive the outage: a new etcd leader gives every lease a fresh
// TTL. keepAlive then renews the same lease and starts a new KeepAlive on it,
// until etcd reports the lease gone. It never grants a new lease: a holder that
// lost its lease no longer holds the lock.
func (s *Server) keepAlive(ctx context.Context, cli *clientv3.Client, leaseID clientv3.LeaseID, nodePath string, leaseKA <-chan *clientv3.LeaseKeepAliveResponse) {
	for {
		for range leaseKA {
		}
		for {
			_, err := cli.KeepAliveOnce(ctx, leaseID)
			// An etcd error can arrive after ctx ended or the client closed.
			if ctx.Err() != nil || cli.Ctx().Err() != nil {
				return
			}
			if err == nil {
				break
			}
			if errors.Is(err, rpctypes.ErrLeaseNotFound) {
				log.Warn(fmt.Sprintf("etcd2topo: lease %x of the lock in %v expired before the lock was released", leaseID, nodePath))
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(keepAliveRetryDelay):
			}
		}
		var err error
		if leaseKA, err = cli.KeepAlive(ctx, leaseID); err != nil {
			// Only a closed client or an ended ctx fail it.
			return
		}
	}
}

// Check is part of the topo.LockDescriptor interface.
// We use KeepAliveOnce to make sure the lease is still active and well.
func (ld *etcdLockDescriptor) Check(ctx context.Context) error {
	_, err := ld.s.cli.KeepAliveOnce(ctx, ld.leaseID)
	if err != nil {
		return convertError(err, "lease")
	}
	return nil
}

// Unlock is part of the topo.LockDescriptor interface.
func (ld *etcdLockDescriptor) Unlock(ctx context.Context) error {
	// Stop the KeepAlive first, so that it does not take the revoked lease for
	// one that expired.
	ld.stopKeepAlive()
	_, err := ld.s.cli.Revoke(ctx, ld.leaseID)
	if err != nil {
		return convertError(err, "lease")
	}
	return nil
}
