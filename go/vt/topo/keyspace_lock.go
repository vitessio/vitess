/*
Copyright 2024 The Vitess Authors.

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

package topo

import (
	"context"
	"path"
	"time"

	"vitess.io/vitess/go/vt/vterrors"
)

const (
	MaxKeyspaceLockLeaseTTL = 10 * time.Minute
	leaseRenewalInterval    = 10 * time.Second
)

type keyspaceLock struct {
	keyspace string
}

var _ iTopoLock = (*keyspaceLock)(nil)

func (s *keyspaceLock) Type() string {
	return "keyspace"
}

func (s *keyspaceLock) ResourceName() string {
	return s.keyspace
}

func (s *keyspaceLock) Path() string {
	return path.Join(KeyspacesPath, s.keyspace)
}

// LockKeyspace will lock the keyspace, and return:
// - a context with a locksInfo structure for future reference.
// - an unlock method
// - an error if anything failed.
func (ts *Server) LockKeyspace(ctx context.Context, keyspace, action string) (context.Context, func(*error), error) {
	return ts.internalLock(ctx, &keyspaceLock{
		keyspace: keyspace,
	}, action, true)
}

// CheckKeyspaceLocked can be called on a context to make sure we have the lock
// for a given keyspace.
func CheckKeyspaceLocked(ctx context.Context, keyspace string) error {
	return checkLocked(ctx, &keyspaceLock{
		keyspace: keyspace,
	})
}

// LockKeyspaceWithLeaseRenewal locks the keyspace and starts a goroutine which runs
// until the MaxKeyspaceLockLeaseTTL is reached -- exiting if the context is
// cancelled, the unlock function is called, or an error is encountered -- refreshing
// the lock's lease every leaseRenewal until it ends.
// It returns a read-only error channel that you should regularly check to ensure that
// you have not lost the lock or encountered any other non-recoverable errors related
// to your lease.
func (ts *Server) LockKeyspaceWithLeaseRenewal(ctx context.Context, keyspace, action string) (context.Context, func(*error), <-chan error, error) {
	ksLock := &keyspaceLock{keyspace: keyspace}
	lockCtx, unlockF, err := ts.internalLock(ctx, ksLock, action, true)
	if err != nil {
		return nil, nil, nil, err
	}
	doneCh := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		renewAttempts := int(MaxKeyspaceLockLeaseTTL.Seconds() / leaseRenewalInterval.Seconds())
		for i := 0; i < renewAttempts; i++ {
			time.Sleep(leaseRenewalInterval)
			select {
			case <-lockCtx.Done():
				return
			case <-doneCh:
				return
			default:
				// Attempt to renew the lease.
				if err := checkLocked(lockCtx, ksLock); err != nil {
					errCh <- vterrors.Wrapf(err, "failed to renew keyspace %s lock lease", keyspace)
					return
				}
			}
		}
	}()
	// Add to the unlock function, closing our related channels first.
	newUnlockF := func(err *error) {
		close(doneCh)
		close(errCh)
		unlockF(err)
	}
	return lockCtx, newUnlockF, errCh, err
}
