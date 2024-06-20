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

// AutoRenewKeyspaceLockLease will renew the keyspace lock lease every renewTime
// in a goroutine until the maxTime is reached -- exiting early if the context
// is cancelled, the done channel is closed, or an error is encountered.
func AutoRenewKeyspaceLockLease(ctx context.Context, keyspace string, renewTime, maxTime time.Duration, doneCh <-chan struct{}, errCh chan error) {
	go func() {
		ksLock := &keyspaceLock{keyspace: keyspace}
		renewAttempts := int(maxTime.Seconds() / renewTime.Seconds())
		for i := 0; i < renewAttempts; i++ {
			time.Sleep(renewTime)
			select {
			case <-ctx.Done():
				return
			case <-doneCh:
				return
			default:
				// Attempt to renew lease.
				if err := checkLocked(ctx, ksLock); err != nil {
					errCh <- vterrors.Wrapf(err, "failed to renew keyspace %s lock lease", keyspace)
					return
				}
			}
		}
	}()
}
