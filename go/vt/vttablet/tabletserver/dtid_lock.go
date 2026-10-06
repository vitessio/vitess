/*
Copyright 2026 The Vitess Authors.

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

package tabletserver

import (
	"context"
	"sync"

	"vitess.io/vitess/go/vt/vterrors"
)

// dtidLocks serializes Prepare, CommitPrepared and RollbackPrepared for the
// same distributed transaction on this tablet.
//
// Prepare puts the connection in the prepared pool before it saves the redo
// log in a separate transaction. Without this lock, a RollbackPrepared for the
// same DTID could run in between: it would find no redo log to delete and roll
// back the pooled connection, and the Prepare would then save a redo log for a
// transaction that was already rolled back. Nothing resolves such a redo log,
// and the next redo prepares the transaction again.
type dtidLocks struct {
	mu   sync.Mutex
	held map[string]*dtidLock
}

type dtidLock struct {
	released chan struct{}
	waiters  int
}

func newDTIDLocks() *dtidLocks {
	return &dtidLocks{held: make(map[string]*dtidLock)}
}

// lock acquires the lock for dtid. It waits until the current holder releases
// it or ctx is done. The returned function releases the lock.
func (l *dtidLocks) lock(ctx context.Context, dtid string) (unlock func(), err error) {
	l.mu.Lock()
	for {
		held, ok := l.held[dtid]
		if !ok {
			break
		}
		held.waiters++
		l.mu.Unlock()
		select {
		case <-held.released:
		case <-ctx.Done():
			l.mu.Lock()
			held.waiters--
			l.mu.Unlock()
			return nil, vterrors.Wrapf(ctx.Err(), "waiting for another operation on distributed transaction %s", dtid)
		}
		l.mu.Lock()
		held.waiters--
	}
	own := &dtidLock{released: make(chan struct{})}
	l.held[dtid] = own
	l.mu.Unlock()
	return func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		delete(l.held, dtid)
		close(own.released)
	}, nil
}

// waiting returns the number of operations waiting for the lock on dtid.
func (l *dtidLocks) waiting(dtid string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if held, ok := l.held[dtid]; ok {
		return held.waiters
	}
	return 0
}
