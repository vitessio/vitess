/*
Copyright 2018 Google Inc.

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

// Package locker provides vitess level support for GET_LOCK
// and RELEASE_LOCK functionality.
package locker

import (
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// Locker is the service for locking funcionality.
type Locker struct {
	mu      sync.Mutex
	isOpen  bool
	timeout time.Duration
	locks   map[string]*lock
}

type lock struct {
	ctx      context.Context
	cancel   context.CancelFunc
	released chan struct{}
}

// NewLocker creates a new Locker.
func NewLocker(config tabletenv.TabletConfig) *Locker {
	return &Locker{
		timeout: time.Duration(config.TransactionTimeout * 1e9),
	}
}

// Open opens the Locker service.
func (lk *Locker) Open() {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	lk.locks = make(map[string]*lock)
	lk.isOpen = true
}

// Close closes the Locker service.
func (lk *Locker) Close() {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	lk.isOpen = false
	lk.locks = nil
}

// Lock obtains a lock of the specified name. The wait time is the minimum
// of the timeout or the timeout in the context (if any). On success the function
// returns true, and false otherwise.
func (lk *Locker) Lock(ctx context.Context, name string, timeout time.Duration) (bool, error) {
	if err := lk.verifyOpen(); err != nil {
		return false, err
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	for {
		l, ok := lk.newLock(name)
		if ok {
			return true, nil
		}
		select {
		case <-l.released:
			// retry
		case <-ctx.Done():
			return false, nil
		}
	}
}

func (lk *Locker) newLock(name string) (*lock, bool) {
	lk.mu.Lock()
	defer lk.mu.Unlock()

	// There exists a prior lock.
	if l, ok := lk.locks[name]; ok {
		return l, false
	}

	// No prior lock. Create one.
	ctx, cancel := context.WithTimeout(context.Background(), lk.timeout)
	l := &lock{
		ctx:      ctx,
		cancel:   cancel,
		released: make(chan struct{}),
	}
	lk.locks[name] = l

	go func() {
		<-l.ctx.Done()

		lk.mu.Lock()
		delete(lk.locks, name)
		lk.mu.Unlock()

		close(l.released)
	}()
	return nil, true
}

// Release releases the named lock. If no lock is held, the function is a no-op.
func (lk *Locker) Release(name string) error {
	if err := lk.verifyOpen(); err != nil {
		return err
	}

	lk.mu.Lock()
	l, ok := lk.locks[name]
	lk.mu.Unlock()
	if !ok {
		return nil
	}

	// Cancel the context, wait for release and return
	l.cancel()
	<-l.released
	return nil
}

func (lk *Locker) verifyOpen() error {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	if !lk.isOpen {
		return vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "cannot perform locking function, probably because this is not a master any more")
	}
	return nil
}
