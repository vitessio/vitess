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

package grpctabletconn

import (
	"context"
	"errors"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var ErrPoolClosed = errors.New("stream pool is closed")

const (
	defaultMaxIdleStreams = 100
	createRetryBackoff    = 5 * time.Millisecond
)

const (
	stateNotInUse int32 = 0
	stateInUse    int32 = 1
	stateRemoved  int32 = -1
)

type poolEntry[T any] struct {
	state   atomic.Int32
	stream  T
	cancel  context.CancelFunc
	evicted atomic.Bool
}

type streamPool[T any] struct {
	// Shared entry list. Reads are lock-free via atomic.Value.Load().
	// Writes (add/remove) are serialized via listMu and copy the slice.
	entries atomic.Value // []*poolEntry[T]
	listMu  sync.Mutex

	// Direct handoff channel (unbuffered). Both put() and the creator
	// goroutine send on this; get() receives. The caller in get() races
	// between a returned stream and a newly created one.
	handoff chan *poolEntry[T]

	// Number of goroutines blocked waiting for an entry.
	waiters atomic.Int32

	maxSize     int32
	maxLifetime time.Duration

	create func() (T, context.CancelFunc, error)

	closed  atomic.Bool
	closeCh chan struct{}

	// Buffered channel that signals a creator goroutine to open a new stream.
	createCh chan struct{}
}

func newStreamPool[T any](maxSize int, maxLifetime time.Duration, create func() (T, context.CancelFunc, error)) *streamPool[T] {
	p := &streamPool[T]{
		handoff:     make(chan *poolEntry[T]),
		maxSize:     int32(maxSize),
		maxLifetime: maxLifetime,
		create:      create,
		closeCh:     make(chan struct{}),
		createCh:    make(chan struct{}, 1),
	}
	p.entries.Store(make([]*poolEntry[T], 0))
	go p.creator()
	return p
}

func (p *streamPool[T]) loadEntries() []*poolEntry[T] {
	return p.entries.Load().([]*poolEntry[T])
}

// get borrows an entry from the pool, creating one asynchronously if needed.
func (p *streamPool[T]) get(ctx context.Context) (*poolEntry[T], error) {
	if p.closed.Load() {
		return nil, ErrPoolClosed
	}

	// Fast path: scan shared list for a NOT_IN_USE entry (lock-free).
	entries := p.loadEntries()
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].state.CompareAndSwap(stateNotInUse, stateInUse) {
			if entries[i].evicted.Load() {
				p.discard(entries[i])
				continue
			}
			return entries[i], nil
		}
	}

	// Slow path: register as waiter, re-scan, then block on handoff.
	p.waiters.Add(1)
	defer p.waiters.Add(-1)

	// Re-scan to catch entries returned between the first scan and waiter registration.
	entries = p.loadEntries()
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].state.CompareAndSwap(stateNotInUse, stateInUse) {
			if entries[i].evicted.Load() {
				p.discard(entries[i])
				continue
			}
			if w := p.waiters.Load(); w > 1 {
				p.requestCreate()
			}
			return entries[i], nil
		}
	}

	// Signal the creator goroutine to open a new stream.
	p.requestCreate()

	// Block until handoff, context cancellation, or pool close.
	// Both put() and the creator goroutine send on the handoff channel,
	// so the caller races between a returned stream and a newly created one.
	//
	// If a scanner steals our handoff entry (CAS fails), we re-scan
	// the list before blocking again to avoid stalling on an empty channel.
	for {
		select {
		case entry := <-p.handoff:
			if entry.state.CompareAndSwap(stateNotInUse, stateInUse) {
				if entry.evicted.Load() {
					p.discard(entry)
					break // re-enter select
				}
				return entry, nil
			}
			// Scanner stole it — re-scan before blocking again.
			// No requestCreate() needed: the scanner that stole the
			// entry will eventually put() or discard() it, serving
			// this waiter via handoff or triggering a replacement.
			entries = p.loadEntries()
			for i := len(entries) - 1; i >= 0; i-- {
				if entries[i].state.CompareAndSwap(stateNotInUse, stateInUse) {
					if entries[i].evicted.Load() {
						p.discard(entries[i])
						continue
					}
					return entries[i], nil
				}
			}
		case <-p.closeCh:
			return nil, ErrPoolClosed
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// put returns an entry to the pool.
func (p *streamPool[T]) put(entry *poolEntry[T]) {
	if entry.evicted.Load() || p.closed.Load() {
		entry.state.Store(stateRemoved)
		entry.cancel()
		if p.waiters.Load() > 0 {
			p.requestCreate()
		}
		return
	}

	entry.state.Store(stateNotInUse)

	// Try to hand off to a waiting goroutine.
	//
	// We busy loop here, but this is fine - it should only take a few iterations
	// until a waiter will either steal the entry via CAS, or receive it via the handoff channel.
	// Or the waiters could all go away, in which case we stop trying to hand off and just leave the
	// entry in the shared list as NOT_IN_USE.
	for p.waiters.Load() > 0 {
		if entry.state.Load() != stateNotInUse {
			return // grabbed by a scanner
		}
		select {
		case p.handoff <- entry:
			return
		default:
			runtime.Gosched()
		}
	}
	// No waiters — entry is already in the shared list as NOT_IN_USE.
}

// discard marks a broken entry as REMOVED without modifying the shared list.
// The slot will be recycled in-place by the creator goroutine when a
// replacement is created.
func (p *streamPool[T]) discard(entry *poolEntry[T]) {
	entry.cancel()
	entry.state.Store(stateRemoved)

	if p.waiters.Load() > 0 {
		p.requestCreate()
	}
}

// softEvict marks an entry for eviction. If the entry is idle (NOT_IN_USE),
// it is discarded immediately. If active (IN_USE), put() will discard it
// when the caller returns it.
func (p *streamPool[T]) softEvict(entry *poolEntry[T]) {
	entry.evicted.Store(true)

	if entry.state.CompareAndSwap(stateNotInUse, stateInUse) {
		p.discard(entry)
		return
	}
	// IN_USE or REMOVED — put() or discard() will handle it.
}

// scheduleEviction sets a timer to soft-evict the entry after maxLifetime
// minus random jitter (up to 25%).
func (p *streamPool[T]) scheduleEviction(entry *poolEntry[T]) {
	if p.maxLifetime <= 0 {
		return
	}
	variance := time.Duration(rand.Int64N(int64(p.maxLifetime) / 4))
	lifetime := p.maxLifetime - variance
	time.AfterFunc(lifetime, func() { p.softEvict(entry) })
}

// add appends a new entry to the shared list and tries to hand it off to a waiter.
func (p *streamPool[T]) add(entry *poolEntry[T]) {
	p.listMu.Lock()
	old := p.loadEntries()
	newEntries := make([]*poolEntry[T], len(old)+1)
	copy(newEntries, old)
	newEntries[len(old)] = entry
	p.entries.Store(newEntries)
	p.listMu.Unlock()

	if p.closed.Load() {
		if entry.state.CompareAndSwap(stateNotInUse, stateRemoved) {
			entry.cancel()
		}
		return
	}

	p.tryHandoff(entry)
}

// tryHandoff attempts to hand off a NOT_IN_USE entry to a blocked get() via the handoff channel.
func (p *streamPool[T]) tryHandoff(entry *poolEntry[T]) {
	for p.waiters.Load() > 0 && entry.state.Load() == stateNotInUse {
		select {
		case p.handoff <- entry:
			return
		default:
			runtime.Gosched()
		}
	}
}

// requestCreate sends a non-blocking signal to the creator goroutine.
// If the channel buffer is full, a creation is already pending.
func (p *streamPool[T]) requestCreate() {
	select {
	case p.createCh <- struct{}{}:
	default:
	}
}

// creator is a long-lived goroutine that serializes stream creation requests.
// A single creator goroutine ensures the capacity check in createOne is
// race-free: no TOCTOU between counting active entries and adding a new one.
//
// Serializing creation is not a meaningful bottleneck: gRPC stream creation
// on a single ClientConn is already largely sequential (HTTP/2 HEADERS frames
// are serialized on the transport), so parallel create calls would not help.
//
// On wake-up, the creator loops calling createOne while waiters remain
// and creation succeeds. This ensures N concurrent waiters on a cold pool
// get N streams without per-waiter re-signaling.
func (p *streamPool[T]) creator() {
	for {
		select {
		case <-p.createCh:
			for p.waiters.Load() > 0 {
				ok, retry := p.createOne()
				if !ok && !retry {
					break
				}
				if retry {
					select {
					case <-time.After(createRetryBackoff):
					case <-p.closeCh:
						return
					}
				}
			}
		case <-p.closeCh:
			return
		}
	}
}

// createOne handles a single stream creation request.
// Returns (created, retry): created=true if a stream was successfully created;
// retry=true if creation failed transiently and should be retried after backoff.
func (p *streamPool[T]) createOne() (created, retry bool) {
	// Check capacity: count non-REMOVED entries.
	var active int32
	entries := p.loadEntries()
	for _, e := range entries {
		if e.state.Load() != stateRemoved {
			active++
		}
	}
	if active >= p.maxSize {
		return false, false
	}

	stream, cancel, err := p.create()
	if err != nil {
		return false, true
	}

	// Try to recycle a REMOVED slot in-place.
	// CAS REMOVED→IN_USE claims exclusive access to write stream/cancel.
	// The subsequent Store(NOT_IN_USE) provides happens-before for any
	// scanner that later CAS's NOT_IN_USE→IN_USE.
	entries = p.loadEntries()
	for _, e := range entries {
		if e.state.CompareAndSwap(stateRemoved, stateInUse) {
			// If the pool is closed, close() may have set this entry to
			// REMOVED and could still be reading its cancel field. Release
			// the slot without writing and cancel the newly created stream.
			if p.closed.Load() {
				e.state.Store(stateRemoved)
				cancel()
				return false, false
			}

			e.stream = stream
			e.cancel = cancel
			e.evicted.Store(false)
			e.state.Store(stateNotInUse)

			if p.closed.Load() {
				if e.state.CompareAndSwap(stateNotInUse, stateRemoved) {
					e.cancel()
				}
				return false, false
			}

			p.scheduleEviction(e)
			p.tryHandoff(e)
			return true, false
		}
	}

	// No REMOVED slot available — append a new entry.
	entry := &poolEntry[T]{stream: stream, cancel: cancel}
	p.scheduleEviction(entry)
	p.add(entry)
	return true, false
}

// close shuts down the pool, cancelling all idle entries.
// IN_USE entries are intentionally left alone: their callers will discover
// the broken stream on the next Send/Recv (after the underlying ClientConn
// is closed) and call discard(), which cancels the stream's context.
// The creator goroutine exits via closeCh.
func (p *streamPool[T]) close() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	close(p.closeCh)

	entries := p.loadEntries()
	for _, entry := range entries {
		if entry.state.CompareAndSwap(stateNotInUse, stateRemoved) {
			entry.cancel()
		}
	}
}
