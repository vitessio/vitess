/*
Copyright 2023 The Vitess Authors.

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

package smartconnpool

import (
	"context"
	"sync"

	"vitess.io/vitess/go/list"
)

// waiter represents a client waiting for a connection in the waitlist
type waiter[C Connection] struct {
	// setting is the connection Setting that we'd like, or nil if we'd like a
	// a connection with no Setting applied
	setting *Setting
	// conn is a channel that will receive the connection when it's ready
	conn chan *Pooled[C]
	// age is the amount of cycles this client has been on the waitlist
	age uint32
}

type waitlist[C Connection] struct {
	nodes              sync.Pool
	mu                 sync.Mutex
	list               list.List[waiter[C]]
	onWait             func()
	onWaiterCapReached func()
}

// waitForConn blocks until a connection with the given Setting is returned by another client,
// or until the given context expires.
// If maxWaiters is > 0 and the waitlist already has that many waiters, it returns
// ErrPoolWaiterCapReached immediately without blocking.
// The returned connection may _not_ have the requested Setting. A nil connection
// with nil error means the caller should retry acquisition.
// If a non-nil error is returned, any conn that was already in flight to this
// waiter is discarded internally — callers never need to handle (conn, err).
func (wl *waitlist[C]) waitForConn(ctx context.Context, setting *Setting, closeChan <-chan struct{}, maxWaiters uint, shouldRetry func() bool) (*Pooled[C], error) {
	elem := wl.nodes.Get().(*list.Element[waiter[C]])
	defer func() {
		// Defensive drain. Every branch below already either returns the
		// conn to the caller or discards it explicitly, but a leftover in
		// this element's buffered channel would surface as a phantom conn
		// for the next user pulling this Element out of the sync.Pool.
		select {
		case conn := <-elem.Value.conn:
			if conn != nil {
				conn.pool.discardConn(conn)
			}
		default:
		}
		wl.nodes.Put(elem)
	}()

	elem.Value = waiter[C]{conn: elem.Value.conn, setting: setting}

	if wl.aboveWaiterCap(maxWaiters) {
		if wl.onWaiterCapReached != nil {
			wl.onWaiterCapReached()
		}
		return nil, ErrPoolWaiterCapReached
	}

	if wl.onWait != nil {
		wl.onWait()
	}

	wl.mu.Lock()
	if wl.aboveWaiterCap(maxWaiters) {
		wl.mu.Unlock()
		if wl.onWaiterCapReached != nil {
			wl.onWaiterCapReached()
		}
		return nil, ErrPoolWaiterCapReached
	}
	wl.list.PushBackValue(elem)
	wl.mu.Unlock()

	if shouldRetry != nil && shouldRetry() {
		removed := false

		wl.mu.Lock()
		for e := wl.list.Front(); e != nil; e = e.Next() {
			if e == elem {
				wl.list.Remove(elem)
				removed = true
				break
			}
		}
		wl.mu.Unlock()

		if removed {
			return nil, nil
		}
	}

	select {
	case <-closeChan:
		// Pool was closed while we were waiting.
		removed := false

		wl.mu.Lock()
		// Try to find and remove ourselves from the list.
		for e := wl.list.Front(); e != nil; e = e.Next() {
			if e == elem {
				wl.list.Remove(elem)
				removed = true
				break
			}
		}
		wl.mu.Unlock()

		if removed {
			return nil, ErrConnPoolClosed
		}

		// We lost the race to remove ourselves: another goroutine handed us a
		// connection we can no longer use. Drain it from the channel and
		// discard so the conn isn't leaked.
		if conn := <-elem.Value.conn; conn != nil {
			conn.pool.discardConn(conn)
		}
		return nil, ErrConnPoolClosed

	case <-ctx.Done():
		// Context expired. We need to try to remove ourselves from the waitlist to
		// prevent another goroutine from trying to hand us a connection later on.
		removed := false

		wl.mu.Lock()
		// Try to find and remove ourselves from the list.
		for e := wl.list.Front(); e != nil; e = e.Next() {
			if e == elem {
				wl.list.Remove(elem)
				removed = true
				break
			}
		}
		wl.mu.Unlock()

		if removed {
			return nil, context.Cause(ctx)
		}

		// We lost the race to remove ourselves: drain the handed-off conn
		// and discard so it isn't leaked to a caller whose ctx is already dead.
		if conn := <-elem.Value.conn; conn != nil {
			conn.pool.discardConn(conn)
		}
		return nil, context.Cause(ctx)

	case conn := <-elem.Value.conn:
		if conn == nil {
			if err := ctx.Err(); err != nil {
				return nil, context.Cause(ctx)
			}
			select {
			case <-closeChan:
				return nil, ErrConnPoolClosed
			default:
			}
			return nil, nil
		}
		select {
		case <-closeChan:
			conn.pool.discardConn(conn)
			return nil, ErrConnPoolClosed
		default:
		}
		return conn, nil
	}
}

func (wl *waitlist[C]) aboveWaiterCap(maxWaiters uint) bool {
	return maxWaiters > 0 && wl.list.Len() >= int(maxWaiters)
}

// tryReturnConn tries handing over a connection to one of the waiters in the pool.
func (wl *waitlist[D]) tryReturnConn(conn *Pooled[D]) bool {
	// fast path: if there's nobody waiting there's nothing to do
	if wl.list.Len() == 0 {
		return false
	}
	// split the slow path into a separate function to enable inlining
	return wl.tryReturnConnSlow(conn)
}

func (wl *waitlist[D]) tryReturnConnSlow(conn *Pooled[D]) bool {
	const maxAge = 8
	var (
		target      *list.Element[waiter[D]]
		connSetting = conn.Conn.Setting()
	)

	wl.mu.Lock()
	target = wl.list.Front()
	// iterate through the waitlist looking for either waiters that have been
	// here too long, or a waiter that is looking exactly for the same Setting
	// as the one we have in our connection.
	for e := target; e != nil; e = e.Next() {
		if e.Value.age > maxAge || e.Value.setting == connSetting {
			target = e
			break
		}
		// this only ages the waiters that are being skipped over: we'll start
		// aging the waiters in the back once they get to the front of the pool.
		// the maxAge of 8 has been set empirically: smaller values cause clients
		// with a specific setting to slightly starve, and aging all the clients
		// in the list every time leads to unfairness when the system is at capacity
		e.Value.age++
	}
	if target != nil {
		wl.list.Remove(target)
	}
	wl.mu.Unlock()

	// maybe there isn't anybody to hand over the connection to, because we've
	// raced with another client returning another connection
	if target == nil {
		return false
	}

	// Write into the waiter's buffered channel; this never blocks because the
	// waiter is the sole receiver and the buffer is sized 1.
	target.Value.conn <- conn

	return true
}

func (wl *waitlist[D]) tryNotifyWaiter() bool {
	if wl.list.Len() == 0 {
		return false
	}

	wl.mu.Lock()
	target := wl.list.Front()
	if target != nil {
		wl.list.Remove(target)
	}
	wl.mu.Unlock()

	if target == nil {
		return false
	}

	target.Value.conn <- nil

	return true
}

func (wl *waitlist[C]) init() {
	wl.nodes.New = func() any {
		// Buffer of 1 so tryReturnConnSlow / tryNotifyWaiter never block on
		// the send. A receiver that bails on ctx.Done or closeChan picks up
		// the queued conn via the blocking drain in waitForConn's
		// not-removed branches (and a defensive drain on the way out).
		return &list.Element[waiter[C]]{
			Value: waiter[C]{conn: make(chan *Pooled[C], 1)},
		}
	}
	wl.list.Init()
}

func (wl *waitlist[C]) waiting() int {
	return wl.list.Len()
}
