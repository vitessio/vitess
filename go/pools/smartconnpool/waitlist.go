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
	"runtime"
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
	// ctx is the request context of the waiting client; returners sample it
	// under the waitlist mutex to evict waiters that can no longer use a
	// connection
	ctx context.Context
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
// The returned connection may _not_ have the requested Setting. This function can
// also return a `nil` connection even if our context has expired, if the pool has
// forced an expiration of all waiters in the waitlist.
func (wl *waitlist[C]) waitForConn(ctx context.Context, setting *Setting, closeChan <-chan struct{}, maxWaiters uint) (*Pooled[C], error) {
	elem := wl.nodes.Get().(*list.Element[waiter[C]])
	defer func() {
		// Drop the references to the request-scoped ctx and setting before
		// recycling the node, so they aren't pinned in the pool. The element
		// is off the list by now, so no returner can observe this write.
		elem.Value = waiter[C]{conn: elem.Value.conn}
		wl.nodes.Put(elem)
	}()

	elem.Value = waiter[C]{conn: elem.Value.conn, setting: setting, ctx: ctx}

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

	select {
	case <-closeChan:
		// Pool was closed while we were waiting.
		wl.mu.Lock()
		removed := wl.list.RemoveIfPresent(elem)
		wl.mu.Unlock()

		if removed {
			return nil, ErrConnPoolClosed
		}

		// if we weren't able to remove ourselves from the waitlist, it means
		// a returner reached us first: it either handed us a connection or
		// evicted us (a nil on the channel), so read the outcome.
		return waitResult(ctx, <-elem.Value.conn)

	case <-ctx.Done():
		// Context expired. We need to try to remove ourselves from the waitlist to
		// prevent another goroutine from trying to hand us a connection later on.
		wl.mu.Lock()
		removed := wl.list.RemoveIfPresent(elem)
		wl.mu.Unlock()

		if removed {
			return nil, context.Cause(ctx)
		}

		// if we weren't able to remove ourselves from the waitlist, it means
		// a returner reached us first: it either handed us a connection or
		// evicted us (a nil on the channel), so read the outcome.
		return waitResult(ctx, <-elem.Value.conn)

	case conn := <-elem.Value.conn:
		return waitResult(ctx, conn)
	}
}

// waitResult interprets what a returner left on the waiter's channel: a real
// connection is a successful handoff, while a nil means the returner evicted
// the waiter because its context had expired.
func waitResult[C Connection](ctx context.Context, conn *Pooled[C]) (*Pooled[C], error) {
	if conn != nil {
		return conn, nil
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	return nil, ErrTimeout
}

func (wl *waitlist[C]) aboveWaiterCap(maxWaiters uint) bool {
	return maxWaiters > 0 && wl.list.Len() >= int(maxWaiters)
}

func (wl *waitlist[C]) maybeStarvingCount() (maybeStarving int) {
	if wl.list.Len() == 0 {
		return
	}

	wl.mu.Lock()
	defer wl.mu.Unlock()

	// count the waiters that no returner has aged yet; they may be starving.
	// Waiters whose context has already expired cannot use a connection and
	// are only listed until a returner evicts them, so they don't count.
	for e := wl.list.Front(); e != nil; e = e.Next() {
		if e.Value.ctx.Err() != nil {
			continue
		}
		if e.Value.age == 0 {
			maybeStarving++
		}
	}

	return
}

// tryReturnConn tries handing over a connection to one of the waiters in the
// pool. Waiters whose context has already expired are evicted; if every
// waiter has expired, the connection is not handed over at all.
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
		next        *list.Element[waiter[D]]
		connSetting = conn.Conn.Setting()
	)

	wl.mu.Lock()
	// iterate through the waitlist looking for either waiters that have been
	// here too long, or a waiter that is looking exactly for the same Setting
	// as the one we have in our connection.
	for e := wl.list.Front(); e != nil; e = next {
		next = e.Next() // capture before any Remove unlinks e

		// Evict waiters whose context has already expired: they cannot use the
		// connection, and removing them here is what keeps the list from
		// accumulating a dead prefix that every later return must re-scan. Wake
		// them with a nil so they stop waiting. This send can't block while we
		// hold the mutex: the channel is buffered and a listed waiter's buffer
		// is always empty, since only a returner sends and only while removing
		// the waiter from the list.
		if e.Value.ctx.Err() != nil {
			wl.list.Remove(e)
			e.Value.conn <- nil
			continue
		}
		if target == nil {
			// the front-most live waiter is the fallback handover target
			target = e
		}
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
	// raced with another client returning another connection, or because all
	// the waiters in the list have an expired context
	if target == nil {
		return false
	}

	// hand the connection to the live target. The channel is buffered, so the
	// send completes without waiting for the waiter to be scheduled.
	target.Value.conn <- conn
	// Allow the goroutine waiting on the channel to start running _now_.
	runtime.Gosched()

	return true
}

func (wl *waitlist[C]) init() {
	wl.nodes.New = func() any {
		return &list.Element[waiter[C]]{
			// buffered (cap 1) so returners never block handing off a
			// connection or waking an evicted waiter
			Value: waiter[C]{conn: make(chan *Pooled[C], 1)},
		}
	}
	wl.list.Init()
}

func (wl *waitlist[C]) waiting() int {
	return wl.list.Len()
}
