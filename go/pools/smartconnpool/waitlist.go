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
	// age is the amount of cycles this client has been on the waitlist
	age uint32
}

type waitlist[C Connection] struct {
	nodes sync.Pool
	mu    sync.Mutex
	list  list.List[waiter[C]]
}

// waitForConn blocks until a connection with the given Setting is returned by another client,
// or until the given context expires.
// The returned connection may _not_ have the requested Setting. This function can
// also return a `nil` connection even if our context has expired, if the pool has
// forced an expiration of all waiters in the waitlist.
func (wl *waitlist[C]) waitForConn(ctx context.Context, setting *Setting, closeChan <-chan struct{}) (*Pooled[C], error) {
	elem := wl.nodes.Get().(*list.Element[waiter[C]])
	defer wl.nodes.Put(elem)

	elem.Value = waiter[C]{conn: elem.Value.conn, setting: setting}

	wl.mu.Lock()
	// add ourselves as a waiter at the end of the waitlist
	wl.list.PushBackValue(elem)
	wl.mu.Unlock()

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

		// if we weren't able to remove ourselves from the waitlist, it means
		// another goroutine is trying to hand us a connection
		return <-elem.Value.conn, nil

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

		// if we weren't able to remove ourselves from the waitlist, it means
		// another goroutine is trying to hand us a connection
		return <-elem.Value.conn, nil

	case conn := <-elem.Value.conn:
		return conn, nil
	}
}

func (wl *waitlist[C]) maybeStarvingCount() (maybeStarving int) {
	if wl.list.Len() == 0 {
		return
	}

	wl.mu.Lock()
	defer wl.mu.Unlock()

	// iterate the waitlist looking for waiters with an expired Context,
	// or remove everything if force is true
	for e := wl.list.Front(); e != nil; e = e.Next() {
		if e.Value.age == 0 {
			maybeStarving++
		}
	}

	return
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

	// if we have a target to return the connection to, simply write the connection
	// into the waiter's channel.
	target.Value.conn <- conn
	// Allow the goroutine waiting on the channel to start running _now_.
	runtime.Gosched()

	return true
}

func (wl *waitlist[C]) init() {
	wl.nodes.New = func() any {
		return &list.Element[waiter[C]]{
			Value: waiter[C]{conn: make(chan *Pooled[C])},
		}
	}
	wl.list.Init()
}

func (wl *waitlist[C]) waiting() int {
	return wl.list.Len()
}
