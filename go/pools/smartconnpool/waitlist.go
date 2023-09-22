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
)

// waiter represents a client waiting for a connection in the waitlist
type waiter[C Connection] struct {
	// setting is the connection Setting that we'd like, or nil if we'd like a
	// a connection with no Setting applied
	setting *Setting
	// conn will be set by another client to hand over the connection to use
	conn *Pooled[C]
	// ctx is the context of the waiting client to check for expiration
	ctx context.Context
	// sema is a synchronization primitive that allows us to block until our request
	// has been fulfilled
	sema semaphore
	// age is the amount of cycles this client has been on the waitlist
	age uint32
}

type waitlist[C Connection] struct {
	nodes sync.Pool
	mu    sync.Mutex
	list  List[waiter[C]]
}

// waitForConn blocks until a connection with the given Setting is returned by another client,
// or until the given context expires.
// The returned connection may _not_ have the requested Setting. This function can
// also return a `nil` connection even if our context has expired, if the pool has
// forced an expiration of all waiters in the waitlist.
func (wl *waitlist[C]) waitForConn(ctx context.Context, setting *Setting) (*Pooled[C], error) {
	elem := wl.nodes.Get().(*Element[waiter[C]])
	elem.Value = waiter[C]{setting: setting, conn: nil, ctx: ctx}

	wl.mu.Lock()
	// add ourselves as a waiter at the end of the waitlist
	wl.list.PushBackValue(elem)
	wl.mu.Unlock()

	// block on our waiter's semaphore until somebody can hand over a connection to us
	elem.Value.sema.wait()

	// we're awake -- the conn in our waiter contains the connection that was handed
	// over to us, or nothing if we've been waken up forcefully. save the conn before
	// we return our waiter to the pool of waiters for reuse.
	conn := elem.Value.conn
	wl.nodes.Put(elem)

	return conn, ctx.Err()
}

// expire removes and wakes any expired waiter in the waitlist.
// if force is true, it'll wake and remove all the waiters.
func (wl *waitlist[C]) expire(force bool) (waiting int) {
	if wl.list.len.Load() == 0 {
		return 0
	}

	var expired []*Element[waiter[C]]

	wl.mu.Lock()
	// iterate the waitlist looking for waiters with an expired Context,
	// or remove everything if force is true
	for e := wl.list.Front(); e != nil; e = e.Next() {
		if force || e.Value.ctx.Err() != nil {
			wl.list.Remove(e)
			expired = append(expired, e)
			continue
		}
		waiting++
	}
	wl.mu.Unlock()

	// once all the expired waiters have been removed from the waitlist, wake them up one by one
	for _, e := range expired {
		e.Value.sema.notify(false)
	}
	return waiting
}

// tryReturnConn tries handing over a connection to one of the waiters in the pool.
func (wl *waitlist[D]) tryReturnConn(conn *Pooled[D]) bool {
	// fast path: if there's nobody waiting there's nothing to do
	if wl.list.len.Load() == 0 {
		return false
	}
	// split the slow path into a separate function to enable inlining
	return wl.tryReturnConnSlow(conn)
}

func (wl *waitlist[D]) tryReturnConnSlow(conn *Pooled[D]) bool {
	const maxAge = 8
	var (
		target      *Element[waiter[D]]
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
	// into the waiter and signal their semaphore. they'll wake up to pick up the
	// connection.
	target.Value.conn = conn
	target.Value.sema.notify(true)
	return true
}

func (wl *waitlist[C]) init() {
	wl.nodes.New = func() any {
		return &Element[waiter[C]]{}
	}
	wl.list.Init()
}

func (wl *waitlist[C]) waiting() int {
	return int(wl.list.len.Load())
}
