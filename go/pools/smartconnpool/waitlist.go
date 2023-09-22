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

type waiter[C Connection] struct {
	Setting *Setting
	Conn    *Pooled[C]
	Context context.Context
	Sema    semaphore
	Age     uint32
}

type waitlist[C Connection] struct {
	nodes sync.Pool
	mu    sync.Mutex
	list  List[waiter[C]]
}

func (wl *waitlist[C]) waitForConn(ctx context.Context, setting *Setting) (*Pooled[C], error) {
	elem := wl.nodes.Get().(*Element[waiter[C]])
	elem.Value = waiter[C]{Setting: setting, Conn: nil, Context: ctx}

	wl.mu.Lock()
	wl.list.PushBackValue(elem)
	wl.mu.Unlock()

	elem.Value.Sema.wait()

	conn := elem.Value.Conn
	wl.nodes.Put(elem)

	return conn, ctx.Err()
}

func (wl *waitlist[C]) expire(force bool) (waiting int) {
	if wl.list.len.Load() == 0 {
		return 0
	}

	var expired []*Element[waiter[C]]

	wl.mu.Lock()
	for e := wl.list.Front(); e != nil; e = e.Next() {
		if force || e.Value.Context.Err() != nil {
			wl.list.Remove(e)
			expired = append(expired, e)
			continue
		}
		waiting++
	}
	wl.mu.Unlock()

	for _, e := range expired {
		e.Value.Sema.notify(false)
	}
	return waiting
}

func (wl *waitlist[D]) tryReturnConn(conn *Pooled[D]) bool {
	if wl.list.len.Load() == 0 {
		return false
	}
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
	for e := target; e != nil; e = e.Next() {
		if e.Value.Age > maxAge || e.Value.Setting == connSetting {
			target = e
			break
		}
		e.Value.Age++
	}
	if target != nil {
		wl.list.Remove(target)
	}
	wl.mu.Unlock()

	if target == nil {
		return false
	}

	target.Value.Conn = conn
	target.Value.Sema.notify(true)
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
