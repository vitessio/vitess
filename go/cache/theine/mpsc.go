/*
Copyright 2023 The Vitess Authors.
Copyright 2023 Yiling-J

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

package theine

// This implementation is based on http://www.1024cores.net/home/lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue

import (
	"sync"
	"sync/atomic"
)

type node[V any] struct {
	next atomic.Pointer[node[V]]
	val  V
}

type Queue[V any] struct {
	head, tail atomic.Pointer[node[V]]
	nodePool   sync.Pool
}

func NewQueue[V any]() *Queue[V] {
	q := &Queue[V]{nodePool: sync.Pool{New: func() any {
		return new(node[V])
	}}}
	stub := &node[V]{}
	q.head.Store(stub)
	q.tail.Store(stub)
	return q
}

// Push adds x to the back of the queue.
//
// Push can be safely called from multiple goroutines
func (q *Queue[V]) Push(x V) {
	n := q.nodePool.Get().(*node[V])
	n.val = x

	// current producer acquires head node
	prev := q.head.Swap(n)

	// release node to consumer
	prev.next.Store(n)
}

// Pop removes the item from the front of the queue or nil if the queue is empty
//
// Pop must be called from a single, consumer goroutine
func (q *Queue[V]) Pop() (V, bool) {
	tail := q.tail.Load()
	next := tail.next.Load()
	if next != nil {
		var null V
		q.tail.Store(next)
		v := next.val
		next.val = null
		tail.next.Store(nil)
		q.nodePool.Put(tail)
		return v, true
	}
	var null V
	return null, false
}

// Empty returns true if the queue is empty
//
// Empty must be called from a single, consumer goroutine
func (q *Queue[V]) Empty() bool {
	tail := q.tail.Load()
	return tail.next.Load() == nil
}
