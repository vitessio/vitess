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
	"runtime"

	"vitess.io/vitess/go/atomic2"
)

// connStack is a lock-free stack for Connection objects. It is safe to
// use from several goroutines.
//
// ALIGNMENT: the 128-bit atomic in this struct faults (SIGBUS) on amd64 and
// arm64 unless it is 16-byte aligned. Go has no supported way to demand
// 16-byte alignment (the maximum natural alignment is 8 bytes), so it
// depends entirely on where the allocator places the enclosing allocation,
// which varies with the allocation's exact size, pointer layout, and Go
// version — e.g. allocation headers (Go 1.22+) shift some pointer-bearing
// objects larger than 512 bytes to an odd 8-byte boundary. ConnPool
// currently lands in a bucket where allocations are 16-byte aligned; growing
// it can silently break that, which is why its waitlist is held behind a
// pointer. Be careful when adding fields to ConnPool or anything embedded
// in it.
type connStack[C Connection] struct {
	// top is a pointer to the top node on the stack and to an increasing
	// counter of pop operations, to prevent A-B-A races.
	// See: https://en.wikipedia.org/wiki/ABA_problem
	top atomic2.PointerAndUint64[Pooled[C]]
}

func (s *connStack[C]) Push(item *Pooled[C]) {
	for {
		oldHead, popCount := s.top.Load()
		item.next.Store(oldHead)
		if s.top.CompareAndSwap(oldHead, popCount, item, popCount) {
			return
		}
		runtime.Gosched()
	}
}

func (s *connStack[C]) Pop() (*Pooled[C], bool) {
	for {
		oldHead, popCount := s.top.Load()
		if oldHead == nil {
			return nil, false
		}

		newHead := oldHead.next.Load()
		if s.top.CompareAndSwap(oldHead, popCount, newHead, popCount+1) {
			oldHead.next.Store(nil)
			return oldHead, true
		}
		runtime.Gosched()
	}
}

func (s *connStack[C]) PopAll() (*Pooled[C], bool) {
	for {
		oldHead, popCount := s.top.Load()
		if oldHead == nil {
			return nil, false
		}

		if s.top.CompareAndSwap(oldHead, popCount, nil, popCount+1) {
			return oldHead, true
		}
		runtime.Gosched()
	}
}

func (s *connStack[C]) Peek() *Pooled[C] {
	top, _ := s.top.Load()
	return top
}
