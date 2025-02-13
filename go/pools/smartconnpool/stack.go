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
			return oldHead, true
		}
		runtime.Gosched()
	}
}

func (s *connStack[C]) Peek() *Pooled[C] {
	top, _ := s.top.Load()
	return top
}
