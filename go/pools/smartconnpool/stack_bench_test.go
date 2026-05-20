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

package smartconnpool

import (
	"fmt"
	"sync/atomic"
	"testing"
)

// pickElim returns nil for the "noelim" mode (plain Treiber) or a fresh
// elimination array for the "elim" mode.
func pickElim(mode string) *elimArray[testPooled] {
	if mode == "elim" {
		return newElimArray[testPooled]()
	}
	return nil
}

// BenchmarkConnStack_Mixed measures Pop+Push throughput under concurrency.
// Each parallel iteration borrows a connection from the stack and returns it,
// modeling the connection pool's hot path.
func BenchmarkConnStack_Mixed(b *testing.B) {
	for _, mode := range []string{"noelim", "elim"} {
		for _, prefill := range []int{8, 64, 512} {
			b.Run(fmt.Sprintf("%s/prefill=%d", mode, prefill), func(b *testing.B) {
				s := &connStack[testPooled]{}
				elim := pickElim(mode)
				for range prefill {
					s.Push(&Pooled[testPooled]{}, elim)
				}
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					var spare *Pooled[testPooled]
					for pb.Next() {
						item, ok := s.Pop(elim)
						if !ok {
							if spare == nil {
								spare = &Pooled[testPooled]{}
							}
							item = spare
							spare = nil
						}
						s.Push(item, elim)
					}
				})
			})
		}
	}
}

// BenchmarkConnStack_PopOnly measures Pop throughput when the stack is
// pre-loaded and never refilled.
func BenchmarkConnStack_PopOnly(b *testing.B) {
	for _, mode := range []string{"noelim", "elim"} {
		b.Run(mode, func(b *testing.B) {
			s := &connStack[testPooled]{}
			elim := pickElim(mode)
			for range b.N {
				s.Push(&Pooled[testPooled]{}, elim)
			}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					s.Pop(elim)
				}
			})
		})
	}
}

// BenchmarkConnStack_PushOnly measures Push throughput on an empty stack.
func BenchmarkConnStack_PushOnly(b *testing.B) {
	for _, mode := range []string{"noelim", "elim"} {
		b.Run(mode, func(b *testing.B) {
			s := &connStack[testPooled]{}
			elim := pickElim(mode)
			items := make([]*Pooled[testPooled], b.N)
			for i := range items {
				items[i] = &Pooled[testPooled]{}
			}
			b.ResetTimer()
			var idx atomic.Int64
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					i := idx.Add(1) - 1
					if int(i) >= len(items) {
						return
					}
					s.Push(items[i], elim)
				}
			})
		})
	}
}

// BenchmarkConnStack_HighContention runs the Pop+Push hot loop with extra
// parallelism, so concurrent CAS retries are more likely.
func BenchmarkConnStack_HighContention(b *testing.B) {
	for _, mode := range []string{"noelim", "elim"} {
		for _, par := range []int{4, 16, 64} {
			b.Run(fmt.Sprintf("%s/par=%d", mode, par), func(b *testing.B) {
				s := &connStack[testPooled]{}
				elim := pickElim(mode)
				for range 128 {
					s.Push(&Pooled[testPooled]{}, elim)
				}
				b.SetParallelism(par)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					var spare *Pooled[testPooled]
					for pb.Next() {
						item, ok := s.Pop(elim)
						if !ok {
							if spare == nil {
								spare = &Pooled[testPooled]{}
							}
							item = spare
							spare = nil
						}
						s.Push(item, elim)
					}
				})
			})
		}
	}
}
