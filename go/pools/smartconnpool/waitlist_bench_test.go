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
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// BenchmarkWaitlistHandoff measures steady-state handoff throughput on the
// common path, with no expired waiters in the list. A number of waiter
// goroutines block in waitForConn and re-queue as soon as they are served,
// while a single returner hands connections back via tryReturnConn. This
// exercises the list push/remove, the per-return scan, and the channel
// handoff that our changes touch.
func BenchmarkWaitlistHandoff(b *testing.B) {
	for _, waiters := range []int{1, 8, 64} {
		b.Run(fmt.Sprintf("waiters=%d", waiters), func(b *testing.B) {
			wl := waitlist[*TestConn]{}
			wl.init()
			closeChan := make(chan struct{})
			ctx := context.Background()

			// Each consumer claims one slot per waitForConn call; the claims
			// across all consumers sum to exactly b.N, so the same number of
			// connections the returner produces are consumed and nobody is
			// left blocked at the end.
			var remaining atomic.Int64
			remaining.Store(int64(b.N))

			var consumers sync.WaitGroup
			for range waiters {
				consumers.Go(func() {
					for remaining.Add(-1) >= 0 {
						wl.waitForConn(ctx, nil, closeChan, 0)
					}
				})
			}

			// reuse a single connection across handoffs: the waitlist only
			// passes the pointer around, so this measures the handoff path
			// without per-iteration allocation/GC jitter.
			conn := &Pooled[*TestConn]{Conn: &TestConn{}}

			b.ResetTimer()
			for range b.N {
				// retry until a waiter takes the connection, so a return that
				// races ahead of a re-queueing waiter doesn't drop it.
				for !wl.tryReturnConn(conn) {
					runtime.Gosched()
				}
			}
			b.StopTimer()

			consumers.Wait()
		})
	}
}

// BenchmarkWaitlistReturnNoWaiters measures the empty-list fast path of
// tryReturnConn, which every connection return hits when nobody is waiting.
func BenchmarkWaitlistReturnNoWaiters(b *testing.B) {
	wl := waitlist[*TestConn]{}
	wl.init()
	conn := &Pooled[*TestConn]{Conn: &TestConn{}}

	b.ResetTimer()
	for range b.N {
		wl.tryReturnConn(conn)
	}
}
