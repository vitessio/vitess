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
	"testing"

	"vitess.io/vitess/go/list"
)

// injectWaiter appends a synthetic waitlist entry with no goroutine behind it.
// The conn channel is buffered so a returner evicting it (sending nil) never
// blocks. Self-contained here so this benchmark compiles on branches that
// predate the test-only pushWaiter helper.
func injectWaiter(wl *waitlist[*TestConn], ctx context.Context) {
	elem := &list.Element[waiter[*TestConn]]{
		Value: waiter[*TestConn]{
			ctx:  ctx,
			conn: make(chan *Pooled[*TestConn], 1),
		},
	}
	wl.mu.Lock()
	wl.list.PushBackValue(elem)
	wl.mu.Unlock()
}

// BenchmarkWaitlistReturnAllExpired measures repeated returns against a list
// whose head is a prefix of expired waiters — the timeout-storm shape. With
// skip-and-leave every return re-scans the whole prefix (O(returns*prefix));
// with eviction the first return drains the prefix and the rest are cheap.
func BenchmarkWaitlistReturnAllExpired(b *testing.B) {
	const returns = 32
	for _, prefix := range []int{256, 2048} {
		b.Run(fmt.Sprintf("prefix=%d", prefix), func(b *testing.B) {
			expiredCtx, cancel := context.WithCancel(context.Background())
			cancel()
			conn := &Pooled[*TestConn]{Conn: &TestConn{}}

			for range b.N {
				b.StopTimer()
				wl := waitlist[*TestConn]{}
				wl.init()
				for range prefix {
					injectWaiter(&wl, expiredCtx)
				}
				b.StartTimer()

				for range returns {
					wl.tryReturnConn(conn)
				}
			}
		})
	}
}
