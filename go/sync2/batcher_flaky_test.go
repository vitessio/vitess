/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sync2

import (
	"testing"
	"time"
)

func expectBatch(testcase string, b *Batcher, want int, t *testing.T) {
	id := b.Wait()
	if id != want {
		t.Errorf("%s: got %d, want %d", testcase, id, want)
	}
}

func TestBatcher(t *testing.T) {
	interval := time.Duration(50 * time.Millisecond)
	b := NewBatcher(interval)

	// test single waiter
	go expectBatch("single waiter", b, 1, t)
	time.Sleep(interval * 2)

	// multiple waiters all at once
	go expectBatch("concurrent waiter", b, 2, t)
	go expectBatch("concurrent waiter", b, 2, t)
	go expectBatch("concurrent waiter", b, 2, t)
	time.Sleep(interval * 2)

	// stagger the waiters out in time but cross two intervals
	go expectBatch("staggered waiter", b, 3, t)
	time.Sleep(interval / 5)
	go expectBatch("staggered waiter", b, 3, t)
	time.Sleep(interval / 5)
	go expectBatch("staggered waiter", b, 3, t)
	time.Sleep(interval / 5)
	go expectBatch("staggered waiter", b, 3, t)
	time.Sleep(interval / 5)
	go expectBatch("staggered waiter", b, 3, t)
	time.Sleep(interval / 5)

	go expectBatch("staggered waiter 2", b, 4, t)
	time.Sleep(interval / 5)
	go expectBatch("staggered waiter 2", b, 4, t)
	time.Sleep(interval / 5)
	go expectBatch("staggered waiter 2", b, 4, t)
	time.Sleep(interval / 5)
	go expectBatch("staggered waiter 2", b, 4, t)
	time.Sleep(interval / 5)
	go expectBatch("staggered waiter 2", b, 4, t)
	time.Sleep(interval / 5)

	time.Sleep(interval * 2)
}
