/*
Copyright 2024 The Vitess Authors.

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

package tabletserver

import "sync"

// requestsWaiter is used to wait for requests. It stores the count of the requests pending,
// and also the number of waiters currently waiting. It has a mutex as well to protects its fields.
type requestsWaiter struct {
	mu sync.Mutex
	wg sync.WaitGroup
	// waitCounter is the number of goroutines that are waiting for wg to be empty.
	// If this value is greater than zero, then we have to ensure that we don't Add to the requests
	// to avoid any panics in the wait.
	waitCounter int
	// counter is the count of the number of outstanding requests.
	counter int
}

// newRequestsWaiter creates a new requestsWaiter.
func newRequestsWaiter() *requestsWaiter {
	return &requestsWaiter{}
}

// Add adds to the requestsWaiter.
func (r *requestsWaiter) Add(val int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counter += val
	r.wg.Add(val)
}

// Done subtracts 1 from the requestsWaiter.
func (r *requestsWaiter) Done() {
	r.Add(-1)
}

// addToWaitCounter adds to the waitCounter while being protected by a mutex.
func (r *requestsWaiter) addToWaitCounter(val int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.waitCounter += val
}

// WaitToBeEmpty waits for requests to be empty. It also increments and decrements the waitCounter as required.
func (r *requestsWaiter) WaitToBeEmpty() {
	r.addToWaitCounter(1)
	r.wg.Wait()
	r.addToWaitCounter(-1)
}

// GetWaiterCount gets the number of go routines currently waiting on the wait group.
func (r *requestsWaiter) GetWaiterCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.waitCounter
}

// GetOutstandingRequestsCount gets the number of requests outstanding.
func (r *requestsWaiter) GetOutstandingRequestsCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counter
}
