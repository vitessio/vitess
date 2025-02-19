/*
   Copyright 2014 Outbrain Inc.

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

/*

package discovery manages a queue of discovery requests: an ordered
queue with no duplicates.

push() operation never blocks while pop() blocks on an empty queue.

*/

package discovery

import (
	"sync"
	"time"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtorc/config"
)

// queueItem represents an item in the discovery.Queue.
type queueItem struct {
	CreatedAt time.Time
	Key       string
}

// Queue is an implementation of discovery.Queue.
type Queue struct {
	sync.Mutex
	enqueued map[string]struct{}
	nowFunc  func() time.Time
	queue    chan queueItem
}

// NewQueue creates a new queue.
func NewQueue() *Queue {
	return &Queue{
		enqueued: make(map[string]struct{}),
		nowFunc:  func() time.Time { return time.Now() },
		queue:    make(chan queueItem, config.DiscoveryQueueCapacity),
	}
}

// QueueLen returns the length of the queue.
func (q *Queue) QueueLen() int {
	q.Lock()
	defer q.Unlock()

	return len(q.queue) + len(q.enqueued)
}

// Push enqueues a key if it is not on a queue and is not being
// processed; silently returns otherwise.
func (q *Queue) Push(key string) {
	q.Lock()
	defer q.Unlock()

	if _, found := q.enqueued[key]; found {
		return
	}
	q.enqueued[key] = struct{}{}
	q.queue <- queueItem{
		CreatedAt: q.nowFunc(),
		Key:       key,
	}
}

// Consume fetches a key to process; blocks if queue is empty.
// Release must be called once after Consume.
func (q *Queue) Consume() string {
	var item queueItem
	func() {
		q.Lock()
		defer q.Unlock()
		item = <-q.queue
		delete(q.enqueued, item.Key)
	}()

	timeOnQueue := time.Since(item.CreatedAt)
	if timeOnQueue > config.GetInstancePollTime() {
		log.Warningf("key %v spent %.4fs waiting on a discoveryQueue", item.Key, timeOnQueue.Seconds())
	}

	return item.Key
}
