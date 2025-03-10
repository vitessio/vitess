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
	Key      string
	PushedAt time.Time
}

// Queue is an ordered queue with deduplication.
type Queue struct {
	mu       sync.Mutex
	enqueued map[string]struct{}
	queue    chan queueItem
}

// NewQueue creates a new queue.
func NewQueue() *Queue {
	return &Queue{
		enqueued: make(map[string]struct{}),
		queue:    make(chan queueItem, config.DiscoveryQueueCapacity),
	}
}

// setKeyCheckEnqueued returns true if a key is already enqueued, if
// not the key will be marked as enqueued and false is returned.
func (q *Queue) setKeyCheckEnqueued(key string) (alreadyEnqueued bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	_, alreadyEnqueued = q.enqueued[key]
	if !alreadyEnqueued {
		q.enqueued[key] = struct{}{}
	}
	return alreadyEnqueued
}

// QueueLen returns the length of the queue.
func (q *Queue) QueueLen() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return len(q.enqueued)
}

// Push enqueues a key if it is not on a queue and is not being
// processed; silently returns otherwise.
func (q *Queue) Push(key string) {
	if q.setKeyCheckEnqueued(key) {
		return
	}
	q.queue <- queueItem{
		Key:      key,
		PushedAt: time.Now(),
	}
}

// Consume fetches a key to process; blocks if queue is empty.
// Release must be called once after Consume.
func (q *Queue) Consume() string {
	item := <-q.queue

	timeOnQueue := time.Since(item.PushedAt)
	if timeOnQueue > time.Duration(config.Config.InstancePollSeconds)*time.Second {
		log.Warningf("key %v spent %.4fs waiting on a discovery queue", item.Key, timeOnQueue.Seconds())
	}

	return item.Key
}

// Release removes a key from a list of being processed keys
// which allows that key to be pushed into the queue again.
func (q *Queue) Release(key string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	delete(q.enqueued, key)
}
