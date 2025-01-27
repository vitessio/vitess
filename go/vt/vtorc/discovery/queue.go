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

// Queue contains information for managing discovery requests
type Queue struct {
	sync.Mutex

	name         string
	done         chan struct{}
	queue        chan string
	queuedKeys   map[string]time.Time
	consumedKeys map[string]time.Time
}

// CreateQueue allows for creation of a new discovery queue
func CreateQueue(name string) *Queue {
	return &Queue{
		name:         name,
		queuedKeys:   make(map[string]time.Time),
		consumedKeys: make(map[string]time.Time),
		queue:        make(chan string, config.DiscoveryQueueCapacity),
	}
}

// QueueLen returns the length of the queue (channel size + queued size)
func (q *Queue) QueueLen() int {
	q.Lock()
	defer q.Unlock()

	return len(q.queue) + len(q.queuedKeys)
}

// Push enqueues a key if it is not on a queue and is not being
// processed; silently returns otherwise.
func (q *Queue) Push(key string) {
	q.Lock()
	defer q.Unlock()

	// is it enqueued already?
	if _, found := q.queuedKeys[key]; found {
		return
	}

	// is it being processed now?
	if _, found := q.consumedKeys[key]; found {
		return
	}

	q.queuedKeys[key] = time.Now()
	q.queue <- key
}

// Consume fetches a key to process; blocks if queue is empty.
// Release must be called once after Consume.
func (q *Queue) Consume() string {
	q.Lock()
	queue := q.queue
	q.Unlock()

	key := <-queue

	q.Lock()
	defer q.Unlock()

	// alarm if have been waiting for too long
	timeOnQueue := time.Since(q.queuedKeys[key])
	if timeOnQueue > config.GetInstancePollTime() {
		log.Warningf("key %v spent %.4fs waiting on a discoveryQueue", key, timeOnQueue.Seconds())
	}

	q.consumedKeys[key] = q.queuedKeys[key]

	delete(q.queuedKeys, key)

	return key
}

// Release removes a key from a list of being processed keys
// which allows that key to be pushed into the queue again.
func (q *Queue) Release(key string) {
	q.Lock()
	defer q.Unlock()

	delete(q.consumedKeys, key)
}
