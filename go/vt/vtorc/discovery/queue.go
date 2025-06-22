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
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtorc/config"
)

// queueItem represents an item in the discovery.Queue.
type queueItem struct {
	TabletAlias *topodatapb.TabletAlias
	PushedAt    time.Time
}

// Queue is an ordered queue with deduplication.
type Queue struct {
	mu       sync.Mutex
	enqueued map[*topodatapb.TabletAlias]struct{}
	queue    chan queueItem
}

// NewQueue creates a new queue.
func NewQueue() *Queue {
	return &Queue{
		enqueued: make(map[*topodatapb.TabletAlias]struct{}),
		queue:    make(chan queueItem, config.DiscoveryQueueCapacity),
	}
}

// checkAndSetEnqueued returns true if a tablet alias is already enqueued, if
// not the tablet alias will be marked as enqueued and false is returned.
func (q *Queue) checkAndSetEnqueued(tabletAlias *topodatapb.TabletAlias) (alreadyEnqueued bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	_, alreadyEnqueued = q.enqueued[tabletAlias]
	if !alreadyEnqueued {
		q.enqueued[tabletAlias] = struct{}{}
	}
	return alreadyEnqueued
}

// QueueLen returns the length of the queue.
func (q *Queue) QueueLen() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return len(q.enqueued)
}

// Push enqueues a tablet alias if it is not on a queue and is not being
// processed; silently returns otherwise.
func (q *Queue) Push(tabletAlias *topodatapb.TabletAlias) {
	if q.checkAndSetEnqueued(tabletAlias) || tabletAlias == nil {
		return
	}
	q.queue <- queueItem{
		TabletAlias: tabletAlias,
		PushedAt:    time.Now(),
	}
}

// Consume fetches a tablet alias to process; blocks if queue is empty.
// Release must be called once after Consume.
func (q *Queue) Consume() *topodatapb.TabletAlias {
	item := <-q.queue

	timeOnQueue := time.Since(item.PushedAt)
	if timeOnQueue > config.GetInstancePollTime() {
		log.Warningf("tablet %v spent %.4fs waiting on a discovery queue",
			topoproto.TabletAliasString(item.TabletAlias),
			timeOnQueue.Seconds(),
		)
	}

	return item.TabletAlias
}

// Release removes a tablet alias from a list of being processed aliases
// which allows that tablet to be pushed into the queue again.
func (q *Queue) Release(tabletAlias *topodatapb.TabletAlias) {
	q.mu.Lock()
	defer q.mu.Unlock()

	delete(q.enqueued, tabletAlias)
}
