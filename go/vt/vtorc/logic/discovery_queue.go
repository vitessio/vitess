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

DiscoveryQueue manages a queue of discovery requests: an ordered
queue with no duplicates.

push() operation never blocks while pop() blocks on an empty queue.

*/

package logic

import (
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtorc/config"
)

// queueItem represents an item in the DiscoveryQueue.
type queueItem struct {
	TabletAlias *topodatapb.TabletAlias
	PushedAt    time.Time
}

// DiscoveryQueue is an ordered queue with deduplication.
type DiscoveryQueue struct {
	mu       sync.Mutex
	enqueued map[string]struct{}
	queue    chan queueItem
}

// NewDiscoveryQueue creates a new queue.
func NewDiscoveryQueue() *DiscoveryQueue {
	return &DiscoveryQueue{
		enqueued: make(map[string]struct{}),
		queue:    make(chan queueItem, config.DiscoveryQueueCapacity),
	}
}

// setKeyCheckEnqueued returns true if a key is already enqueued, if
// not the key will be marked as enqueued and false is returned.
func (q *DiscoveryQueue) setKeyCheckEnqueued(tabletAlias *topodatapb.TabletAlias) (alreadyEnqueued bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	tabletAliasString := topoproto.TabletAliasString(tabletAlias)
	_, alreadyEnqueued = q.enqueued[tabletAliasString]
	if !alreadyEnqueued {
		q.enqueued[tabletAliasString] = struct{}{}
	}
	return alreadyEnqueued
}

// QueueLen returns the length of the queue.
func (q *DiscoveryQueue) QueueLen() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return len(q.enqueued)
}

// Push enqueues a tablet alias if it is not on a queue and is not being
// processed; silently returns otherwise.
func (q *DiscoveryQueue) Push(tabletAlias *topodatapb.TabletAlias) {
	if q.setKeyCheckEnqueued(tabletAlias) {
		return
	}
	q.queue <- queueItem{
		TabletAlias: tabletAlias,
		PushedAt:    time.Now(),
	}
}

// Consume fetches a tablet alias to process; blocks if queue is empty.
// Release must be called once after Consume.
func (q *DiscoveryQueue) Consume() *topodatapb.TabletAlias {
	item := <-q.queue

	timeOnQueue := time.Since(item.PushedAt)
	if timeOnQueue > config.GetInstancePollTime() {
		log.Warn(fmt.Sprintf("tablet %v spent %.4fs waiting on a discovery queue",
			topoproto.TabletAliasString(item.TabletAlias),
			timeOnQueue.Seconds(),
		))
	}

	return item.TabletAlias
}

// Release removes a tablet alias from a list of being processed aliases
// which allows that tablet to be pushed into the queue again.
func (q *DiscoveryQueue) Release(tabletAlias *topodatapb.TabletAlias) {
	q.mu.Lock()
	defer q.mu.Unlock()

	delete(q.enqueued, topoproto.TabletAliasString(tabletAlias))
}
