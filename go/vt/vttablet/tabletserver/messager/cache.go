/*
Copyright 2019 The Vitess Authors.

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

package messager

import (
	"container/heap"
	"sync"

	"vitess.io/vitess/go/sqltypes"
)

//_______________________________________________

// MessageRow represents a message row.
// The first column in Row is always the "id".
type MessageRow struct {
	TimeNext    int64
	Epoch       int64
	TimeCreated int64
	Row         []sqltypes.Value

	// defunct is set if the row was asked to be removed
	// from cache.
	defunct bool
}

type messageHeap []*MessageRow

func (mh messageHeap) Len() int {
	return len(mh)
}

func (mh messageHeap) Less(i, j int) bool {
	// Lower epoch is more important.
	// If epochs match, newer messages are more important.
	return mh[i].Epoch < mh[j].Epoch ||
		(mh[i].Epoch == mh[j].Epoch && mh[i].TimeNext > mh[j].TimeNext)
}

func (mh messageHeap) Swap(i, j int) {
	mh[i], mh[j] = mh[j], mh[i]
}

func (mh *messageHeap) Push(x interface{}) {
	*mh = append(*mh, x.(*MessageRow))
}

func (mh *messageHeap) Pop() interface{} {
	old := *mh
	n := len(old)
	x := old[n-1]
	*mh = old[0 : n-1]
	return x
}

//_______________________________________________

// cache is the cache for the messager. Messages initially
// start in the sendQueue. When they are popped, they move
// to the inFlight set. They are eventually discarded
// after being successfully sent. Messages can be discarded
// early (while still in the send queue) by any kind of
// update to a message (like an ack). If so, such messages
// are marked as defunct in the cache, and are eventually
// discarded when popped.
type cache struct {
	mu   sync.Mutex
	size int

	sendQueue messageHeap
	// inQueue is used to efficiently find items in sendQueue.
	// The message id is the key.
	inQueue map[string]*MessageRow

	// inFlight are messages that are still being sent.
	// They guard from such messages from being added back prematurely.
	// The message id is the key.
	inFlight map[string]bool
}

// NewMessagerCache creates a new cache.
func newCache(size int) *cache {
	mc := &cache{
		size:     size,
		inQueue:  make(map[string]*MessageRow),
		inFlight: make(map[string]bool),
	}
	return mc
}

// Clear clears the cache.
func (mc *cache) Clear() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.sendQueue = nil
	mc.inQueue = make(map[string]*MessageRow)
	mc.inFlight = make(map[string]bool)
}

// Add adds a MessageRow to the cache. It returns
// false if the cache is full.
func (mc *cache) Add(mr *MessageRow) bool {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if len(mc.sendQueue) >= mc.size {
		return false
	}
	id := mr.Row[0].ToString()
	if mc.inFlight[id] {
		return true
	}
	if _, ok := mc.inQueue[id]; ok {
		return true
	}
	heap.Push(&mc.sendQueue, mr)
	mc.inQueue[id] = mr
	return true
}

// Pop removes the next MessageRow. Once the
// message has been sent, Discard must be called.
// The discard has to happen as a separate operation
// to prevent the poller thread from repopulating the
// message while it's being sent.
// If the Cache is empty Pop returns nil.
func (mc *cache) Pop() *MessageRow {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	for {
		if len(mc.sendQueue) == 0 {
			return nil
		}
		mr := heap.Pop(&mc.sendQueue).(*MessageRow)
		// If message was previously marked as defunct, drop
		// it and continue.
		if mr.defunct {
			continue
		}
		id := mr.Row[0].ToString()

		// Move the message from inQueue to inFlight.
		delete(mc.inQueue, id)
		mc.inFlight[id] = true
		return mr
	}
}

// Discard forgets the specified id.
func (mc *cache) Discard(ids []string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	for _, id := range ids {
		if mr := mc.inQueue[id]; mr != nil {
			// The row is still in the queue somewhere. Mark
			// it as defunct. It will be "garbage collected" later.
			mr.defunct = true
		}
		delete(mc.inQueue, id)
		delete(mc.inFlight, id)
	}
}

// Size returns the max size of cache.
func (mc *cache) Size() int {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return mc.size
}
