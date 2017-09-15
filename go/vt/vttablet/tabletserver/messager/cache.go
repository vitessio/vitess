/*
Copyright 2017 Google Inc.

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

	"github.com/youtube/vitess/go/sqltypes"
)

//_______________________________________________

// MessageRow represents a message row.
// The first column in Row is always the "id".
type MessageRow struct {
	TimeNext    int64
	Epoch       int64
	TimeCreated int64
	Row         []sqltypes.Value
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

// cache is the cache for the messager.
type cache struct {
	mu        sync.Mutex
	size      int
	sendQueue messageHeap
	messages  map[string]*MessageRow
}

// NewMessagerCache creates a new cache.
func newCache(size int) *cache {
	mc := &cache{
		size:     size,
		messages: make(map[string]*MessageRow),
	}
	return mc
}

// Clear clears the cache.
func (mc *cache) Clear() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.sendQueue = nil
	mc.messages = make(map[string]*MessageRow)
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
	// Don't check for nil. Messages that are popped for
	// send are nilled out.
	if _, ok := mc.messages[id]; ok {
		return true
	}
	heap.Push(&mc.sendQueue, mr)
	mc.messages[id] = mr
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
		id := mr.Row[0].ToString()
		// If message was previously marked as defunct, drop
		// it and continue.
		if id == "" {
			continue
		}
		// Point the message entry to nil. If there is a race
		// with Discard while the item is being sent, it won't
		// be reachable.
		mc.messages[id] = nil
		return mr
	}
}

// Discard forgets the specified id.
func (mc *cache) Discard(ids []string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	for _, id := range ids {
		if mr := mc.messages[id]; mr != nil {
			// The row is still in the queue somewhere. Mark
			// it as defunct. It will be "garbage collected" later.
			mr.Row[0] = sqltypes.NULL
		}
		delete(mc.messages, id)
	}
}

// Size returns the max size of cache.
func (mc *cache) Size() int {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return mc.size
}
