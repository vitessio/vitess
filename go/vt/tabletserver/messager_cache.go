// Copyright 2017, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"container/heap"
	"errors"
	"sync"

	"github.com/youtube/vitess/go/sqltypes"
)

var (
	// ErrQueueFull is returned if the queue is full.
	ErrQueueFull = errors.New("queue is full")
	// ErrDupKey is returned if a messager row of the same key
	// is already in the queue.
	ErrDupKey = errors.New("duplicate key")
)

//_______________________________________________

// MessageRow represents a message row.
type MessageRow struct {
	ID       sqltypes.Value
	TimeNext int64
	Epoch    int64
	Message  sqltypes.Value

	// id is the string representation of id
	id string
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

// MessagerCache is the cache for the messager.
type MessagerCache struct {
	maxSendItems int

	mu        sync.Mutex
	sendQueue messageHeap
	ids       map[string]bool
}

// NewMessagerCache creates a new MessagerCache.
func NewMessagerCache(maxSendItems int) *MessagerCache {
	return &MessagerCache{
		maxSendItems: maxSendItems,
		ids:          make(map[string]bool),
	}
}

// Add adds a MessageRow to the cache. It returns
// ErrQueuFull or ErrDupKey on failure.
func (mc *MessagerCache) Add(mr *MessageRow) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if len(mc.sendQueue) >= mc.maxSendItems {
		return ErrQueueFull
	}
	if mc.ids[mr.id] {
		return ErrDupKey
	}
	heap.Push(&mc.sendQueue, mr)
	mc.ids[mr.id] = true
	return nil
}

// Pop removes the next MessageRow. Once the
// message has been sent, Discard must be called.
// The discard has to happen as a separate operation
// to prevent the poller thread from repopulating the
// message while it's being sent.
func (mc *MessagerCache) Pop() *MessageRow {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if len(mc.sendQueue) == 0 {
		return nil
	}
	return heap.Pop(&mc.sendQueue).(*MessageRow)
}

// Discard forgets the specified id.
func (mc *MessagerCache) Discard(id string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	delete(mc.ids, id)
}
