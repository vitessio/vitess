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

package concurrency

import (
	"sync"
)

// MessageQueue provides semantics required from a message queue.
// It behaves like a channel with an infinite capacity, where-in the
// sender never blocks, and the receiver is guaranteed to see all updates.
type MessageQueue[T any] struct {
	mu     sync.Mutex
	cond   *sync.Cond
	queue  []T
	closed bool
}

// NewMessageQueue creates a new generic MessageQueue.
func NewMessageQueue[T any]() *MessageQueue[T] {
	mq := &MessageQueue[T]{}
	mq.cond = sync.NewCond(&mq.mu)
	return mq
}

// Send adds a message of type T to the queue.
func (mq *MessageQueue[T]) Send(value T) {
	mq.mu.Lock()
	defer mq.mu.Unlock()
	if mq.closed {
		// We panic here just like a channel would
		// if the user tried to send on a closed channel.
		panic("cannot send on closed MessageQueue")
	}
	mq.queue = append(mq.queue, value)
	mq.cond.Signal() // Notify a waiting receiver.
}

// Receive fetches a message of type T from the queue.
// Blocks if no messages are available or until the queue is closed.
func (mq *MessageQueue[T]) Receive() (T, bool) {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	var zeroValue T // Zero value of T

	// Wait until a message is available or the queue is closed.
	for len(mq.queue) == 0 && !mq.closed {
		mq.cond.Wait()
	}

	if len(mq.queue) == 0 && mq.closed {
		return zeroValue, false // Queue is closed and empty.
	}

	// Get the first message from the queue.
	value := mq.queue[0]
	mq.queue = mq.queue[1:]
	return value, true
}

// Close signals that no more messages will be sent.
func (mq *MessageQueue[T]) Close() {
	mq.mu.Lock()
	defer mq.mu.Unlock()
	mq.closed = true
	mq.cond.Broadcast() // Notify all waiting receivers.
}
