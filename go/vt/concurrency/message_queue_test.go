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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMessageQueue_SendReceive(t *testing.T) {
	queue := NewMessageQueue[int]()
	defer queue.Close()

	// Send some data
	queue.Send(10)
	queue.Send(20)

	// Receive data and validate
	value, ok := queue.Receive()
	require.True(t, ok)
	require.EqualValues(t, 10, value)

	value, ok = queue.Receive()
	require.True(t, ok)
	require.EqualValues(t, 20, value)

	// Ensure receiving from empty queue blocks
	done := make(chan struct{})
	go func() {
		_, _ = queue.Receive() // Should block
		close(done)
	}()

	select {
	case <-done:
		t.Error("expected Receive to block on empty queue")
	case <-time.After(100 * time.Millisecond):
		// Test passes if it doesn't unblock
	}
}

func TestMessageQueue_Close(t *testing.T) {
	queue := NewMessageQueue[string]()
	// Close the queue and validate behavior
	queue.Close()

	_, ok := queue.Receive()
	require.False(t, ok, "expected Receive to return false when queue is closed")

	// Sending to a closed queue should panic
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on Send to closed queue, got none")
		}
	}()
	queue.Send("test")
}

func TestMessageQueue_ConcurrentAccess(t *testing.T) {
	queue := NewMessageQueue[int]()
	wg := sync.WaitGroup{}
	wg.Add(2)

	// Producer goroutine
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			queue.Send(i)
		}
		queue.Close()
	}()

	// Consumer goroutine
	go func() {
		defer wg.Done()
		count := 0
		for {
			value, ok := queue.Receive()
			if !ok {
				break
			}
			require.EqualValues(t, count, value)
			count++
		}
		require.EqualValues(t, 100, count, "expected to receive 100 values")
	}()

	wg.Wait()
}

func TestMessageQueue_BlockingReceive(t *testing.T) {
	queue := NewMessageQueue[int]()

	// Test that Receive blocks until data is sent
	done := make(chan struct{})
	go func() {
		value, ok := queue.Receive()
		require.True(t, ok)
		require.EqualValues(t, 42, value)
		close(done)
	}()

	select {
	case <-done:
		t.Error("expected Receive to block, but it returned immediately")
	case <-time.After(50 * time.Millisecond):
		// Pass: Receive is blocking
	}

	// Unblock Receive by sending data
	queue.Send(42)

	select {
	case <-done:
		// Pass: Receive unblocked
	case <-time.After(100 * time.Millisecond):
		t.Error("expected Receive to unblock after data was sent")
	}
}

func TestMessageQueue_MultipleReceivers(t *testing.T) {
	queue := NewMessageQueue[int]()
	wg := sync.WaitGroup{}
	numReceivers := 5
	numMessages := 50

	wg.Add(numReceivers)
	valCounts := make([]int, numReceivers)

	// Start multiple receivers
	for i := 0; i < numReceivers; i++ {
		go func(id int) {
			defer wg.Done()
			for {
				_, ok := queue.Receive()
				if !ok {
					return
				}
				valCounts[i]++
			}
		}(i)
	}

	// Send messages
	for i := 0; i < numMessages; i++ {
		queue.Send(i)
	}
	queue.Close()

	wg.Wait()

	// Verify all messages were received by someone.
	totalReceived := 0
	for i := 0; i < numReceivers; i++ {
		totalReceived += valCounts[i]
	}
	require.EqualValues(t, numMessages, totalReceived, "expected all messages to be received")
}
