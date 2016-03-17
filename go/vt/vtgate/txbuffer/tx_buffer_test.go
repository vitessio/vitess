// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package txbuffer

import (
	"sync"
	"testing"
	"time"
)

// fakeSleepController is used to control a fake sleepFunc
type fakeSleepController struct {
	called bool
	block  bool
	// block until the done channel if closed, if configured to do so.
	done chan struct{}
	// will close this channel when blocked
	blocked chan struct{}
}

type sleepFunc func(d time.Duration)

// createFakeSleep creates a function that can be called to fake sleeping.
// The created fake is managed by the passed in fakeSleepController.
func createFakeSleep(c *fakeSleepController) sleepFunc {
	return func(d time.Duration) {
		c.called = true
		if !c.block {
			return
		}
		close(c.blocked)
		select {
		case <-c.done:
			return
		}
	}
}

func TestFakeBuffer(t *testing.T) {
	unbufferedKeyspace := "ukeyspace"
	unbufferedShard := "80-"
	bufferedKeyspace := "bkeyspace"
	bufferedShard := "-80"

	*bufferKeyspace = bufferedKeyspace
	*bufferShard = bufferedShard

	for _, test := range []struct {
		desc                 string
		enableFakeBuffer     bool
		keyspace             string
		shard                string
		attemptNumber        int
		bufferedTransactions int
		// was this transaction buffered?
		wantCalled bool
		// expected value of BufferedTransactionAttempts
		wantAttempted int
	}{
		{
			desc:             "enableFakeBuffer=False",
			enableFakeBuffer: false,
		},
		{
			desc:             "attemptNumber != 0",
			enableFakeBuffer: true,
			attemptNumber:    1,
		},
		{
			desc:             "unbuffered keyspace",
			enableFakeBuffer: true,
			keyspace:         unbufferedKeyspace,
			shard:            bufferedShard,
		},
		{
			desc:             "unbuffered shard",
			enableFakeBuffer: true,
			keyspace:         bufferedKeyspace,
			shard:            unbufferedShard,
		},
		{
			desc:                 "buffer full",
			enableFakeBuffer:     true,
			keyspace:             bufferedKeyspace,
			shard:                bufferedShard,
			bufferedTransactions: *maxBufferSize,
			// When the buffer is full, bufferedTransactionsAttempted should still be incremented
			wantAttempted: 1,
		},
		{
			desc:             "buffered successful",
			enableFakeBuffer: true,
			keyspace:         bufferedKeyspace,
			shard:            bufferedShard,
			wantCalled:       true,
			wantAttempted:    1,
		},
	} {
		controller := &fakeSleepController{}
		timeSleep = createFakeSleep(controller)
		// reset counters
		bufferedTransactionsAttempted.Set(0)
		bufferedTransactionsSuccessful.Set(0)
		bufferedTransactions.Set(int64(test.bufferedTransactions))

		*enableFakeTxBuffer = test.enableFakeBuffer

		FakeBuffer(test.keyspace, test.shard, test.attemptNumber)

		if controller.called != test.wantCalled {
			t.Errorf("With %v, FakeBuffer() => timeSleep.called: %v; want: %v",
				test.desc, controller.called, test.wantCalled)
		}

		if bufferedTransactionsAttempted.Get() != int64(test.wantAttempted) {
			t.Errorf("With %v, FakeBuffer() => BufferedTransactionsAttempted got: %v; want: %v",
				test.desc, bufferedTransactionsAttempted.Get(), test.wantAttempted)
		}

		if (!test.wantCalled && (bufferedTransactionsSuccessful.Get() == 1)) ||
			(test.wantCalled && (bufferedTransactionsSuccessful.Get() != 1)) {
			t.Errorf("With %v, FakeBuffer() => BufferedTransactionsSuccessful got: %v; want: 1",
				test.desc, bufferedTransactionsSuccessful.Get())
		}
	}
}

// min for ints
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func TestParallelFakeBuffer(t *testing.T) {
	bufferedKeyspace := "bkeyspace"
	bufferedShard := "-80"

	*bufferKeyspace = bufferedKeyspace
	*bufferShard = bufferedShard
	*enableFakeTxBuffer = true

	// reset counters
	bufferedTransactionsAttempted.Set(0)
	bufferedTransactionsSuccessful.Set(0)

	var controllers []*fakeSleepController
	var wg sync.WaitGroup

	for i := 1; i <= *maxBufferSize+2; i++ {
		controller := &fakeSleepController{
			block:   true,
			done:    make(chan struct{}),
			blocked: make(chan struct{}),
		}
		timeSleep = createFakeSleep(controller)
		// Only the first maxBufferSize calls to FakeBuffer should actually call fakeSleep
		wantFakeSleepCalled := (i <= *maxBufferSize)

		wg.Add(1)
		finished := make(chan struct{})
		go func() {
			defer wg.Done()
			FakeBuffer(*bufferKeyspace, *bufferShard, 0)
			close(finished)
		}()

		if wantFakeSleepCalled {
			// the first maxBufferSize calls to FakeBuffer
			// should call sleep, wait until they do
			<-controller.blocked
		} else {
			// the rest should not block, wait until they're done
			<-finished
		}

		if controller.called {
			controllers = append(controllers, controller)
		}

		if controller.called != wantFakeSleepCalled {
			t.Errorf("On iteration %v, FakeBuffer() => timeSleep.called: %v; want: %v",
				i, controller.called, wantFakeSleepCalled)
		}

		if int(bufferedTransactionsAttempted.Get()) != i {
			t.Errorf("On iteration %v, FakeBuffer() => BufferedTransactionsAttempted got: %v; want: %v",
				i, bufferedTransactionsAttempted.Get(), i)
		}

		if int(bufferedTransactions.Get()) != min(i, *maxBufferSize) {
			t.Errorf("On iteration %v, FakeBuffer() => BufferedTransactions got: %v; want: %v",
				i, bufferedTransactions.Get(), min(i, *maxBufferSize))
		}

		if int(bufferedTransactionsSuccessful.Get()) != 0 {
			t.Errorf("On iteration %v, FakeBuffer() => BufferedTransactionsSuccessful got: %v; want: 0",
				i, bufferedTransactionsSuccessful.Get())
		}
	}

	// signal to all the buffered calls that they can stop buffering, and wait for them.
	for _, c := range controllers {
		close(c.done)
	}
	wg.Wait()

	if int(bufferedTransactionsSuccessful.Get()) != *maxBufferSize {
		t.Errorf("After all FakeBuffer() calls are done, BufferedTransactionsSuccessful got: %v; want: %v",
			bufferedTransactionsSuccessful.Get(), *maxBufferSize)
	}
	if int(bufferedTransactions.Get()) != 0 {
		t.Errorf("After all FakeBuffer() calls are done, BufferedTransactions got: %v; want: %v",
			bufferedTransactions.Get(), 0)
	}
}
