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

package masterbuffer

import (
	"sync"
	"testing"
	"time"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
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
		desc             string
		enableFakeBuffer bool
		keyspace         string
		shard            string
		tabletType       topodatapb.TabletType
		inTransaction    bool
		attemptNumber    int
		bufferedRequests int
		// was this request buffered?
		wantCalled bool
		// expected value of BufferedRequestsAttempts
		wantAttempted int
		wantErr       error
	}{
		{
			desc:             "enableFakeBuffer=False",
			enableFakeBuffer: false,
		},
		{
			desc:             "tabletType=REPLICA",
			enableFakeBuffer: true,
			tabletType:       topodatapb.TabletType_REPLICA,
		},
		{
			desc:             "inTransaction=True",
			enableFakeBuffer: true,
			inTransaction:    true,
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
			desc:             "buffer full",
			enableFakeBuffer: true,
			keyspace:         bufferedKeyspace,
			shard:            bufferedShard,
			bufferedRequests: *maxBufferSize,
			// When the buffer is full, bufferedRequestsAttempted should still be incremented
			wantAttempted: 1,
			wantErr:       errBufferFull,
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
		bufferedRequestsAttempted.Set(0)
		bufferedRequestsSuccessful.Set(0)
		bufferedRequests.Set(int64(test.bufferedRequests))

		*enableFakeMasterBuffer = test.enableFakeBuffer

		var tabletType topodatapb.TabletType
		// Default to MASTER tablet type.
		if test.tabletType == topodatapb.TabletType_UNKNOWN {
			tabletType = topodatapb.TabletType_MASTER
		}

		target := &querypb.Target{
			Keyspace:   test.keyspace,
			Shard:      test.shard,
			TabletType: tabletType,
		}
		gotErr := FakeBuffer(target, test.inTransaction, test.attemptNumber)

		if gotErr != test.wantErr {
			t.Errorf("With %v, FakeBuffer() => %v; want: %v", test.desc, gotErr, test.wantErr)
		}

		if controller.called != test.wantCalled {
			t.Errorf("With %v, FakeBuffer() => timeSleep.called: %v; want: %v",
				test.desc, controller.called, test.wantCalled)
		}

		if bufferedRequestsAttempted.Get() != int64(test.wantAttempted) {
			t.Errorf("With %v, FakeBuffer() => bufferedRequestsAttempted got: %v; want: %v",
				test.desc, bufferedRequestsAttempted.Get(), test.wantAttempted)
		}

		if (!test.wantCalled && (bufferedRequestsSuccessful.Get() == 1)) ||
			(test.wantCalled && (bufferedRequestsSuccessful.Get() != 1)) {
			t.Errorf("With %v, FakeBuffer() => bufferedRequestsSuccessful got: %v; want: 1",
				test.desc, bufferedRequestsSuccessful.Get())
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
	*enableFakeMasterBuffer = true

	// reset counters
	bufferedRequestsAttempted.Set(0)
	bufferedRequestsSuccessful.Set(0)

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
		var gotErr error
		go func() {
			defer wg.Done()
			target := &querypb.Target{
				Keyspace:   *bufferKeyspace,
				Shard:      *bufferShard,
				TabletType: topodatapb.TabletType_MASTER,
			}
			gotErr = FakeBuffer(target, false, 0)
			close(finished)
		}()

		// wait until either the gorouotine is blocked (because it's buffering) or until
		// it's finished (because it shouldn't be buffered).
		select {
		case <-controller.blocked:
		case <-finished:
		}

		if controller.called {
			controllers = append(controllers, controller)
		} else {
			// if we didn't call fakeSleep, the buffer is full and should return an error saying so.
			if gotErr != errBufferFull {
				t.Errorf("On iteration %v, FakeBuffer() => %v; want: %v", i, gotErr, errBufferFull)
			}
		}

		if controller.called != wantFakeSleepCalled {
			t.Errorf("On iteration %v, FakeBuffer() => timeSleep.called: %v; want: %v",
				i, controller.called, wantFakeSleepCalled)
		}

		if int(bufferedRequestsAttempted.Get()) != i {
			t.Errorf("On iteration %v, FakeBuffer() => bufferedRequestsAttempted got: %v; want: %v",
				i, bufferedRequestsAttempted.Get(), i)
		}

		if int(bufferedRequests.Get()) != min(i, *maxBufferSize) {
			t.Errorf("On iteration %v, FakeBuffer() => bufferedRequests got: %v; want: %v",
				i, bufferedRequests.Get(), min(i, *maxBufferSize))
		}

		if int(bufferedRequestsSuccessful.Get()) != 0 {
			t.Errorf("On iteration %v, FakeBuffer() => bufferedRequestsSuccessful got: %v; want: 0",
				i, bufferedRequestsSuccessful.Get())
		}
	}

	// signal to all the buffered calls that they can stop buffering, and wait for them.
	for _, c := range controllers {
		close(c.done)
	}
	wg.Wait()

	if int(bufferedRequestsSuccessful.Get()) != *maxBufferSize {
		t.Errorf("After all FakeBuffer() calls are done, bufferedRequestsSuccessful got: %v; want: %v",
			bufferedRequestsSuccessful.Get(), *maxBufferSize)
	}
	if int(bufferedRequests.Get()) != 0 {
		t.Errorf("After all FakeBuffer() calls are done, bufferedRequests got: %v; want: %v",
			bufferedRequests.Get(), 0)
	}
}
