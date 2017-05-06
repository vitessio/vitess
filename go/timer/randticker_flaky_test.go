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

package timer

import (
	"testing"
	"time"
)

const (
	testDuration = 100 * time.Millisecond
	testVariance = 20 * time.Millisecond
)

func TestTick(t *testing.T) {
	tkr := NewRandTicker(testDuration, testVariance)
	for i := 0; i < 5; i++ {
		start := time.Now()
		end := <-tkr.C
		diff := start.Add(testDuration).Sub(end)
		tolerance := testVariance + 20*time.Millisecond
		if diff < -tolerance || diff > tolerance {
			t.Errorf("start: %v, end: %v, diff %v. Want <%v tolerenace", start, end, diff, tolerance)
		}
	}
	tkr.Stop()
	_, ok := <-tkr.C
	if ok {
		t.Error("Channel was not closed")
	}
}

func TestTickSkip(t *testing.T) {
	tkr := NewRandTicker(10*time.Millisecond, 1*time.Millisecond)
	time.Sleep(35 * time.Millisecond)
	end := <-tkr.C
	diff := time.Now().Sub(end)
	if diff < 20*time.Millisecond {
		t.Errorf("diff: %v, want >20ms", diff)
	}

	// This tick should be up-to-date
	end = <-tkr.C
	diff = time.Now().Sub(end)
	if diff > 1*time.Millisecond {
		t.Errorf("diff: %v, want <1ms", diff)
	}
	tkr.Stop()
}
