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

package throttler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntervalHistory_AverageIncludesPartialIntervals(t *testing.T) {
	// average() should include intervals which aren't fully covered by from and
	// to at least partially.
	h := newIntervalHistory(10, 1*time.Second)

	h.add(record{sinceZero(0 * time.Second), 10000000})
	h.add(record{sinceZero(1 * time.Second), 1000})
	h.add(record{sinceZero(2 * time.Second), 2000})
	h.add(record{sinceZero(3 * time.Second), 10000000})
	// Rate within [1s, 2s) = 1000 and within [2s, 3s) = 2000 = average of 1500
	want := 1500.0
	got := h.average(sinceZero(1500*time.Millisecond), sinceZero(2500*time.Millisecond))
	assert.Equal(t, want, got)
}

func TestIntervalHistory_AverageRangeSmallerThanInterval(t *testing.T) {
	h := newIntervalHistory(10, 1*time.Second)

	h.add(record{sinceZero(0 * time.Second), 10000})
	want := 10000.0
	got := h.average(sinceZero(250*time.Millisecond), sinceZero(750*time.Millisecond))
	assert.Equal(t, want, got)
}

func TestIntervalHistory_GapsCountedAsZero(t *testing.T) {
	h := newIntervalHistory(10, 1*time.Second)

	h.add(record{sinceZero(0 * time.Second), 1000})
	h.add(record{sinceZero(3 * time.Second), 1000})

	want := 500.0
	got := h.average(sinceZero(0*time.Second), sinceZero(4*time.Second))
	assert.Equal(t, want, got)
}

func TestIntervalHistory_AddNoDuplicateInterval(t *testing.T) {
	defer func() {
		r := recover()
		require.NotNil(t, r, "add() did not panic")

		want := "BUG: cannot add record because it is already covered by a previous entry"
		require.Contains(t, r, want, "add() did panic for the wrong reason")
	}()

	h := newIntervalHistory(10, 1*time.Second)

	h.add(record{sinceZero(0 * time.Second), 1000})
	h.add(record{sinceZero(100 * time.Millisecond), 1000})
}

func TestIntervalHistory_RecordDoesNotStartAtInterval(t *testing.T) {
	defer func() {
		r := recover()
		require.NotNil(t, r, "add() did not panic")

		want := "BUG: cannot add record because it does not start at the beginning of the interval"
		require.Contains(t, r, want, "add() did panic for the wrong reason")
	}()

	h := newIntervalHistory(1, 1*time.Second)

	h.add(record{sinceZero(10 * time.Millisecond), 1000})
}
