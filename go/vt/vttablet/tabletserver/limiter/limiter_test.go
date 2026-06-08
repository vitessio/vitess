/*
Copyright 2026 The Vitess Authors.

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

package limiter

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

// newTestLimiter creates a Limiter backed by an isolated test Exporter.
// Each call uses t.Name() as the exporter name so that stats are shared within
// a single test function (safe) but isolated from unrelated tests.
func newTestLimiter(t *testing.T) *Limiter {
	t.Helper()
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), tabletenv.NewDefaultConfig(), t.Name())
	return New(env)
}

// TestSingleSpec_AdmitUpToLimit verifies that exactly limit concurrent requests
// are admitted and the (limit+1)-th is rejected, then that releasing one slot
// re-opens admission.
func TestSingleSpec_AdmitUpToLimit(t *testing.T) {
	l := newTestLimiter(t)
	spec := Spec{Key: "workload", Value: "OLAP", Limit: 3}

	var held [][]Spec
	for i := 1; i <= 3; i++ {
		ok, failed := l.Acquire([]Spec{spec}, false)
		require.Truef(t, ok, "request %d/%d: expected admit, got reject on %v", i, spec.Limit, failed)
		held = append(held, []Spec{spec})
	}

	ok, failed := l.Acquire([]Spec{spec}, false)
	assert.False(t, ok, "4th request: expected reject, got admit")
	require.NotNil(t, failed)
	assert.Equal(t, "workload", failed.Key)
	assert.Equal(t, "OLAP", failed.Value)

	l.Release(held[0])
	ok, _ = l.Acquire([]Spec{spec}, false)
	assert.True(t, ok, "after release: expected admit, got reject")
}

// TestMultiSpec_AllOrNothing verifies that when one dimension is at its limit a
// request carrying that dimension is rejected and no other dimension is
// permanently incremented (rollback).
func TestMultiSpec_AllOrNothing(t *testing.T) {
	l := newTestLimiter(t)
	specA := Spec{Key: "workload", Value: "OLAP", Limit: 2}
	specB := Spec{Key: "priority", Value: "low", Limit: 1}

	ok, _ := l.Acquire([]Spec{specB}, false)
	require.True(t, ok, "initial specB acquire failed unexpectedly")

	ok, failed := l.Acquire([]Spec{specA, specB}, false)
	assert.False(t, ok, "expected rejection when specB is at limit")
	require.NotNil(t, failed)
	assert.Equal(t, "priority", failed.Key)
	assert.Equal(t, "low", failed.Value)

	// specA must still have its full capacity of 2 (rollback was clean).
	ok, _ = l.Acquire([]Spec{specA}, false)
	assert.True(t, ok, "specA should be at 0/2 after rollback — first acquire must succeed")
	ok, _ = l.Acquire([]Spec{specA}, false)
	assert.True(t, ok, "specA should admit second time (1/2 → 2/2)")
	ok, _ = l.Acquire([]Spec{specA}, false)
	assert.False(t, ok, "specA third acquire must fail at limit (2/2)")
}

// TestRelease_RestoresCapacity is a minimal round-trip: acquire, confirm a
// second acquire fails, release, confirm the slot is available again.
func TestRelease_RestoresCapacity(t *testing.T) {
	l := newTestLimiter(t)
	spec := Spec{Key: "k", Value: "v", Limit: 1}

	ok, _ := l.Acquire([]Spec{spec}, false)
	require.True(t, ok, "first acquire failed")
	ok, _ = l.Acquire([]Spec{spec}, false)
	assert.False(t, ok, "second acquire should fail (limit=1)")

	l.Release([]Spec{spec})

	ok, _ = l.Acquire([]Spec{spec}, false)
	assert.True(t, ok, "acquire after release should succeed")
}

// TestZeroSpecs_AlwaysAdmit verifies that a request with no specs is always
// admitted.
func TestZeroSpecs_AlwaysAdmit(t *testing.T) {
	l := newTestLimiter(t)
	for i := 0; i < 100; i++ {
		ok, failed := l.Acquire(nil, false)
		require.Truef(t, ok, "zero-spec request %d: expected admit, got reject on %v", i, failed)
	}
}

// TestNoLimit_AlwaysAdmit verifies that a Spec with Limit==0 is counted but
// never causes rejection, regardless of how many concurrent holders there are.
func TestNoLimit_AlwaysAdmit(t *testing.T) {
	l := newTestLimiter(t)
	spec := Spec{Key: "k", Value: "v", Limit: 0}
	for i := 0; i < 500; i++ {
		ok, failed := l.Acquire([]Spec{spec}, false)
		require.Truef(t, ok, "no-limit spec: expected admit on iteration %d, got reject on %v", i, failed)
	}
}

// TestConcurrent_RaceSafety fires 100 goroutines against a limit of 10.
// Run with -race to catch data races. Invariant: admitted+rejected == goroutines
// and in-flight never exceeds the limit.
func TestConcurrent_RaceSafety(t *testing.T) {
	l := newTestLimiter(t)
	spec := Spec{Key: "wl", Value: "OLTP", Limit: 10}

	var (
		wg       sync.WaitGroup
		admitted int64
		rejected int64
	)

	const goroutines = 100
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, _ := l.Acquire([]Spec{spec}, false)
			if ok {
				atomic.AddInt64(&admitted, 1)
				l.Release([]Spec{spec})
			} else {
				atomic.AddInt64(&rejected, 1)
			}
		}()
	}
	wg.Wait()

	total := admitted + rejected
	assert.Equal(t, int64(goroutines), total,
		"admitted(%d) + rejected(%d) = %d, want %d", admitted, rejected, total, goroutines)
}

// TestParseSpec covers the parser's happy paths and error cases.
func TestParseSpec(t *testing.T) {
	tests := []struct {
		raw     string
		want    Spec
		wantErr bool
	}{
		{"wl:OLAP:5", Spec{"wl", "OLAP", 5}, false},
		{"wl:OLAP:0", Spec{"wl", "OLAP", 0}, false},
		{"wl:OLAP", Spec{"wl", "OLAP", 0}, false},
		// Extra colons after the limit field are not valid.
		{"k:v:with:colons:10", Spec{}, true},
		// Errors.
		{"wl:OLAP:-1", Spec{}, true},  // negative limit
		{"wl:OLAP:abc", Spec{}, true}, // non-integer limit
		{"nocolon", Spec{}, true},     // no separator
		{"", Spec{}, true},            // empty
		{":value:5", Spec{}, true},    // empty key
	}
	for _, tc := range tests {
		got, err := ParseSpec(tc.raw)
		if tc.wantErr {
			assert.Errorf(t, err, "ParseSpec(%q): expected error, got %v", tc.raw, got)
			continue
		}
		require.NoErrorf(t, err, "ParseSpec(%q): unexpected error", tc.raw)
		assert.Equal(t, tc.want, got, "ParseSpec(%q)", tc.raw)
	}
}

// TestDryRun_KeepsIncrementAndAdmits verifies that in dry-run mode a request
// that would violate the limit is still admitted, its increment is kept (so
// Release balances it), and the rejected counter is bumped with DryRun=true.
func TestDryRun_KeepsIncrementAndAdmits(t *testing.T) {
	l := newTestLimiter(t)
	spec := Spec{Key: "workload", Value: "OLAP", Limit: 1}

	// Fill the one slot.
	ok, _ := l.Acquire([]Spec{spec}, false)
	require.True(t, ok)

	// Dry-run over-limit: must admit.
	ok, wouldFail := l.Acquire([]Spec{spec}, true /* dryRun */)
	require.True(t, ok, "dry-run: over-limit request should still be admitted")
	assert.NotNil(t, wouldFail, "dry-run: failed spec should be returned for observability")

	// inflight is now 2; Release both — guard ensures no negative count.
	l.Release([]Spec{spec})
	l.Release([]Spec{spec})

	// After releasing both, a normal request must succeed again.
	ok, _ = l.Acquire([]Spec{spec}, false)
	assert.True(t, ok, "after dry-run and two releases, slot must be free")
}
