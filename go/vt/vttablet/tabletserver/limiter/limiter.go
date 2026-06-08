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

// Package limiter implements a multi-dimensional in-flight concurrency limiter
// for use in vttablet's query execution pipeline.
//
// Each query may carry one or more [Spec] values encoded in a SQL directive
// (/*vt+ CONCURRENCY=key:value:limit[,...] */). [Limiter.Acquire] increments
// the in-flight counter for every dimension; if all counters remain within
// their stated limits the request is admitted. If any counter would exceed
// its limit, all increments are rolled back atomically and the first failing
// spec is returned. [Limiter.Release] must be called exactly once after a
// successful [Limiter.Acquire].
//
// Dry-run mode: when dryRun is true, Acquire keeps the increment even on a
// limit violation and returns ok=true so the caller always admits. This keeps
// the concurrency_level_reached histogram honest and guarantees every increment
// is balanced by exactly one Release.
package limiter

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"vitess.io/vitess/go/stats"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

// Spec describes one dimension of a concurrency constraint.
// Limit == 0 means the dimension is observed (counted and gauged) but never
// causes rejection.
type Spec struct {
	Key   string
	Value string
	Limit int64
}

// String returns the canonical "key:value:limit" form.
func (s Spec) String() string {
	return fmt.Sprintf("%s:%s:%d", s.Key, s.Value, s.Limit)
}

// ParseSpec parses "key:value" or "key:value:limit".
// Limit must be a non-negative integer; omitting it is equivalent to Limit=0.
func ParseSpec(raw string) (Spec, error) {
	parts := strings.SplitN(raw, ":", 3)
	if len(parts) < 2 || parts[0] == "" {
		return Spec{}, fmt.Errorf("spec %q: must be key:value or key:value:limit (key must not be empty)", raw)
	}
	if len(parts) == 2 {
		return Spec{Key: parts[0], Value: parts[1]}, nil
	}
	limit, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil || limit < 0 {
		return Spec{}, fmt.Errorf("spec %q: limit must be a non-negative integer", raw)
	}
	return Spec{Key: parts[0], Value: parts[1], Limit: limit}, nil
}

// mapKey returns a collision-free internal map key for a key/value pair.
func mapKey(key, value string) string { return key + "\x00" + value }

// RejectedError returns a RESOURCE_EXHAUSTED error naming the concurrency dimension that
// prevented admission.
func RejectedError(failed *Spec) error {
	return vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED,
		"concurrency limit exceeded for %s:%s (limit %d)", failed.Key, failed.Value, failed.Limit)
}

type limiterStats struct {
	concurrency     *stats.GaugesWithMultiLabels
	levelReached    *stats.CountersWithMultiLabels
	rejected        *stats.CountersWithMultiLabels
	admitted        *stats.CountersWithMultiLabels
	specParseErrors *stats.Counter
}

// Limiter is a multi-dimensional in-flight concurrency limiter.
// It is safe for concurrent use by multiple goroutines.
type Limiter struct {
	mu       sync.Mutex
	inflight map[string]int64 // mapKey(k,v) → current count
	s        limiterStats
}

// New creates a Limiter whose stats are registered via env's Exporter.
// The Exporter bridges all stats to the Prometheus /metrics endpoint
// automatically; prometheusbackend.Init need not be called here.
func New(env tabletenv.Env) *Limiter {
	exp := env.Exporter()
	return &Limiter{
		inflight: make(map[string]int64),
		s: limiterStats{
			concurrency: exp.NewGaugesWithMultiLabels(
				"LimiterConcurrency",
				"Current in-flight requests per concurrency dimension.",
				[]string{"Key"},
			),
			levelReached: exp.NewCountersWithMultiLabels(
				"LimiterConcurrencyLevelReached",
				"Times the in-flight count reached exactly level N for a given dimension. "+
					"Plot as a histogram to decide on limits.",
				[]string{"Key", "Level"},
			),
			rejected: exp.NewCountersWithMultiLabels(
				"LimiterRejected",
				"Requests rejected because a concurrency limit was exceeded.",
				[]string{"Key", "Value", "DryRun"},
			),
			admitted: exp.NewCountersWithMultiLabels(
				"LimiterAdmitted",
				"Requests admitted (all concurrency limits satisfied).",
				[]string{"Key"},
			),
			specParseErrors: exp.NewCounter(
				"LimiterSpecParseErrors",
				"Number of times a CONCURRENCY directive could not be parsed.",
			),
		},
	}
}

// Acquire attempts to admit the request described by specs.
//
// When dryRun is false: all-or-nothing admission under the mutex. If every
// spec's in-flight count stays within its limit, the request is admitted and
// (true, nil) is returned. On the first violation all increments are rolled
// back and (false, &failedSpec) is returned; the caller must NOT call Release.
//
// When dryRun is true: increments are always kept. On a limit violation the
// rejected counter is bumped (DryRun=true), but (true, violatingSpec) is still
// returned — the caller must call Release regardless. This keeps the level-
// reached histogram accurate for limit-setting purposes.
//
// Specs with Limit==0 are counted and gauged but never cause rejection.
// An empty specs slice is always admitted.
func (l *Limiter) Acquire(specs []Spec, dryRun bool) (ok bool, failed *Spec) {
	if len(specs) == 0 {
		return true, nil
	}

	levels := make([]int64, len(specs))

	l.mu.Lock()

	// Pass 1: increment every in-flight counter and record the resulting level.
	for i, s := range specs {
		k := mapKey(s.Key, s.Value)
		l.inflight[k]++
		levels[i] = l.inflight[k]
	}

	// Pass 2: check limits; on first violation either roll back (normal) or keep (dry-run).
	for i, s := range specs {
		if s.Limit > 0 && levels[i] > s.Limit {
			sp := specs[i] // copy before unlock
			if !dryRun {
				// Normal mode: roll back all increments.
				for _, s2 := range specs {
					k := mapKey(s2.Key, s2.Value)
					l.inflight[k]--
					if l.inflight[k] == 0 {
						delete(l.inflight, k)
					}
				}
				l.mu.Unlock()
				l.s.rejected.Add([]string{sp.Key, sp.Value, "0"}, 1)
				return false, &sp
			}
			// Dry-run mode: keep increments, record would-reject, admit.
			l.mu.Unlock()
			l.s.rejected.Add([]string{sp.Key, sp.Value, "1"}, 1)
			// Still record admitted + levels outside the lock below.
			l.recordAdmitted(specs, levels)
			return true, &sp
		}
	}

	l.mu.Unlock()

	// Stats updates outside the lock. levels[i] was captured under the lock so
	// its value is the true in-flight count at the moment this request was
	// admitted; it is the correct level to record even if inflight has moved on.
	l.recordAdmitted(specs, levels)
	return true, nil
}

// recordAdmitted updates the level-reached, concurrency gauge, and admitted
// counters for an admitted request. Must be called outside the mutex.
func (l *Limiter) recordAdmitted(specs []Spec, levels []int64) {
	for i, s := range specs {
		n := levels[i]
		l.s.concurrency.Set([]string{s.Key}, n)
		l.s.levelReached.Add([]string{s.Key, strconv.FormatInt(n, 10)}, 1)
		l.s.admitted.Add([]string{s.Key}, 1)
	}
}

// Release decrements the in-flight counter for each spec and updates the
// concurrency gauge. It must be called exactly once per successful Acquire
// (including dry-run Acquires that reported a violation but still admitted).
func (l *Limiter) Release(specs []Spec) {
	if len(specs) == 0 {
		return
	}

	counts := make([]int64, len(specs))

	l.mu.Lock()
	for i, s := range specs {
		k := mapKey(s.Key, s.Value)
		if l.inflight[k] > 0 {
			l.inflight[k]--
		}
		counts[i] = l.inflight[k]
		if counts[i] == 0 {
			delete(l.inflight, k)
		}
	}
	l.mu.Unlock()

	for i, s := range specs {
		l.s.concurrency.Set([]string{s.Key}, counts[i])
	}
}

// IncSpecParseErrors increments the spec-parse-error counter. Called from the
// plan-builder when a CONCURRENCY directive cannot be parsed.
func (l *Limiter) IncSpecParseErrors() {
	l.s.specParseErrors.Add(1)
}
