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
)

// TestReplicationLagCache tests that the ring buffer in "replicationLagHistory"
// wraps around correctly.
// Other parts of the code are already covered by
// max_replication_lag_module_test.go.
func TestReplicationLagCache(t *testing.T) {
	c := newReplicationLagCache(2)
	r1Key := tabletStats(r1, 1).Key

	// If there is no entry yet, a zero struct is returned.
	zeroEntry := c.atOrAfter(r1Key, sinceZero(0*time.Second))
	if !zeroEntry.isZero() {
		t.Fatalf("atOrAfter(<non existent key>) should have returned a zero entry but did not: %v", zeroEntry)
	}

	// First entry at 1s.
	c.add(lagRecord(sinceZero(1*time.Second), r1, 1))
	if got, want := c.latest(r1Key).time, sinceZero(1*time.Second); got != want {
		t.Fatalf("latest(r1) = %v, want = %v", got, want)
	}

	// Second entry at 2s makes the cache full.
	c.add(lagRecord(sinceZero(2*time.Second), r1, 2))
	if got, want := c.latest(r1Key).time, sinceZero(2*time.Second); got != want {
		t.Fatalf("latest(r1) = %v, want = %v", got, want)
	}
	if got, want := c.atOrAfter(r1Key, sinceZero(1*time.Second)).time, sinceZero(1*time.Second); got != want {
		t.Fatalf("atOrAfter(r1) = %v, want = %v", got, want)
	}

	// Third entry at 3s evicts the 1s entry.
	c.add(lagRecord(sinceZero(3*time.Second), r1, 3))
	if got, want := c.latest(r1Key).time, sinceZero(3*time.Second); got != want {
		t.Fatalf("latest(r1) = %v, want = %v", got, want)
	}
	// Requesting an entry at 1s or after gets us the entry for 2s.
	if got, want := c.atOrAfter(r1Key, sinceZero(1*time.Second)).time, sinceZero(2*time.Second); got != want {
		t.Fatalf("atOrAfter(r1) = %v, want = %v", got, want)
	}

	// Wrap around one more time. Entries at 4s and 5s should be left.
	c.add(lagRecord(sinceZero(4*time.Second), r1, 4))
	c.add(lagRecord(sinceZero(5*time.Second), r1, 5))
	if got, want := c.latest(r1Key).time, sinceZero(5*time.Second); got != want {
		t.Fatalf("latest(r1) = %v, want = %v", got, want)
	}
	if got, want := c.atOrAfter(r1Key, sinceZero(1*time.Second)).time, sinceZero(4*time.Second); got != want {
		t.Fatalf("atOrAfter(r1) = %v, want = %v", got, want)
	}
}

func TestReplicationLagCache_SortByLag(t *testing.T) {
	c := newReplicationLagCache(2)
	r1Key := tabletStats(r1, 1).Key

	c.add(lagRecord(sinceZero(1*time.Second), r1, 30))
	c.sortByLag(1 /* ignoreNSlowestReplicas */, 30 /* minimumReplicationLag */)

	if c.slowReplicas[r1Key] {
		t.Fatal("the only replica tracked should not get ignored")
	}

	c.add(lagRecord(sinceZero(1*time.Second), r2, 1))
	c.sortByLag(1 /* ignoreNSlowestReplicas */, 1 /* minimumReplicationLag */)

	if !c.slowReplicas[r1Key] {
		t.Fatal("r1 should be tracked as a slow replica")
	}
}
