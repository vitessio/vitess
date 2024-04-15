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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/discovery"
)

// TestReplicationLagCache tests that the ring buffer in "replicationLagHistory"
// wraps around correctly.
// Other parts of the code are already covered by
// max_replication_lag_module_test.go.
func TestReplicationLagCache(t *testing.T) {
	c := newReplicationLagCache(2)
	r1Key := discovery.TabletToMapKey(tabletStats(r1, 1).Tablet)

	// If there is no entry yet, a zero struct is returned.
	zeroEntry := c.atOrAfter(r1Key, sinceZero(0*time.Second))
	require.True(t, zeroEntry.isZero(), "atOrAfter(<non existent key>) should have returned a zero entry")

	// First entry at 1s.
	c.add(lagRecord(sinceZero(1*time.Second), r1, 1))
	got, want := c.latest(r1Key).time, sinceZero(1*time.Second)
	require.Equal(t, want, got)

	// Second entry at 2s makes the cache full.
	c.add(lagRecord(sinceZero(2*time.Second), r1, 2))
	got, want = c.latest(r1Key).time, sinceZero(2*time.Second)
	require.Equal(t, want, got)

	got, want = c.atOrAfter(r1Key, sinceZero(1*time.Second)).time, sinceZero(1*time.Second)
	require.Equal(t, want, got)

	// Third entry at 3s evicts the 1s entry.
	c.add(lagRecord(sinceZero(3*time.Second), r1, 3))
	got, want = c.latest(r1Key).time, sinceZero(3*time.Second)
	require.Equal(t, want, got)

	// Requesting an entry at 1s or after gets us the entry for 2s.
	got, want = c.atOrAfter(r1Key, sinceZero(1*time.Second)).time, sinceZero(2*time.Second)
	require.Equal(t, want, got)

	// Wrap around one more time. Entries at 4s and 5s should be left.
	c.add(lagRecord(sinceZero(4*time.Second), r1, 4))
	c.add(lagRecord(sinceZero(5*time.Second), r1, 5))

	got, want = c.latest(r1Key).time, sinceZero(5*time.Second)
	require.Equal(t, want, got)

	got, want = c.atOrAfter(r1Key, sinceZero(1*time.Second)).time, sinceZero(4*time.Second)
	require.Equal(t, want, got)
}

func TestReplicationLagCache_SortByLag(t *testing.T) {
	c := newReplicationLagCache(2)
	r1Key := discovery.TabletToMapKey(tabletStats(r1, 1).Tablet)

	c.add(lagRecord(sinceZero(1*time.Second), r1, 30))
	c.sortByLag(1 /* ignoreNSlowestReplicas */, 30 /* minimumReplicationLag */)

	require.False(t, c.slowReplicas[r1Key], "the only replica tracked should not get ignored")

	c.add(lagRecord(sinceZero(1*time.Second), r2, 1))
	c.sortByLag(1 /* ignoreNSlowestReplicas */, 1 /* minimumReplicationLag */)

	require.True(t, c.slowReplicas[r1Key], "r1 should be tracked as a slow replica")
}
