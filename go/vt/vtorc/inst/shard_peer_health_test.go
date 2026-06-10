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

package inst

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/protoutil"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func alias(uid uint32) *topodatapb.TabletAlias {
	return &topodatapb.TabletAlias{Cell: "zone1", Uid: uid}
}

func reportFor(primary *topodatapb.TabletAlias, failures int64, attemptedAgo time.Duration, now time.Time) []*replicationdatapb.ShardPeerHealth {
	return []*replicationdatapb.ShardPeerHealth{{
		TabletAlias:                primary,
		ConsecutivePingFailures:    failures,
		LastAttemptedPing:          protoutil.TimeToProto(now.Add(-attemptedAgo)),
		TimeSinceLastAttemptedPing: protoutil.DurationToProto(attemptedAgo),
	}}
}

func defaultOpts() QuorumOptions {
	return QuorumOptions{FailureThreshold: 3, Freshness: 5 * time.Second, Fraction: 1.0, MinObservers: 1}
}

func TestPrimaryDownByQuorum(t *testing.T) {
	now := time.Now()
	primary := alias(100)

	tests := []struct {
		name     string
		seed     func()
		opts     QuorumOptions
		expected bool
	}{
		{
			name: "both replicas report down -> quorum",
			seed: func() {
				resetShardPeerHealth()
				RecordShardPeerHealth(alias(101), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 3, 0, now), now)
				RecordShardPeerHealth(alias(102), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 5, 0, now), now)
			},
			opts:     defaultOpts(),
			expected: true,
		},
		{
			name: "one down one up, unanimous default -> no quorum",
			seed: func() {
				resetShardPeerHealth()
				RecordShardPeerHealth(alias(101), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 3, 0, now), now)
				RecordShardPeerHealth(alias(102), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 0, 0, now), now)
			},
			opts:     defaultOpts(),
			expected: false,
		},
		{
			name: "one down one up, fraction 0.5 -> quorum",
			seed: func() {
				resetShardPeerHealth()
				RecordShardPeerHealth(alias(101), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 3, 0, now), now)
				RecordShardPeerHealth(alias(102), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 0, 0, now), now)
			},
			opts:     QuorumOptions{FailureThreshold: 3, Freshness: 5 * time.Second, Fraction: 0.5, MinObservers: 1},
			expected: true,
		},
		{
			name: "rdonly observers count",
			seed: func() {
				resetShardPeerHealth()
				RecordShardPeerHealth(alias(101), topodatapb.TabletType_RDONLY, "ks", "0", reportFor(primary, 4, 0, now), now)
			},
			opts:     defaultOpts(),
			expected: true,
		},
		{
			name: "stale observer discounted -> below min observers",
			seed: func() {
				resetShardPeerHealth()
				// recorded 1 minute ago, freshness is 5s
				RecordShardPeerHealth(alias(101), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 3, 0, now), now.Add(-time.Minute))
			},
			opts:     defaultOpts(),
			expected: false,
		},
		{
			name: "fresh observer with stale primary ping is discounted",
			seed: func() {
				resetShardPeerHealth()
				RecordShardPeerHealth(alias(101), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 3, time.Minute, now), now)
			},
			opts:     defaultOpts(),
			expected: false,
		},
		{
			name: "missing primary ping timestamp is discounted",
			seed: func() {
				resetShardPeerHealth()
				RecordShardPeerHealth(alias(101), topodatapb.TabletType_REPLICA, "ks", "0", []*replicationdatapb.ShardPeerHealth{{
					TabletAlias:             primary,
					ConsecutivePingFailures: 3,
				}}, now)
			},
			opts:     defaultOpts(),
			expected: false,
		},
		{
			name: "ping age fallback from absolute timestamp (older tablet)",
			seed: func() {
				resetShardPeerHealth()
				// No time_since_last_attempted_ping: the age is derived once at ingest from the
				// absolute timestamp, so a recent ping still counts.
				RecordShardPeerHealth(alias(101), topodatapb.TabletType_REPLICA, "ks", "0", []*replicationdatapb.ShardPeerHealth{{
					TabletAlias:             primary,
					ConsecutivePingFailures: 3,
					LastAttemptedPing:       protoutil.TimeToProto(now),
				}}, now)
			},
			opts:     defaultOpts(),
			expected: true,
		},
		{
			name: "negative reported ping age fails closed",
			seed: func() {
				resetShardPeerHealth()
				RecordShardPeerHealth(alias(101), topodatapb.TabletType_REPLICA, "ks", "0", []*replicationdatapb.ShardPeerHealth{{
					TabletAlias:                primary,
					ConsecutivePingFailures:    3,
					TimeSinceLastAttemptedPing: protoutil.DurationToProto(-10 * time.Second),
				}}, now)
			},
			opts:     defaultOpts(),
			expected: false,
		},
		{
			name: "ping age within freshness after record aging",
			seed: func() {
				resetShardPeerHealth()
				// Ping was 2s old when the report was ingested 2s ago: effective age 4s <= 5s freshness.
				RecordShardPeerHealth(alias(101), topodatapb.TabletType_REPLICA, "ks", "0",
					reportFor(primary, 3, 2*time.Second, now.Add(-2*time.Second)), now.Add(-2*time.Second))
			},
			opts:     defaultOpts(),
			expected: true,
		},
		{
			name: "ping age accumulates past freshness with record age",
			seed: func() {
				resetShardPeerHealth()
				// Ping was 2s old when the report was ingested 4s ago: effective age 6s > 5s freshness,
				// even though the record itself (4s) is still fresh.
				RecordShardPeerHealth(alias(101), topodatapb.TabletType_REPLICA, "ks", "0",
					reportFor(primary, 3, 2*time.Second, now.Add(-4*time.Second)), now.Add(-4*time.Second))
			},
			opts:     defaultOpts(),
			expected: false,
		},
		{
			name: "failures below threshold -> up",
			seed: func() {
				resetShardPeerHealth()
				RecordShardPeerHealth(alias(101), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 2, 0, now), now)
				RecordShardPeerHealth(alias(102), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 2, 0, now), now)
			},
			opts:     defaultOpts(),
			expected: false,
		},
		{
			name: "min observers not met -> no quorum",
			seed: func() {
				resetShardPeerHealth()
				RecordShardPeerHealth(alias(101), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 3, 0, now), now)
			},
			opts:     QuorumOptions{FailureThreshold: 3, Freshness: 5 * time.Second, Fraction: 1.0, MinObservers: 2},
			expected: false,
		},
		{
			name: "zero failure threshold fails closed",
			seed: func() {
				resetShardPeerHealth()
				RecordShardPeerHealth(alias(101), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 0, 0, now), now)
			},
			opts:     QuorumOptions{FailureThreshold: 0, Freshness: 5 * time.Second, Fraction: 1.0, MinObservers: 1},
			expected: false,
		},
		{
			name: "zero quorum fraction fails closed",
			seed: func() {
				resetShardPeerHealth()
				RecordShardPeerHealth(alias(101), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 0, 0, now), now)
			},
			opts:     QuorumOptions{FailureThreshold: 3, Freshness: 5 * time.Second, Fraction: 0, MinObservers: 1},
			expected: false,
		},
		{
			name: "negative freshness fails closed",
			seed: func() {
				resetShardPeerHealth()
				RecordShardPeerHealth(alias(101), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 3, 0, now), now)
			},
			opts:     QuorumOptions{FailureThreshold: 3, Freshness: -time.Second, Fraction: 1.0, MinObservers: 1},
			expected: false,
		},
		{
			name: "observers in another shard ignored",
			seed: func() {
				resetShardPeerHealth()
				RecordShardPeerHealth(alias(201), topodatapb.TabletType_REPLICA, "ks", "80-", reportFor(primary, 5, 0, now), now)
			},
			opts:     defaultOpts(),
			expected: false,
		},
		{
			name: "primary observer (self) does not vote",
			seed: func() {
				resetShardPeerHealth()
				// PRIMARY type observers are not counted as voters.
				RecordShardPeerHealth(alias(100), topodatapb.TabletType_PRIMARY, "ks", "0", reportFor(primary, 9, 0, now), now)
			},
			opts:     defaultOpts(),
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.seed()
			got := PrimaryDownByQuorum(primary, "ks", "0", tc.opts, now)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestEvaluatePrimaryQuorum(t *testing.T) {
	now := time.Now()
	primary := alias(100)
	resetShardPeerHealth()
	// 101 replica: fresh, 5 failures -> down
	RecordShardPeerHealth(alias(101), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 5, 0, now), now)
	// 102 rdonly: fresh, 0 failures -> up
	RecordShardPeerHealth(alias(102), topodatapb.TabletType_RDONLY, "ks", "0", reportFor(primary, 0, 0, now), now)
	// 103 replica: stale (recorded a minute ago) -> stale, not counted
	RecordShardPeerHealth(alias(103), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 9, 0, now), now.Add(-time.Minute))

	opts := QuorumOptions{FailureThreshold: 3, Freshness: 5 * time.Second, Fraction: 0.5, MinObservers: 1}
	r := EvaluatePrimaryQuorum(primary, "ks", "0", opts, now)

	assert.Equal(t, "zone1-0000000100", r.PrimaryAlias)
	assert.Equal(t, 2, r.TotalObservers) // 101 + 102 are fresh; 103 is stale
	assert.Equal(t, 1, r.DownVotes)      // only 101
	assert.True(t, r.Down)               // 1/2 >= 0.5
	require.Len(t, r.Observers, 3)       // all three are reported, incl. the stale one
	// sorted by alias: 101, 102, 103
	assert.Equal(t, "zone1-0000000101", r.Observers[0].Alias)
	assert.Equal(t, "down", r.Observers[0].Vote)
	assert.True(t, r.Observers[0].Fresh)
	assert.Equal(t, int64(5), r.Observers[0].ConsecutiveFailures)
	assert.Equal(t, "up", r.Observers[1].Vote)
	assert.Equal(t, "stale", r.Observers[2].Vote)
	assert.False(t, r.Observers[2].Fresh)

	// Summary renders this same evaluation for the decision log and the audit.
	s := r.Summary()
	assert.Contains(t, s, "ks/0 primary zone1-0000000100 DOWN")
	assert.Contains(t, s, "1/2 fresh observers down")
	assert.Contains(t, s, "(fraction 0.5, min 1)")
	assert.Contains(t, s, "zone1-0000000101=down(5)")
	assert.Contains(t, s, "zone1-0000000102=up(0)")
	assert.Contains(t, s, "zone1-0000000103=stale")

	// MinObservers gate: requiring more fresh observers than exist yields no down verdict, even
	// though a fresh observer reports the primary down. Reuses the same store (2 fresh, 1 down).
	gated := EvaluatePrimaryQuorum(primary, "ks", "0", QuorumOptions{FailureThreshold: 3, Freshness: 5 * time.Second, Fraction: 0.5, MinObservers: 3}, now)
	assert.False(t, gated.Down)
	assert.Equal(t, 2, gated.TotalObservers)
	assert.Equal(t, 1, gated.DownVotes)

	// A nil primary (e.g. the shard has no primary) yields an empty evaluation with no verdict.
	empty := EvaluatePrimaryQuorum(nil, "ks", "0", opts, now)
	assert.False(t, empty.Down)
	assert.Empty(t, empty.Observers)
}

func TestRecordShardPeerHealth(t *testing.T) {
	resetShardPeerHealth()
	now := time.Now()
	primary := alias(100)

	// Nil and empty entries are ignored entirely; nothing is recorded.
	RecordShardPeerHealth(alias(101), topodatapb.TabletType_REPLICA, "ks", "0", nil, now)
	RecordShardPeerHealth(alias(102), topodatapb.TabletType_REPLICA, "ks", "0", []*replicationdatapb.ShardPeerHealth{
		nil,
		{TabletAlias: nil, ConsecutivePingFailures: 3},
	}, now)
	assert.Empty(t, ObservedShards())

	// Valid reports surface their distinct shards, sorted.
	RecordShardPeerHealth(alias(101), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 3, 0, now), now)
	RecordShardPeerHealth(alias(201), topodatapb.TabletType_REPLICA, "ks", "80-", reportFor(alias(200), 1, 0, now), now)
	assert.Equal(t, []KeyspaceShard{{Keyspace: "ks", Shard: "0"}, {Keyspace: "ks", Shard: "80-"}}, ObservedShards())

	// Records older than the TTL are pruned, so deleted tablets do not accumulate.
	resetShardPeerHealth()
	RecordShardPeerHealth(alias(101), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 3, 0, now), now.Add(-2*time.Minute))
	assert.Empty(t, ObservedShards())
}
