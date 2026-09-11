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
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtorc/config"

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
		name              string
		seed              func()
		expectedObservers int
		opts              QuorumOptions
		expected          bool
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
		{
			// A SPARE observer must not vote: only REPLICA/RDONLY are shard-health observers. The lone
			// REPLICA reports down (a 1/1 majority of the real observer); if the SPARE were counted as
			// a (non-down) voter, the unanimous fraction would see 1/2 and not reach quorum.
			name: "spare observer does not vote",
			seed: func() {
				resetShardPeerHealth()
				RecordShardPeerHealth(alias(101), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 3, 0, now), now)
				RecordShardPeerHealth(alias(102), topodatapb.TabletType_SPARE, "ks", "0", reportFor(primary, 0, 0, now), now)
			},
			expectedObservers: 1,
			opts:              defaultOpts(),
			expected:          true,
		},
		{
			// The core safety property: one fresh down vote while the rest of the shard's
			// observers are stale must NOT reach a verdict — a minority cannot drive ERS.
			name: "single fresh down vote with stale majority -> no quorum",
			seed: func() {
				resetShardPeerHealth()
				RecordShardPeerHealth(alias(101), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 5, 0, now), now)
				// 102-105 reported a minute ago (freshness is 5s) -> stale, but still in the map.
				for _, uid := range []uint32{102, 103, 104, 105} {
					RecordShardPeerHealth(alias(uid), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 5, 0, now), now.Add(-time.Minute))
				}
			},
			expectedObservers: 5,
			opts:              defaultOpts(),
			expected:          false,
		},
		{
			// Even with no stale records in the map, a single fresh down vote must not act when
			// the shard is known (via topo) to have more eligible observers that never reported.
			name: "single fresh down vote with missing majority -> no quorum",
			seed: func() {
				resetShardPeerHealth()
				RecordShardPeerHealth(alias(101), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 5, 0, now), now)
			},
			expectedObservers: 5,
			opts:              defaultOpts(),
			expected:          false,
		},
		{
			// A majority of the shard freshly reporting down -> ERS may proceed. A stale or
			// missing minority does not block it (the ratio is over fresh reporters).
			name: "majority fresh down with stale minority -> quorum",
			seed: func() {
				resetShardPeerHealth()
				for _, uid := range []uint32{101, 102, 103} {
					RecordShardPeerHealth(alias(uid), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 5, 0, now), now)
				}
				RecordShardPeerHealth(alias(104), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 5, 0, now), now.Add(-time.Minute))
			},
			expectedObservers: 5,
			opts:              defaultOpts(),
			expected:          true,
		},
		{
			// Exactly half of the shard reporting fresh is not a strict majority -> no quorum.
			name: "half the shard fresh down -> no quorum",
			seed: func() {
				resetShardPeerHealth()
				RecordShardPeerHealth(alias(101), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 5, 0, now), now)
				RecordShardPeerHealth(alias(102), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 5, 0, now), now)
			},
			expectedObservers: 4,
			opts:              defaultOpts(),
			expected:          false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.seed()
			got := PrimaryDownByQuorum(primary, "ks", "0", tc.expectedObservers, tc.opts, now)
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
	r := EvaluatePrimaryQuorum(primary, "ks", "0", 3, opts, now)

	assert.Equal(t, "zone1-0000000100", r.PrimaryAlias)
	assert.Equal(t, 2, r.TotalObservers)    // 101 + 102 are fresh; 103 is stale
	assert.Equal(t, 3, r.EligibleObservers) // all three pinged the primary, incl. the stale one
	assert.Equal(t, 3, r.ExpectedObservers) // the topo replica count passed in
	assert.Equal(t, 1, r.DownVotes)         // only 101
	assert.True(t, r.Down)                  // 2 fresh of 3 is a majority reporting; 1/2 >= 0.5
	require.Len(t, r.Observers, 3)          // all three are reported, incl. the stale one
	// sorted by alias: 101, 102, 103
	assert.Equal(t, "zone1-0000000101", r.Observers[0].Alias)
	assert.Equal(t, voteDown, r.Observers[0].Vote)
	assert.True(t, r.Observers[0].Fresh)
	assert.Equal(t, int64(5), r.Observers[0].ConsecutiveFailures)
	assert.Equal(t, voteUp, r.Observers[1].Vote)
	assert.Equal(t, voteStale, r.Observers[2].Vote)
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
	gated := EvaluatePrimaryQuorum(primary, "ks", "0", 3, QuorumOptions{FailureThreshold: 3, Freshness: 5 * time.Second, Fraction: 0.5, MinObservers: 3}, now)
	assert.False(t, gated.Down)
	assert.Equal(t, 2, gated.TotalObservers)
	assert.Equal(t, 1, gated.DownVotes)

	// A nil primary (e.g. the shard has no primary) yields an empty evaluation with no verdict.
	empty := EvaluatePrimaryQuorum(nil, "ks", "0", 3, opts, now)
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

	// The ingest path rate-limits pruning so a polling cycle does not scan the whole
	// observer map once per tablet. A stale record injected after the first ingest
	// survives a second ingest within shardPeerPruneInterval, and is dropped only once
	// the interval elapses.
	resetShardPeerHealth()
	RecordShardPeerHealth(alias(101), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 3, 0, now), now)
	shardPeerHealthMu.Lock()
	shardPeerHealthByObserver["zone1-0000000999"] = &observerRecord{
		observerType: topodatapb.TabletType_REPLICA,
		keyspace:     "ks",
		shard:        "0",
		recordedAt:   now.Add(-2 * staleShardPeerRecordTTL),
		peers:        map[string]peerReport{},
	}
	shardPeerHealthMu.Unlock()
	// Within the interval: the second ingest must not scan/prune, so the stale record survives.
	RecordShardPeerHealth(alias(102), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 3, 0, now), now)
	shardPeerHealthMu.Lock()
	_, present := shardPeerHealthByObserver["zone1-0000000999"]
	shardPeerHealthMu.Unlock()
	assert.True(t, present, "prune is rate-limited: a within-interval ingest must not drop the stale record")
	// Past the interval: the ingest prunes, dropping the stale record.
	RecordShardPeerHealth(alias(103), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 3, 0, now), now.Add(shardPeerPruneInterval+time.Second))
	shardPeerHealthMu.Lock()
	_, present = shardPeerHealthByObserver["zone1-0000000999"]
	shardPeerHealthMu.Unlock()
	assert.False(t, present, "prune runs once the interval elapses, dropping the stale record")

	// A deleted tablet's record lingers for up to the TTL and inflates the quorum
	// denominator, which can block the verdict in small shards until it is pruned.
	// ForgetInstance calls RemoveShardPeerObserver so the departed observer stops
	// counting immediately.
	resetShardPeerHealth()
	RecordShardPeerHealth(alias(101), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 3, 0, now), now)
	shardPeerHealthMu.Lock()
	shardPeerHealthByObserver["zone1-0000000888"] = &observerRecord{
		observerType: topodatapb.TabletType_REPLICA,
		keyspace:     "ks",
		shard:        "0",
		recordedAt:   now.Add(-30 * time.Second), // Within the TTL, but stale for voting.
		peers:        map[string]peerReport{topoproto.TabletAliasString(primary): {}},
	}
	shardPeerHealthMu.Unlock()
	// The lingering record makes 2 eligible observers with only 1 fresh reporter:
	// not a strict majority, so the down verdict is blocked.
	assert.False(t, PrimaryDownByQuorum(primary, "ks", "0", 1, defaultOpts(), now),
		"a deleted observer's lingering record must block the verdict (denominator inflated)")
	RemoveShardPeerObserver("zone1-0000000888")
	// With the departed observer dropped, the single fresh down-voter is the whole shard.
	assert.True(t, PrimaryDownByQuorum(primary, "ks", "0", 1, defaultOpts(), now),
		"removing the forgotten observer must unblock the quorum verdict")
}

// TestIsShardHealthObserverType pins the single predicate behind the quorum voter filter and the
// expected-observer counts: only REPLICA and RDONLY are shard-health observers. In particular
// SPARE/EXPERIMENTAL/UNKNOWN (which topo.IsReplicaType counts as replica types) must NOT qualify,
// so the population that can vote and the population counted in the denominator always agree.
func TestIsShardHealthObserverType(t *testing.T) {
	observers := []topodatapb.TabletType{topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY}
	nonObservers := []topodatapb.TabletType{
		topodatapb.TabletType_PRIMARY,
		topodatapb.TabletType_SPARE,
		topodatapb.TabletType_EXPERIMENTAL,
		topodatapb.TabletType_BACKUP,
		topodatapb.TabletType_RESTORE,
		topodatapb.TabletType_DRAINED,
		topodatapb.TabletType_UNKNOWN,
	}
	for _, tt := range observers {
		assert.Truef(t, IsShardHealthObserverType(tt), "%s should be a shard-health observer", tt)
	}
	for _, tt := range nonObservers {
		assert.Falsef(t, IsShardHealthObserverType(tt), "%s must not be a shard-health observer", tt)
	}
}

// TestQuorumOptionsFromConfig pins that each config getter is wired to the right QuorumOptions field.
// Elsewhere only hand-built QuorumOptions literals are tested, so a getter swap (e.g. FailureThreshold
// <-> MinObservers, both int) would otherwise ship silently. Asserting each field against its own
// getter catches such a swap as long as the two int getters return distinct values (they do by
// default); the Duration and float64 fields cannot be swapped without a compile error.
func TestQuorumOptionsFromConfig(t *testing.T) {
	opts := QuorumOptionsFromConfig()
	assert.Equal(t, config.GetShardTabletHealthFailureThreshold(), opts.FailureThreshold, "FailureThreshold")
	assert.Equal(t, config.GetShardTabletHealthFreshness(), opts.Freshness, "Freshness")
	assert.Equal(t, config.GetShardQuorumFraction(), opts.Fraction, "Fraction")
	assert.Equal(t, config.GetShardQuorumMinObservers(), opts.MinObservers, "MinObservers")
	// Sanity: the two same-typed fields differ by default, so the swap above is actually detectable.
	require.NotEqual(t, opts.FailureThreshold, opts.MinObservers)
}

// TestQuorumOptionsValid is the direct unit test of the fail-closed validity gate (the quorum
// behavior tests exercise it only indirectly). It covers the boundaries, including Fraction>1 and
// MinObservers<1, which were otherwise untested.
func TestQuorumOptionsValid(t *testing.T) {
	base := QuorumOptions{FailureThreshold: 3, Freshness: 5 * time.Second, Fraction: 1.0, MinObservers: 1}
	require.True(t, base.valid(), "the baseline options must be valid")

	tests := []struct {
		name string
		mut  func(*QuorumOptions)
	}{
		{"fraction above 1", func(o *QuorumOptions) { o.Fraction = 1.5 }},
		{"fraction zero", func(o *QuorumOptions) { o.Fraction = 0 }},
		{"min observers below 1", func(o *QuorumOptions) { o.MinObservers = 0 }},
		{"failure threshold below 1", func(o *QuorumOptions) { o.FailureThreshold = 0 }},
		{"non-positive freshness", func(o *QuorumOptions) { o.Freshness = 0 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := base
			tt.mut(&o)
			assert.False(t, o.valid(), "%s must be rejected (fail closed)", tt.name)
		})
	}
}

// TestEvaluatePrimaryQuorumZeroExpectedObservers covers the degenerate floor: with no topo observer
// count supplied (expectedObservers=0) the majority gate falls back to the observers actually seen
// (2*fresh > eligible), so a lone fresh down report suffices. Callers that must match the ERS
// verdict pass the real topo count instead (see ShardEligibleObserverCount / the analysis matcher).
func TestEvaluatePrimaryQuorumZeroExpectedObservers(t *testing.T) {
	defer resetShardPeerHealth()
	resetShardPeerHealth()
	now := time.Now()
	primary := alias(100)
	RecordShardPeerHealth(alias(101), topodatapb.TabletType_REPLICA, "ks", "0", reportFor(primary, 5, 0, now), now)

	r := EvaluatePrimaryQuorum(primary, "ks", "0", 0, defaultOpts(), now)
	assert.True(t, r.Down, "with expectedObservers=0 the gate uses the single seen observer as the base")
	assert.Equal(t, 0, r.ExpectedObservers)
	assert.Equal(t, 1, r.EligibleObservers)
	assert.Equal(t, 1, r.TotalObservers)
}
