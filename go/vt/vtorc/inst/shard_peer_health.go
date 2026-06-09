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
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/protoutil"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

// staleShardPeerRecordTTL bounds how long an observer's report is retained before it is
// pruned, so records for deleted tablets do not accumulate. Votes are additionally gated
// by the (much shorter) freshness window in QuorumOptions.
const staleShardPeerRecordTTL = time.Minute

// QuorumOptions tunes the PrimaryDownByQuorum decision. All fields are supplied by the
// caller (from config) so the policy itself stays config-free and unit-testable.
type QuorumOptions struct {
	FailureThreshold int           // consecutive ping failures for an observer to consider a peer down
	Freshness        time.Duration // max age of an observer's report for it to vote
	Fraction         float64       // required fraction of down votes among eligible observers (1.0 = unanimous)
	MinObservers     int           // minimum eligible observers required to act
}

// valid keeps invalid dynamic config from turning quorum detection into a fail-open path.
func (opts QuorumOptions) valid() bool {
	return opts.FailureThreshold >= 1 && opts.Freshness > 0 && opts.Fraction > 0 && opts.Fraction <= 1 && opts.MinObservers >= 1
}

// ObserverVote is one observer's reported view of the primary, as evaluated against the quorum policy.
type ObserverVote struct {
	Alias               string // observer tablet alias, e.g. "zone1-0000000101"
	TabletType          string // "REPLICA" | "RDONLY"
	Fresh               bool   // the observer's record is within the freshness window
	ConsecutiveFailures int64  // observer's consecutive ping failures against the primary
	Vote                string // "down" | "up" | "stale"
}

// QuorumResult is the full evaluation of whether a shard's primary is down by quorum, including
// the per-observer breakdown. It is the single source of truth behind the decision log, the audit
// detail, and the /api/shard-quorum endpoint.
type QuorumResult struct {
	PrimaryAlias   string
	Keyspace       string
	Shard          string
	Down           bool // the verdict
	DownVotes      int  // among FRESH observers only
	TotalObservers int  // FRESH observers that pinged the primary
	Fraction       float64
	MinObservers   int
	Observers      []ObserverVote // ALL relevant observers incl. stale (stale ones are shown but not counted)
	EvaluatedAt    time.Time
}

// KeyspaceShard identifies a shard.
type KeyspaceShard struct {
	Keyspace string
	Shard    string
}

type peerReport struct {
	consecutiveFailures int64
	lastAttemptedPing   time.Time
}

type observerRecord struct {
	observerType topodatapb.TabletType
	keyspace     string
	shard        string
	recordedAt   time.Time
	peers        map[string]peerReport // peer alias string -> report
}

var (
	shardPeerHealthMu sync.Mutex
	// keyed by observer alias string
	shardPeerHealthByObserver = make(map[string]*observerRecord)
)

// RecordShardPeerHealth stores one observer's shard-peer report, captured from a FullStatus poll.
func RecordShardPeerHealth(
	observerAlias *topodatapb.TabletAlias,
	observerType topodatapb.TabletType,
	keyspace, shard string,
	entries []*replicationdatapb.ShardPeerHealth,
	now time.Time,
) {
	if observerAlias == nil || len(entries) == 0 {
		return
	}
	observer := topoproto.TabletAliasString(observerAlias)

	peers := make(map[string]peerReport, len(entries))
	for _, e := range entries {
		if e == nil || e.TabletAlias == nil {
			continue
		}
		report := peerReport{
			consecutiveFailures: e.ConsecutivePingFailures,
			// protoutil.TimeFromProto handles nil safely, returning time.Time{}.
			lastAttemptedPing: protoutil.TimeFromProto(e.LastAttemptedPing),
		}
		peers[topoproto.TabletAliasString(e.TabletAlias)] = report
	}
	if len(peers) == 0 {
		return
	}

	shardPeerHealthMu.Lock()
	defer shardPeerHealthMu.Unlock()
	shardPeerHealthByObserver[observer] = &observerRecord{
		observerType: observerType,
		keyspace:     keyspace,
		shard:        shard,
		recordedAt:   now,
		peers:        peers,
	}
	pruneStaleShardPeerRecordsLocked(now)
}

// pruneStaleShardPeerRecordsLocked bounds records for deleted tablets without a background task.
func pruneStaleShardPeerRecordsLocked(now time.Time) {
	for alias, rec := range shardPeerHealthByObserver {
		if now.Sub(rec.recordedAt) > staleShardPeerRecordTTL {
			delete(shardPeerHealthByObserver, alias)
		}
	}
}

// EvaluatePrimaryQuorum computes the quorum verdict for a primary plus the per-observer tally.
// Only fresh REPLICA/RDONLY observers in the keyspace/shard that have pinged the primary count
// toward the verdict; stale observers are surfaced (Vote "stale") for visibility but not counted.
func EvaluatePrimaryQuorum(primaryAlias *topodatapb.TabletAlias, keyspace, shard string, opts QuorumOptions, now time.Time) QuorumResult {
	result := QuorumResult{
		Keyspace:     keyspace,
		Shard:        shard,
		Fraction:     opts.Fraction,
		MinObservers: opts.MinObservers,
		EvaluatedAt:  now,
	}
	if primaryAlias == nil {
		return result
	}
	primary := topoproto.TabletAliasString(primaryAlias)
	result.PrimaryAlias = primary

	shardPeerHealthMu.Lock()
	defer shardPeerHealthMu.Unlock()
	pruneStaleShardPeerRecordsLocked(now)

	for observerAlias, rec := range shardPeerHealthByObserver {
		if rec.keyspace != keyspace || rec.shard != shard {
			continue
		}
		if rec.observerType != topodatapb.TabletType_REPLICA && rec.observerType != topodatapb.TabletType_RDONLY {
			continue
		}
		report, ok := rec.peers[primary]
		if !ok {
			continue // this observer has not pinged the primary
		}
		recordFresh := now.Sub(rec.recordedAt) <= opts.Freshness
		// Ping freshness: discount observers whose last ping attempt against the primary is missing,
		// in the future, or older than the freshness window — their report is not current evidence.
		pingFresh := !report.lastAttemptedPing.IsZero() && !report.lastAttemptedPing.After(now) && now.Sub(report.lastAttemptedPing) <= opts.Freshness
		fresh := recordFresh && pingFresh
		vote := "stale"
		if fresh {
			if report.consecutiveFailures >= int64(opts.FailureThreshold) {
				vote = "down"
			} else {
				vote = "up"
			}
		}
		result.Observers = append(result.Observers, ObserverVote{
			Alias:               observerAlias,
			TabletType:          rec.observerType.String(),
			Fresh:               fresh,
			ConsecutiveFailures: report.consecutiveFailures,
			Vote:                vote,
		})
		if fresh {
			result.TotalObservers++
			if vote == "down" {
				result.DownVotes++
			}
		}
	}

	sort.Slice(result.Observers, func(i, j int) bool { return result.Observers[i].Alias < result.Observers[j].Alias })

	// opts.valid() keeps an invalid dynamic config (e.g. Fraction 0) from turning the verdict
	// fail-open; an invalid config yields no down verdict while still surfacing observer data.
	if opts.valid() && result.TotalObservers >= opts.MinObservers && result.TotalObservers > 0 {
		result.Down = float64(result.DownVotes)/float64(result.TotalObservers) >= opts.Fraction
	}
	return result
}

// Summary renders a one-line human-readable summary of the evaluation for logs and the audit.
func (r QuorumResult) Summary() string {
	verdict := "not-down"
	if r.Down {
		verdict = "DOWN"
	}
	parts := make([]string, 0, len(r.Observers))
	for _, o := range r.Observers {
		if o.Vote == "stale" {
			parts = append(parts, o.Alias+"=stale")
			continue
		}
		parts = append(parts, fmt.Sprintf("%s=%s(%d)", o.Alias, o.Vote, o.ConsecutiveFailures))
	}
	return fmt.Sprintf("shard quorum %s/%s primary %s %s: %d/%d fresh observers down (fraction %g, min %d) — %s",
		r.Keyspace, r.Shard, r.PrimaryAlias, verdict, r.DownVotes, r.TotalObservers, r.Fraction, r.MinObservers, strings.Join(parts, ", "))
}

// ObservedShards returns the distinct keyspace/shards that currently have shard-peer observer data,
// sorted for stable output.
func ObservedShards() []KeyspaceShard {
	shardPeerHealthMu.Lock()
	defer shardPeerHealthMu.Unlock()
	pruneStaleShardPeerRecordsLocked(time.Now())
	seen := make(map[KeyspaceShard]bool)
	for _, rec := range shardPeerHealthByObserver {
		seen[KeyspaceShard{Keyspace: rec.keyspace, Shard: rec.shard}] = true
	}
	out := make([]KeyspaceShard, 0, len(seen))
	for ks := range seen {
		out = append(out, ks)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Keyspace != out[j].Keyspace {
			return out[i].Keyspace < out[j].Keyspace
		}
		return out[i].Shard < out[j].Shard
	})
	return out
}

// PrimaryDownByQuorum reports whether a quorum of fresh REPLICA/RDONLY observers in the
// given keyspace/shard consider the primary's vttablet unreachable.
func PrimaryDownByQuorum(primaryAlias *topodatapb.TabletAlias, keyspace, shard string, opts QuorumOptions, now time.Time) bool {
	return EvaluatePrimaryQuorum(primaryAlias, keyspace, shard, opts, now).Down
}

// resetShardPeerHealth clears all stored reports. Test helper.
func resetShardPeerHealth() {
	shardPeerHealthMu.Lock()
	defer shardPeerHealthMu.Unlock()
	shardPeerHealthByObserver = make(map[string]*observerRecord)
}

// ResetShardPeerHealthForTest clears all stored reports. Exported for use by other packages' tests.
func ResetShardPeerHealthForTest() { resetShardPeerHealth() }
