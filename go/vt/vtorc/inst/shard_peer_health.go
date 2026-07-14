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
	"vitess.io/vitess/go/vt/topo/topoproto"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// staleShardPeerRecordTTL bounds how long an observer's report is retained before it is
// pruned, so records for deleted tablets do not accumulate. Votes are additionally gated
// by the (much shorter) freshness window in QuorumOptions.
const staleShardPeerRecordTTL = time.Minute

// shardPeerPruneInterval rate-limits how often the hot ingest path (RecordShardPeerHealth, once
// per polled tablet per cycle) scans the whole observer map to drop stale records. Pruning is
// coarse memory hygiene — the TTL is far longer than the freshness window that actually gates
// votes — so scanning at most once per interval keeps each ingest amortized O(1) instead of O(n)
// and bounds the shardPeerHealthMu critical section in large clusters. Read paths still prune on
// every call, where the cost is not multiplied by the tablet count.
const shardPeerPruneInterval = staleShardPeerRecordTTL

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

// voteType is an observer's vote on the primary's liveness, derived from its report against the
// quorum policy. It is serialized as a string in the /api/shard-tablet-health-quorum output.
type voteType string

const (
	voteStale voteType = "stale" // the observer's report is not fresh enough to count
	voteDown  voteType = "down"  // fresh report, consecutive ping failures at or above the threshold
	voteUp    voteType = "up"    // fresh report, primary still reachable
)

// ObserverVote is one observer's reported view of the primary, as evaluated against the quorum policy.
type ObserverVote struct {
	Alias               string   // observer tablet alias, e.g. "zone1-0000000101"
	TabletType          string   // "REPLICA" | "RDONLY"
	Fresh               bool     // the observer's record is within the freshness window
	ConsecutiveFailures int64    // observer's consecutive ping failures against the primary
	Vote                voteType // voteDown | voteUp | voteStale
}

// QuorumResult is the full evaluation of whether a shard's primary is down by quorum, including
// the per-observer breakdown. It is the single source of truth behind the decision log, the audit
// detail, and the /api/shard-tablet-health-quorum endpoint.
type QuorumResult struct {
	PrimaryAlias   string
	Keyspace       string
	Shard          string
	Down           bool // the verdict
	DownVotes      int  // among FRESH observers only
	TotalObservers int  // FRESH observers that pinged the primary
	// EligibleObservers is every REPLICA/RDONLY observer that pinged the primary, fresh or
	// stale. ExpectedObservers is the shard's eligible replica count from VTOrc's topo view
	// (0 when the caller cannot supply it). The larger of the two is the shard size that the
	// fresh reporters must form a majority of before a verdict is trusted (see EvaluatePrimaryQuorum).
	EligibleObservers int
	ExpectedObservers int
	Fraction          float64
	MinObservers      int
	Observers         []ObserverVote // ALL relevant observers incl. stale (stale ones are shown but not counted)
	EvaluatedAt       time.Time
}

// KeyspaceShard identifies a shard.
type KeyspaceShard struct {
	Keyspace string
	Shard    string
}

type peerReport struct {
	consecutiveFailures int64
	// pingAge is how old the observer's last ping attempt against the peer was when the report
	// was ingested. It is preferably the tablet-measured time_since_last_attempted_ping (no
	// cross-host clock comparison); see RecordShardPeerHealth for the fallback.
	pingAge    time.Duration
	hasPingAge bool
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
	// lastShardPeerPruneAt is when the observer map was last scanned for stale records. It
	// rate-limits pruning on the hot ingest path. Guarded by shardPeerHealthMu.
	lastShardPeerPruneAt time.Time
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
		report := peerReport{consecutiveFailures: e.ConsecutivePingFailures}
		if age, ok, err := protoutil.DurationFromProto(e.TimeSinceLastAttemptedPing); ok && err == nil {
			// Preferred: the reporting tablet measured this age with its own clock, so no
			// cross-host clock comparison is involved.
			report.pingAge = age
			report.hasPingAge = true
		} else if t := protoutil.TimeFromProto(e.LastAttemptedPing); !t.IsZero() {
			// Fallback for reports from tablets that predate time_since_last_attempted_ping:
			// derive the age once at ingest from the tablet-stamped absolute time. This crosses
			// host clocks, so it is only as accurate as the clock sync between the tablet and
			// VTOrc, but deriving it once at ingest (when the report is freshest) beats
			// re-deriving it at every evaluation. This path only runs for not-yet-upgraded tablets,
			// so the cross-host skew self-resolves once the fleet sends time_since_last_attempted_ping.
			report.pingAge = now.Sub(t)
			report.hasPingAge = true
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
	maybePruneStaleShardPeerRecordsLocked(now)
}

// maybePruneStaleShardPeerRecordsLocked prunes at most once per shardPeerPruneInterval. It guards
// the hot ingest path: pruning on every FullStatus ingest would make a polling cycle O(n²) and
// lengthen the shardPeerHealthMu critical section in large clusters.
func maybePruneStaleShardPeerRecordsLocked(now time.Time) {
	if !lastShardPeerPruneAt.IsZero() && now.Sub(lastShardPeerPruneAt) < shardPeerPruneInterval {
		return
	}
	pruneStaleShardPeerRecordsLocked(now)
}

// IsShardHealthObserverType reports whether a tablet of this type participates in the shard-peer
// health quorum: only REPLICA and RDONLY tablets both run the shard-peer monitor and are eligible
// to vote on a primary's liveness. It is the single predicate behind the quorum voter filter and
// the expected-observer counts, so the population that can vote and the population counted in the
// denominator always agree (e.g. SPARE/EXPERIMENTAL/UNKNOWN tablets are excluded from both).
func IsShardHealthObserverType(tt topodatapb.TabletType) bool {
	return tt == topodatapb.TabletType_REPLICA || tt == topodatapb.TabletType_RDONLY
}

// shardObserverTabletTypeList renders the shard-health observer tablet types (the REPLICA/RDONLY set
// behind IsShardHealthObserverType) as a SQL IN-list of proto enum values. Both the analysis query's
// shard_eligible_observers count and ShardEligibleObserverCount build their tablet-type filter from
// it, so the SQL population can never drift from the predicate.
func shardObserverTabletTypeList() string {
	return fmt.Sprintf("%d, %d", int(topodatapb.TabletType_REPLICA), int(topodatapb.TabletType_RDONLY))
}

// RemoveShardPeerObserver drops all health reports from a single observer, if any.
// It is called when VTOrc forgets a tablet (e.g. it was deleted from the topo) so
// that the departed observer stops counting toward the quorum denominator right
// away, instead of blocking quorum in small shards until the stale-record TTL
// prunes it.
func RemoveShardPeerObserver(tabletAliasString string) {
	shardPeerHealthMu.Lock()
	defer shardPeerHealthMu.Unlock()
	delete(shardPeerHealthByObserver, tabletAliasString)
}

// pruneStaleShardPeerRecordsLocked bounds records for deleted tablets without a background task.
func pruneStaleShardPeerRecordsLocked(now time.Time) {
	lastShardPeerPruneAt = now
	for alias, rec := range shardPeerHealthByObserver {
		if now.Sub(rec.recordedAt) > staleShardPeerRecordTTL {
			delete(shardPeerHealthByObserver, alias)
		}
	}
}

// EvaluatePrimaryQuorum computes the quorum verdict for a primary plus the per-observer tally.
// Only fresh REPLICA/RDONLY observers in the keyspace/shard that have pinged the primary vote;
// stale observers are surfaced (Vote "stale") for visibility but do not vote.
//
// expectedObservers is the shard's eligible replica count from VTOrc's topo view (pass 0 when it
// cannot be supplied, e.g. the observability API). To trust a verdict, the fresh reporters must
// form a strict majority of the shard — the larger of expectedObservers and the observers actually
// seen — so a minority (a single partitioned replica while the rest are stale or missing) cannot
// drive ERS from a minority view. The down decision is then taken over the FRESH reporters only,
// so a stale or dead replica cannot block a real failover.
func EvaluatePrimaryQuorum(primaryAlias *topodatapb.TabletAlias, keyspace, shard string, expectedObservers int, opts QuorumOptions, now time.Time) QuorumResult {
	result := QuorumResult{
		Keyspace:          keyspace,
		Shard:             shard,
		ExpectedObservers: expectedObservers,
		Fraction:          opts.Fraction,
		MinObservers:      opts.MinObservers,
		EvaluatedAt:       now,
	}
	if primaryAlias == nil {
		return result
	}
	primary := topoproto.TabletAliasString(primaryAlias)
	result.PrimaryAlias = primary

	shardPeerHealthMu.Lock()
	defer shardPeerHealthMu.Unlock()
	// Rate-limited prune: the verdict below freshness-gates every observer, so stale records cannot
	// affect it — pruning here is only memory hygiene and need not run on every evaluation.
	maybePruneStaleShardPeerRecordsLocked(now)

	for observerAlias, rec := range shardPeerHealthByObserver {
		if rec.keyspace != keyspace || rec.shard != shard {
			continue
		}
		if !IsShardHealthObserverType(rec.observerType) {
			continue
		}
		report, ok := rec.peers[primary]
		if !ok {
			continue // this observer has not pinged the primary
		}
		// recordAge is how long ago we ingested the report. recordedAt and now are two same-process,
		// never-serialized time.Now() values, so Go's monotonic clock makes this immune to NTP steps
		// and it cannot go negative — hence no >=0 guard here, unlike pingAge below. (It would need
		// one if recordedAt ever round-tripped through proto/DB.)
		recordAge := now.Sub(rec.recordedAt)
		recordFresh := recordAge <= opts.Freshness
		// Ping freshness: the age of the observer's last ping attempt against the primary is the
		// age it reported (measured with its own clock) plus how long ago we ingested the report.
		// Observers with no ping, a negative age (clock anomaly), or an age beyond the freshness
		// window are discounted — their report is not current evidence. This omits the
		// report-production-to-ingest latency (network + VTOrc poll backlog), so the effective
		// window is freshness plus that (bounded) latency rather than freshness exactly.
		pingFresh := report.hasPingAge && report.pingAge >= 0 && report.pingAge+recordAge <= opts.Freshness
		fresh := recordFresh && pingFresh
		vote := voteStale
		if fresh {
			if report.consecutiveFailures >= int64(opts.FailureThreshold) {
				vote = voteDown
			} else {
				vote = voteUp
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
			if vote == voteDown {
				result.DownVotes++
			}
		}
	}

	sort.Slice(result.Observers, func(i, j int) bool { return result.Observers[i].Alias < result.Observers[j].Alias })

	// Every eligible observer that pinged the primary, fresh or stale: the denominator for the
	// reporting-majority gate below. The shard size is the larger of this and VTOrc's topo view.
	result.EligibleObservers = len(result.Observers)
	quorumBase := max(expectedObservers, result.EligibleObservers)

	// A verdict is trusted only when the fresh reporters form a strict majority of the shard
	// (2*fresh > quorumBase). This stops a minority view — a single partitioned replica while the
	// rest are stale or missing — from driving ERS. It is deliberately fail-closed in the other
	// direction too: replicas that are stale, dead, or not yet re-polled still count in quorumBase
	// (via the topo-derived expectedObservers), so a fresh majority of the WHOLE shard is required
	// before any down verdict — a shard where most reports are stale blocks quorum ERS rather than
	// firing on the few fresh ones. The down *ratio*, by contrast, is taken over the fresh
	// reporters only, so a stale replica never contributes a down vote it did not cast.
	//
	// opts.valid() keeps an invalid dynamic config (e.g. Fraction 0) from turning the verdict
	// fail-open; an invalid config yields no down verdict while still surfacing observer data.
	if opts.valid() && result.TotalObservers >= opts.MinObservers && result.TotalObservers > 0 &&
		2*result.TotalObservers > quorumBase {
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
		if o.Vote == voteStale {
			parts = append(parts, o.Alias+"="+string(voteStale))
			continue
		}
		parts = append(parts, fmt.Sprintf("%s=%s(%d)", o.Alias, o.Vote, o.ConsecutiveFailures))
	}
	return fmt.Sprintf("shard quorum %s/%s primary %s %s: %d/%d fresh observers down, %d fresh of %d eligible, %d expected (fraction %g, min %d) — %s",
		r.Keyspace, r.Shard, r.PrimaryAlias, verdict, r.DownVotes, r.TotalObservers, r.TotalObservers, r.EligibleObservers, r.ExpectedObservers, r.Fraction, r.MinObservers, strings.Join(parts, ", "))
}

// ObservedShards returns the distinct keyspace/shards that currently have shard-peer observer data,
// sorted for stable output.
func ObservedShards() []KeyspaceShard {
	shardPeerHealthMu.Lock()
	defer shardPeerHealthMu.Unlock()
	// Rate-limited prune (memory hygiene only; callers freshness-gate their own use of the data).
	maybePruneStaleShardPeerRecordsLocked(time.Now())
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
// given keyspace/shard consider the primary's vttablet unreachable. See EvaluatePrimaryQuorum
// for the meaning of expectedObservers.
func PrimaryDownByQuorum(primaryAlias *topodatapb.TabletAlias, keyspace, shard string, expectedObservers int, opts QuorumOptions, now time.Time) bool {
	return EvaluatePrimaryQuorum(primaryAlias, keyspace, shard, expectedObservers, opts, now).Down
}

// resetShardPeerHealth clears all stored reports. Test helper.
func resetShardPeerHealth() {
	shardPeerHealthMu.Lock()
	defer shardPeerHealthMu.Unlock()
	shardPeerHealthByObserver = make(map[string]*observerRecord)
	lastShardPeerPruneAt = time.Time{}
}

// ResetShardPeerHealthForTest clears all stored reports. Exported for use by other packages' tests.
func ResetShardPeerHealthForTest() { resetShardPeerHealth() }
