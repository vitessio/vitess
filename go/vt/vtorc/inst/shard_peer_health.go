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
	if observerAlias == nil {
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

	shardPeerHealthMu.Lock()
	defer shardPeerHealthMu.Unlock()
	shardPeerHealthByObserver[observer] = &observerRecord{
		observerType: observerType,
		keyspace:     keyspace,
		shard:        shard,
		recordedAt:   now,
		peers:        peers,
	}
	// Opportunistically prune very old records (e.g. deleted tablets).
	for alias, rec := range shardPeerHealthByObserver {
		if now.Sub(rec.recordedAt) > staleShardPeerRecordTTL {
			delete(shardPeerHealthByObserver, alias)
		}
	}
}

// PrimaryDownByQuorum reports whether a quorum of fresh REPLICA/RDONLY observers in the
// given keyspace/shard consider the primary's vttablet unreachable.
func PrimaryDownByQuorum(primaryAlias *topodatapb.TabletAlias, keyspace, shard string, opts QuorumOptions, now time.Time) bool {
	if primaryAlias == nil || opts.MinObservers < 1 {
		return false
	}
	primary := topoproto.TabletAliasString(primaryAlias)

	shardPeerHealthMu.Lock()
	defer shardPeerHealthMu.Unlock()

	total := 0
	down := 0
	for _, rec := range shardPeerHealthByObserver {
		if rec.keyspace != keyspace || rec.shard != shard {
			continue
		}
		if rec.observerType != topodatapb.TabletType_REPLICA && rec.observerType != topodatapb.TabletType_RDONLY {
			continue
		}
		if now.Sub(rec.recordedAt) > opts.Freshness {
			continue
		}
		report, ok := rec.peers[primary]
		if !ok {
			continue // this observer has not pinged the primary
		}
		total++
		if report.consecutiveFailures >= int64(opts.FailureThreshold) {
			down++
		}
	}

	if total < opts.MinObservers || total == 0 {
		return false
	}
	return float64(down)/float64(total) >= opts.Fraction
}

// resetShardPeerHealth clears all stored reports. Test helper.
func resetShardPeerHealth() {
	shardPeerHealthMu.Lock()
	defer shardPeerHealthMu.Unlock()
	shardPeerHealthByObserver = make(map[string]*observerRecord)
}
