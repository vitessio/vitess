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

package logic

import (
	"fmt"

	"vitess.io/vitess/go/vt/gossip"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtorc/inst"
)

type (
	// GossipStateProvider abstracts the Gossip agent for testing.
	GossipStateProvider interface {
		Members() []gossip.Member
		Snapshot() map[gossip.NodeID]gossip.State
	}

	shardGroup struct {
		keyspace string
		shard    string
		members  []gossip.Member
	}
)

const (
	// minQuorumObservers is the minimum number of Alive replicas required
	// to form a quorum. Fewer than two observers makes a shared hallucination
	// too easy.
	minQuorumObservers = 2

	// smallShardThreshold is the total-replica count at which strict
	// majority equals unanimous. For shards at or below this threshold we
	// require an additional corroborating signal (VTOrc's own health
	// check) before triggering ERS, since the two remaining replicas
	// can fail correlatedly (for example, under a cross-cell partition).
	smallShardThreshold = 2
)

// VTOrcView captures VTOrc's own evidence about each shard's primary.
// It is intentionally separate from the gossip quorum: gossip is a
// peer-to-peer consensus, whereas VTOrc's health check is a single probe
// from a single vantage point.
type VTOrcView struct {
	// HealthCheckFailed is true when VTOrc's most recent probe to the
	// primary failed (the primary was unreachable from VTOrc's own
	// vantage point). This is CORROBORATING evidence only — it does
	// not independently prove the primary is down, because VTOrc and
	// the primary could be on opposite sides of a partition.
	HealthCheckFailed map[string]bool
	// SelfLikelyPartitioned is true when VTOrc cannot reach most
	// primaries in its view. In that case the HealthCheckFailed signal
	// is probably about VTOrc's own network, not about the primaries,
	// and callers should ignore it.
	SelfLikelyPartitioned bool
}

// confirmsPrimaryDown reports whether VTOrc's own view corroborates
// that the given shard's primary is down.
func (v *VTOrcView) confirmsPrimaryDown(key string) bool {
	if v == nil || v.SelfLikelyPartitioned {
		return false
	}
	return v.HealthCheckFailed[key]
}

// AnalyzeGossipQuorum evaluates gossip state to detect primaries that a
// quorum of replicas consider unreachable.
func AnalyzeGossipQuorum(state GossipStateProvider, primaries map[string]string, currentMembers map[string]map[string]struct{}, view *VTOrcView) []*inst.DetectionAnalysis {
	if state == nil {
		return nil
	}

	members := state.Members()
	states := state.Snapshot()

	// Group members by keyspace/shard in one pass.
	groups := make(map[string]*shardGroup)
	for _, m := range members {
		ks := m.Meta[gossip.MetaKeyKeyspace]
		shard := m.Meta[gossip.MetaKeyShard]
		key := ks + "/" + shard
		g, ok := groups[key]
		if !ok {
			g = &shardGroup{keyspace: ks, shard: shard}
			groups[key] = g
		}
		g.members = append(g.members, m)
	}

	var results []*inst.DetectionAnalysis
	for key, group := range groups {
		primaryAlias, ok := primaries[key]
		if !ok {
			continue
		}

		shardMembers := currentMembers[key]

		if da := analyzeShardQuorum(group, states, primaryAlias, shardMembers, view.confirmsPrimaryDown(key)); da != nil {
			results = append(results, da)
		}
	}

	return results
}

// analyzeShardQuorum applies the per-shard quorum rules to the gossip
// view. This is the function that decides whether to produce a
// PrimaryTabletUnreachableByQuorum analysis (and therefore trigger
// ERS). The rules live here so tests can exercise them in isolation,
// without standing up a full gossip agent.
func analyzeShardQuorum(group *shardGroup, states map[gossip.NodeID]gossip.State, primaryAlias string, currentMembers map[string]struct{}, vtorcCorroboratesDown bool) *inst.DetectionAnalysis {
	aliasStates := aggregateShardMemberStates(group, states, currentMembers)
	primaryState, found := aliasStates[primaryAlias]
	if !found {
		return nil
	}

	// Primary must be Down (not Suspect, not Alive).
	if primaryState.Status != gossip.StatusDown {
		return nil
	}

	// Count alive and total non-primary replicas.
	aliveReplicas, totalReplicas := replicaQuorumCounts(aliasStates, primaryAlias, currentMembers)

	if aliveReplicas < minQuorumObservers {
		return nil
	}

	hasMajority := aliveReplicas*2 > totalReplicas
	isTied := aliveReplicas*2 == totalReplicas

	// Small shards (<=2 replicas) have thin quorums — strict majority
	// equals unanimous, so a single correlated failure (partition,
	// cross-cell outage) can trigger a false ERS. Require VTOrc's own
	// check to also report trouble before acting. Note: the caller is
	// responsible for zeroing out this signal when VTOrc appears to be
	// partitioned itself.
	if totalReplicas <= smallShardThreshold && !vtorcCorroboratesDown {
		return nil
	}

	if !hasMajority && !(isTied && vtorcCorroboratesDown) {
		return nil
	}

	alias, err := topoproto.ParseTabletAlias(primaryAlias)
	if err != nil {
		return nil
	}

	return &inst.DetectionAnalysis{
		AnalyzedInstanceAlias: alias,
		AnalyzedKeyspace:      group.keyspace,
		AnalyzedShard:         group.shard,
		Analysis:              inst.PrimaryTabletUnreachableByQuorum,
		IsClusterPrimary:      true,
		IsPrimary:             true,
		Description:           fmt.Sprintf("gossip quorum agrees primary %s is unreachable in %s/%s", primaryAlias, group.keyspace, group.shard),
	}
}

// aggregateShardMemberStates collapses a shard's gossip members down to
// one entry per tablet alias. Multiple gossip records can map to the
// same alias when a tablet is replaced and the old record hasn't aged
// out yet — picking one deterministically (via preferGossipState) is
// what keeps a stale Down record from masking the new tablet's Alive.
func aggregateShardMemberStates(group *shardGroup, states map[gossip.NodeID]gossip.State, currentMembers map[string]struct{}) map[string]gossip.State {
	aliasStates := make(map[string]gossip.State, len(group.members))
	for _, m := range group.members {
		if !isCurrentShardMember(m, currentMembers) {
			continue
		}
		alias := m.Meta[gossip.MetaKeyTabletAlias]
		if alias == "" {
			continue
		}
		state := states[m.ID]
		current, ok := aliasStates[alias]
		if !ok || preferGossipState(state, current) {
			aliasStates[alias] = state
		}
	}
	return aliasStates
}

// replicaQuorumCounts produces the alive/total replica counts that the
// quorum check needs. currentMembers is treated as authoritative when
// set so a tablet missing from gossip counts toward the denominator
// (it didn't observe the primary as Alive) without contributing to the
// numerator. The aliasStates-only fallback only fires from tests.
func replicaQuorumCounts(aliasStates map[string]gossip.State, primaryAlias string, currentMembers map[string]struct{}) (aliveReplicas int, totalReplicas int) {
	if len(currentMembers) > 0 {
		for alias := range currentMembers {
			if alias == "" || alias == primaryAlias {
				continue
			}
			totalReplicas++
			if aliasStates[alias].Status == gossip.StatusAlive {
				aliveReplicas++
			}
		}
		return aliveReplicas, totalReplicas
	}

	for alias, state := range aliasStates {
		if alias == primaryAlias {
			continue
		}
		totalReplicas++
		if state.Status == gossip.StatusAlive {
			aliveReplicas++
		}
	}
	return aliveReplicas, totalReplicas
}

// preferGossipState is the tiebreak used by aggregateShardMemberStates
// when two gossip records share a tablet alias. Newer LastUpdate wins;
// on equal timestamps Alive beats Down/Suspect so a stale Down record
// from a replaced tablet can never veto an Alive observation of the
// fresh tablet — a false ERS would be far worse than a missed one.
func preferGossipState(candidate gossip.State, current gossip.State) bool {
	if candidate.LastUpdate.After(current.LastUpdate) {
		return true
	}
	if candidate.LastUpdate.Equal(current.LastUpdate) && candidate.Status == gossip.StatusAlive && current.Status != gossip.StatusAlive {
		return true
	}
	return current.LastUpdate.IsZero() && !candidate.LastUpdate.IsZero()
}

// isCurrentShardMember filters out stale gossip entries for tablets
// that are no longer in VTOrc's current shard membership view — e.g.
// a replaced tablet whose gossip record hasn't aged out yet. Returning
// true on an empty filter map is the "no filter configured" case used
// by tests.
func isCurrentShardMember(member gossip.Member, currentMembers map[string]struct{}) bool {
	if len(currentMembers) == 0 {
		return true
	}
	_, ok := currentMembers[member.Meta[gossip.MetaKeyTabletAlias]]
	return ok
}

// getGossipQuorumAnalyses queries the gossip agent and VTOrc's DB to produce
// quorum-based analyses for each shard with a known primary.
// It also enriches analyses with ERS-disabled flags from keyspace/shard metadata
// and builds the VTOrc corroborating view from instance health data.
func getGossipQuorumAnalyses() []*inst.DetectionAnalysis {
	agent := currentGossipAgent()
	if agent == nil {
		return nil
	}

	primaries, currentMembers, ersDisabled, err := gossipShardPrimaries(agent)
	if err != nil || len(primaries) == 0 {
		return nil
	}

	view := buildVTOrcView(primaries)

	analyses := AnalyzeGossipQuorum(agent, primaries, currentMembers, view)

	for _, a := range analyses {
		key := a.AnalyzedKeyspace + "/" + a.AnalyzedShard
		if disabled, ok := ersDisabled[key]; ok {
			a.AnalyzedKeyspaceEmergencyReparentDisabled = disabled.keyspace
			a.AnalyzedShardEmergencyReparentDisabled = disabled.shard
		}
	}

	return analyses
}

// buildVTOrcView collects VTOrc's own evidence about each primary's
// reachability. It then applies a partition heuristic: if VTOrc cannot
// reach most of the primaries it knows about, its view is probably about
// VTOrc itself (overloaded, network-partitioned) and must not be trusted
// as corroborating evidence of a primary failure.
func buildVTOrcView(primaries map[string]string) *VTOrcView {
	view := &VTOrcView{HealthCheckFailed: make(map[string]bool, len(primaries))}

	aliases := make([]string, 0, len(primaries))
	aliasByKey := make(map[string]string, len(primaries))
	for key, primaryAlias := range primaries {
		if _, parseErr := topoproto.ParseTabletAlias(primaryAlias); parseErr != nil {
			// Can't parse alias — abstain for this shard.
			continue
		}
		aliasByKey[key] = primaryAlias
		aliases = append(aliases, primaryAlias)
	}

	validByAlias, err := inst.ReadInstanceLastCheckValidByAlias(aliases)
	if err != nil {
		return view
	}

	var probed, failed int
	for key, primaryAlias := range aliasByKey {
		valid, found := validByAlias[primaryAlias]
		if !found {
			continue
		}
		probed++
		if !valid {
			failed++
			view.HealthCheckFailed[key] = true
		}
	}

	// If >50% of probed primaries look unreachable from VTOrc's side,
	// the most likely cause is VTOrc itself — not a simultaneous
	// failure of many unrelated primaries. Suppress the corroborating
	// signal. We require at least 2 probes before applying the
	// heuristic to avoid surprising behavior on single-keyspace setups.
	if probed >= 2 && failed*2 > probed {
		view.SelfLikelyPartitioned = true
	}
	return view
}

// ersDisabledFlags carries the keyspace/shard ERS-disabled bits
// through to the analyses we produce so isERSEnabled() sees the
// correct values downstream. Kept as a struct rather than a single
// bool because the DetectionAnalysis struct has separate fields for
// the two levels.
type ersDisabledFlags struct {
	keyspace bool
	shard    bool
}

// gossipShardPrimaries returns a map of "keyspace/shard" -> primary tablet alias
// and the current shard tablet membership plus ERS-disabled flags.
func gossipShardPrimaries(state GossipStateProvider) (map[string]string, map[string]map[string]struct{}, map[string]ersDisabledFlags, error) {
	primaries := make(map[string]string)
	currentMembers, err := inst.ReadTabletAliasesByShard()
	if err != nil {
		return nil, nil, nil, err
	}
	disabled := make(map[string]ersDisabledFlags)
	seen := make(map[string]bool)

	// Pre-load shard-level ERS-disabled state from VTOrc's DB.
	// ReadKeyspaceShardStats returns per-shard data with both keyspace
	// and shard disable flags already joined.
	// If this fails, we return the error so the caller skips gossip
	// analyses for this cycle rather than failing open.
	shardStats, err := inst.ReadKeyspaceShardStats()
	if err != nil {
		return nil, nil, nil, err
	}
	ersMap := make(map[string]bool, len(shardStats))
	for _, s := range shardStats {
		ersMap[s.Keyspace+"/"+s.Shard] = s.DisableEmergencyReparent
	}

	primaryAliases, err := inst.ReadPrimaryAliasesByShard()
	if err != nil {
		return nil, nil, nil, err
	}

	for _, m := range state.Members() {
		ks := m.Meta[gossip.MetaKeyKeyspace]
		shard := m.Meta[gossip.MetaKeyShard]
		if ks == "" || shard == "" {
			continue
		}
		key := ks + "/" + shard
		if seen[key] {
			continue
		}
		seen[key] = true

		primaryAlias := primaryAliases[key]
		if primaryAlias == "" {
			continue
		}
		primaries[key] = primaryAlias

		// ersMap has the combined keyspace OR shard disable flag.
		// Set both fields so isERSEnabled() correctly blocks recovery
		// when either keyspace or shard has ERS disabled.
		if ersDisabled := ersMap[key]; ersDisabled {
			disabled[key] = ersDisabledFlags{keyspace: true, shard: true}
		}
	}
	return primaries, currentMembers, disabled, nil
}
