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
	// gossipStateProvider abstracts the Gossip agent for testing.
	gossipStateProvider interface {
		Members() []gossip.Member
		Snapshot() map[gossip.NodeID]gossip.State
	}

	shardGroup struct {
		keyspace string
		shard    string
		members  []gossip.Member
	}
)

const minQuorumObservers = 2

func analyzeGossipQuorum(state gossipStateProvider, primaries map[string]string, vtorcView map[string]bool) []*inst.DetectionAnalysis {
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

		vtorcSeesPrimaryDown := vtorcView[key]

		if da := analyzeShardQuorum(group, states, primaryAlias, vtorcSeesPrimaryDown); da != nil {
			results = append(results, da)
		}
	}

	return results
}

func analyzeShardQuorum(group *shardGroup, states map[gossip.NodeID]gossip.State, primaryAlias string, vtorcSeesPrimaryDown bool) *inst.DetectionAnalysis {
	// Find the primary member.
	var primaryID gossip.NodeID
	found := false
	for _, m := range group.members {
		if m.Meta[gossip.MetaKeyTabletAlias] == primaryAlias {
			primaryID = m.ID
			found = true
			break
		}
	}
	if !found {
		return nil
	}

	// Primary must be Down (not Suspect, not Alive).
	if states[primaryID].Status != gossip.StatusDown {
		return nil
	}

	// Count alive and total non-primary replicas.
	var aliveReplicas, totalReplicas int
	for _, m := range group.members {
		if m.ID == primaryID {
			continue
		}
		totalReplicas++
		if states[m.ID].Status == gossip.StatusAlive {
			aliveReplicas++
		}
	}

	if aliveReplicas < minQuorumObservers {
		return nil
	}

	hasMajority := aliveReplicas*2 > totalReplicas
	isTied := aliveReplicas*2 == totalReplicas

	if !hasMajority && !(isTied && vtorcSeesPrimaryDown) {
		return nil
	}

	return &inst.DetectionAnalysis{
		AnalyzedInstanceAlias: primaryAlias,
		AnalyzedKeyspace:      group.keyspace,
		AnalyzedShard:         group.shard,
		Analysis:              inst.PrimaryTabletUnreachableByQuorum,
		IsClusterPrimary:      true,
		IsPrimary:             true,
		Description:           fmt.Sprintf("gossip quorum agrees primary %s is unreachable in %s/%s", primaryAlias, group.keyspace, group.shard),
	}
}

// getGossipQuorumAnalyses queries the gossip agent and VTOrc's DB to produce
// quorum-based analyses for each shard with a known primary.
// It also enriches analyses with ERS-disabled flags from keyspace/shard metadata
// and builds the VTOrc tiebreaker view from instance health data.
func getGossipQuorumAnalyses() []*inst.DetectionAnalysis {
	if gossipAgent == nil {
		return nil
	}

	primaries, ersDisabled := gossipShardPrimaries(gossipAgent)
	if len(primaries) == 0 {
		return nil
	}

	vtorcView := make(map[string]bool, len(primaries))
	for key, primaryAlias := range primaries {
		instance, found, err := inst.ReadInstance(primaryAlias)
		if err != nil || !found || instance == nil {
			vtorcView[key] = true
			continue
		}
		vtorcView[key] = !instance.IsLastCheckValid
	}

	analyses := analyzeGossipQuorum(gossipAgent, primaries, vtorcView)

	for _, a := range analyses {
		key := a.AnalyzedKeyspace + "/" + a.AnalyzedShard
		if disabled, ok := ersDisabled[key]; ok {
			a.AnalyzedKeyspaceEmergencyReparentDisabled = disabled.keyspace
			a.AnalyzedShardEmergencyReparentDisabled = disabled.shard
		}
	}

	return analyses
}

type ersDisabledFlags struct {
	keyspace bool
	shard    bool
}

// gossipShardPrimaries returns a map of "keyspace/shard" -> primary tablet alias
// and a map of ERS-disabled flags.
func gossipShardPrimaries(state gossipStateProvider) (map[string]string, map[string]ersDisabledFlags) {
	primaries := make(map[string]string)
	disabled := make(map[string]ersDisabledFlags)
	seen := make(map[string]bool)

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

		primary, err := shardPrimary(ks, shard)
		if err != nil || primary == nil {
			continue
		}
		primaries[key] = topoproto.TabletAliasString(primary.Alias)

		ksInfo, err := inst.ReadKeyspace(ks)
		if err == nil && ksInfo != nil && ksInfo.VtorcState != nil {
			disabled[key] = ersDisabledFlags{
				keyspace: ksInfo.VtorcState.DisableEmergencyReparent,
			}
		}
	}
	return primaries, disabled
}
