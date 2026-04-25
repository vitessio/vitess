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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/gossip"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtorc/inst"
)

type fakeGossipState struct {
	members []gossip.Member
	states  map[gossip.NodeID]gossip.State
}

func (f *fakeGossipState) Members() []gossip.Member                 { return f.members }
func (f *fakeGossipState) Snapshot() map[gossip.NodeID]gossip.State { return f.states }

func makeMember(id, keyspace, shard, alias string) gossip.Member {
	return gossip.Member{
		ID:   gossip.NodeID(id),
		Addr: id,
		Meta: map[string]string{
			gossip.MetaKeyKeyspace:    keyspace,
			gossip.MetaKeyShard:       shard,
			gossip.MetaKeyTabletAlias: alias,
		},
	}
}

func makeCurrentMembers(shard string, aliases ...string) map[string]map[string]struct{} {
	members := make(map[string]struct{}, len(aliases))
	for _, alias := range aliases {
		members[alias] = struct{}{}
	}
	return map[string]map[string]struct{}{shard: members}
}

func TestQuorumAnalysis_PrimaryDownMajorityAlive(t *testing.T) {
	// 3-tablet shard (1 primary + 2 replicas). For small shards we
	// require VTOrc's own health check to also fail; otherwise the
	// thin quorum is not considered reliable.
	state := &fakeGossipState{
		members: []gossip.Member{
			makeMember("node1", "ks", "0", "zone1-0000000100"),
			makeMember("node2", "ks", "0", "zone1-0000000200"),
			makeMember("node3", "ks", "0", "zone1-0000000300"),
		},
		states: map[gossip.NodeID]gossip.State{
			"node1": {Status: gossip.StatusDown, LastUpdate: time.Now()},
			"node2": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
			"node3": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
		},
	}
	primaries := map[string]string{"ks/0": "zone1-0000000100"}
	vtorcView := &VTOrcView{HealthCheckFailed: map[string]bool{"ks/0": true}}

	results := AnalyzeGossipQuorum(state, primaries, nil, vtorcView)

	require.Len(t, results, 1)
	assert.Equal(t, inst.PrimaryTabletUnreachableByQuorum, results[0].Analysis)
	assert.Equal(t, "zone1-0000000100", topoproto.TabletAliasString(results[0].AnalyzedInstanceAlias))
	assert.Equal(t, "ks", results[0].AnalyzedKeyspace)
	assert.Equal(t, "0", results[0].AnalyzedShard)
	assert.True(t, results[0].IsClusterPrimary)
	assert.True(t, results[0].IsPrimary)
}

func TestQuorumAnalysis_SmallShardWithoutVTOrcCorroboration(t *testing.T) {
	// 3-tablet shard (1 primary + 2 replicas) with unanimous gossip
	// Down verdict but no VTOrc corroboration. Should abstain because
	// strict majority equals unanimous at this size — too easy for a
	// correlated failure (partition) to trigger a false ERS.
	state := &fakeGossipState{
		members: []gossip.Member{
			makeMember("node1", "ks", "0", "zone1-0000000100"),
			makeMember("node2", "ks", "0", "zone1-0000000200"),
			makeMember("node3", "ks", "0", "zone1-0000000300"),
		},
		states: map[gossip.NodeID]gossip.State{
			"node1": {Status: gossip.StatusDown, LastUpdate: time.Now()},
			"node2": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
			"node3": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
		},
	}
	primaries := map[string]string{"ks/0": "zone1-0000000100"}

	// No VTOrc view at all — no corroboration.
	assert.Empty(t, AnalyzeGossipQuorum(state, primaries, nil, nil))

	// VTOrc view present but reports healthy — no corroboration.
	aliveView := &VTOrcView{HealthCheckFailed: map[string]bool{"ks/0": false}}
	assert.Empty(t, AnalyzeGossipQuorum(state, primaries, nil, aliveView))
}

func TestQuorumAnalysis_SmallShardIgnoresVTOrcIfPartitioned(t *testing.T) {
	// Small shard with unanimous gossip Down AND VTOrc reports health
	// check failure — but VTOrc believes it is itself partitioned.
	// The signal must be suppressed: do not trigger ERS.
	state := &fakeGossipState{
		members: []gossip.Member{
			makeMember("node1", "ks", "0", "zone1-0000000100"),
			makeMember("node2", "ks", "0", "zone1-0000000200"),
			makeMember("node3", "ks", "0", "zone1-0000000300"),
		},
		states: map[gossip.NodeID]gossip.State{
			"node1": {Status: gossip.StatusDown, LastUpdate: time.Now()},
			"node2": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
			"node3": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
		},
	}
	primaries := map[string]string{"ks/0": "zone1-0000000100"}
	view := &VTOrcView{
		HealthCheckFailed:     map[string]bool{"ks/0": true},
		SelfLikelyPartitioned: true,
	}

	assert.Empty(t, AnalyzeGossipQuorum(state, primaries, nil, view))
}

func TestBuildVTOrcViewDetectsLikelyPartition(t *testing.T) {
	// buildVTOrcView is exercised indirectly here by manipulating the
	// output struct — the real impl reads from the VTOrc DB.
	view := &VTOrcView{
		HealthCheckFailed: map[string]bool{
			"ks/0":    true,
			"ks/80-":  true,
			"ks/c0-":  true,
			"other/0": true,
		},
	}

	// Without the flag, all keys look confirmed down.
	assert.True(t, view.confirmsPrimaryDown("ks/0"))
	assert.True(t, view.confirmsPrimaryDown("ks/80-"))

	// When partition flag is set, nothing corroborates.
	view.SelfLikelyPartitioned = true
	assert.False(t, view.confirmsPrimaryDown("ks/0"))
	assert.False(t, view.confirmsPrimaryDown("ks/80-"))
}

func TestQuorumAnalysis_PrimaryAlive(t *testing.T) {
	state := &fakeGossipState{
		members: []gossip.Member{
			makeMember("node1", "ks", "0", "zone1-0000000100"),
			makeMember("node2", "ks", "0", "zone1-0000000200"),
			makeMember("node3", "ks", "0", "zone1-0000000300"),
		},
		states: map[gossip.NodeID]gossip.State{
			"node1": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
			"node2": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
			"node3": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
		},
	}
	primaries := map[string]string{"ks/0": "zone1-0000000100"}

	results := AnalyzeGossipQuorum(state, primaries, nil, nil)

	assert.Empty(t, results)
}

func TestQuorumAnalysis_NoQuorum_MostReplicasDown(t *testing.T) {
	state := &fakeGossipState{
		members: []gossip.Member{
			makeMember("node1", "ks", "0", "zone1-0000000100"),
			makeMember("node2", "ks", "0", "zone1-0000000200"),
			makeMember("node3", "ks", "0", "zone1-0000000300"),
			makeMember("node4", "ks", "0", "zone1-0000000400"),
		},
		states: map[gossip.NodeID]gossip.State{
			"node1": {Status: gossip.StatusDown, LastUpdate: time.Now()},
			"node2": {Status: gossip.StatusDown, LastUpdate: time.Now()},
			"node3": {Status: gossip.StatusDown, LastUpdate: time.Now()},
			"node4": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
		},
	}
	primaries := map[string]string{"ks/0": "zone1-0000000100"}

	results := AnalyzeGossipQuorum(state, primaries, nil, nil)

	assert.Empty(t, results)
}

func TestQuorumAnalysis_InsufficientObservers(t *testing.T) {
	state := &fakeGossipState{
		members: []gossip.Member{
			makeMember("node1", "ks", "0", "zone1-0000000100"),
			makeMember("node2", "ks", "0", "zone1-0000000200"),
		},
		states: map[gossip.NodeID]gossip.State{
			"node1": {Status: gossip.StatusDown, LastUpdate: time.Now()},
			"node2": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
		},
	}
	primaries := map[string]string{"ks/0": "zone1-0000000100"}

	results := AnalyzeGossipQuorum(state, primaries, nil, nil)

	assert.Empty(t, results)
}

func TestQuorumAnalysis_MinimalValidQuorum(t *testing.T) {
	// Same 3-tablet small-shard layout — valid quorum requires VTOrc
	// corroboration.
	state := &fakeGossipState{
		members: []gossip.Member{
			makeMember("node1", "ks", "0", "zone1-0000000100"),
			makeMember("node2", "ks", "0", "zone1-0000000200"),
			makeMember("node3", "ks", "0", "zone1-0000000300"),
		},
		states: map[gossip.NodeID]gossip.State{
			"node1": {Status: gossip.StatusDown, LastUpdate: time.Now()},
			"node2": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
			"node3": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
		},
	}
	primaries := map[string]string{"ks/0": "zone1-0000000100"}
	vtorcView := &VTOrcView{HealthCheckFailed: map[string]bool{"ks/0": true}}

	results := AnalyzeGossipQuorum(state, primaries, nil, vtorcView)

	require.Len(t, results, 1)
	assert.Equal(t, inst.PrimaryTabletUnreachableByQuorum, results[0].Analysis)
}

func TestQuorumAnalysis_MajorityNotMet_NoTiebreaker(t *testing.T) {
	state := &fakeGossipState{
		members: []gossip.Member{
			makeMember("node1", "ks", "0", "zone1-0000000100"),
			makeMember("node2", "ks", "0", "zone1-0000000200"),
			makeMember("node3", "ks", "0", "zone1-0000000300"),
			makeMember("node4", "ks", "0", "zone1-0000000400"),
			makeMember("node5", "ks", "0", "zone1-0000000500"),
		},
		states: map[gossip.NodeID]gossip.State{
			"node1": {Status: gossip.StatusDown, LastUpdate: time.Now()},
			"node2": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
			"node3": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
			"node4": {Status: gossip.StatusDown, LastUpdate: time.Now()},
			"node5": {Status: gossip.StatusDown, LastUpdate: time.Now()},
		},
	}
	primaries := map[string]string{"ks/0": "zone1-0000000100"}

	results := AnalyzeGossipQuorum(state, primaries, nil, nil)

	assert.Empty(t, results)
}

func TestQuorumAnalysis_TieBrokenByVTOrc(t *testing.T) {
	state := &fakeGossipState{
		members: []gossip.Member{
			makeMember("node1", "ks", "0", "zone1-0000000100"),
			makeMember("node2", "ks", "0", "zone1-0000000200"),
			makeMember("node3", "ks", "0", "zone1-0000000300"),
			makeMember("node4", "ks", "0", "zone1-0000000400"),
			makeMember("node5", "ks", "0", "zone1-0000000500"),
		},
		states: map[gossip.NodeID]gossip.State{
			"node1": {Status: gossip.StatusDown, LastUpdate: time.Now()},
			"node2": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
			"node3": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
			"node4": {Status: gossip.StatusDown, LastUpdate: time.Now()},
			"node5": {Status: gossip.StatusDown, LastUpdate: time.Now()},
		},
	}
	primaries := map[string]string{"ks/0": "zone1-0000000100"}
	vtorcView := &VTOrcView{HealthCheckFailed: map[string]bool{"ks/0": true}}

	results := AnalyzeGossipQuorum(state, primaries, nil, vtorcView)

	require.Len(t, results, 1)
	assert.Equal(t, inst.PrimaryTabletUnreachableByQuorum, results[0].Analysis)
}

func TestQuorumAnalysis_TieNotBroken_VTOrcSeesPrimaryAlive(t *testing.T) {
	state := &fakeGossipState{
		members: []gossip.Member{
			makeMember("node1", "ks", "0", "zone1-0000000100"),
			makeMember("node2", "ks", "0", "zone1-0000000200"),
			makeMember("node3", "ks", "0", "zone1-0000000300"),
			makeMember("node4", "ks", "0", "zone1-0000000400"),
			makeMember("node5", "ks", "0", "zone1-0000000500"),
		},
		states: map[gossip.NodeID]gossip.State{
			"node1": {Status: gossip.StatusDown, LastUpdate: time.Now()},
			"node2": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
			"node3": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
			"node4": {Status: gossip.StatusDown, LastUpdate: time.Now()},
			"node5": {Status: gossip.StatusDown, LastUpdate: time.Now()},
		},
	}
	primaries := map[string]string{"ks/0": "zone1-0000000100"}
	vtorcView := &VTOrcView{HealthCheckFailed: map[string]bool{"ks/0": false}}

	results := AnalyzeGossipQuorum(state, primaries, nil, vtorcView)

	assert.Empty(t, results)
}

func TestQuorumAnalysis_ClearMajority_VTOrcViewIgnored(t *testing.T) {
	state := &fakeGossipState{
		members: []gossip.Member{
			makeMember("node1", "ks", "0", "zone1-0000000100"),
			makeMember("node2", "ks", "0", "zone1-0000000200"),
			makeMember("node3", "ks", "0", "zone1-0000000300"),
			makeMember("node4", "ks", "0", "zone1-0000000400"),
		},
		states: map[gossip.NodeID]gossip.State{
			"node1": {Status: gossip.StatusDown, LastUpdate: time.Now()},
			"node2": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
			"node3": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
			"node4": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
		},
	}
	primaries := map[string]string{"ks/0": "zone1-0000000100"}

	results := AnalyzeGossipQuorum(state, primaries, nil, nil)

	require.Len(t, results, 1)
	assert.Equal(t, inst.PrimaryTabletUnreachableByQuorum, results[0].Analysis)
}

func TestQuorumAnalysis_MultipleShardsIndependent(t *testing.T) {
	state := &fakeGossipState{
		members: []gossip.Member{
			makeMember("node1", "ks", "0", "zone1-0000000100"),
			makeMember("node2", "ks", "0", "zone1-0000000200"),
			makeMember("node3", "ks", "0", "zone1-0000000300"),
			makeMember("node4", "ks", "80-", "zone1-0000000400"),
			makeMember("node5", "ks", "80-", "zone1-0000000500"),
			makeMember("node6", "ks", "80-", "zone1-0000000600"),
		},
		states: map[gossip.NodeID]gossip.State{
			"node1": {Status: gossip.StatusDown, LastUpdate: time.Now()},
			"node2": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
			"node3": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
			"node4": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
			"node5": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
			"node6": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
		},
	}
	primaries := map[string]string{
		"ks/0":   "zone1-0000000100",
		"ks/80-": "zone1-0000000400",
	}
	vtorcView := &VTOrcView{HealthCheckFailed: map[string]bool{"ks/0": true}}

	results := AnalyzeGossipQuorum(state, primaries, nil, vtorcView)

	require.Len(t, results, 1)
	assert.Equal(t, "zone1-0000000100", topoproto.TabletAliasString(results[0].AnalyzedInstanceAlias))
	assert.Equal(t, "0", results[0].AnalyzedShard)
}

func TestQuorumAnalysis_PrimarySuspect(t *testing.T) {
	state := &fakeGossipState{
		members: []gossip.Member{
			makeMember("node1", "ks", "0", "zone1-0000000100"),
			makeMember("node2", "ks", "0", "zone1-0000000200"),
			makeMember("node3", "ks", "0", "zone1-0000000300"),
		},
		states: map[gossip.NodeID]gossip.State{
			"node1": {Status: gossip.StatusSuspect, LastUpdate: time.Now()},
			"node2": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
			"node3": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
		},
	}
	primaries := map[string]string{"ks/0": "zone1-0000000100"}

	results := AnalyzeGossipQuorum(state, primaries, nil, nil)

	assert.Empty(t, results)
}

func TestQuorumAnalysis_NilState(t *testing.T) {
	results := AnalyzeGossipQuorum(nil, map[string]string{"ks/0": "zone1-0000000100"}, nil, nil)

	assert.Empty(t, results)
}

func TestQuorumAnalysis_ShardNotInPrimariesMap(t *testing.T) {
	state := &fakeGossipState{
		members: []gossip.Member{
			makeMember("node1", "ks", "0", "zone1-0000000100"),
			makeMember("node2", "ks", "0", "zone1-0000000200"),
			makeMember("node3", "ks", "0", "zone1-0000000300"),
		},
		states: map[gossip.NodeID]gossip.State{
			"node1": {Status: gossip.StatusDown, LastUpdate: time.Now()},
			"node2": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
			"node3": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
		},
	}
	primaries := map[string]string{}

	results := AnalyzeGossipQuorum(state, primaries, nil, nil)

	assert.Empty(t, results)
}

func TestQuorumAnalysis_IgnoresStaleReplicaMembers(t *testing.T) {
	state := &fakeGossipState{
		members: []gossip.Member{
			makeMember("node1", "ks", "0", "zone1-0000000100"),
			makeMember("node2", "ks", "0", "zone1-0000000200"),
			makeMember("node3", "ks", "0", "zone1-0000000300"),
			makeMember("stale1", "ks", "0", "zone1-0000000400"),
			makeMember("stale2", "ks", "0", "zone1-0000000500"),
		},
		states: map[gossip.NodeID]gossip.State{
			"node1":  {Status: gossip.StatusDown, LastUpdate: time.Now()},
			"node2":  {Status: gossip.StatusAlive, LastUpdate: time.Now()},
			"node3":  {Status: gossip.StatusAlive, LastUpdate: time.Now()},
			"stale1": {Status: gossip.StatusDown, LastUpdate: time.Now()},
			"stale2": {Status: gossip.StatusDown, LastUpdate: time.Now()},
		},
	}
	primaries := map[string]string{"ks/0": "zone1-0000000100"}
	currentMembers := makeCurrentMembers("ks/0",
		"zone1-0000000100",
		"zone1-0000000200",
		"zone1-0000000300",
	)
	// Current membership has 3 tablets (1 primary + 2 replicas) so this
	// is a small shard — VTOrc corroboration is required.
	vtorcView := &VTOrcView{HealthCheckFailed: map[string]bool{"ks/0": true}}

	results := AnalyzeGossipQuorum(state, primaries, currentMembers, vtorcView)

	require.Len(t, results, 1)
	assert.Equal(t, inst.PrimaryTabletUnreachableByQuorum, results[0].Analysis)
}

func TestQuorumAnalysis_CurrentMembersMissingFromGossipCountAgainstQuorum(t *testing.T) {
	state := &fakeGossipState{
		members: []gossip.Member{
			makeMember("node1", "ks", "0", "zone1-0000000100"),
			makeMember("node2", "ks", "0", "zone1-0000000200"),
			makeMember("node3", "ks", "0", "zone1-0000000300"),
		},
		states: map[gossip.NodeID]gossip.State{
			"node1": {Status: gossip.StatusDown, LastUpdate: time.Now()},
			"node2": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
			"node3": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
		},
	}
	primaries := map[string]string{"ks/0": "zone1-0000000100"}
	currentMembers := makeCurrentMembers("ks/0",
		"zone1-0000000100",
		"zone1-0000000200",
		"zone1-0000000300",
		"zone1-0000000400",
		"zone1-0000000500",
		"zone1-0000000600",
	)
	vtorcView := &VTOrcView{HealthCheckFailed: map[string]bool{"ks/0": true}}

	results := AnalyzeGossipQuorum(state, primaries, currentMembers, vtorcView)

	assert.Empty(t, results)
}

func TestQuorumAnalysis_DoesNotDoubleCountDuplicateReplicaAliases(t *testing.T) {
	state := &fakeGossipState{
		members: []gossip.Member{
			makeMember("node1", "ks", "0", "zone1-0000000100"),
			makeMember("node2", "ks", "0", "zone1-0000000200"),
			makeMember("node2-duplicate", "ks", "0", "zone1-0000000200"),
			makeMember("node3", "ks", "0", "zone1-0000000300"),
		},
		states: map[gossip.NodeID]gossip.State{
			"node1":           {Status: gossip.StatusDown, LastUpdate: time.Now()},
			"node2":           {Status: gossip.StatusAlive, LastUpdate: time.Now()},
			"node2-duplicate": {Status: gossip.StatusAlive, LastUpdate: time.Now()},
			"node3":           {Status: gossip.StatusDown, LastUpdate: time.Now()},
		},
	}
	primaries := map[string]string{"ks/0": "zone1-0000000100"}
	currentMembers := makeCurrentMembers("ks/0",
		"zone1-0000000100",
		"zone1-0000000200",
		"zone1-0000000300",
	)
	vtorcView := &VTOrcView{HealthCheckFailed: map[string]bool{"ks/0": true}}

	results := AnalyzeGossipQuorum(state, primaries, currentMembers, vtorcView)

	assert.Empty(t, results)
}

func TestQuorumAnalysis_DuplicatePrimaryAliasAlivePreventsDownVerdict(t *testing.T) {
	now := time.Now()
	state := &fakeGossipState{
		members: []gossip.Member{
			makeMember("primary-old", "ks", "0", "zone1-0000000100"),
			makeMember("primary-current", "ks", "0", "zone1-0000000100"),
			makeMember("node2", "ks", "0", "zone1-0000000200"),
			makeMember("node3", "ks", "0", "zone1-0000000300"),
		},
		states: map[gossip.NodeID]gossip.State{
			"primary-old":     {Status: gossip.StatusDown, LastUpdate: now},
			"primary-current": {Status: gossip.StatusAlive, LastUpdate: now.Add(time.Second)},
			"node2":           {Status: gossip.StatusAlive, LastUpdate: now},
			"node3":           {Status: gossip.StatusAlive, LastUpdate: now},
		},
	}
	primaries := map[string]string{"ks/0": "zone1-0000000100"}
	currentMembers := makeCurrentMembers("ks/0",
		"zone1-0000000100",
		"zone1-0000000200",
		"zone1-0000000300",
	)
	vtorcView := &VTOrcView{HealthCheckFailed: map[string]bool{"ks/0": true}}

	results := AnalyzeGossipQuorum(state, primaries, currentMembers, vtorcView)

	assert.Empty(t, results)
}
