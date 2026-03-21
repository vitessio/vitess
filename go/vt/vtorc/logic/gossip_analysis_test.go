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

func TestQuorumAnalysis_PrimaryDownMajorityAlive(t *testing.T) {
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

	results := analyzeGossipQuorum(state, primaries, nil)

	require.Len(t, results, 1)
	assert.Equal(t, inst.PrimaryTabletUnreachableByQuorum, results[0].Analysis)
	assert.Equal(t, "zone1-0000000100", topoproto.TabletAliasString(results[0].AnalyzedInstanceAlias))
	assert.Equal(t, "ks", results[0].AnalyzedKeyspace)
	assert.Equal(t, "0", results[0].AnalyzedShard)
	assert.True(t, results[0].IsClusterPrimary)
	assert.True(t, results[0].IsPrimary)
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

	results := analyzeGossipQuorum(state, primaries, nil)

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

	results := analyzeGossipQuorum(state, primaries, nil)

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

	results := analyzeGossipQuorum(state, primaries, nil)

	assert.Empty(t, results)
}

func TestQuorumAnalysis_MinimalValidQuorum(t *testing.T) {
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

	results := analyzeGossipQuorum(state, primaries, nil)

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

	results := analyzeGossipQuorum(state, primaries, nil)

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
	vtorcView := map[string]bool{"ks/0": true}

	results := analyzeGossipQuorum(state, primaries, vtorcView)

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
	vtorcView := map[string]bool{"ks/0": false}

	results := analyzeGossipQuorum(state, primaries, vtorcView)

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

	results := analyzeGossipQuorum(state, primaries, nil)

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

	results := analyzeGossipQuorum(state, primaries, nil)

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

	results := analyzeGossipQuorum(state, primaries, nil)

	assert.Empty(t, results)
}

func TestQuorumAnalysis_NilState(t *testing.T) {
	results := analyzeGossipQuorum(nil, map[string]string{"ks/0": "zone1-0000000100"}, nil)

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

	results := analyzeGossipQuorum(state, primaries, nil)

	assert.Empty(t, results)
}
