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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/gossip"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtorc/inst"
)

type (
	fakeGossipState struct {
		members []gossip.Member
		states  map[gossip.NodeID]gossip.State
	}

	quorumMember struct {
		id         string
		keyspace   string
		shard      string
		alias      string
		status     gossip.Status
		lastUpdate time.Time
	}
)

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

func aliasAt(index int) string {
	return fmt.Sprintf("zone1-%010d", (index+1)*100)
}

func quorumState(members ...quorumMember) *fakeGossipState {
	now := time.Now()
	state := &fakeGossipState{
		members: make([]gossip.Member, 0, len(members)),
		states:  make(map[gossip.NodeID]gossip.State, len(members)),
	}
	for _, member := range members {
		if member.keyspace == "" {
			member.keyspace = "ks"
		}
		if member.shard == "" {
			member.shard = "0"
		}
		if member.lastUpdate.IsZero() {
			member.lastUpdate = now
		}
		state.members = append(state.members, makeMember(member.id, member.keyspace, member.shard, member.alias))
		state.states[gossip.NodeID(member.id)] = gossip.State{
			Status:     member.status,
			LastUpdate: member.lastUpdate,
		}
	}
	return state
}

func singleShardState(statuses ...gossip.Status) *fakeGossipState {
	members := make([]quorumMember, 0, len(statuses))
	for index, status := range statuses {
		members = append(members, quorumMember{
			id:     fmt.Sprintf("node%d", index+1),
			alias:  aliasAt(index),
			status: status,
		})
	}
	return quorumState(members...)
}

func failedVTOrcView(shards ...string) *VTOrcView {
	view := &VTOrcView{HealthCheckFailed: make(map[string]bool, len(shards))}
	for _, shard := range shards {
		view.HealthCheckFailed[shard] = true
	}
	return view
}

func TestBuildVTOrcViewDetectsLikelyPartition(t *testing.T) {
	view := &VTOrcView{
		HealthCheckFailed: map[string]bool{
			"ks/0":    true,
			"ks/80-":  true,
			"ks/c0-":  true,
			"other/0": true,
		},
	}

	assert.True(t, view.confirmsPrimaryDown("ks/0"))
	assert.True(t, view.confirmsPrimaryDown("ks/80-"))

	view.SelfLikelyPartitioned = true
	assert.False(t, view.confirmsPrimaryDown("ks/0"))
	assert.False(t, view.confirmsPrimaryDown("ks/80-"))
}

func TestQuorumAnalysisCases(t *testing.T) {
	now := time.Now()
	tieState := singleShardState(
		gossip.StatusDown,
		gossip.StatusAlive,
		gossip.StatusAlive,
		gossip.StatusDown,
		gossip.StatusDown,
	)

	tests := []struct {
		name           string
		state          GossipStateProvider
		primaries      map[string]string
		currentMembers map[string]map[string]struct{}
		view           *VTOrcView
		wantAnalysis   bool
		wantAlias      string
		wantShard      string
	}{{
		name:         "small shard with vtorc corroboration",
		state:        singleShardState(gossip.StatusDown, gossip.StatusAlive, gossip.StatusAlive),
		primaries:    map[string]string{"ks/0": aliasAt(0)},
		view:         failedVTOrcView("ks/0"),
		wantAnalysis: true,
		wantAlias:    aliasAt(0),
		wantShard:    "0",
	}, {
		name:      "small shard without vtorc view",
		state:     singleShardState(gossip.StatusDown, gossip.StatusAlive, gossip.StatusAlive),
		primaries: map[string]string{"ks/0": aliasAt(0)},
	}, {
		name:      "small shard with primary healthy from vtorc view",
		state:     singleShardState(gossip.StatusDown, gossip.StatusAlive, gossip.StatusAlive),
		primaries: map[string]string{"ks/0": aliasAt(0)},
		view:      &VTOrcView{HealthCheckFailed: map[string]bool{"ks/0": false}},
	}, {
		name:      "small shard ignores vtorc when self partitioned",
		state:     singleShardState(gossip.StatusDown, gossip.StatusAlive, gossip.StatusAlive),
		primaries: map[string]string{"ks/0": aliasAt(0)},
		view: &VTOrcView{
			HealthCheckFailed:     map[string]bool{"ks/0": true},
			SelfLikelyPartitioned: true,
		},
	}, {
		name:      "primary alive",
		state:     singleShardState(gossip.StatusAlive, gossip.StatusAlive, gossip.StatusAlive),
		primaries: map[string]string{"ks/0": aliasAt(0)},
	}, {
		name: "no quorum when most replicas are down",
		state: singleShardState(
			gossip.StatusDown,
			gossip.StatusDown,
			gossip.StatusDown,
			gossip.StatusAlive,
		),
		primaries: map[string]string{"ks/0": aliasAt(0)},
	}, {
		name:      "insufficient observers",
		state:     singleShardState(gossip.StatusDown, gossip.StatusAlive),
		primaries: map[string]string{"ks/0": aliasAt(0)},
	}, {
		name:      "majority not met without tiebreaker",
		state:     tieState,
		primaries: map[string]string{"ks/0": aliasAt(0)},
	}, {
		name:         "tie broken by vtorc",
		state:        tieState,
		primaries:    map[string]string{"ks/0": aliasAt(0)},
		view:         failedVTOrcView("ks/0"),
		wantAnalysis: true,
		wantAlias:    aliasAt(0),
		wantShard:    "0",
	}, {
		name:      "tie not broken when vtorc sees primary alive",
		state:     tieState,
		primaries: map[string]string{"ks/0": aliasAt(0)},
		view:      &VTOrcView{HealthCheckFailed: map[string]bool{"ks/0": false}},
	}, {
		name: "clear majority ignores vtorc view",
		state: singleShardState(
			gossip.StatusDown,
			gossip.StatusAlive,
			gossip.StatusAlive,
			gossip.StatusAlive,
		),
		primaries:    map[string]string{"ks/0": aliasAt(0)},
		wantAnalysis: true,
		wantAlias:    aliasAt(0),
		wantShard:    "0",
	}, {
		name: "multiple shards stay independent",
		state: quorumState(
			quorumMember{id: "node1", shard: "0", alias: aliasAt(0), status: gossip.StatusDown},
			quorumMember{id: "node2", shard: "0", alias: aliasAt(1), status: gossip.StatusAlive},
			quorumMember{id: "node3", shard: "0", alias: aliasAt(2), status: gossip.StatusAlive},
			quorumMember{id: "node4", shard: "80-", alias: aliasAt(3), status: gossip.StatusAlive},
			quorumMember{id: "node5", shard: "80-", alias: aliasAt(4), status: gossip.StatusAlive},
			quorumMember{id: "node6", shard: "80-", alias: aliasAt(5), status: gossip.StatusAlive},
		),
		primaries: map[string]string{
			"ks/0":   aliasAt(0),
			"ks/80-": aliasAt(3),
		},
		view:         failedVTOrcView("ks/0"),
		wantAnalysis: true,
		wantAlias:    aliasAt(0),
		wantShard:    "0",
	}, {
		name:      "primary suspect",
		state:     singleShardState(gossip.StatusSuspect, gossip.StatusAlive, gossip.StatusAlive),
		primaries: map[string]string{"ks/0": aliasAt(0)},
	}, {
		name:      "nil state",
		state:     nil,
		primaries: map[string]string{"ks/0": aliasAt(0)},
	}, {
		name:      "shard not in primaries map",
		state:     singleShardState(gossip.StatusDown, gossip.StatusAlive, gossip.StatusAlive),
		primaries: map[string]string{},
	}, {
		name: "ignores stale replica members",
		state: quorumState(
			quorumMember{id: "node1", alias: aliasAt(0), status: gossip.StatusDown},
			quorumMember{id: "node2", alias: aliasAt(1), status: gossip.StatusAlive},
			quorumMember{id: "node3", alias: aliasAt(2), status: gossip.StatusAlive},
			quorumMember{id: "stale1", alias: aliasAt(3), status: gossip.StatusDown},
			quorumMember{id: "stale2", alias: aliasAt(4), status: gossip.StatusDown},
		),
		primaries:      map[string]string{"ks/0": aliasAt(0)},
		currentMembers: makeCurrentMembers("ks/0", aliasAt(0), aliasAt(1), aliasAt(2)),
		view:           failedVTOrcView("ks/0"),
		wantAnalysis:   true,
		wantAlias:      aliasAt(0),
		wantShard:      "0",
	}, {
		name:           "current members missing from gossip count against quorum",
		state:          singleShardState(gossip.StatusDown, gossip.StatusAlive, gossip.StatusAlive),
		primaries:      map[string]string{"ks/0": aliasAt(0)},
		currentMembers: makeCurrentMembers("ks/0", aliasAt(0), aliasAt(1), aliasAt(2), aliasAt(3), aliasAt(4), aliasAt(5)),
		view:           failedVTOrcView("ks/0"),
	}, {
		name: "does not double count duplicate replica aliases",
		state: quorumState(
			quorumMember{id: "node1", alias: aliasAt(0), status: gossip.StatusDown},
			quorumMember{id: "node2", alias: aliasAt(1), status: gossip.StatusAlive},
			quorumMember{id: "node2-duplicate", alias: aliasAt(1), status: gossip.StatusAlive},
			quorumMember{id: "node3", alias: aliasAt(2), status: gossip.StatusDown},
		),
		primaries:      map[string]string{"ks/0": aliasAt(0)},
		currentMembers: makeCurrentMembers("ks/0", aliasAt(0), aliasAt(1), aliasAt(2)),
		view:           failedVTOrcView("ks/0"),
	}, {
		name: "duplicate primary alias alive prevents down verdict",
		state: quorumState(
			quorumMember{id: "primary-old", alias: aliasAt(0), status: gossip.StatusDown, lastUpdate: now},
			quorumMember{id: "primary-current", alias: aliasAt(0), status: gossip.StatusAlive, lastUpdate: now.Add(time.Second)},
			quorumMember{id: "node2", alias: aliasAt(1), status: gossip.StatusAlive, lastUpdate: now},
			quorumMember{id: "node3", alias: aliasAt(2), status: gossip.StatusAlive, lastUpdate: now},
		),
		primaries:      map[string]string{"ks/0": aliasAt(0)},
		currentMembers: makeCurrentMembers("ks/0", aliasAt(0), aliasAt(1), aliasAt(2)),
		view:           failedVTOrcView("ks/0"),
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := AnalyzeGossipQuorum(tt.state, tt.primaries, tt.currentMembers, tt.view)
			if !tt.wantAnalysis {
				assert.Empty(t, results)
				return
			}

			require.Len(t, results, 1)
			assert.Equal(t, inst.PrimaryTabletUnreachableByQuorum, results[0].Analysis)
			assert.Equal(t, tt.wantAlias, topoproto.TabletAliasString(results[0].AnalyzedInstanceAlias))
			assert.Equal(t, "ks", results[0].AnalyzedKeyspace)
			assert.Equal(t, tt.wantShard, results[0].AnalyzedShard)
			assert.True(t, results[0].IsClusterPrimary)
			assert.True(t, results[0].IsPrimary)
		})
	}
}
