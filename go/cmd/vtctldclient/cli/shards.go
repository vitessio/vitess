/*
Copyright 2021 The Vitess Authors.

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

package cli

import (
	"sort"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/topo/topoproto"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// ParseKeyspaceShards takes a list of positional arguments and converts them to
// vtctldatapb.Shard objects.
func ParseKeyspaceShards(args []string) ([]*vtctldatapb.Shard, error) {
	shards := make([]*vtctldatapb.Shard, 0, len(args))

	for _, arg := range args {
		keyspace, shard, err := topoproto.ParseKeyspaceShard(arg)
		if err != nil {
			return nil, err
		}

		shards = append(shards, &vtctldatapb.Shard{
			Keyspace: keyspace,
			Name:     shard,
		})
	}

	return shards, nil
}

// ReplicatingTablet is a struct to group a Tablet together with its replication
// Status.
type ReplicatingTablet struct {
	*replicationdatapb.Status
	*topodatapb.Tablet
}

type rTablets []*ReplicatingTablet

func (rts rTablets) Len() int      { return len(rts) }
func (rts rTablets) Swap(i, j int) { rts[i], rts[j] = rts[j], rts[i] }
func (rts rTablets) Less(i, j int) bool {
	l, r := rts[i], rts[j]

	// l or r ReplicationStatus would be nil if we failed to get
	// the position (put them at the beginning of the list)
	if l.Status == nil {
		return r.Status != nil
	}

	if r.Status == nil {
		return false
	}

	// the type proto has MASTER first, so sort by that. Will show
	// the MASTER first, then each replica type sorted by
	// replication position.
	if l.Tablet.Type < r.Tablet.Type {
		return true
	}

	if l.Tablet.Type > r.Tablet.Type {
		return false
	}

	// then compare replication positions
	lpos, err := mysql.DecodePosition(l.Status.Position)
	if err != nil {
		return true
	}

	rpos, err := mysql.DecodePosition(r.Status.Position)
	if err != nil {
		return false
	}

	return !lpos.AtLeast(rpos)
}

// SortedReplicatingTablets returns a sorted list of replicating tablets (which
// is a struct grouping a Tablet together with its replication Status).
//
// The sorting order is:
// 1. Tablets that do not have a replication Status.
// 2. Any tablets of type MASTER.
// 3. Remaining tablets sorted by comparing replication positions.
func SortedReplicatingTablets(tabletMap map[string]*topodatapb.Tablet, replicationStatuses map[string]*replicationdatapb.Status) []*ReplicatingTablet {
	rtablets := make([]*ReplicatingTablet, 0, len(tabletMap))

	for alias, tablet := range tabletMap {
		if status, ok := replicationStatuses[alias]; ok {
			rtablets = append(rtablets, &ReplicatingTablet{
				Status: status,
				Tablet: tablet,
			})
		} else {
			rtablets = append(rtablets, &ReplicatingTablet{Tablet: tablet})
		}
	}

	sort.Sort(rTablets(rtablets))

	return rtablets
}
