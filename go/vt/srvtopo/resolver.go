/*
Copyright 2017 Google Inc.

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

package srvtopo

import (
	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vttablet/queryservice"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// A Resolver can resolve keyspace ids and key ranges into ResolvedShard*
// objects. It uses an underlying srvtopo.Server to find the topology,
// and a TargetStats object to find the healthy destinations.
type Resolver struct {
	// topoServ is the srvtopo.Server to use for topo queries.
	topoServ Server

	// stats provides the health information.
	stats TargetStats

	// localCell is the local cell for the queries.
	localCell string

	// FIXME(alainjobart) also need a list of remote cells.
	// FIXME(alainjobart) and a policy on how to use them.
	// But for now we only use the local cell.
}

// NewResolver creates a new Resolver.
func NewResolver(topoServ Server, stats TargetStats, localCell string) *Resolver {
	return &Resolver{
		topoServ:  topoServ,
		stats:     stats,
		localCell: localCell,
	}
}

// ResolvedShard contains everything we need to send a query to a shard.
type ResolvedShard struct {
	// Target describes the target shard.
	Target *querypb.Target

	// QueryService is the actual way to execute the query.
	QueryService queryservice.QueryService
}

// ResolvedShardEqual is an equality check on *ResolvedShard.
func ResolvedShardEqual(rs1, rs2 *ResolvedShard) bool {
	return proto.Equal(rs1.Target, rs2.Target)
}

// ResolvedShardsEqual is an equality check on []*ResolvedShard.
func ResolvedShardsEqual(rss1, rss2 []*ResolvedShard) bool {
	if len(rss1) != len(rss2) {
		return false
	}
	for i, rs1 := range rss1 {
		if !ResolvedShardEqual(rs1, rss2[i]) {
			return false
		}
	}
	return true
}

// ResolveKeyspaceIds turns a list of KeyspaceIds into a list of ResolvedShard.
// The returned ResolvedShard objects can then be used to execute the queries.
func (r *Resolver) ResolveKeyspaceIds(ctx context.Context, keyspace string, tabletType topodatapb.TabletType, keyspaceIds [][]byte) ([]*ResolvedShard, error) {
	keyspace, _, allShards, err := GetKeyspaceShards(ctx, r.topoServ, r.localCell, keyspace, tabletType)
	if err != nil {
		return nil, err
	}
	var res []*ResolvedShard
	visited := make(map[string]bool)
	for _, ksID := range keyspaceIds {
		shard, err := GetShardForKeyspaceID(allShards, ksID)
		if err != nil {
			return nil, err
		}
		if !visited[shard] {
			// First time we see this shard.
			target := &querypb.Target{
				Keyspace:   keyspace,
				Shard:      shard,
				TabletType: tabletType,
				Cell:       r.localCell,
			}
			_, qs, err := r.stats.GetAggregateStats(target)
			if err != nil {
				return nil, resolverError(err, target)
			}

			// FIXME(alainjobart) we ignore the stats for now.
			// Later we can fallback to another cell if needed.
			// We would then need to read the SrvKeyspace there too.
			target.Cell = ""
			res = append(res, &ResolvedShard{
				Target:       target,
				QueryService: qs,
			})
			visited[shard] = true
		}
	}
	return res, nil
}

// GetAnyShard returns a ResolvedShard object for a random shard in the
// keyspace. In practice, the implementation now returns the first one.
func (r *Resolver) GetAnyShard(ctx context.Context, keyspace string, tabletType topodatapb.TabletType) (*ResolvedShard, error) {
	keyspace, _, allShards, err := GetKeyspaceShards(ctx, r.topoServ, r.localCell, keyspace, tabletType)
	if err != nil {
		return nil, err
	}
	if len(allShards) == 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "keyspace: %v, tabletType: %v, no shard", keyspace, topoproto.TabletTypeLString(tabletType))
	}

	shard := allShards[0].Name
	target := &querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: tabletType,
		Cell:       r.localCell,
	}
	_, qs, err := r.stats.GetAggregateStats(target)
	if err != nil {
		return nil, resolverError(err, target)
	}
	target.Cell = ""
	return &ResolvedShard{
		Target:       target,
		QueryService: qs,
	}, nil
}

// GetAllShards returns the list of ResolvedShards associated with all the shards in a keyspace.
func (r *Resolver) GetAllShards(ctx context.Context, keyspace string, tabletType topodatapb.TabletType) ([]*ResolvedShard, error) {
	keyspace, _, allShards, err := GetKeyspaceShards(ctx, r.topoServ, r.localCell, keyspace, tabletType)
	if err != nil {
		return nil, err
	}

	res := make([]*ResolvedShard, len(allShards))
	for i, shard := range allShards {
		target := &querypb.Target{
			Keyspace:   keyspace,
			Shard:      shard.Name,
			TabletType: tabletType,
			Cell:       r.localCell,
		}
		_, qs, err := r.stats.GetAggregateStats(target)
		if err != nil {
			return nil, resolverError(err, target)
		}

		// FIXME(alainjobart) we ignore the stats for now.
		// Later we can fallback to another cell if needed.
		// We would then need to read the SrvKeyspace there too.
		target.Cell = ""
		res[i] = &ResolvedShard{
			Target:       target,
			QueryService: qs,
		}
	}
	return res, nil
}

// ResolveShards returns the list of ResolvedShards associated with the
// Shard list.
func (r *Resolver) ResolveShards(ctx context.Context, keyspace string, shards []string, tabletType topodatapb.TabletType) ([]*ResolvedShard, error) {
	// FIXME(alainjobart) make sure the list of shards if unique.
	res := make([]*ResolvedShard, len(shards))
	for i, shard := range shards {
		target := &querypb.Target{
			Keyspace:   keyspace,
			Shard:      shard,
			TabletType: tabletType,
			Cell:       r.localCell,
		}
		_, qs, err := r.stats.GetAggregateStats(target)
		if err != nil {
			return nil, resolverError(err, target)
		}

		// FIXME(alainjobart) we ignore the stats for now.
		// Later we can fallback to another cell if needed.
		target.Cell = ""
		res[i] = &ResolvedShard{
			Target:       target,
			QueryService: qs,
		}
	}
	return res, nil
}

// ResolveKeyRanges returns the set of shards that "intersect"
// with a collection of key-ranges; that is, a shard is included if
// and only if its corresponding key-space ids are in one of the key-ranges.
func (r *Resolver) ResolveKeyRanges(ctx context.Context, keyspace string, tabletType topodatapb.TabletType, krs []*topodatapb.KeyRange) ([]*ResolvedShard, error) {
	keyspace, _, allShards, err := GetKeyspaceShards(ctx, r.topoServ, r.localCell, keyspace, tabletType)
	if err != nil {
		return nil, err
	}
	uniqueShards := make(map[string]bool)
	for _, kr := range krs {
		ResolveKeyRangeToShards(allShards, uniqueShards, kr)
	}
	var res = make([]*ResolvedShard, 0, len(uniqueShards))
	for shard := range uniqueShards {
		target := &querypb.Target{
			Keyspace:   keyspace,
			Shard:      shard,
			TabletType: tabletType,
			Cell:       r.localCell,
		}
		_, qs, err := r.stats.GetAggregateStats(target)
		if err != nil {
			return nil, resolverError(err, target)
		}

		// FIXME(alainjobart) we ignore the stats for now.
		// Later we can fallback to another cell if needed.
		// We would then need to read the SrvKeyspace there too.
		target.Cell = ""
		res = append(res, &ResolvedShard{
			Target:       target,
			QueryService: qs,
		})
	}
	return res, nil
}

// ResolveEntityIds returns a map of shards to values to use in that shard.
func (r *Resolver) ResolveEntityIds(ctx context.Context, keyspace string, entityIds []*vtgatepb.ExecuteEntityIdsRequest_EntityId, tabletType topodatapb.TabletType) ([]*ResolvedShard, [][]*querypb.Value, error) {
	keyspace, _, allShards, err := GetKeyspaceShards(ctx, r.topoServ, r.localCell, keyspace, tabletType)
	if err != nil {
		return nil, nil, err
	}
	var result []*ResolvedShard
	var values [][]*querypb.Value
	resolved := make(map[string]int)
	for _, eid := range entityIds {
		shard, err := GetShardForKeyspaceID(allShards, eid.KeyspaceId)
		if err != nil {
			return nil, nil, err
		}
		i, ok := resolved[shard]
		if !ok {
			target := &querypb.Target{
				Keyspace:   keyspace,
				Shard:      shard,
				TabletType: tabletType,
				Cell:       r.localCell,
			}
			_, qs, err := r.stats.GetAggregateStats(target)
			if err != nil {
				return nil, nil, resolverError(err, target)
			}

			// FIXME(alainjobart) we ignore the stats for now.
			// Later we can fallback to another cell if needed.
			// We would then need to read the SrvKeyspace there too.
			target.Cell = ""
			i = len(result)
			result = append(result, &ResolvedShard{
				Target:       target,
				QueryService: qs,
			})
			values = append(values, nil)
			resolved[shard] = i
		}
		values[i] = append(values[i], &querypb.Value{Type: eid.Type, Value: eid.Value})
	}
	return result, values, nil
}

// ValuesEqual is a helper method to compare arrays of values.
func ValuesEqual(vss1, vss2 [][]*querypb.Value) bool {
	if len(vss1) != len(vss2) {
		return false
	}
	for i, vs1 := range vss1 {
		if len(vs1) != len(vss2[i]) {
			return false
		}
		for j, v1 := range vs1 {
			if !proto.Equal(v1, vss2[i][j]) {
				return false
			}
		}
	}
	return true
}

func resolverError(in error, target *querypb.Target) error {
	return vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "target: %s.%s.%s, no valid tablet: %v", target.Keyspace, target.Shard, topoproto.TabletTypeLString(target.TabletType), in)
}
