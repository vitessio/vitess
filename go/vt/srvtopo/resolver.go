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
	"sort"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/queryservice"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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

// GetKeyspaceShards return all the shards in a keyspace. It follows
// redirection if ServedFrom is set. It is only valid for the local cell.
// Do not use it to further resolve shards, instead use the Resolve* methods.
func (r *Resolver) GetKeyspaceShards(ctx context.Context, keyspace string, tabletType topodatapb.TabletType) (string, *topodatapb.SrvKeyspace, []*topodatapb.ShardReference, error) {
	srvKeyspace, err := r.topoServ.GetSrvKeyspace(ctx, r.localCell, keyspace)
	if err != nil {
		return "", nil, nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "keyspace %v fetch error: %v", keyspace, err)
	}

	// check if the keyspace has been redirected for this tabletType.
	for _, sf := range srvKeyspace.ServedFrom {
		if sf.TabletType == tabletType {
			keyspace = sf.Keyspace
			srvKeyspace, err = r.topoServ.GetSrvKeyspace(ctx, r.localCell, keyspace)
			if err != nil {
				return "", nil, nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "keyspace %v fetch error: %v", keyspace, err)
			}
		}
	}

	partition := topoproto.SrvKeyspaceGetPartition(srvKeyspace, tabletType)
	if partition == nil {
		return "", nil, nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "No partition found for tabletType %v in keyspace %v", topoproto.TabletTypeLString(tabletType), keyspace)
	}
	return keyspace, srvKeyspace, partition.ShardReferences, nil
}

// ResolveKeyspaceIds turns a list of KeyspaceIds into a list of ResolvedShard.
// The returned ResolvedShard objects can then be used to execute the queries.
func (r *Resolver) ResolveKeyspaceIds(ctx context.Context, keyspace string, tabletType topodatapb.TabletType, keyspaceIds [][]byte) ([]*ResolvedShard, error) {
	keyspace, _, allShards, err := r.GetKeyspaceShards(ctx, keyspace, tabletType)
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
	keyspace, _, allShards, err := r.GetKeyspaceShards(ctx, keyspace, tabletType)
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

// GetAllShards returns the list of ResolvedShards associated with all
// the shards in a keyspace.
func (r *Resolver) GetAllShards(ctx context.Context, keyspace string, tabletType topodatapb.TabletType) ([]*ResolvedShard, *topodatapb.SrvKeyspace, error) {
	keyspace, srvKeyspace, allShards, err := r.GetKeyspaceShards(ctx, keyspace, tabletType)
	if err != nil {
		return nil, nil, err
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
			return nil, nil, resolverError(err, target)
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
	return res, srvKeyspace, nil
}

// GetAllKeyspaces returns all the known keyspaces in the local cell.
func (r *Resolver) GetAllKeyspaces(ctx context.Context) ([]string, error) {
	keyspaces, err := r.topoServ.GetSrvKeyspaceNames(ctx, r.localCell)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "keyspace names fetch error: %v", err)
	}
	// FIXME(alainjobart) this should be unnecessary. The results
	// of ListDir are sorted, and that's the underlying topo code.
	// But the tests depend on this behavior now.
	sort.Strings(keyspaces)
	return keyspaces, nil
}

// ResolveShards returns the list of ResolvedShards associated with the
// Shard list.
// FIXME(alainjobart) this should first resolve keyspace to allow for redirects.
func (r *Resolver) ResolveShards(ctx context.Context, keyspace string, shards []string, tabletType topodatapb.TabletType) ([]*ResolvedShard, error) {
	res := make([]*ResolvedShard, 0, len(shards))
	visited := make(map[string]bool)
	for _, shard := range shards {
		// Make sure we don't duplicate shards.
		if visited[shard] {
			continue
		}
		visited[shard] = true

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
		res = append(res, &ResolvedShard{
			Target:       target,
			QueryService: qs,
		})
	}
	return res, nil
}

// ResolveExactShards resolves a keyrange to shards only if there's a complete
// match. If there's any partial match the function returns no match.
func (r *Resolver) ResolveExactShards(ctx context.Context, keyspace string, tabletType topodatapb.TabletType, kr *topodatapb.KeyRange) ([]*ResolvedShard, error) {
	keyspace, _, allShards, err := r.GetKeyspaceShards(ctx, keyspace, tabletType)
	if err != nil {
		return nil, err
	}
	var res []*ResolvedShard
	shardnum := 0
	for shardnum < len(allShards) {
		if key.KeyRangeStartEqual(kr, allShards[shardnum].KeyRange) {
			break
		}
		shardnum++
	}
	for shardnum < len(allShards) {
		if !key.KeyRangesIntersect(kr, allShards[shardnum].KeyRange) {
			// If we are over the requested keyrange, we
			// can stop now, we won't find more.
			break
		}

		target := &querypb.Target{
			Keyspace:   keyspace,
			Shard:      allShards[shardnum].Name,
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
		res = append(res, &ResolvedShard{
			Target:       target,
			QueryService: qs,
		})
		if key.KeyRangeEndEqual(kr, allShards[shardnum].KeyRange) {
			return res, nil
		}
		shardnum++
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "keyrange %v does not exactly match shards", key.KeyRangeString(kr))
}

// ResolveKeyRanges returns the set of shards that "intersect"
// with a collection of key-ranges; that is, a shard is included if
// and only if its corresponding key-space ids are in one of the key-ranges.
func (r *Resolver) ResolveKeyRanges(ctx context.Context, keyspace string, tabletType topodatapb.TabletType, krs []*topodatapb.KeyRange) ([]*ResolvedShard, error) {
	keyspace, _, allShards, err := r.GetKeyspaceShards(ctx, keyspace, tabletType)
	if err != nil {
		return nil, err
	}
	var res []*ResolvedShard
	visited := make(map[string]bool)
	for _, kr := range krs {
		for _, shard := range allShards {
			if !key.KeyRangesIntersect(kr, shard.KeyRange) {
				// We don't need that shard.
				continue
			}
			if visited[shard.Name] {
				// We've already added that shard.
				continue
			}
			// We need to add this shard.
			visited[shard.Name] = true
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
			res = append(res, &ResolvedShard{
				Target:       target,
				QueryService: qs,
			})
		}
	}
	return res, nil
}

// ResolveEntityIds returns a list of shards and values to use in that shard.
func (r *Resolver) ResolveEntityIds(ctx context.Context, keyspace string, entityIds []*vtgatepb.ExecuteEntityIdsRequest_EntityId, tabletType topodatapb.TabletType) ([]*ResolvedShard, [][]*querypb.Value, error) {
	keyspace, _, allShards, err := r.GetKeyspaceShards(ctx, keyspace, tabletType)
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

// ResolveKeyspaceIdsValues resolves keyspace IDs and values into their
// respective shards. Same logic as ResolveEntityIds.
func (r *Resolver) ResolveKeyspaceIdsValues(ctx context.Context, keyspace string, ids []*querypb.Value, ksids [][]byte, tabletType topodatapb.TabletType) ([]*ResolvedShard, [][]*querypb.Value, error) {
	keyspace, _, allShards, err := r.GetKeyspaceShards(ctx, keyspace, tabletType)
	if err != nil {
		return nil, nil, err
	}
	var result []*ResolvedShard
	var values [][]*querypb.Value
	resolved := make(map[string]int)
	for i, id := range ids {
		shard, err := GetShardForKeyspaceID(allShards, ksids[i])
		if err != nil {
			return nil, nil, err
		}
		in, ok := resolved[shard]
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
			in = len(result)
			result = append(result, &ResolvedShard{
				Target:       target,
				QueryService: qs,
			})
			values = append(values, nil)
			resolved[shard] = in
		}
		values[in] = append(values[in], id)
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
