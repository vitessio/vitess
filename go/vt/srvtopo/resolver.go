/*
Copyright 2019 The Vitess Authors.

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

	"context"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/queryservice"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// A Gateway is the query processing module for each shard,
// which is used by ScatterConn.
type Gateway interface {
	// the query service that this Gateway wraps around
	queryservice.QueryService

	// QueryServiceByAlias returns a QueryService
	QueryServiceByAlias(alias *topodatapb.TabletAlias) (queryservice.QueryService, error)
}

// A Resolver can resolve keyspace ids and key ranges into ResolvedShard*
// objects. It uses an underlying srvtopo.Server to find the topology,
// and a TargetStats object to find the healthy destinations.
type Resolver struct {
	// topoServ is the srvtopo.Server to use for topo queries.
	topoServ Server

	// gateway
	gateway Gateway

	// localCell is the local cell for the queries.
	localCell string

	// FIXME(alainjobart) also need a list of remote cells.
	// FIXME(alainjobart) and a policy on how to use them.
	// But for now we only use the local cell.
}

// NewResolver creates a new Resolver.
func NewResolver(topoServ Server, gateway Gateway, localCell string) *Resolver {
	return &Resolver{
		topoServ:  topoServ,
		gateway:   gateway,
		localCell: localCell,
	}
}

// ResolvedShard contains everything we need to send a query to a shard.
type ResolvedShard struct {
	// Target describes the target shard.
	Target *querypb.Target

	// Gateway is the way to execute a query on this shard
	Gateway Gateway
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

// GetAllShards returns the list of ResolvedShards associated with all
// the shards in a keyspace.
// FIXME(alainjobart) callers should convert to ResolveDestination(),
// and GetSrvKeyspace.
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
		// Right now we always set the Cell to ""
		// Later we can fallback to another cell if needed.
		// We would then need to read the SrvKeyspace there too.
		target.Cell = ""
		res[i] = &ResolvedShard{
			Target:  target,
			Gateway: r.gateway,
		}
	}
	return res, srvKeyspace, nil
}

// GetAllKeyspaces returns all the known keyspaces in the local cell.
func (r *Resolver) GetAllKeyspaces(ctx context.Context) ([]string, error) {
	keyspaces, err := r.topoServ.GetSrvKeyspaceNames(ctx, r.localCell, true)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "keyspace names fetch error: %v", err)
	}
	// FIXME(alainjobart) this should be unnecessary. The results
	// of ListDir are sorted, and that's the underlying topo code.
	// But the tests depend on this behavior now.
	sort.Strings(keyspaces)
	return keyspaces, nil
}

// ResolveDestinations resolves values and their destinations into their
// respective shards.
//
// If ids is nil, the returned [][]*querypb.Value is also nil.
// Otherwise, len(ids) has to match len(destinations), and then the returned
// [][]*querypb.Value is populated with all the values that go in each shard,
// and len([]*ResolvedShard) matches len([][]*querypb.Value).
//
// Sample input / output:
// - destinations: dst1, dst2, dst3
// - ids:          id1,  id2,  id3
// If dst1 is in shard1, and dst2 and dst3 are in shard2, the output will be:
// - []*ResolvedShard:   shard1, shard2
// - [][]*querypb.Value: [id1],  [id2, id3]
func (r *Resolver) ResolveDestinations(ctx context.Context, keyspace string, tabletType topodatapb.TabletType, ids []*querypb.Value, destinations []key.Destination) ([]*ResolvedShard, [][]*querypb.Value, error) {
	keyspace, _, allShards, err := r.GetKeyspaceShards(ctx, keyspace, tabletType)
	if err != nil {
		return nil, nil, err
	}

	var result []*ResolvedShard
	var values [][]*querypb.Value
	resolved := make(map[string]int)
	for i, destination := range destinations {
		if err := destination.Resolve(allShards, func(shard string) error {
			s, ok := resolved[shard]
			if !ok {
				target := &querypb.Target{
					Keyspace:   keyspace,
					Shard:      shard,
					TabletType: tabletType,
					Cell:       r.localCell,
				}
				// Right now we always set the Cell to ""
				// Later we can fallback to another cell if needed.
				// We would then need to read the SrvKeyspace there too.
				target.Cell = ""
				s = len(result)
				result = append(result, &ResolvedShard{
					Target:  target,
					Gateway: r.gateway,
				})
				if ids != nil {
					values = append(values, nil)
				}
				resolved[shard] = s
			}
			if ids != nil {
				values[s] = append(values[s], ids[i])
			}
			return nil
		}); err != nil {
			return nil, nil, err
		}
	}
	return result, values, nil
}

// ResolveDestination is a shortcut to ResolveDestinations with only
// one Destination, and no ids.
func (r *Resolver) ResolveDestination(ctx context.Context, keyspace string, tabletType topodatapb.TabletType, destination key.Destination) ([]*ResolvedShard, error) {
	rss, _, err := r.ResolveDestinations(ctx, keyspace, tabletType, nil, []key.Destination{destination})
	return rss, err
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

// GetGateway returns the used gateway
func (r *Resolver) GetGateway() Gateway {
	return r.gateway
}
