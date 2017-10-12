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

package vtexplain

import (
	"fmt"
	"sync"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
)

// ExplainTopo satisfies the SrvTopoServer interface.
// Modeled after the vtgate test sandboxTopo
type ExplainTopo struct {
	// Map of keyspace name to vschema
	Keyspaces map[string]*vschemapb.Keyspace

	// Map of ks/shard to test tablet connection
	TabletConns map[string]*explainTablet

	// Synchronization lock
	Lock sync.Mutex

	// Number of shards for sharded keyspaces
	NumShards int
}

func (et *ExplainTopo) getSrvVSchema() *vschemapb.SrvVSchema {
	et.Lock.Lock()
	defer et.Lock.Unlock()

	return &vschemapb.SrvVSchema{
		Keyspaces: et.Keyspaces,
	}
}

// GetSrvKeyspaceNames is part of SrvTopoServer.
func (et *ExplainTopo) GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error) {
	et.Lock.Lock()
	defer et.Lock.Unlock()

	keyspaces := make([]string, 0, 1)
	for k := range et.Keyspaces {
		keyspaces = append(keyspaces, k)
	}
	return keyspaces, nil
}

// GetSrvKeyspace is part of SrvTopoServer.
func (et *ExplainTopo) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	et.Lock.Lock()
	defer et.Lock.Unlock()

	vschema := et.Keyspaces[keyspace]
	if vschema == nil {
		return nil, fmt.Errorf("no vschema for keyspace %s", keyspace)
	}

	var srvKeyspace *topodatapb.SrvKeyspace
	if vschema.Sharded {
		shards := make([]*topodatapb.ShardReference, 0, et.NumShards)
		for i := 0; i < et.NumShards; i++ {
			kr, err := key.EvenShardsKeyRange(i, et.NumShards)
			if err != nil {
				return nil, err
			}

			shard := &topodatapb.ShardReference{
				Name:     key.KeyRangeString(kr),
				KeyRange: kr,
			}
			shards = append(shards, shard)
		}

		srvKeyspace = &topodatapb.SrvKeyspace{
			ShardingColumnName: "", // exact value is ignored
			ShardingColumnType: 0,
			Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
				{
					ServedType:      topodatapb.TabletType_MASTER,
					ShardReferences: shards,
				},
				{
					ServedType:      topodatapb.TabletType_REPLICA,
					ShardReferences: shards,
				},
				{
					ServedType:      topodatapb.TabletType_RDONLY,
					ShardReferences: shards,
				},
			},
		}

	} else {
		// unsharded
		kr, err := key.EvenShardsKeyRange(0, 1)
		if err != nil {
			return nil, err
		}

		shard := &topodatapb.ShardReference{
			Name: key.KeyRangeString(kr),
		}

		srvKeyspace = &topodatapb.SrvKeyspace{
			Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
				{
					ServedType:      topodatapb.TabletType_MASTER,
					ShardReferences: []*topodatapb.ShardReference{shard},
				},
				{
					ServedType:      topodatapb.TabletType_REPLICA,
					ShardReferences: []*topodatapb.ShardReference{shard},
				},
				{
					ServedType:      topodatapb.TabletType_RDONLY,
					ShardReferences: []*topodatapb.ShardReference{shard},
				},
			},
		}
	}

	return srvKeyspace, nil
}

// WatchSrvVSchema is part of SrvTopoServer.
func (et *ExplainTopo) WatchSrvVSchema(ctx context.Context, cell string) (*topo.WatchSrvVSchemaData, <-chan *topo.WatchSrvVSchemaData, topo.CancelFunc) {
	return &topo.WatchSrvVSchemaData{
		Value: et.getSrvVSchema(),
	}, make(chan *topo.WatchSrvVSchemaData), func() {}
}
