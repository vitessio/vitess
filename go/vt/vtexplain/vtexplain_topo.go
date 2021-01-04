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

package vtexplain

import (
	"fmt"
	"sync"

	"context"

	"vitess.io/vitess/go/vt/topo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// ExplainTopo satisfies the srvtopo.Server interface.
// Modeled after the vtgate test sandboxTopo
type ExplainTopo struct {
	// Map of keyspace name to vschema
	Keyspaces map[string]*vschemapb.Keyspace

	// Map of ks/shard to test tablet connection
	TabletConns map[string]*explainTablet

	KeyspaceShards map[string]map[string]*topodatapb.ShardReference

	// Synchronization lock
	Lock sync.Mutex

	// Number of shards for sharded keyspaces
	NumShards int

	TopoServer *topo.Server
}

func (et *ExplainTopo) getSrvVSchema() *vschemapb.SrvVSchema {
	et.Lock.Lock()
	defer et.Lock.Unlock()

	return &vschemapb.SrvVSchema{
		Keyspaces: et.Keyspaces,
	}
}

// GetTopoServer is part of the srvtopo.Server interface
func (et *ExplainTopo) GetTopoServer() (*topo.Server, error) {
	return et.TopoServer, nil
}

// GetSrvKeyspaceNames is part of the srvtopo.Server interface.
func (et *ExplainTopo) GetSrvKeyspaceNames(ctx context.Context, cell string, staleOK bool) ([]string, error) {
	et.Lock.Lock()
	defer et.Lock.Unlock()

	keyspaces := make([]string, 0, 1)
	for k := range et.Keyspaces {
		keyspaces = append(keyspaces, k)
	}
	return keyspaces, nil
}

// GetSrvKeyspace is part of the srvtopo.Server interface.
func (et *ExplainTopo) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	et.Lock.Lock()
	defer et.Lock.Unlock()

	vschema := et.Keyspaces[keyspace]
	if vschema == nil {
		return nil, fmt.Errorf("no vschema for keyspace %s", keyspace)
	}

	shards := make([]*topodatapb.ShardReference, 0, len(et.KeyspaceShards[keyspace]))
	for _, shard := range et.KeyspaceShards[keyspace] {
		shards = append(shards, shard)
	}

	srvKeyspace := &topodatapb.SrvKeyspace{
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

	if vschema.Sharded {
		srvKeyspace.ShardingColumnName = "" // exact value is ignored
		srvKeyspace.ShardingColumnType = 0
	}

	return srvKeyspace, nil
}

// WatchSrvVSchema is part of the srvtopo.Server interface.
func (et *ExplainTopo) WatchSrvVSchema(ctx context.Context, cell string, callback func(*vschemapb.SrvVSchema, error)) {
	callback(et.getSrvVSchema(), nil)
}
