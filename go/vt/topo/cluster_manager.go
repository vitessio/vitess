/*
Copyright 2018 The Vitess Authors
 Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
     http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package topo

import (
	"path"

	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"

	clustermanagerdatapb "vitess.io/vitess/go/vt/proto/clustermanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// Path for all object types.
const (
	ClusterManagerPath = "cluster_manager"
	TabletTypesPath    = "tablets_types"
)

// Server ...
type ClusterManager struct {
	topoServer *Server
}

func NewClusterManager(ts *Server) *ClusterManager {
	return &ClusterManager{topoServer: ts}
}

// SetShardConfig
func (cm *ClusterManager) SetShardConfig(ctx context.Context, cell, keyspace, shard string, tabletType topodatapb.TabletType, nodes int32) error {
	tabletTypeConfig := &clustermanagerdatapb.TabletTypeConfig{Nodes: nodes}
	data, err := proto.Marshal(tabletTypeConfig)
	if err != nil {
		return err
	}
	shardPath := path.Join(ClusterManagerPath, CellsPath, cell, KeyspacesPath, keyspace, ShardsPath, shard, TabletTypesPath, tabletType.String())
	_, err = cm.topoServer.globalCell.Update(ctx, shardPath, data, nil)
	if err != nil {
		return err
	}
	return nil
}

// DeleteShardConfig
func (cm *ClusterManager) DeleteShardConfig(cell, keyspace, shard string, tabletType topodatapb.TabletType) error {
	return nil
}
