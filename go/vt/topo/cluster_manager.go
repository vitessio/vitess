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
	"fmt"
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

// ClusterManager ...
type ClusterManager struct {
	topoServer *Server
}

// NewClusterManager ...
func NewClusterManager(ts *Server) *ClusterManager {
	return &ClusterManager{topoServer: ts}
}

// SetShardConfig ...
func (cm *ClusterManager) SetShardConfig(ctx context.Context, cell, keyspace, shard string, tabletType topodatapb.TabletType, nodes int32) error {
	if cell == "" || keyspace == "" || shard == "" {
		return fmt.Errorf("SetShardConfig: cell, keyspace, shard can't be empty")
	}
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

// GetClusterConfig ...
func (cm *ClusterManager) GetClusterConfig(ctx context.Context, cell, keyspace, shard, tabletType string) (shardClusterConfig *clustermanagerdatapb.ClusterConfig, err error) {
	resp := &clustermanagerdatapb.ClusterConfig{
		Cells: make(map[string]*clustermanagerdatapb.CellConfig),
	}

	// Validate provided tabletType
	if _, ok := topodatapb.TabletType_value[tabletType]; tabletType != "" && !ok {
		return nil, fmt.Errorf("GetClusterConfig: invalid tabletType: %v", tabletType)
	}

	// Query can be generic, but once you provide a subpath, all subsequent paths are required
	// (e.g you can't provide shard if keyspace is "")
	dirEntryNames := []string{"cell", "keyspace", "shard", "tabletType"}
	dirEntries := []string{cell, keyspace, shard, tabletType}
	for i := 1; i < 4; i++ {
		if dirEntries[i-1] == "" && dirEntries[i] != "" {
			return nil, fmt.Errorf("GetClusterConfig: expected parameter to not be empty: %v", dirEntryNames[i-1])
		}
	}

	var cells []string
	if cell != "" {
		cells = []string{cell}
	} else {
		cells, err = cm.getCells(ctx)
		if err != nil {
			return nil, err
		}
	}
	for _, cell := range cells {
		var keyspaces []string
		resp.Cells[cell] = &clustermanagerdatapb.CellConfig{
			Keyspaces: make(map[string]*clustermanagerdatapb.KeyspaceConfig),
		}
		if keyspace != "" {
			keyspaces = []string{keyspace}
		} else {
			keyspaces, err = cm.getCellKeyspaces(ctx, cell)
			if err != nil {
				return nil, err
			}
		}
		for _, keyspace := range keyspaces {
			resp.Cells[cell].Keyspaces[keyspace] = &clustermanagerdatapb.KeyspaceConfig{
				Shards: make(map[string]*clustermanagerdatapb.ShardConfig),
			}
			var shards []string
			if shard != "" {
				shards = []string{shard}
			} else {
				shards, err = cm.getShards(ctx, cell, keyspace)
			}
			if err != nil {
				return nil, err
			}
			for _, shard := range shards {
				resp.Cells[cell].Keyspaces[keyspace].Shards[shard] = &clustermanagerdatapb.ShardConfig{
					TabletTypes: make(map[string]*clustermanagerdatapb.TabletTypeConfig),
				}
				var tabletTypes []string
				if tabletType != "" {
					tabletTypes = []string{shard}
				} else {
					tabletTypes, err = cm.getShardTabletTypes(ctx, cell, keyspace, shard)
				}
				if err != nil {
					return nil, err
				}
				for _, tabletType := range tabletTypes {
					node, err := cm.getTabletTypeConfig(ctx, cell, keyspace, shard, tabletType)
					if err != nil {
						return nil, err
					}
					resp.Cells[cell].Keyspaces[keyspace].Shards[shard].TabletTypes[tabletType] = node
				}
			}
		}
	}
	return resp, nil
}

// DeleteShardConfig ...
func (cm *ClusterManager) DeleteShardConfig(ctx context.Context, cell, keyspace, shard string, tabletType topodatapb.TabletType) error {
	shardPath := path.Join(ClusterManagerPath, CellsPath, cell, KeyspacesPath, keyspace, ShardsPath, shard, TabletTypesPath, tabletType.String())
	return cm.topoServer.globalCell.Delete(ctx, shardPath, nil)
}

func (cm *ClusterManager) getCells(ctx context.Context) ([]string, error) {
	path := path.Join(ClusterManagerPath, CellsPath)
	children, err := cm.topoServer.globalCell.ListDir(ctx, path, false /*full*/)
	switch {
	case err == nil:
		return DirEntriesToStringArray(children), nil
	case IsErrType(err, NoNode):
		return []string{}, nil
	default:
		return nil, err
	}
}

func (cm *ClusterManager) getCellKeyspaces(ctx context.Context, cell string) ([]string, error) {
	path := path.Join(ClusterManagerPath, CellsPath, cell, KeyspacesPath)
	children, err := cm.topoServer.globalCell.ListDir(ctx, path, false /*full*/)
	switch {
	case err == nil:
		return DirEntriesToStringArray(children), nil
	case IsErrType(err, NoNode):
		return []string{}, nil
	default:
		return nil, err
	}
}

func (cm *ClusterManager) getShards(ctx context.Context, cell, keyspace string) ([]string, error) {
	path := path.Join(ClusterManagerPath, CellsPath, cell, KeyspacesPath, keyspace, ShardsPath)
	children, err := cm.topoServer.globalCell.ListDir(ctx, path, false /*full*/)
	switch {
	case err == nil:
		return DirEntriesToStringArray(children), nil
	case IsErrType(err, NoNode):
		return []string{}, nil
	default:
		return nil, err
	}
}

func (cm *ClusterManager) getShardTabletTypes(ctx context.Context, cell, keyspace, shard string) ([]string, error) {
	path := path.Join(ClusterManagerPath, CellsPath, cell, KeyspacesPath, keyspace, ShardsPath, shard, TabletTypesPath)
	children, err := cm.topoServer.globalCell.ListDir(ctx, path, false /*full*/)
	switch {
	case err == nil:
		return DirEntriesToStringArray(children), nil
	case IsErrType(err, NoNode):
		return []string{}, nil
	default:
		return nil, err
	}
}

func (cm *ClusterManager) getTabletTypeConfig(ctx context.Context, cell, keyspace, shard, tabletType string) (*clustermanagerdatapb.TabletTypeConfig, error) {
	path := path.Join(ClusterManagerPath, CellsPath, cell, KeyspacesPath, keyspace, ShardsPath, shard, TabletTypesPath, tabletType)
	data, _, err := cm.topoServer.globalCell.Get(ctx, path)
	if err != nil {
		return nil, err
	}
	value := &clustermanagerdatapb.TabletTypeConfig{}
	if err = proto.Unmarshal(data, value); err != nil {
		return nil, fmt.Errorf("GetClusterConfig(%v,%v,%v,%v): bad shard data: %v", cell, keyspace, shard, tabletType, err)
	}
	return value, nil
}
