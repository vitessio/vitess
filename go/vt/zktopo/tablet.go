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

package zktopo

import (
	"encoding/json"
	"fmt"
	"sort"

	zookeeper "github.com/samuel/go-zookeeper/zk"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/zk"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

/*
This file contains the tablet management parts of zktopo.Server
*/

// TabletPathForAlias converts a tablet alias to the zk path
func TabletPathForAlias(alias *topodatapb.TabletAlias) string {
	return fmt.Sprintf("/zk/%v/vt/tablets/%v", alias.Cell, topoproto.TabletAliasUIDStr(alias))
}

func tabletDirectoryForCell(cell string) string {
	return fmt.Sprintf("/zk/%v/vt/tablets", cell)
}

// CreateTablet is part of the topo.Server interface
func (zkts *Server) CreateTablet(ctx context.Context, tablet *topodatapb.Tablet) error {
	zkTabletPath := TabletPathForAlias(tablet.Alias)

	data, err := json.MarshalIndent(tablet, "  ", "  ")
	if err != nil {
		return err
	}

	// Create /zk/<cell>/vt/tablets/<uid>
	_, err = zk.CreateRecursive(zkts.zconn, zkTabletPath, data, 0, zookeeper.WorldACL(zookeeper.PermAll))
	if err != nil {
		return convertError(err)
	}
	return nil
}

// UpdateTablet is part of the topo.Server interface
func (zkts *Server) UpdateTablet(ctx context.Context, tablet *topodatapb.Tablet, existingVersion int64) (int64, error) {
	zkTabletPath := TabletPathForAlias(tablet.Alias)
	data, err := json.MarshalIndent(tablet, "  ", "  ")
	if err != nil {
		return 0, err
	}

	stat, err := zkts.zconn.Set(zkTabletPath, data, int32(existingVersion))
	if err != nil {
		return 0, convertError(err)
	}
	return int64(stat.Version), nil
}

// DeleteTablet is part of the topo.Server interface
func (zkts *Server) DeleteTablet(ctx context.Context, alias *topodatapb.TabletAlias) error {
	zkTabletPath := TabletPathForAlias(alias)
	if err := zk.DeleteRecursive(zkts.zconn, zkTabletPath, -1); err != nil {
		return convertError(err)
	}
	return nil
}

// GetTablet is part of the topo.Server interface
func (zkts *Server) GetTablet(ctx context.Context, alias *topodatapb.TabletAlias) (*topodatapb.Tablet, int64, error) {
	zkTabletPath := TabletPathForAlias(alias)
	data, stat, err := zkts.zconn.Get(zkTabletPath)
	if err != nil {
		return nil, 0, convertError(err)
	}

	tablet := &topodatapb.Tablet{}
	if err := json.Unmarshal(data, tablet); err != nil {
		return nil, 0, err
	}
	return tablet, int64(stat.Version), nil
}

// GetTabletsByCell is part of the topo.Server interface
func (zkts *Server) GetTabletsByCell(ctx context.Context, cell string) ([]*topodatapb.TabletAlias, error) {
	zkTabletsPath := tabletDirectoryForCell(cell)
	children, _, err := zkts.zconn.Children(zkTabletsPath)
	if err != nil {
		return nil, convertError(err)
	}

	sort.Strings(children)
	result := make([]*topodatapb.TabletAlias, len(children))
	for i, child := range children {
		uid, err := topoproto.ParseUID(child)
		if err != nil {
			return nil, err
		}
		result[i] = &topodatapb.TabletAlias{
			Cell: cell,
			Uid:  uid,
		}
	}
	return result, nil
}
