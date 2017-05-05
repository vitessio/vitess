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

package zk2topo

import (
	"path"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// This file contains the tablet management parts of zktopo.Server.
// Eventually this will be moved to the go/vt/topo package.

// tabletPathForAlias converts a tablet alias to the zk path.
func tabletPathForAlias(alias *topodatapb.TabletAlias) string {
	return path.Join(tabletsPath, topoproto.TabletAliasString(alias), topo.TabletFile)
}

// CreateTablet is part of the topo.Server interface
func (zs *Server) CreateTablet(ctx context.Context, tablet *topodatapb.Tablet) error {
	data, err := proto.Marshal(tablet)
	if err != nil {
		return err
	}

	zkPath := tabletPathForAlias(tablet.Alias)
	_, err = zs.Create(ctx, tablet.Alias.Cell, zkPath, data)
	return err
}

// UpdateTablet is part of the topo.Server interface
func (zs *Server) UpdateTablet(ctx context.Context, tablet *topodatapb.Tablet, existingVersion int64) (int64, error) {
	data, err := proto.Marshal(tablet)
	if err != nil {
		return 0, err
	}

	zkPath := tabletPathForAlias(tablet.Alias)
	version, err := zs.Update(ctx, tablet.Alias.Cell, zkPath, data, ZKVersion(existingVersion))
	if err != nil {
		return 0, err
	}
	return int64(version.(ZKVersion)), nil
}

// DeleteTablet is part of the topo.Server interface
func (zs *Server) DeleteTablet(ctx context.Context, alias *topodatapb.TabletAlias) error {
	zkPath := tabletPathForAlias(alias)
	return zs.Delete(ctx, alias.Cell, zkPath, nil)
}

// GetTablet is part of the topo.Server interface
func (zs *Server) GetTablet(ctx context.Context, alias *topodatapb.TabletAlias) (*topodatapb.Tablet, int64, error) {
	zkPath := tabletPathForAlias(alias)
	data, version, err := zs.Get(ctx, alias.Cell, zkPath)
	if err != nil {
		return nil, 0, err
	}

	tablet := &topodatapb.Tablet{}
	if err := proto.Unmarshal(data, tablet); err != nil {
		return nil, 0, err
	}
	return tablet, int64(version.(ZKVersion)), nil
}

// GetTabletsByCell is part of the topo.Server interface
func (zs *Server) GetTabletsByCell(ctx context.Context, cell string) ([]*topodatapb.TabletAlias, error) {
	children, err := zs.ListDir(ctx, cell, tabletsPath)
	if err != nil {
		return nil, err
	}

	result := make([]*topodatapb.TabletAlias, len(children))
	for i, child := range children {
		result[i], err = topoproto.ParseTabletAlias(child)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}
