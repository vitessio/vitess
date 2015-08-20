// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/zk"
	"golang.org/x/net/context"
	"launchpad.net/gozk/zookeeper"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

/*
This file contains the tablet management parts of zktopo.Server
*/

// TabletPathForAlias converts a tablet alias to the zk path
func TabletPathForAlias(alias *pb.TabletAlias) string {
	return fmt.Sprintf("/zk/%v/vt/tablets/%v", alias.Cell, topoproto.TabletAliasUIDStr(alias))
}

func tabletDirectoryForCell(cell string) string {
	return fmt.Sprintf("/zk/%v/vt/tablets", cell)
}

// CreateTablet is part of the topo.Server interface
func (zkts *Server) CreateTablet(ctx context.Context, tablet *pb.Tablet) error {
	zkTabletPath := TabletPathForAlias(tablet.Alias)

	data, err := json.MarshalIndent(tablet, "  ", "  ")
	if err != nil {
		return err
	}

	// Create /zk/<cell>/vt/tablets/<uid>
	_, err = zk.CreateRecursive(zkts.zconn, zkTabletPath, string(data), 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
			err = topo.ErrNodeExists
		}
		return err
	}
	return nil
}

// UpdateTablet is part of the topo.Server interface
func (zkts *Server) UpdateTablet(ctx context.Context, tablet *pb.Tablet, existingVersion int64) (int64, error) {
	zkTabletPath := TabletPathForAlias(tablet.Alias)
	data, err := json.MarshalIndent(tablet, "  ", "  ")
	if err != nil {
		return 0, err
	}

	stat, err := zkts.zconn.Set(zkTabletPath, string(data), int(existingVersion))
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZBADVERSION) {
			err = topo.ErrBadVersion
		} else if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}

		return 0, err
	}
	return int64(stat.Version()), nil
}

// UpdateTabletFields is part of the topo.Server interface
func (zkts *Server) UpdateTabletFields(ctx context.Context, tabletAlias *pb.TabletAlias, update func(*pb.Tablet) error) (*pb.Tablet, error) {
	// Store the last tablet value so we can log it if the change succeeds.
	var lastTablet *pb.Tablet

	zkTabletPath := TabletPathForAlias(tabletAlias)
	f := func(oldValue string, oldStat zk.Stat) (string, error) {
		if oldValue == "" {
			return "", fmt.Errorf("no data for tablet addr update: %v", tabletAlias)
		}

		tablet := &pb.Tablet{}
		if err := json.Unmarshal([]byte(oldValue), tablet); err != nil {
			return "", err
		}
		if err := update(tablet); err != nil {
			return "", err
		}
		lastTablet = tablet
		data, err := json.MarshalIndent(tablet, "", "  ")
		if err != nil {
			return "", err
		}
		return string(data), nil
	}
	err := zkts.zconn.RetryChange(zkTabletPath, 0, zookeeper.WorldACL(zookeeper.PERM_ALL), f)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, err
	}
	return lastTablet, nil
}

// DeleteTablet is part of the topo.Server interface
func (zkts *Server) DeleteTablet(ctx context.Context, alias *pb.TabletAlias) error {
	zkTabletPath := TabletPathForAlias(alias)
	if err := zk.DeleteRecursive(zkts.zconn, zkTabletPath, -1); err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return err
	}
	return nil
}

// GetTablet is part of the topo.Server interface
func (zkts *Server) GetTablet(ctx context.Context, alias *pb.TabletAlias) (*pb.Tablet, int64, error) {
	zkTabletPath := TabletPathForAlias(alias)
	data, stat, err := zkts.zconn.Get(zkTabletPath)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, 0, err
	}

	tablet := &pb.Tablet{}
	if err := json.Unmarshal([]byte(data), tablet); err != nil {
		return nil, 0, err
	}
	return tablet, int64(stat.Version()), nil
}

// GetTabletsByCell is part of the topo.Server interface
func (zkts *Server) GetTabletsByCell(ctx context.Context, cell string) ([]*pb.TabletAlias, error) {
	zkTabletsPath := tabletDirectoryForCell(cell)
	children, _, err := zkts.zconn.Children(zkTabletsPath)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, err
	}

	sort.Strings(children)
	result := make([]*pb.TabletAlias, len(children))
	for i, child := range children {
		uid, err := topoproto.ParseUID(child)
		if err != nil {
			return nil, err
		}
		result[i] = &pb.TabletAlias{
			Cell: cell,
			Uid:  uid,
		}
	}
	return result, nil
}
