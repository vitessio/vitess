// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"encoding/json"
	"fmt"
	"path"
	"sort"

	"github.com/youtube/vitess/go/jscfg"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

/*
This file contains the tablet management parts of zktopo.Server
*/

func TabletPathForAlias(alias topo.TabletAlias) string {
	return fmt.Sprintf("/zk/%v/vt/tablets/%v", alias.Cell, alias.TabletUidStr())
}

func TabletActionPathForAlias(alias topo.TabletAlias) string {
	return fmt.Sprintf("/zk/%v/vt/tablets/%v/action", alias.Cell, alias.TabletUidStr())
}

func tabletDirectoryForCell(cell string) string {
	return fmt.Sprintf("/zk/%v/vt/tablets", cell)
}

func tabletFromJson(data string) (*topo.Tablet, error) {
	t := &topo.Tablet{}
	err := json.Unmarshal([]byte(data), t)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func tabletInfoFromJson(data string, version int) (*topo.TabletInfo, error) {
	tablet, err := tabletFromJson(data)
	if err != nil {
		return nil, err
	}
	return topo.NewTabletInfo(tablet, version), nil
}

func (zkts *Server) CreateTablet(tablet *topo.Tablet) error {
	zkTabletPath := TabletPathForAlias(tablet.Alias())

	// Create /zk/<cell>/vt/tablets/<uid>
	_, err := zk.CreateRecursive(zkts.zconn, zkTabletPath, tablet.Json(), 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
			err = topo.ErrNodeExists
		}
		return err
	}

	// Create /zk/<cell>/vt/tablets/<uid>/action
	tap := path.Join(zkTabletPath, "action")
	_, err = zkts.zconn.Create(tap, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		return err
	}

	// Create /zk/<cell>/vt/tablets/<uid>/actionlog
	talp := path.Join(zkTabletPath, "actionlog")
	_, err = zkts.zconn.Create(talp, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		return err
	}

	return nil
}

func (zkts *Server) UpdateTablet(tablet *topo.TabletInfo, existingVersion int) (int, error) {
	zkTabletPath := TabletPathForAlias(tablet.Alias())
	stat, err := zkts.zconn.Set(zkTabletPath, tablet.Json(), existingVersion)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZBADVERSION) {
			err = topo.ErrBadVersion
		} else if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}

		return 0, err
	}
	return stat.Version(), nil
}

func (zkts *Server) UpdateTabletFields(tabletAlias topo.TabletAlias, update func(*topo.Tablet) error) error {
	zkTabletPath := TabletPathForAlias(tabletAlias)
	f := func(oldValue string, oldStat zk.Stat) (string, error) {
		if oldValue == "" {
			return "", fmt.Errorf("no data for tablet addr update: %v", tabletAlias)
		}

		tablet, err := tabletFromJson(oldValue)
		if err != nil {
			return "", err
		}
		if err := update(tablet); err != nil {
			return "", err
		}
		return jscfg.ToJson(tablet), nil
	}
	err := zkts.zconn.RetryChange(zkTabletPath, 0, zookeeper.WorldACL(zookeeper.PERM_ALL), f)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return err
	}
	return nil
}

func (zkts *Server) DeleteTablet(alias topo.TabletAlias) error {
	zkTabletPath := TabletPathForAlias(alias)
	err := zk.DeleteRecursive(zkts.zconn, zkTabletPath, -1)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
	}
	return err
}

func (zkts *Server) ValidateTablet(alias topo.TabletAlias) error {
	zkTabletPath := TabletPathForAlias(alias)
	zkPaths := []string{
		path.Join(zkTabletPath, "action"),
		path.Join(zkTabletPath, "actionlog"),
	}

	for _, zkPath := range zkPaths {
		if _, _, err := zkts.zconn.Get(zkPath); err != nil {
			return err
		}
	}
	return nil
}

func (zkts *Server) GetTablet(alias topo.TabletAlias) (*topo.TabletInfo, error) {
	zkTabletPath := TabletPathForAlias(alias)
	data, stat, err := zkts.zconn.Get(zkTabletPath)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, err
	}
	return tabletInfoFromJson(data, stat.Version())
}

func (zkts *Server) GetTabletsByCell(cell string) ([]topo.TabletAlias, error) {
	zkTabletsPath := tabletDirectoryForCell(cell)
	children, _, err := zkts.zconn.Children(zkTabletsPath)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			err = topo.ErrNoNode
		}
		return nil, err
	}

	sort.Strings(children)
	result := make([]topo.TabletAlias, len(children))
	for i, child := range children {
		result[i].Cell = cell
		result[i].Uid, err = topo.ParseUid(child)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}
