// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"path"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/zk"
)

/*
This file contains the code to support the local agent process for zktopo.Server
*/

func (zkts *Server) CreateTabletPidNode(tabletAlias topo.TabletAlias, contents string, done chan struct{}) error {
	zkTabletPath := TabletPathForAlias(tabletAlias)
	path := path.Join(zkTabletPath, "pid")
	return zk.CreatePidNode(zkts.zconn, path, contents, done)
}

func (zkts *Server) ValidateTabletPidNode(tabletAlias topo.TabletAlias) error {
	zkTabletPath := TabletPathForAlias(tabletAlias)
	path := path.Join(zkTabletPath, "pid")
	_, _, err := zkts.zconn.Get(path)
	return err
}

func (zkts *Server) GetSubprocessFlags() []string {
	return zk.GetZkSubprocessFlags()
}
