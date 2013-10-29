// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

// Imports and register the Zookeeper TopologyServer with Zkocc Connection

import (
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/zktopo"
	"github.com/youtube/vitess/go/zk"
)

func init() {
	zkoccconn := zk.NewMetaConn(true)
	stats.Publish("ZkOccConn", zkoccconn)
	topo.RegisterServer("zkocc", zktopo.NewServer(zkoccconn))
}
