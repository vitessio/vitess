// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"expvar"

	"code.google.com/p/vitess/go/vt/naming"
	"code.google.com/p/vitess/go/zk"
)

// ZkTopologyServer is the zookeeper TopologyServer implementation.
// For now Zconn is public, until we finish the transition to TopologyServer
type ZkTopologyServer struct {
	Zconn zk.Conn
}

func (zkts *ZkTopologyServer) Close() {
	zkts.Zconn.Close()
}

func init() {
	zconn := zk.NewMetaConn(false)
	expvar.Publish("ZkMetaConn", zconn)
	naming.RegisterTopologyServer("zookeeper", &ZkTopologyServer{Zconn: zconn})
}
