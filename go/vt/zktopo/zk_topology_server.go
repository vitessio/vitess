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
type ZkTopologyServer struct {
	zconn zk.Conn
}

func (zkts *ZkTopologyServer) Close() {
	zkts.zconn.Close()
}

func (zkts *ZkTopologyServer) GetZConn() zk.Conn {
	return zkts.zconn
}

// NewZkTopologyServer can be used to create a custom ZkTopologyServer
// (for tests for instance) but it cannot change the globally
// registered one.
func NewZkTopologyServer(zconn zk.Conn) *ZkTopologyServer {
	return &ZkTopologyServer{zconn: zconn}
}

func init() {
	zconn := zk.NewMetaConn(false)
	expvar.Publish("ZkMetaConn", zconn)
	naming.RegisterTopologyServer("zookeeper", NewZkTopologyServer(zconn))
}
