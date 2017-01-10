// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

// Imports and register the 'zookeeper' topo.Server and its Explorer.

import (
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/vtctld"
	"github.com/youtube/vitess/go/vt/zktopo"
)

func init() {
	// Wait until flags are parsed, so we can check which topo server is in use.
	servenv.OnRun(func() {
		if zkServer, ok := ts.Impl.(*zktopo.Server); ok {
			vtctld.HandleExplorer("zk", zktopo.NewZkExplorer(zkServer.GetZConn()))
		}
	})
}
