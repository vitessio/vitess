// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

// This plugin imports etcdtopo to register the etcd implementation of TopoServer.

import (
	"github.com/youtube/vitess/go/vt/etcdtopo"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
)

func init() {
	// Wait until flags are parsed, so we can check which topo server is in use.
	servenv.OnRun(func() {
		if etcdServer, ok := topo.GetServer().(*etcdtopo.Server); ok {
			HandleExplorer("etcd", "/etcd/", "etcd.html", etcdtopo.NewExplorer(etcdServer))
		}
	})
}
