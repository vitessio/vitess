// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

// This plugin imports etcdtopo to register the etcd implementation of TopoServer.

import (
	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/etcdtopo"
	"github.com/youtube/vitess/go/vt/topo"
)

func init() {
	ts := topo.GetServerByName("etcd")
	if ts == nil {
		log.Warning("etcd explorer disabled: no etcdtopo.Server")
		return
	}

	HandleExplorer("etcd", "/etcd/", "etcd.html", etcdtopo.NewExplorer(ts.(*etcdtopo.Server)))
}
