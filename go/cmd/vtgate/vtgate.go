// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"time"

	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate"
)

var (
	cell       = flag.String("cell", "test_nj", "cell to use")
	retryDelay = flag.Duration("retry-delay", 200*time.Millisecond, "retry delay")
	retryCount = flag.Int("retry-count", 10, "retry count")
	timeout    = flag.Duration("timeout", 5*time.Second, "connection and call timeout")
)

var resilientSrvTopoServer *vtgate.ResilientSrvTopoServer
var topoReader *TopoReader

func init() {
	servenv.RegisterDefaultFlags()
	servenv.RegisterDefaultSecureFlags()
	servenv.RegisterDefaultSocketFileFlags()
}

func main() {
	flag.Parse()
	servenv.Init()

	ts := topo.GetServer()
	defer topo.CloseServers()

	resilientSrvTopoServer = vtgate.NewResilientSrvTopoServer(ts, "ResilientSrvTopoServer")

	labels := []string{"Cell", "Keyspace", "ShardName", "DbType"}
	_ = stats.NewMultiCountersFunc("EndpointCount", labels, resilientSrvTopoServer.EndpointCount)
	_ = stats.NewMultiCountersFunc("DegradedEndpointCount", labels, resilientSrvTopoServer.DegradedEndpointCount)

	// For the initial phase vtgate is exposing
	// topoReader api. This will be subsumed by
	// vtgate once vtgate's client functions become active.
	topoReader = NewTopoReader(resilientSrvTopoServer)
	topo.RegisterTopoReader(topoReader)

	vtgate.Init(resilientSrvTopoServer, *cell, *retryDelay, *retryCount, *timeout)
	servenv.RunDefault()
}
