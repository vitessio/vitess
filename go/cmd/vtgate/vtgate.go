// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/jscfg"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
)

var (
	cell        = flag.String("cell", "test_nj", "cell to use")
	schemaFile  = flag.String("schema-file", "", "JSON schema file")
	retryDelay  = flag.Duration("retry-delay", 200*time.Millisecond, "retry delay")
	retryCount  = flag.Int("retry-count", 10, "retry count")
	timeout     = flag.Duration("timeout", 5*time.Second, "connection and call timeout")
	maxInFlight = flag.Int("max-in-flight", 0, "maximum number of calls to allow simultaneously")
)

var resilientSrvTopoServer *vtgate.ResilientSrvTopoServer
var topoReader *TopoReader

func init() {
	servenv.RegisterDefaultFlags()
	servenv.RegisterDefaultSecureFlags()
	servenv.RegisterDefaultSocketFileFlags()
	servenv.InitServiceMapForBsonRpcService("toporeader")
	servenv.InitServiceMapForBsonRpcService("vtgateservice")
}

func main() {
	flag.Parse()
	servenv.Init()
	var schema *planbuilder.VTGateSchema
	if *schemaFile != "" {
		if err := jscfg.ReadJson(*schemaFile, &schema); err != nil {
			log.Fatal(err)
		}
	}

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
	servenv.Register("toporeader", topoReader)

	vtgate.Init(resilientSrvTopoServer, schema, *cell, *retryDelay, *retryCount, *timeout, *maxInFlight)
	servenv.RunDefault()
}
