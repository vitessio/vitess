// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
	"golang.org/x/net/context"
)

var (
	cell               = flag.String("cell", "test_nj", "cell to use")
	schemaFile         = flag.String("vschema_file", "", "JSON schema file")
	retryDelay         = flag.Duration("retry-delay", 2*time.Millisecond, "retry delay")
	retryCount         = flag.Int("retry-count", 2, "retry count")
	connTimeoutTotal   = flag.Duration("conn-timeout-total", 3*time.Second, "vttablet connection timeout (total)")
	connTimeoutPerConn = flag.Duration("conn-timeout-per-conn", 1500*time.Millisecond, "vttablet connection timeout (per connection)")
	connLife           = flag.Duration("conn-life", 365*24*time.Hour, "average life of vttablet connections")
	maxInFlight        = flag.Int("max-in-flight", 0, "maximum number of calls to allow simultaneously")
)

var resilientSrvTopoServer *vtgate.ResilientSrvTopoServer
var topoReader *TopoReader

func init() {
	servenv.RegisterDefaultFlags()
	servenv.InitServiceMapForBsonRpcService("toporeader")
	servenv.InitServiceMapForBsonRpcService("vtgateservice")
}

func main() {
	defer exit.Recover()

	flag.Parse()
	servenv.Init()

	ts := topo.GetServer()
	defer topo.CloseServers()

	var schema *planbuilder.Schema
	if *schemaFile != "" {
		var err error
		if schema, err = planbuilder.LoadFile(*schemaFile); err != nil {
			log.Error(err)
			exit.Return(1)
		}
		log.Infof("v3 is enabled: loaded schema from file: %v", *schemaFile)
	} else {
		ctx := context.Background()
		schemaJSON, err := ts.GetVSchema(ctx)
		if err != nil {
			log.Warningf("Skipping v3 initialization: GetVSchema failed: %v", err)
			goto startServer
		}
		schema, err = planbuilder.NewSchema([]byte(schemaJSON))
		if err != nil {
			log.Warningf("Skipping v3 initialization: GetVSchema failed: %v", err)
			goto startServer
		}
		log.Infof("v3 is enabled: loaded schema from topo")
	}

startServer:
	resilientSrvTopoServer = vtgate.NewResilientSrvTopoServer(ts, "ResilientSrvTopoServer")

	// For the initial phase vtgate is exposing
	// topoReader api. This will be subsumed by
	// vtgate once vtgate's client functions become active.
	topoReader = NewTopoReader(resilientSrvTopoServer)
	servenv.Register("toporeader", topoReader)

	vtgate.Init(resilientSrvTopoServer, schema, *cell, *retryDelay, *retryCount, *connTimeoutTotal, *connTimeoutPerConn, *connLife, *maxInFlight)
	servenv.RunDefault()
}
