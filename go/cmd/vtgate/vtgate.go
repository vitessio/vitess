// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"time"

	log "github.com/golang/glog"
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

	ts := topo.GetServer()
	defer topo.CloseServers()

	var schema *planbuilder.Schema
	log.Info(*cell, *schemaFile)
	if *schemaFile != "" {
		var err error
		if schema, err = planbuilder.LoadFile(*schemaFile); err != nil {
			log.Fatal(err)
		}
		log.Infof("v3 is enabled: loaded schema from file: %v", *schemaFile)
	} else {
		schemafier, ok := ts.(topo.Schemafier)
		if !ok {
			log.Infof("Skipping v3 initialization: topo does not suppurt schemafier interface")
			goto startServer
		}
		schemaJSON, err := schemafier.GetVSchema()
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

	vtgate.Init(resilientSrvTopoServer, schema, *cell, *retryDelay, *retryCount, *timeout, *maxInFlight)
	servenv.RunDefault()
}
