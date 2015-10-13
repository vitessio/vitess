// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"math/rand"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
)

var (
	cell                  = flag.String("cell", "test_nj", "cell to use")
	schemaFile            = flag.String("vschema_file", "", "JSON schema file")
	retryDelay            = flag.Duration("retry-delay", 2*time.Millisecond, "retry delay")
	retryCount            = flag.Int("retry-count", 2, "retry count")
	connTimeoutTotal      = flag.Duration("conn-timeout-total", 3*time.Second, "vttablet connection timeout (total)")
	connTimeoutPerConn    = flag.Duration("conn-timeout-per-conn", 1500*time.Millisecond, "vttablet connection timeout (per connection)")
	connLife              = flag.Duration("conn-life", 365*24*time.Hour, "average life of vttablet connections")
	maxInFlight           = flag.Int("max-in-flight", 0, "maximum number of calls to allow simultaneously")
	healthCheckRetryDelay = flag.Duration("healthcheck_retry_delay", 5*time.Second, "health check retry delay")
	testGateway           = flag.String("test_gateway", "", "additional gateway to test health check module")
)

var resilientSrvTopoServer *vtgate.ResilientSrvTopoServer
var healthCheck discovery.HealthCheck

var initFakeZK func()

func init() {
	rand.Seed(time.Now().UnixNano())
	servenv.RegisterDefaultFlags()
}

func main() {
	defer exit.Recover()

	flag.Parse()
	servenv.Init()

	if initFakeZK != nil {
		initFakeZK()
	}
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

	healthCheck = discovery.NewHealthCheck(*connTimeoutTotal, *healthCheckRetryDelay)

	vtgate.Init(healthCheck, ts, resilientSrvTopoServer, schema, *cell, *retryDelay, *retryCount, *connTimeoutTotal, *connTimeoutPerConn, *connLife, *maxInFlight, *testGateway)
	servenv.RunDefault()
}
