// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"math/rand"
	"strings"
	"time"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vtgate"
	"github.com/youtube/vitess/go/vt/vtgate/l2vtgate"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	cell                   = flag.String("cell", "test_nj", "cell to use")
	retryCount             = flag.Int("retry-count", 2, "retry count")
	healthCheckConnTimeout = flag.Duration("healthcheck_conn_timeout", 3*time.Second, "healthcheck connection timeout")
	healthCheckRetryDelay  = flag.Duration("healthcheck_retry_delay", 2*time.Millisecond, "health check retry delay")
	healthCheckTimeout     = flag.Duration("healthcheck_timeout", time.Minute, "the health check timeout period")
	tabletTypesToWait      = flag.String("tablet_types_to_wait", "", "wait till connected for specified tablet types during Gateway initialization")
)

var resilientSrvTopoServer *vtgate.ResilientSrvTopoServer
var healthCheck discovery.HealthCheck

func init() {
	rand.Seed(time.Now().UnixNano())
	servenv.RegisterDefaultFlags()
}

func main() {
	defer exit.Recover()

	flag.Parse()
	servenv.Init()

	ts := topo.Open()
	defer ts.Close()

	resilientSrvTopoServer = vtgate.NewResilientSrvTopoServer(ts, "ResilientSrvTopoServer")

	healthCheck = discovery.NewHealthCheck(*healthCheckConnTimeout, *healthCheckRetryDelay, *healthCheckTimeout)
	healthCheck.RegisterStats()

	tabletTypes := make([]topodatapb.TabletType, 0, 1)
	if len(*tabletTypesToWait) != 0 {
		for _, ttStr := range strings.Split(*tabletTypesToWait, ",") {
			tt, err := topoproto.ParseTabletType(ttStr)
			if err != nil {
				log.Errorf("unknown tablet type: %v", ttStr)
				continue
			}
			tabletTypes = append(tabletTypes, tt)
		}
	}
	l2vtg := l2vtgate.Init(healthCheck, ts, resilientSrvTopoServer, "VttabletCall", *cell, *retryCount, tabletTypes)

	servenv.OnRun(func() {
		addStatusParts(l2vtg)
	})
	servenv.RunDefault()
}
