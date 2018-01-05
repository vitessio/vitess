/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"math/rand"
	"os"
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
	cell                  = flag.String("cell", "test_nj", "cell to use")
	retryCount            = flag.Int("retry-count", 2, "retry count")
	healthCheckRetryDelay = flag.Duration("healthcheck_retry_delay", 2*time.Millisecond, "health check retry delay")
	healthCheckTimeout    = flag.Duration("healthcheck_timeout", time.Minute, "the health check timeout period")
	tabletTypesToWait     = flag.String("tablet_types_to_wait", "", "wait till connected for specified tablet types during Gateway initialization")
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

	if *servenv.Version {
		servenv.AppVersion.Print()
		os.Exit(0)
	}

	ts := topo.Open()
	defer ts.Close()

	resilientSrvTopoServer = vtgate.NewResilientSrvTopoServer(ts, "ResilientSrvTopoServer")

	healthCheck = discovery.NewHealthCheck(*healthCheckRetryDelay, *healthCheckTimeout)
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
