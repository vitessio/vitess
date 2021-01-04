/*
Copyright 2019 The Vitess Authors.

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
	"strings"
	"time"

	"context"

	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtgate"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	cell              = flag.String("cell", "test_nj", "cell to use")
	tabletTypesToWait = flag.String("tablet_types_to_wait", "", "wait till connected for specified tablet types during Gateway initialization")
)

var resilientServer *srvtopo.ResilientServer
var legacyHealthCheck discovery.LegacyHealthCheck

func init() {
	rand.Seed(time.Now().UnixNano())
	servenv.RegisterDefaultFlags()
}

func main() {
	defer exit.Recover()

	servenv.ParseFlags("vtgate")
	servenv.Init()

	ts := topo.Open()
	defer ts.Close()

	resilientServer = srvtopo.NewResilientServer(ts, "ResilientSrvTopoServer")

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

	var vtg *vtgate.VTGate
	if *vtgate.GatewayImplementation == vtgate.GatewayImplementationDiscovery {
		// default value
		legacyHealthCheck = discovery.NewLegacyHealthCheck(*vtgate.HealthCheckRetryDelay, *vtgate.HealthCheckTimeout)
		legacyHealthCheck.RegisterStats()

		vtg = vtgate.LegacyInit(context.Background(), legacyHealthCheck, resilientServer, *cell, *vtgate.RetryCount, tabletTypes)
	} else {
		// use new Init otherwise
		vtg = vtgate.Init(context.Background(), resilientServer, *cell, tabletTypes)
	}

	servenv.OnRun(func() {
		// Flags are parsed now. Parse the template using the actual flag value and overwrite the current template.
		discovery.ParseTabletURLTemplateFromFlag()
		addStatusParts(vtg)
	})
	servenv.OnClose(func() {
		_ = vtg.Gateway().Close(context.Background())
		if legacyHealthCheck != nil {
			_ = legacyHealthCheck.Close()
		}
	})
	servenv.RunDefault()
}
