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
	"context"
	"flag"
	"math/rand"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtgate"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"

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

// CheckCellFlags will check validation of cell and cells_to_watch flag
// it will help to avoid strange behaviors when vtgate runs but actually does not work
func CheckCellFlags(ctx context.Context, serv srvtopo.Server, cell string, cellsToWatch string) error {
	// topo check
	var topoServer *topo.Server
	if serv != nil {
		var err error
		topoServer, err = serv.GetTopoServer()
		if err != nil {
			log.Exitf("Unable to create gateway: %v", err)
		}
	} else {
		log.Exitf("topo server cannot be nil")
	}
	cellsInTopo, err := topoServer.GetKnownCells(ctx)
	if err != nil {
		return err
	}
	if len(cellsInTopo) == 0 {
		return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "topo server should have at least one cell")
	}

	// cell valid check
	if cell == "" {
		return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "cell flag must be set")
	}
	hasCell := false
	for _, v := range cellsInTopo {
		if v == cell {
			hasCell = true
			break
		}
	}
	if !hasCell {
		return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "cell:[%v] does not exist in topo", cell)
	}

	// cells_to_watch valid check
	cells := make([]string, 0, 1)
	for _, c := range strings.Split(cellsToWatch, ",") {
		if c == "" {
			continue
		}
		// cell should contained in cellsInTopo
		if exists := topo.InCellList(c, cellsInTopo); !exists {
			return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "cell: [%v] is not valid. Available cells: [%v]", c, strings.Join(cellsInTopo, ","))
		}
		cells = append(cells, c)
	}
	if len(cells) == 0 {
		return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "cells_to_watch flag cannot be empty")
	}

	return nil
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
			if tabletserver.IsServingType(tt) {
				tabletTypes = append(tabletTypes, tt)
			}
		}
	} else {
		log.Exitf("tablet_types_to_wait flag must be set")
	}

	if len(tabletTypes) == 0 {
		log.Exitf("tablet_types_to_wait should contain at least one serving tablet type")
	}

	err := CheckCellFlags(context.Background(), resilientServer, *cell, *vtgate.CellsToWatch)
	if err != nil {
		log.Exitf("cells_to_watch validation failed: %v", err)
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
