/*
Copyright 2023 The Vitess Authors.

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

package cli

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/srvtopo/fakesrvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	cell              string
	tabletTypesToWait []topodatapb.TabletType
	plannerName       string
	resilientServer   *srvtopo.ResilientServer

	// testTopoOpener and testRunDefault are set by tests to inject a topo
	// and skip servenv.RunDefault. Must be nil in production. Tests that set
	// these must not call t.Parallel().
	testTopoOpener func() *topo.Server
	testRunDefault func()

	Main = &cobra.Command{
		Use:   "vtgate",
		Short: "VTGate is a stateless proxy responsible for accepting requests from applications and routing them to the appropriate tablet server(s) for query execution. It speaks both the MySQL Protocol and a gRPC protocol.",
		Long: `VTGate is a stateless proxy responsible for accepting requests from applications and routing them to the appropriate tablet server(s) for query execution. It speaks both the MySQL Protocol and a gRPC protocol.

### Key Options
` +
			"\n* `--srv-topo-cache-ttl`: There may be instances where you will need to increase the cached TTL from the default of 1 second to a higher number:\n" +
			`	* You may want to increase this option if you see that your topo leader goes down and keeps your queries waiting for a few seconds.`,
		Example: `vtgate \
	--topo-implementation etcd2 \
	--topo-global-server-address localhost:2379 \
	--topo-global-root /vitess/global \
	--log_dir $VTDATAROOT/tmp \
	--port 15001 \
	--grpc-port 15991 \
	--mysql-server-port 15306 \
	--cell test \
	--cells_to_watch test \
	--tablet-types-to-wait PRIMARY,REPLICA \
	--service-map 'grpc-vtgateservice' \
	--pid-file $VTDATAROOT/tmp/vtgate.pid \
	--mysql-auth-server-impl none`,
		Args:    cobra.NoArgs,
		Version: servenv.AppVersion.String(),
		PreRunE: servenv.CobraPreRunE,
		RunE:    run,
	}

	srvTopoCounts *stats.CountersWithSingleLabel
)

// CheckCellFlags will check validation of cell and cells_to_watch flag
// it will help to avoid strange behaviors when vtgate runs but actually does not work
func CheckCellFlags(ctx context.Context, serv srvtopo.Server, cell string, cellsToWatch string) error {
	// topo check
	var topoServer *topo.Server
	if serv != nil {
		var err error
		topoServer, err = serv.GetTopoServer()
		if err != nil {
			return fmt.Errorf("Unable to create gateway: %w", err)
		}
	} else {
		return errors.New("topo server cannot be nil")
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
	hasCell := slices.Contains(cellsInTopo, cell)
	if !hasCell {
		return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "cell:[%v] does not exist in topo", cell)
	}

	// cells_to_watch valid check
	cells := make([]string, 0, 1)
	for c := range strings.SplitSeq(cellsToWatch, ",") {
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

// runWithTopo runs validation after topo is open (CheckCellFlags first to avoid
// creating ResilientServer on invalid cells), then creates ResilientServer and
// returns the filtered serving tablet types for use by run().
func runWithTopo(ctx context.Context, cmd *cobra.Command, ts *topo.Server) ([]topodatapb.TabletType, error) {
	tabletTypes := make([]topodatapb.TabletType, 0, 1)
	for _, tt := range tabletTypesToWait {
		if topoproto.IsServingType(tt) {
			tabletTypes = append(tabletTypes, tt)
		}
	}

	if len(tabletTypes) == 0 {
		return nil, errors.New("tablet-types-to-wait must contain at least one serving tablet type")
	}

	// Validate cells before creating ResilientServer so we don't leak it on error.
	if err := CheckCellFlags(ctx, &fakesrvtopo.FakeSrvTopo{Ts: ts}, cell, vtgate.CellsToWatch); err != nil {
		return nil, fmt.Errorf("cells_to_watch validation failed: %v", err)
	}

	resilientServer = srvtopo.NewResilientServer(ctx, ts, srvTopoCounts)
	return tabletTypes, nil
}

func run(cmd *cobra.Command, args []string) error {
	defer exit.Recover()

	servenv.Init()

	// Ensure we open the topo before we start the context, so that the
	// defer that closes the topo runs after cancelling the context.
	// This ensures that we've properly closed things like the watchers
	// at that point.
	var ts *topo.Server
	if testTopoOpener != nil {
		ts = testTopoOpener()
	} else {
		ts = topo.Open()
	}
	defer ts.Close()

	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	tabletTypes, err := runWithTopo(ctx, cmd, ts)
	if err != nil {
		return err
	}

	plannerVersion, ok := plancontext.PlannerNameToVersion(plannerName)
	if !ok {
		return fmt.Errorf("invalid planner-version %q", plannerName)
	}

	env, err := vtenv.New(vtenv.Options{
		MySQLServerVersion: servenv.MySQLServerVersion(),
		TruncateUILen:      servenv.TruncateUILen,
		TruncateErrLen:     servenv.TruncateErrLen,
	})
	if err != nil {
		return fmt.Errorf("unable to initialize env: %v", err)
	}

	// pass nil for HealthCheck and it will be created
	vtg := vtgate.Init(ctx, env, nil, resilientServer, cell, tabletTypes, plannerVersion)

	servenv.OnRun(func() {
		// Flags are parsed now. Parse the template using the actual flag value and overwrite the current template.
		discovery.ParseTabletURLTemplateFromFlag()
		addStatusParts(vtg)
	})
	servenv.OnClose(func() {
		_ = vtg.Gateway().Close(ctx)
	})
	if testRunDefault != nil {
		testRunDefault()
	} else {
		servenv.RunDefault()
	}

	return nil
}

func init() {
	srvTopoCounts = stats.NewCountersWithSingleLabel("ResilientSrvTopoServer", "Resilient srvtopo server operations", "type")

	servenv.RegisterDefaultFlags()
	servenv.RegisterFlags()
	servenv.RegisterGRPCServerFlags()
	servenv.RegisterGRPCServerAuthFlags()
	servenv.RegisterServiceMapFlag()

	servenv.MoveFlagsToCobraCommand(Main)

	acl.RegisterFlags(Main.Flags())
	Main.Flags().StringVar(&cell, "cell", cell, "cell to use (required)")
	utils.SetFlagVar(Main.Flags(), (*topoproto.TabletTypeListFlag)(&tabletTypesToWait), "tablet-types-to-wait", "Wait till connected for specified tablet types during Gateway initialization. Should be provided as a comma-separated set of tablet types.")
	Main.Flags().StringVar(&plannerName, "planner-version", plannerName, "Sets the default planner to use when the session has not changed it. Valid values are: Gen4, Gen4Greedy, Gen4Left2Right")

	// Support both variants until v25
	Main.MarkFlagsOneRequired("tablet-types-to-wait")
}
