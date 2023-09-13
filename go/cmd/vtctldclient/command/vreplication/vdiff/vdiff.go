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

package vdiff

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"
	"vitess.io/vitess/go/protoutil"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	topoprotopb "vitess.io/vitess/go/vt/topo/topoproto"
)

var (
	tabletTypesDefault = []topodatapb.TabletType{
		topodatapb.TabletType_RDONLY,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_PRIMARY,
	}

	vDiffCreateOptions = struct {
		UUID                        uuid.UUID
		SourceCells                 []string
		TargetCells                 []string
		TabletTypes                 []topodatapb.TabletType
		Tables                      []string
		Limit                       uint32 // We only accept positive values but pass on an int64
		FilteredReplicationWaitTime time.Duration
		DebugQuery                  bool
		OnlyPKs                     bool
		UpdateTableStats            bool
		MaxExtraRowsToCompare       uint32 // We only accept positive values but pass on an int64
		Wait                        bool
		WaitUpdateInterval          time.Duration
		AutoRetry                   bool
		Verbose                     bool
	}{}

	parseAndValidateCreate = func(cmd *cobra.Command, args []string) error {
		var err error
		if len(args) == 1 { // Validate UUID if provided
			if vDiffCreateOptions.UUID, err = uuid.Parse(args[0]); err != nil {
				return fmt.Errorf("invalid UUID provided: %v", err)
			}
		} else { // Generate a UUID
			vDiffCreateOptions.UUID = uuid.New()
		}
		if !cmd.Flags().Lookup("tablet-types").Changed {
			vDiffCreateOptions.TabletTypes = tabletTypesDefault
		}
		if cmd.Flags().Lookup("source-cells").Changed {
			for i, cell := range vDiffCreateOptions.SourceCells {
				vDiffCreateOptions.SourceCells[i] = strings.TrimSpace(cell)
			}
		}
		if cmd.Flags().Lookup("target-cells").Changed {
			for i, cell := range vDiffCreateOptions.TargetCells {
				vDiffCreateOptions.TargetCells[i] = strings.TrimSpace(cell)
			}
		}
		if cmd.Flags().Lookup("tables").Changed {
			for i, table := range vDiffCreateOptions.Tables {
				vDiffCreateOptions.Tables[i] = strings.TrimSpace(table)
			}
		}
		return nil
	}

	// vDiff is the base command for all actions related to VDiff. This
	// command creates a new VDiff workflow, auto generating a UUID for it.
	vDiff = &cobra.Command{
		Use:   "VDiff --workflow <workflow> --keyspace <keyspace> [command] [command-flags]",
		Short: "Perform commands related to diffing tables between the source keyspace and target keyspace.",
		Long: `VDiff commands: create, resume, show, stop, and delete.
See the --help output for each command for more details.`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"vdiff"},
		Args:                  cobra.NoArgs,
	}

	// vDiffCreate makes a vDiffCreate gRPC call to a vtctld.
	vDiffCreate = &cobra.Command{
		Use:                   "create",
		Short:                 "Create and run a VDiff to compare the tables involved in a VReplication workflow between the source and target.",
		Example:               `vtctldclient --server localhost:15999 vdiff --workflow commerce2customer --target-keyspace customer create b3f59678-5241-11ee-be56-0242ac120002`,
		SilenceUsage:          true,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Create"},
		Args:                  cobra.MaximumNArgs(1),
		PreRunE:               parseAndValidateCreate,
		RunE:                  commandVDiffCreate,
	}

	// vDiffShow makes a vDiffShow gRPC call to a vtctld.
	vDiffShow = &cobra.Command{
		Use:                   "show",
		Short:                 "Show the status of a VDiff.",
		Example:               `vtctldclient --server localhost:15999 vdiff --workflow commerce2customer --target-keyspace show last`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Show"},
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandVDiffShow,
	}
)

func commandVDiffCreate(cmd *cobra.Command, args []string) error {
	format, err := common.GetOutputFormat(cmd)
	if err != nil {
		return err
	}
	tsp := common.GetTabletSelectionPreference(cmd)
	cli.FinishedParsing(cmd)

	resp, err := common.GetClient().VDiffCreate(common.GetCommandCtx(), &vtctldatapb.VDiffCreateRequest{
		Workflow:                    common.BaseOptions.Workflow,
		TargetKeyspace:              common.BaseOptions.TargetKeyspace,
		SourceCells:                 vDiffCreateOptions.SourceCells,
		TargetCells:                 vDiffCreateOptions.TargetCells,
		TabletTypes:                 vDiffCreateOptions.TabletTypes,
		TabletSelectionPreference:   tsp,
		Tables:                      vDiffCreateOptions.Tables,
		Limit:                       int64(vDiffCreateOptions.Limit),
		FilteredReplicationWaitTime: protoutil.DurationToProto(vDiffCreateOptions.FilteredReplicationWaitTime),
		DebugQuery:                  vDiffCreateOptions.DebugQuery,
		OnlyPKs:                     vDiffCreateOptions.OnlyPKs,
		UpdateTableStats:            vDiffCreateOptions.UpdateTableStats,
		MaxExtraRowsToCompare:       int64(vDiffCreateOptions.MaxExtraRowsToCompare),
		Wait:                        vDiffCreateOptions.Wait,
		WaitUpdateInterval:          protoutil.DurationToProto(vDiffCreateOptions.WaitUpdateInterval),
		AutoRetry:                   vDiffCreateOptions.AutoRetry,
		Verbose:                     vDiffCreateOptions.Verbose,
	})

	if err != nil {
		return err
	}

	var data []byte
	if format == "json" {
		data, err = cli.MarshalJSON(resp)
		if err != nil {
			return err
		}
	}

	fmt.Printf("%s\n", data)

	return nil
}

func commandVDiffShow(cmd *cobra.Command, args []string) error {
	return nil
}

func registerVDiffCommands(root *cobra.Command) {
	common.AddCommonFlags(vDiff)
	root.AddCommand(vDiff)

	vDiffCreate.Flags().StringSliceVar(&vDiffCreateOptions.SourceCells, "source-cells", nil, "The source cell(s) to compare from; default is any available cell")
	vDiffCreate.Flags().StringSliceVar(&vDiffCreateOptions.TargetCells, "target-cells", nil, "The target cell(s) to compare with; default is any available cell")
	vDiffCreate.Flags().Var((*topoprotopb.TabletTypeListFlag)(&vDiffCreateOptions.TabletTypes), "tablet-types", "Tablet types to use on the source and target")
	vDiffCreate.Flags().DurationVar(&vDiffCreateOptions.FilteredReplicationWaitTime, "filtered-replication-wait-time", 30*time.Second, "Specifies the maximum time to wait, in seconds, for replication to catch up when syncing tablet streams.")
	vDiffCreate.Flags().Uint32Var(&vDiffCreateOptions.Limit, "limit", math.MaxUint32, "Max rows to stop comparing after")
	vDiffCreate.Flags().BoolVar(&vDiffCreateOptions.DebugQuery, "debug-query", false, "Adds a mysql query to the report that can be used for further debugging")
	vDiffCreate.Flags().BoolVar(&vDiffCreateOptions.OnlyPKs, "only-pks", false, "When reporting missing rows, only show primary keys in the report.")
	vDiffCreate.Flags().StringSliceVar(&vDiffCreateOptions.Tables, "tables", nil, "Only run vdiff for these tables in the workflow")
	vDiffCreate.Flags().Uint32Var(&vDiffCreateOptions.MaxExtraRowsToCompare, "max-extra-rows-to-compare", 1000, "If there are collation differences between the source and target, you can have rows that are identical but simply returned in a different order from MySQL. We will do a second pass to compare the rows for any actual differences in this case and this flag allows you to control the resources used for this operation.")
	vDiff.AddCommand(vDiffCreate)

	vDiff.AddCommand(vDiffShow)
}

func init() {
	common.RegisterCommandHandler("VDiff", registerVDiffCommands)
}
