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

package command

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/vt/topo/topoproto"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	tabletTypesDefault = []string{"in_order:REPLICA", "PRIMARY"}
	onDDLDefault       = binlogdatapb.OnDDLAction_name[int32(binlogdatapb.OnDDLAction_IGNORE)]

	// MoveTables is the base command for all related actions.
	MoveTables = &cobra.Command{
		Use:   "movetables --workflow <workflow> --target-keyspace <keyspace> [command]",
		Short: "Perform commands related to moving tables from a source keyspace to a target keyspace.",
		Long: `MoveTables commands: Create, Show, Progress, SwitchTraffic, ReverseTraffic, Stop, Cancel, and Delete.
See the --help output for each command for more details.`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"MoveTables"},
		Args:                  cobra.ExactArgs(1),
	}

	// MoveTablesCancel makes a MoveTablesCancel gRPC call to a vtctld.
	MoveTablesCancel = &cobra.Command{
		Use:                   "cancel",
		Short:                 "Cancel a MoveTables VReplication workflow",
		Example:               `vtctldclient --server=localhost:15999 MoveTables --workflow "commerce2customer" --target-keyspace "customer" Cancel`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Cancel"},
		Args:                  cobra.NoArgs,
		RunE:                  commandMoveTablesCancel,
	}

	// MoveTablesCreate makes a MoveTablesCreate gRPC call to a vtctld.
	MoveTablesCreate = &cobra.Command{
		Use:                   "create",
		Short:                 "Create and optionally run a MoveTables VReplication workflow",
		Example:               `vtctldclient --server=localhost:15999 MoveTables --workflow "commerce2customer" --target-keyspace "customer" Create --source-keyspace "commerce" --cells "zone1" --cells "zone2" --tablet-types "replica"`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Create"},
		Args:                  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			// Source and Target keyspace are required.
			if !cmd.Flags().Lookup("source-keyspace").Changed || !cmd.Flags().Lookup("target-keyspace").Changed {
				return fmt.Errorf("source-keyspace and target-keyspace are required")
			}
			// Either specific tables or the all tables flags are required.
			if !cmd.Flags().Lookup("tables").Changed && !cmd.Flags().Lookup("all-tables").Changed {
				return fmt.Errorf("tables or all-tables are required to specify which tables to move")
			}
			if cmd.Flags().Lookup("cells").Changed { // Validate the provided value(s)
				for i, cell := range moveTablesCreateOptions.Cells { // Which only means trimming whitespace
					moveTablesCreateOptions.Cells[i] = strings.TrimSpace(cell)
				}
			}
			if cmd.Flags().Lookup("tablet-types").Changed { // Validate the provided value(s)
				for i, tabletType := range moveTablesCreateOptions.TabletTypes {
					moveTablesCreateOptions.TabletTypes[i] = strings.ToUpper(strings.TrimSpace(tabletType))
					if _, err := topoproto.ParseTabletType(moveTablesCreateOptions.TabletTypes[i]); err != nil {
						return err
					}
				}
			}
			if _, ok := binlogdatapb.OnDDLAction_value[strings.ToUpper(moveTablesCreateOptions.OnDDL)]; !ok {
				return fmt.Errorf("invalid on-ddl value: %s", moveTablesCreateOptions.OnDDL)
			}
			return nil
		},
		RunE: commandMoveTablesCreate,
	}
)

var (
	// Required options for all commands.
	moveTablesOptions = struct {
		Workflow       string
		TargetKeyspace string
	}{}
	moveTablesCancelOptions = struct {
		KeepData         bool
		KeepRoutingRules bool
	}{}
	moveTablesCreateOptions = struct {
		Workflow            string
		SourceKeyspace      string
		Cells               []string
		TabletTypes         []string
		SourceShards        []string
		ExternalClusterName string
		AllTables           bool
		IncludeTables       []string
		ExcludeTables       []string
		SourceTimeZone      string
		OnDDL               string
		Timeout             time.Duration
		DeferSecondaryKeys  bool
		AutoStart           bool
		StopAfterCopy       bool
	}{}
)

func commandMoveTablesCreate(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	req := &vtctldatapb.MoveTablesCreateRequest{
		Workflow:       moveTablesOptions.Workflow,
		TargetKeyspace: moveTablesOptions.TargetKeyspace,
		SourceKeyspace: moveTablesCreateOptions.SourceKeyspace,
		Cells:          moveTablesCreateOptions.Cells,
		TabletTypes:    moveTablesCreateOptions.TabletTypes,
		AllTables:      moveTablesCreateOptions.AllTables,
		IncludeTables:  moveTablesCreateOptions.IncludeTables,
		ExcludeTables:  moveTablesCreateOptions.ExcludeTables,
		OnDdl:          moveTablesCreateOptions.OnDDL,
		AutoStart:      moveTablesCreateOptions.AutoStart,
		StopAfterCopy:  moveTablesCreateOptions.StopAfterCopy,
		//Timeout:        protoutil.DurationToProto(moveTablesCreateOptions.Timeout),
	}

	resp, err := client.MoveTablesCreate(commandCtx, req)
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

func commandMoveTablesCancel(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	req := &vtctldatapb.WorkflowDeleteRequest{
		Keyspace:         moveTablesOptions.TargetKeyspace,
		Workflow:         moveTablesOptions.Workflow,
		KeepData:         moveTablesCancelOptions.KeepData,
		KeepRoutingRules: moveTablesCancelOptions.KeepRoutingRules,
	}
	resp, err := client.WorkflowDelete(commandCtx, req)
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

func init() {
	MoveTables.PersistentFlags().StringVar(&moveTablesOptions.TargetKeyspace, "target-keyspace", "", "Keyspace where the tables are being moved to and where the workflow exists (required)")
	MoveTables.MarkPersistentFlagRequired("target-keyspace")
	MoveTables.Flags().StringVarP(&moveTablesOptions.Workflow, "workflow", "w", "", "The workflow you want to perform the command on (required)")
	MoveTables.MarkPersistentFlagRequired("workflow")
	Root.AddCommand(MoveTables)

	MoveTablesCreate.PersistentFlags().StringVar(&moveTablesCreateOptions.SourceKeyspace, "source-keyspace", "", "Keyspace where the tables are being moved from (required)")
	MoveTablesCreate.MarkPersistentFlagRequired("source-keyspace")
	MoveTablesCreate.Flags().StringSliceVarP(&moveTablesCreateOptions.Cells, "cells", "c", nil, "Cells and/or CellAliases to copy table data from")
	MoveTablesCreate.Flags().StringSliceVar(&moveTablesCreateOptions.SourceShards, "source-shards", nil, "Source shards to copy data from when performing a partial MoveTables (experimental)")
	MoveTablesCreate.Flags().StringSliceVar(&moveTablesCreateOptions.TabletTypes, "tablet-types", tabletTypesDefault, "Source tablet types to replicate table data from (e.g. PRIMARY,REPLICA,RDONLY)")
	MoveTablesCreate.Flags().BoolVar(&moveTablesCreateOptions.AllTables, "all-tables", false, "Copy all tables from the source")
	MoveTablesCreate.Flags().StringSliceVar(&moveTablesCreateOptions.IncludeTables, "tables", nil, "Source tables to copy")
	MoveTablesCreate.Flags().StringSliceVar(&moveTablesCreateOptions.ExcludeTables, "exclude-tables", nil, "Source tables to exclude from copying")
	MoveTablesCreate.Flags().StringVar(&moveTablesCreateOptions.OnDDL, "on-ddl", onDDLDefault, "What to do when DDL is encountered in the VReplication stream. Possible values are IGNORE, STOP, EXEC, and EXEC_IGNORE")
	MoveTablesCreate.Flags().BoolVar(&moveTablesCreateOptions.AutoStart, "auto-start", true, "Start the MoveTables workflow after creating it")
	MoveTablesCreate.Flags().BoolVar(&moveTablesCreateOptions.StopAfterCopy, "stop-after-copy", false, "Stop the MoveTables workflow after it's finished copying the existing rows and before it starts replicating changes")
	MoveTables.AddCommand(MoveTablesCreate)

	MoveTablesCancel.Flags().BoolVar(&moveTablesCancelOptions.KeepData, "keep-data", false, "Keep the partially copied table data from the MoveTables workflow in the target keyspace")
	MoveTablesCancel.Flags().BoolVar(&moveTablesCancelOptions.KeepRoutingRules, "keep-routing-rules", false, "Keep the routing rules created for the MoveTables workflow")
	MoveTables.AddCommand(MoveTablesCancel)
}
