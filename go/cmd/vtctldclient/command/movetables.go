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
	"bytes"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/workflow"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	// The generic default for most commands.
	tabletTypesDefault = []topodatapb.TabletType{
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_PRIMARY,
	}
	onDDLDefault             = binlogdatapb.OnDDLAction_IGNORE.String()
	maxReplicationLagDefault = 30 * time.Second
	timeoutDefault           = 30 * time.Second

	// MoveTables is the base command for all related actions.
	MoveTables = &cobra.Command{
		Use:   "MoveTables --workflow <workflow> --target-keyspace <keyspace> [command] [command-flags]",
		Short: "Perform commands related to moving tables from a source keyspace to a target keyspace.",
		Long: `MoveTables commands: Create, Show, Status, SwitchTraffic, ReverseTraffic, Stop, Start, Cancel, and Delete.
See the --help output for each command for more details.`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"movetables"},
		Args:                  cobra.ExactArgs(1),
	}

	// MoveTablesCancel makes a MoveTablesCancel gRPC call to a vtctld.
	MoveTablesCancel = &cobra.Command{
		Use:                   "cancel",
		Short:                 "Cancel a MoveTables VReplication workflow.",
		Example:               `vtctldclient --server localhost:15999 movetables --workflow commerce2customer --target-keyspace customer cancel`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Cancel"},
		Args:                  cobra.NoArgs,
		RunE:                  commandMoveTablesCancel,
	}

	// MoveTablesComplete makes a MoveTablesComplete gRPC call to a vtctld.
	MoveTablesComplete = &cobra.Command{
		Use:                   "complete",
		Short:                 "Complete a MoveTables VReplication workflow.",
		Example:               `vtctldclient --server localhost:15999 movetables --workflow commerce2customer --target-keyspace customer complete`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Complete"},
		Args:                  cobra.NoArgs,
		RunE:                  commandMoveTablesComplete,
	}

	// MoveTablesCreate makes a MoveTablesCreate gRPC call to a vtctld.
	MoveTablesCreate = &cobra.Command{
		Use:                   "create",
		Short:                 "Create and optionally run a MoveTables VReplication workflow.",
		Example:               `vtctldclient --server localhost:15999 movetables --workflow commerce2customer --target-keyspace customer create --source-keyspace commerce --cells zone1 --cells zone2 --tablet-types replica`,
		SilenceUsage:          true,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Create"},
		Args:                  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			// Either specific tables or the all tables flags are required.
			if !cmd.Flags().Lookup("tables").Changed && !cmd.Flags().Lookup("all-tables").Changed {
				return fmt.Errorf("tables or all-tables are required to specify which tables to move")
			}
			if cmd.Flags().Lookup("cells").Changed { // Validate the provided value(s)
				for i, cell := range moveTablesCreateOptions.Cells { // Which only means trimming whitespace
					moveTablesCreateOptions.Cells[i] = strings.TrimSpace(cell)
				}
			}
			if !cmd.Flags().Lookup("tablet-types").Changed {
				moveTablesCreateOptions.TabletTypes = tabletTypesDefault
			}
			if _, ok := binlogdatapb.OnDDLAction_value[strings.ToUpper(moveTablesCreateOptions.OnDDL)]; !ok {
				return fmt.Errorf("invalid on-ddl value: %s", moveTablesCreateOptions.OnDDL)
			}
			return nil
		},
		RunE: commandMoveTablesCreate,
	}

	// MoveTablesShow makes a GetWorkflows gRPC call to a vtctld.
	MoveTablesShow = &cobra.Command{
		Use:                   "show",
		Short:                 "Show the details for a MoveTables VReplication workflow.",
		Example:               `vtctldclient --server localhost:15999 movetables --workflow commerce2customer --target-keyspace customer show`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Show"},
		Args:                  cobra.NoArgs,
		RunE:                  commandMoveTablesShow,
	}

	// MoveTablesStart makes a WorfklowUpdate gRPC call to a vtctld.
	MoveTablesStart = &cobra.Command{
		Use:                   "start",
		Short:                 "Start the MoveTables workflow.",
		Example:               `vtctldclient --server localhost:15999 movetables --workflow commerce2customer --target-keyspace customer start`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Start"},
		Args:                  cobra.NoArgs,
		PreRun:                bridgeMoveTablesToWorkflow,
		RunE:                  commandWorkflowUpdateState,
	}

	// MoveTablesStatus makes a GetWorkflows gRPC call to a vtctld.
	MoveTablesStatus = &cobra.Command{
		Use:                   "status",
		Short:                 "Show the current status for a MoveTables VReplication workflow.",
		Example:               `vtctldclient --server localhost:15999 MoveTables --workflow commerce2customer --target-keyspace customer status`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Status", "progress", "Progress"},
		Args:                  cobra.NoArgs,
		RunE:                  commandMoveTablesStatus,
	}

	// MoveTablesStop makes a WorfklowUpdate gRPC call to a vtctld.
	MoveTablesStop = &cobra.Command{
		Use:                   "stop",
		Short:                 "Stop a MoveTables workflow.",
		Example:               `vtctldclient --server localhost:15999 movetables --workflow commerce2customer --target-keyspace customer stop`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Stop"},
		Args:                  cobra.NoArgs,
		PreRun:                bridgeMoveTablesToWorkflow,
		RunE:                  commandWorkflowUpdateState,
	}

	// MoveTablesReverseTraffic makes a WorkflowSwitchTraffic gRPC call to a vtctld.
	MoveTablesReverseTraffic = &cobra.Command{
		Use:                   "reversetraffic",
		Short:                 "Reverse traffic for a MoveTables VReplication workflow.",
		Example:               `vtctldclient --server localhost:15999 movetables --workflow commerce2customer --target-keyspace customer reversetraffic`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"ReverseTraffic"},
		Args:                  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			moveTablesSwitchTrafficOptions.Direction = workflow.DirectionBackward
			if !cmd.Flags().Lookup("tablet-types").Changed {
				// We switch traffic for all tablet types if none are provided.
				moveTablesSwitchTrafficOptions.TabletTypes = []topodatapb.TabletType{
					topodatapb.TabletType_PRIMARY,
					topodatapb.TabletType_REPLICA,
					topodatapb.TabletType_RDONLY,
				}
			}
			return nil
		},
		RunE: commandMoveTablesSwitchTraffic,
	}

	// MoveTablesSwitchTraffic makes a MoveTablesSwitchTraffic gRPC call to a vtctld.
	MoveTablesSwitchTraffic = &cobra.Command{
		Use:                   "switchtraffic",
		Short:                 "Switch traffic for a MoveTables VReplication workflow.",
		Example:               `vtctldclient --server localhost:15999 movetables --workflow commerce2customer --target-keyspace customer switchtraffic --tablet-types "replica,rdonly"`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"SwitchTraffic"},
		Args:                  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			moveTablesSwitchTrafficOptions.Direction = workflow.DirectionForward
			if !cmd.Flags().Lookup("tablet-types").Changed {
				// We switch traffic for all tablet types if none are provided.
				moveTablesSwitchTrafficOptions.TabletTypes = []topodatapb.TabletType{
					topodatapb.TabletType_PRIMARY,
					topodatapb.TabletType_REPLICA,
					topodatapb.TabletType_RDONLY,
				}
			}
			return nil
		},
		RunE: commandMoveTablesSwitchTraffic,
	}
)

var (
	// Required options for all commands.
	moveTablesOptions = struct {
		Workflow       string
		TargetKeyspace string
		Format         string
	}{}
	moveTablesCancelOptions = struct {
		KeepData         bool
		KeepRoutingRules bool
	}{}
	moveTablesCompleteOptions = struct {
		KeepData         bool
		KeepRoutingRules bool
		RenameTables     bool
		DryRun           bool
	}{}
	moveTablesCreateOptions = struct {
		Workflow                     string
		SourceKeyspace               string
		Cells                        []string
		TabletTypes                  []topodatapb.TabletType
		TabletTypesInPreferenceOrder bool
		SourceShards                 []string
		ExternalClusterName          string
		AllTables                    bool
		IncludeTables                []string
		ExcludeTables                []string
		SourceTimeZone               string
		OnDDL                        string
		DeferSecondaryKeys           bool
		AutoStart                    bool
		StopAfterCopy                bool
	}{}
	moveTablesSwitchTrafficOptions = struct {
		Cells                    []string
		TabletTypes              []topodatapb.TabletType
		MaxReplicationLagAllowed time.Duration
		EnableReverseReplication bool
		Timeout                  time.Duration
		DryRun                   bool
		Direction                workflow.TrafficSwitchDirection
	}{}
)

func bridgeMoveTablesToWorkflow(cmd *cobra.Command, args []string) {
	workflowUpdateOptions.Workflow = moveTablesOptions.Workflow
	workflowOptions.Keyspace = moveTablesOptions.TargetKeyspace
}

func commandMoveTablesCreate(cmd *cobra.Command, args []string) error {
	format := strings.ToLower(strings.TrimSpace(moveTablesOptions.Format))
	switch format {
	case "text", "json":
	default:
		return fmt.Errorf("invalid output format, got %s", moveTablesOptions.Format)
	}

	cli.FinishedParsing(cmd)

	tsp := tabletmanagerdatapb.TabletSelectionPreference_ANY
	if moveTablesCreateOptions.TabletTypesInPreferenceOrder {
		tsp = tabletmanagerdatapb.TabletSelectionPreference_INORDER
	}

	req := &vtctldatapb.MoveTablesCreateRequest{
		Workflow:                  moveTablesOptions.Workflow,
		TargetKeyspace:            moveTablesOptions.TargetKeyspace,
		SourceKeyspace:            moveTablesCreateOptions.SourceKeyspace,
		Cells:                     moveTablesCreateOptions.Cells,
		TabletTypes:               moveTablesCreateOptions.TabletTypes,
		TabletSelectionPreference: tsp,
		AllTables:                 moveTablesCreateOptions.AllTables,
		IncludeTables:             moveTablesCreateOptions.IncludeTables,
		ExcludeTables:             moveTablesCreateOptions.ExcludeTables,
		OnDdl:                     moveTablesCreateOptions.OnDDL,
		AutoStart:                 moveTablesCreateOptions.AutoStart,
		StopAfterCopy:             moveTablesCreateOptions.StopAfterCopy,
	}

	resp, err := client.MoveTablesCreate(commandCtx, req)
	if err != nil {
		return err
	}

	var output []byte
	if format == "json" {
		output, err = cli.MarshalJSON(resp)
		if err != nil {
			return err
		}
	} else {
		tout := bytes.Buffer{}
		tout.WriteString(fmt.Sprintf("The following vreplication streams exist for workflow %s.%s:\n\n",
			moveTablesOptions.TargetKeyspace, moveTablesOptions.Workflow))
		for _, shardstreams := range resp.ShardStreams {
			for _, shardstream := range shardstreams.Streams {
				tablet := fmt.Sprintf("%s-%d", shardstream.Tablet.Cell, shardstream.Tablet.Uid)
				tout.WriteString(fmt.Sprintf("id=%d on %s/%s: Status: %s. %s.\n",
					shardstream.Id, moveTablesOptions.TargetKeyspace, tablet, shardstream.Status, shardstream.Info))
			}
		}
		output = tout.Bytes()
	}
	fmt.Printf("%s\n", output)

	return nil
}

func commandMoveTablesCancel(cmd *cobra.Command, args []string) error {
	format := strings.ToLower(strings.TrimSpace(moveTablesOptions.Format))
	switch format {
	case "text", "json":
	default:
		return fmt.Errorf("invalid output format, got %s", moveTablesOptions.Format)
	}

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

	var output []byte
	if format == "json" {
		// Sort the inner TabletInfo slice for deterministic output.
		sort.Slice(resp.Details, func(i, j int) bool {
			return resp.Details[i].Tablet.String() < resp.Details[j].Tablet.String()
		})
		output, err = cli.MarshalJSONCompact(resp)
		if err != nil {
			return err
		}
	} else {
		output = []byte(resp.Summary + "\n")
	}
	fmt.Printf("%s\n", output)

	return nil
}

func commandMoveTablesComplete(cmd *cobra.Command, args []string) error {
	format := strings.ToLower(strings.TrimSpace(moveTablesOptions.Format))
	switch format {
	case "text", "json":
	default:
		return fmt.Errorf("invalid output format, got %s", moveTablesOptions.Format)
	}

	cli.FinishedParsing(cmd)

	req := &vtctldatapb.MoveTablesCompleteRequest{
		Workflow:         moveTablesOptions.Workflow,
		TargetKeyspace:   moveTablesOptions.TargetKeyspace,
		KeepData:         moveTablesCompleteOptions.KeepData,
		KeepRoutingRules: moveTablesCompleteOptions.KeepRoutingRules,
		RenameTables:     moveTablesCompleteOptions.RenameTables,
		DryRun:           moveTablesCompleteOptions.DryRun,
	}
	resp, err := client.MoveTablesComplete(commandCtx, req)
	if err != nil {
		return err
	}

	var output []byte
	if format == "json" {
		output, err = cli.MarshalJSONCompact(resp)
		if err != nil {
			return err
		}
	} else {
		tout := bytes.Buffer{}
		tout.WriteString(resp.Summary + "\n")
		if len(resp.DryRunResults) > 0 {
			tout.WriteString("\n")
			for _, r := range resp.DryRunResults {
				tout.WriteString(r + "\n")
			}
		}
		output = tout.Bytes()
	}
	fmt.Printf("%s\n", output)

	return nil
}

func commandMoveTablesStatus(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	req := &vtctldatapb.WorkflowStatusRequest{
		Keyspace: moveTablesOptions.TargetKeyspace,
		Workflow: moveTablesOptions.Workflow,
	}
	resp, err := client.WorkflowStatus(commandCtx, req)
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

func commandMoveTablesShow(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	req := &vtctldatapb.GetWorkflowsRequest{
		Keyspace: moveTablesOptions.TargetKeyspace,
		Workflow: moveTablesOptions.Workflow,
	}
	resp, err := client.GetWorkflows(commandCtx, req)
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

func commandMoveTablesSwitchTraffic(cmd *cobra.Command, args []string) error {
	format := strings.ToLower(strings.TrimSpace(moveTablesOptions.Format))
	switch format {
	case "text", "json":
	default:
		return fmt.Errorf("invalid output format, got %s", moveTablesOptions.Format)
	}

	cli.FinishedParsing(cmd)

	req := &vtctldatapb.WorkflowSwitchTrafficRequest{
		Keyspace:                 moveTablesOptions.TargetKeyspace,
		Workflow:                 moveTablesOptions.Workflow,
		TabletTypes:              moveTablesSwitchTrafficOptions.TabletTypes,
		MaxReplicationLagAllowed: protoutil.DurationToProto(moveTablesSwitchTrafficOptions.MaxReplicationLagAllowed),
		Timeout:                  protoutil.DurationToProto(moveTablesSwitchTrafficOptions.Timeout),
		DryRun:                   moveTablesSwitchTrafficOptions.DryRun,
		EnableReverseReplication: moveTablesSwitchTrafficOptions.EnableReverseReplication,
		Direction:                int32(moveTablesSwitchTrafficOptions.Direction),
	}
	resp, err := client.WorkflowSwitchTraffic(commandCtx, req)
	if err != nil {
		return err
	}

	var output []byte
	if format == "json" {
		output, err = cli.MarshalJSONCompact(resp)
		if err != nil {
			return err
		}
	} else {
		tout := bytes.Buffer{}
		tout.WriteString(resp.Summary + "\n\n")
		if req.DryRun {
			for _, line := range resp.DryRunResults {
				tout.WriteString(line + "\n")
			}
		} else {
			tout.WriteString(fmt.Sprintf("Start State: %s\n", resp.StartState))
			tout.WriteString(fmt.Sprintf("Current State: %s\n", resp.CurrentState))
		}
		output = tout.Bytes()
	}
	fmt.Printf("%s\n", output)

	return nil
}

func init() {
	MoveTables.PersistentFlags().StringVar(&moveTablesOptions.TargetKeyspace, "target-keyspace", "", "Keyspace where the tables are being moved to and where the workflow exists (required)")
	MoveTables.MarkPersistentFlagRequired("target-keyspace")
	MoveTables.Flags().StringVarP(&moveTablesOptions.Workflow, "workflow", "w", "", "The workflow you want to perform the command on (required)")
	MoveTables.MarkPersistentFlagRequired("workflow")
	MoveTables.Flags().StringVar(&moveTablesOptions.Format, "format", "text", "The format of the output; supported formats are: text,json")
	Root.AddCommand(MoveTables)

	MoveTablesCancel.Flags().BoolVar(&moveTablesCancelOptions.KeepData, "keep-data", false, "Keep the partially copied table data from the MoveTables workflow in the target keyspace")
	MoveTablesCancel.Flags().BoolVar(&moveTablesCancelOptions.KeepRoutingRules, "keep-routing-rules", false, "Keep the routing rules created for the MoveTables workflow")
	MoveTables.AddCommand(MoveTablesCancel)

	MoveTablesComplete.Flags().BoolVar(&moveTablesCompleteOptions.KeepData, "keep-data", false, "Keep the original source table data that was copied by the MoveTables workflow")
	MoveTablesComplete.Flags().BoolVar(&moveTablesCompleteOptions.KeepRoutingRules, "keep-routing-rules", false, "Keep the routing rules in place that direct table traffic from the source keyspace to the target keyspace of the MoveTables workflow")
	MoveTablesComplete.Flags().BoolVar(&moveTablesCompleteOptions.RenameTables, "rename-tables", false, "Keep the original source table data that was copied by the MoveTables workflow, but rename each table to '_<tablename>_old'")
	MoveTablesComplete.Flags().BoolVar(&moveTablesCompleteOptions.DryRun, "dry-run", false, "Print the actions that would be taken and report any known errors that would have occurred")
	MoveTables.AddCommand(MoveTablesComplete)

	MoveTablesCreate.PersistentFlags().StringVar(&moveTablesCreateOptions.SourceKeyspace, "source-keyspace", "", "Keyspace where the tables are being moved from (required)")
	MoveTablesCreate.MarkPersistentFlagRequired("source-keyspace")
	MoveTablesCreate.Flags().StringSliceVarP(&moveTablesCreateOptions.Cells, "cells", "c", nil, "Cells and/or CellAliases to copy table data from")
	MoveTablesCreate.Flags().StringSliceVar(&moveTablesCreateOptions.SourceShards, "source-shards", nil, "Source shards to copy data from when performing a partial MoveTables (experimental)")
	MoveTablesCreate.Flags().Var((*topoproto.TabletTypeListFlag)(&moveTablesCreateOptions.TabletTypes), "tablet-types", "Source tablet types to replicate table data from (e.g. PRIMARY,REPLICA,RDONLY)")
	MoveTablesCreate.Flags().BoolVar(&moveTablesCreateOptions.TabletTypesInPreferenceOrder, "tablet-types-in-preference-order", true, "When performing source tablet selection, look for candidates in the type order as they are listed in the tablet-types flag")
	MoveTablesCreate.Flags().BoolVar(&moveTablesCreateOptions.AllTables, "all-tables", false, "Copy all tables from the source")
	MoveTablesCreate.Flags().StringSliceVar(&moveTablesCreateOptions.IncludeTables, "tables", nil, "Source tables to copy")
	MoveTablesCreate.Flags().StringSliceVar(&moveTablesCreateOptions.ExcludeTables, "exclude-tables", nil, "Source tables to exclude from copying")
	MoveTablesCreate.Flags().StringVar(&moveTablesCreateOptions.OnDDL, "on-ddl", onDDLDefault, "What to do when DDL is encountered in the VReplication stream. Possible values are IGNORE, STOP, EXEC, and EXEC_IGNORE")
	MoveTablesCreate.Flags().BoolVar(&moveTablesCreateOptions.AutoStart, "auto-start", true, "Start the MoveTables workflow after creating it")
	MoveTablesCreate.Flags().BoolVar(&moveTablesCreateOptions.StopAfterCopy, "stop-after-copy", false, "Stop the MoveTables workflow after it's finished copying the existing rows and before it starts replicating changes")
	MoveTables.AddCommand(MoveTablesCreate)

	MoveTables.AddCommand(MoveTablesShow)

	MoveTables.AddCommand(MoveTablesStart)

	MoveTables.AddCommand(MoveTablesStatus)

	MoveTables.AddCommand(MoveTablesStop)

	MoveTablesSwitchTraffic.Flags().StringSliceVarP(&moveTablesSwitchTrafficOptions.Cells, "cells", "c", nil, "Cells and/or CellAliases to switch traffic in")
	MoveTablesSwitchTraffic.Flags().Var((*topoproto.TabletTypeListFlag)(&moveTablesSwitchTrafficOptions.TabletTypes), "tablet-types", "Tablet types to switch traffic for")
	MoveTablesSwitchTraffic.Flags().DurationVar(&moveTablesSwitchTrafficOptions.Timeout, "timeout", timeoutDefault, "Specifies the maximum time to wait, in seconds, for VReplication to catch up on primary tablets. The traffic switch will be cancelled on timeout.")
	MoveTablesSwitchTraffic.Flags().DurationVar(&moveTablesSwitchTrafficOptions.MaxReplicationLagAllowed, "max-replication-lag-allowed", maxReplicationLagDefault, "Allow traffic to be switched only if VReplication lag is below this")
	MoveTablesSwitchTraffic.Flags().BoolVar(&moveTablesSwitchTrafficOptions.DryRun, "dry-run", false, "Print the actions that would be taken and report any known errors that would have occurred")
	MoveTables.AddCommand(MoveTablesSwitchTraffic)

	MoveTablesReverseTraffic.Flags().StringSliceVarP(&moveTablesSwitchTrafficOptions.Cells, "cells", "c", nil, "Cells and/or CellAliases to switch traffic in")
	MoveTablesReverseTraffic.Flags().BoolVar(&moveTablesSwitchTrafficOptions.DryRun, "dry-run", false, "Print the actions that would be taken and report any known errors that would have occurred")
	MoveTablesReverseTraffic.Flags().DurationVar(&moveTablesSwitchTrafficOptions.MaxReplicationLagAllowed, "max-replication-lag-allowed", maxReplicationLagDefault, "Allow traffic to be switched only if VReplication lag is below this")
	MoveTablesReverseTraffic.Flags().BoolVar(&moveTablesSwitchTrafficOptions.EnableReverseReplication, "enable-reverse-replication", true, "Setup replication going back to the original source keyspace to support rolling back the traffic cutover")
	MoveTablesReverseTraffic.Flags().Var((*topoproto.TabletTypeListFlag)(&moveTablesSwitchTrafficOptions.TabletTypes), "tablet-types", "Tablet types to switch traffic for")
	MoveTablesReverseTraffic.Flags().DurationVar(&moveTablesSwitchTrafficOptions.Timeout, "timeout", timeoutDefault, "Specifies the maximum time to wait, in seconds, for VReplication to catch up on primary tablets. The traffic switch will be cancelled on timeout.")
	MoveTables.AddCommand(MoveTablesReverseTraffic)
}
