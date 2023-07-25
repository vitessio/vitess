/*
Copyright 2021 The Vitess Authors.

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
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vt/topo/topoproto"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	// GetWorkflows makes a GetWorkflows gRPC call to a vtctld.
	GetWorkflows = &cobra.Command{
		Use:                   "GetWorkflows <keyspace>",
		Short:                 "Gets all vreplication workflows (Reshard, MoveTables, etc) in the given keyspace.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandGetWorkflows,
	}

	// Workflow is a parent command for Workflow* sub commands.
	Workflow = &cobra.Command{
		Use:   "Workflow --keyspace <keyspace> [command] [command-flags]",
		Short: "Administer VReplication workflows (Reshard, MoveTables, etc) in the given keyspace.",
		Long: `Workflow commands: List, Show, Start, Stop, Update, and Delete.
See the --help output for each command for more details.`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"workflow"},
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandGetWorkflows,
	}

	// WorkflowDelete makes a WorkflowDelete gRPC call to a vtctld.
	WorkflowDelete = &cobra.Command{
		Use:                   "delete",
		Short:                 "Delete a VReplication workflow.",
		Example:               `vtctldclient --server localhost:15999 workflow --keyspace customer delete --workflow commerce2customer`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Delete"},
		Args:                  cobra.NoArgs,
		RunE:                  commandWorkflowDelete,
	}

	// WorkflowList makes a GetWorkflows gRPC call to a vtctld.
	WorkflowList = &cobra.Command{
		Use:                   "list",
		Short:                 "List the VReplication workflows in the given keyspace.",
		Example:               `vtctldclient --server localhost:15999 workflow --keyspace customer list`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"List"},
		Args:                  cobra.NoArgs,
		RunE:                  commandWorkflowShow,
	}

	// WorkflowShow makes a GetWorkflows gRPC call to a vtctld.
	WorkflowShow = &cobra.Command{
		Use:                   "show",
		Short:                 "Show the details for a VReplication workflow.",
		Example:               `vtctldclient --server localhost:15999 workflow --keyspace customer show --workflow commerce2customer`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Show"},
		Args:                  cobra.NoArgs,
		RunE:                  commandWorkflowShow,
	}

	// WorkflowStart makes a WorfklowUpdate gRPC call to a vtctld.
	WorkflowStart = &cobra.Command{
		Use:                   "start",
		Short:                 "Start a VReplication workflow.",
		Example:               `vtctldclient --server localhost:15999 workflow --keyspace customer start --workflow commerce2customer`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Start"},
		Args:                  cobra.NoArgs,
		RunE:                  commandWorkflowUpdateState,
	}

	// WorkflowStop makes a WorfklowUpdate gRPC call to a vtctld.
	WorkflowStop = &cobra.Command{
		Use:                   "stop",
		Short:                 "Stop a VReplication workflow.",
		Example:               `vtctldclient --server localhost:15999 workflow --keyspace customer stop --workflow commerce2customer`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Stop"},
		Args:                  cobra.NoArgs,
		RunE:                  commandWorkflowUpdateState,
	}

	// WorkflowUpdate makes a WorkflowUpdate gRPC call to a vtctld.
	WorkflowUpdate = &cobra.Command{
		Use:                   "update",
		Short:                 "Update the configuration parameters for a VReplication workflow.",
		Example:               `vtctldclient --server localhost:15999 workflow --keyspace customer update --workflow commerce2customer --cells zone1 --cells zone2 -c "zone3,zone4" -c zone5`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Update"},
		Args:                  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			changes := false
			if cmd.Flags().Lookup("cells").Changed { // Validate the provided value(s)
				changes = true
				for i, cell := range workflowUpdateOptions.Cells { // Which only means trimming whitespace
					workflowUpdateOptions.Cells[i] = strings.TrimSpace(cell)
				}
			} else {
				workflowUpdateOptions.Cells = textutil.SimulatedNullStringSlice
			}
			if cmd.Flags().Lookup("tablet-types").Changed {
				changes = true
			} else {
				workflowUpdateOptions.TabletTypes = []topodatapb.TabletType{topodatapb.TabletType(textutil.SimulatedNullInt)}
			}
			if cmd.Flags().Lookup("on-ddl").Changed { // Validate the provided value
				changes = true
				if _, ok := binlogdatapb.OnDDLAction_value[strings.ToUpper(workflowUpdateOptions.OnDDL)]; !ok {
					return fmt.Errorf("invalid on-ddl value: %s", workflowUpdateOptions.OnDDL)
				}
			} // Simulated NULL will need to be handled in command
			if !changes {
				return fmt.Errorf("no configuration options specified to update")
			}
			return nil
		},
		RunE: commandWorkflowUpdate,
	}
)

var getWorkflowsOptions = struct {
	ShowAll bool
}{}

func commandGetWorkflows(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	ks := cmd.Flags().Arg(0)

	resp, err := client.GetWorkflows(commandCtx, &vtctldatapb.GetWorkflowsRequest{
		Keyspace:   ks,
		ActiveOnly: !getWorkflowsOptions.ShowAll,
	})

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

var (
	workflowOptions = struct {
		Keyspace string
	}{}
	workflowDeleteOptions = struct {
		Workflow         string
		KeepData         bool
		KeepRoutingRules bool
	}{}
	workflowUpdateOptions = struct {
		Workflow                     string
		Cells                        []string
		TabletTypes                  []topodatapb.TabletType
		TabletTypesInPreferenceOrder bool
		OnDDL                        string
	}{}
)

func commandWorkflowDelete(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	req := &vtctldatapb.WorkflowDeleteRequest{
		Keyspace:         workflowOptions.Keyspace,
		Workflow:         workflowDeleteOptions.Workflow,
		KeepData:         workflowDeleteOptions.KeepData,
		KeepRoutingRules: workflowDeleteOptions.KeepRoutingRules,
	}
	resp, err := client.WorkflowDelete(commandCtx, req)
	if err != nil {
		return err
	}

	// Sort the inner TabletInfo slice for deterministic output.
	sort.Slice(resp.Details, func(i, j int) bool {
		return resp.Details[i].Tablet.String() < resp.Details[j].Tablet.String()
	})

	data, err := cli.MarshalJSON(resp)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

func commandWorkflowShow(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	req := &vtctldatapb.GetWorkflowsRequest{
		Keyspace: workflowOptions.Keyspace,
		Workflow: workflowDeleteOptions.Workflow,
	}
	resp, err := client.GetWorkflows(commandCtx, req)
	if err != nil {
		return err
	}

	var data []byte
	if strings.ToLower(cmd.Name()) == "list" {
		// We only want the names
		Names := make([]string, len(resp.Workflows))
		for i, wf := range resp.Workflows {
			Names[i] = wf.Name
		}
		data, err = cli.MarshalJSON(Names)
	} else {
		data, err = cli.MarshalJSON(resp)
	}
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", data)

	return nil
}

func commandWorkflowUpdate(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	// We've already validated any provided value, if one WAS provided.
	// Now we need to do the mapping from the string representation to
	// the enum value.
	onddl := int32(textutil.SimulatedNullInt) // Simulated NULL when no value provided
	if val, ok := binlogdatapb.OnDDLAction_value[strings.ToUpper(workflowUpdateOptions.OnDDL)]; ok {
		onddl = val
	}

	// Simulated NULL when no value is provided.
	tsp := tabletmanagerdatapb.TabletSelectionPreference_UNKNOWN
	if cmd.Flags().Lookup("tablet-types-in-order").Changed {
		if workflowUpdateOptions.TabletTypesInPreferenceOrder {
			tsp = tabletmanagerdatapb.TabletSelectionPreference_INORDER
		} else {
			tsp = tabletmanagerdatapb.TabletSelectionPreference_ANY
		}
	}

	req := &vtctldatapb.WorkflowUpdateRequest{
		Keyspace: workflowOptions.Keyspace,
		TabletRequest: &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
			Workflow:                  workflowUpdateOptions.Workflow,
			Cells:                     workflowUpdateOptions.Cells,
			TabletTypes:               workflowUpdateOptions.TabletTypes,
			TabletSelectionPreference: tsp,
			OnDdl:                     binlogdatapb.OnDDLAction(onddl),
		},
	}

	resp, err := client.WorkflowUpdate(commandCtx, req)
	if err != nil {
		return err
	}

	// Sort the inner TabletInfo slice for deterministic output.
	sort.Slice(resp.Details, func(i, j int) bool {
		return resp.Details[i].Tablet.String() < resp.Details[j].Tablet.String()
	})

	data, err := cli.MarshalJSON(resp)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

func commandWorkflowUpdateState(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	var state binlogdatapb.VReplicationWorkflowState
	switch strings.ToLower(cmd.Name()) {
	case "start":
		state = binlogdatapb.VReplicationWorkflowState_Running
	case "stop":
		state = binlogdatapb.VReplicationWorkflowState_Stopped
	default:
		return fmt.Errorf("invalid workstate: %s", args[0])
	}

	// The only thing we're updating is the state.
	req := &vtctldatapb.WorkflowUpdateRequest{
		Keyspace: workflowOptions.Keyspace,
		TabletRequest: &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
			Workflow:    workflowUpdateOptions.Workflow,
			Cells:       textutil.SimulatedNullStringSlice,
			TabletTypes: []topodatapb.TabletType{topodatapb.TabletType(textutil.SimulatedNullInt)},
			OnDdl:       binlogdatapb.OnDDLAction(textutil.SimulatedNullInt),
			State:       state,
		},
	}

	resp, err := client.WorkflowUpdate(commandCtx, req)
	if err != nil {
		return err
	}

	// Sort the inner TabletInfo slice for deterministic output.
	sort.Slice(resp.Details, func(i, j int) bool {
		return resp.Details[i].Tablet.String() < resp.Details[j].Tablet.String()
	})

	data, err := cli.MarshalJSON(resp)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

func init() {
	GetWorkflows.Flags().BoolVarP(&getWorkflowsOptions.ShowAll, "show-all", "a", false, "Show all workflows instead of just active workflows.")
	Root.AddCommand(GetWorkflows)

	Workflow.PersistentFlags().StringVarP(&workflowOptions.Keyspace, "keyspace", "k", "", "Keyspace context for the workflow (required)")
	Workflow.MarkPersistentFlagRequired("keyspace")
	Root.AddCommand(Workflow)

	WorkflowDelete.Flags().StringVarP(&workflowDeleteOptions.Workflow, "workflow", "w", "", "The workflow you want to delete (required)")
	WorkflowDelete.MarkFlagRequired("workflow")
	WorkflowDelete.Flags().BoolVar(&workflowDeleteOptions.KeepData, "keep-data", false, "Keep the partially copied table data from the workflow in the target keyspace")
	WorkflowDelete.Flags().BoolVar(&workflowDeleteOptions.KeepRoutingRules, "keep-routing-rules", false, "Keep the routing rules created for the workflow")
	Workflow.AddCommand(WorkflowDelete)

	Workflow.AddCommand(WorkflowList)

	WorkflowShow.Flags().StringVarP(&workflowDeleteOptions.Workflow, "workflow", "w", "", "The workflow you want the details for (required)")
	WorkflowShow.MarkFlagRequired("workflow")
	Workflow.AddCommand(WorkflowShow)

	WorkflowStart.Flags().StringVarP(&workflowUpdateOptions.Workflow, "workflow", "w", "", "The workflow you want to start (required)")
	WorkflowStart.MarkFlagRequired("workflow")
	Workflow.AddCommand(WorkflowStart)

	WorkflowStop.Flags().StringVarP(&workflowUpdateOptions.Workflow, "workflow", "w", "", "The workflow you want to stop (required)")
	WorkflowStop.MarkFlagRequired("workflow")
	Workflow.AddCommand(WorkflowStop)

	WorkflowUpdate.Flags().StringVarP(&workflowUpdateOptions.Workflow, "workflow", "w", "", "The workflow you want to update (required)")
	WorkflowUpdate.MarkFlagRequired("workflow")
	WorkflowUpdate.Flags().StringSliceVarP(&workflowUpdateOptions.Cells, "cells", "c", nil, "New Cell(s) or CellAlias(es) (comma-separated) to replicate from")
	WorkflowUpdate.Flags().VarP((*topoproto.TabletTypeListFlag)(&workflowUpdateOptions.TabletTypes), "tablet-types", "t", "New source tablet types to replicate from (e.g. PRIMARY,REPLICA,RDONLY)")
	WorkflowUpdate.Flags().BoolVar(&workflowUpdateOptions.TabletTypesInPreferenceOrder, "tablet-types-in-order", true, "When performing source tablet selection, look for candidates in the type order as they are listed in the tablet-types flag")
	WorkflowUpdate.Flags().StringVar(&workflowUpdateOptions.OnDDL, "on-ddl", "", "New instruction on what to do when DDL is encountered in the VReplication stream. Possible values are IGNORE, STOP, EXEC, and EXEC_IGNORE")
	Workflow.AddCommand(WorkflowUpdate)
}
