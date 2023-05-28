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
		Use:                   "workflow",
		Short:                 "Administer VReplication workflows (Reshard, MoveTables, etc) in the given keyspace",
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Workflow"},
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandGetWorkflows,
	}

	// WorkflowUpdate makes a WorkflowUpdate gRPC call to a vtctld.
	WorkflowUpdate = &cobra.Command{
		Use:                   "update",
		Short:                 "Update the configuration parameters for a VReplication workflow",
		Example:               `vtctldclient --server=localhost:15999 workflow --keyspace=customer update --workflow=commerce2customer --cells "zone1" --cells "zone2" -c "zone3,zone4" -c "zone5"`,
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
			if cmd.Flags().Lookup("tablet-types").Changed { // Validate the provided value(s)
				changes = true
				for i, tabletType := range workflowUpdateOptions.TabletTypes {
					workflowUpdateOptions.TabletTypes[i] = strings.ToUpper(strings.TrimSpace(tabletType))
					if _, err := topoproto.ParseTabletType(workflowUpdateOptions.TabletTypes[i]); err != nil {
						return err
					}
				}
			} else {
				workflowUpdateOptions.TabletTypes = textutil.SimulatedNullStringSlice
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
	workflowUpdateOptions = struct {
		Workflow    string
		Cells       []string
		TabletTypes []string
		OnDDL       string
	}{}
)

func commandWorkflowUpdate(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	// We've already validated any provided value, if one WAS provided.
	// Now we need to do the mapping from the string representation to
	// the enum value.
	onddl := int32(textutil.SimulatedNullInt) // Simulated NULL when no value provided
	if val, ok := binlogdatapb.OnDDLAction_value[strings.ToUpper(workflowUpdateOptions.OnDDL)]; ok {
		onddl = val
	}

	req := &vtctldatapb.WorkflowUpdateRequest{
		Keyspace: workflowOptions.Keyspace,
		TabletRequest: &tabletmanagerdatapb.UpdateVRWorkflowRequest{
			Workflow:    workflowUpdateOptions.Workflow,
			Cells:       workflowUpdateOptions.Cells,
			TabletTypes: workflowUpdateOptions.TabletTypes,
			OnDdl:       binlogdatapb.OnDDLAction(onddl),
		},
	}

	resp, err := client.WorkflowUpdate(commandCtx, req)
	if err != nil {
		return err
	}

	// Sort the inner TabletInfo slice for deterministic output.
	sort.Slice(resp.Details, func(i, j int) bool {
		return resp.Details[i].Tablet < resp.Details[j].Tablet
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
	WorkflowUpdate.Flags().StringVarP(&workflowUpdateOptions.Workflow, "workflow", "w", "", "The workflow you want to update (required)")
	WorkflowUpdate.MarkFlagRequired("workflow")
	WorkflowUpdate.Flags().StringSliceVarP(&workflowUpdateOptions.Cells, "cells", "c", nil, "New Cell(s) or CellAlias(es) (comma-separated) to replicate from")
	WorkflowUpdate.Flags().StringSliceVarP(&workflowUpdateOptions.TabletTypes, "tablet-types", "t", nil, "New source tablet types to replicate from (e.g. PRIMARY,REPLICA,RDONLY)")
	WorkflowUpdate.Flags().StringVar(&workflowUpdateOptions.OnDDL, "on-ddl", "", "New instruction on what to do when DDL is encountered in the VReplication stream. Possible values are IGNORE, STOP, EXEC, and EXEC_IGNORE")
	Workflow.AddCommand(WorkflowUpdate)
}
