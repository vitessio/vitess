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
	"strings"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/textutil"

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

	getWorkflowsOptions = struct {
		ShowAll bool
	}{}
)

var (
	Workflow = &cobra.Command{
		Use:                   "Workflow",
		Short:                 "Interface with vreplication workflows (Reshard, MoveTables, etc) in the given keyspace",
		DisableFlagsInUseLine: true,
		Aliases:               []string{"workflow", "workflow", "Workflows"},
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandGetWorkflows,
	}
	workflowOptions = struct {
		Keyspace string
	}{}

	// WorkflowUpdate makes a WorkflowUpdate gRPC call to a vtctld.
	WorkflowUpdate = &cobra.Command{
		Use:                   "update",
		Short:                 "Update the configuration parameters for a vreplication workflow",
		Example:               "vtctldclient --server=localhost:15999 Workflow --keyspace=customer update --workflow=commerce2customer --cells=zone1,zone2,zone3",
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Update"},
		Args:                  cobra.ExactArgs(0),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if !cmd.Flags().Lookup("cells").Changed && !cmd.Flags().Lookup("tablet-types").Changed && !cmd.Flags().Lookup("on-ddl").Changed {
				return fmt.Errorf("no configuration options specified to update")
			}
			return nil
		},
		RunE: commandWorkflowUpdate,
	}

	workflowUpdateOptions = struct {
		Workflow    string
		Cells       []string
		TabletTypes []string
		OnDDL       string
	}{}
)

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

func commandWorkflowUpdate(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	if !cmd.Flags().Lookup("cells").Changed {
		workflowUpdateOptions.Cells = textutil.SimulatedNullStringSlice
	}
	if !cmd.Flags().Lookup("tablet-types").Changed {
		workflowUpdateOptions.TabletTypes = textutil.SimulatedNullStringSlice
	} else { // Validate the provided value
		for _, tabletType := range workflowUpdateOptions.TabletTypes {
			if _, ok := topodatapb.TabletType_value[strings.ToUpper(strings.TrimSpace(tabletType))]; !ok {
				return fmt.Errorf("invalid tablet type: %s", tabletType)
			}
		}
	}
	onddl := int32(textutil.SimulatedNullInt)
	var valid bool
	if !cmd.Flags().Lookup("on-ddl").Changed {
	} else { // Validate the provided value
		onddl, valid = binlogdatapb.OnDDLAction_value[strings.ToUpper(workflowUpdateOptions.OnDDL)]
		if !valid {
			return fmt.Errorf("invalid on-ddl value: %s", workflowUpdateOptions.OnDDL)
		}
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

	fmt.Printf("%s\n", resp.Results)

	return nil
}

func init() {
	GetWorkflows.Flags().BoolVarP(&getWorkflowsOptions.ShowAll, "show-all", "a", false, "Show all workflows instead of just active workflows.")
	Root.AddCommand(GetWorkflows)

	Workflow.PersistentFlags().StringVarP(&workflowOptions.Keyspace, "keyspace", "k", "", "Keyspace context for the workflow (required)")
	_ = Workflow.MarkPersistentFlagRequired("keyspace")
	Root.AddCommand(Workflow)

	WorkflowUpdate.Flags().StringVarP(&workflowUpdateOptions.Workflow, "workflow", "w", "", "The workflow you want to update (required)")
	_ = WorkflowUpdate.MarkFlagRequired("workflow")
	WorkflowUpdate.Flags().StringSliceVarP(&workflowUpdateOptions.Cells, "cells", "c", []string{}, "New Cell(s) or CellAlias(es) (comma-separated) to replicate from")
	WorkflowUpdate.Flags().StringSliceVarP(&workflowUpdateOptions.TabletTypes, "tablet-types", "t", []string{}, "New source tablet types to replicate from (e.g. PRIMARY, REPLICA, RDONLY)")
	WorkflowUpdate.Flags().StringVarP(&workflowUpdateOptions.OnDDL, "on-ddl", "o", "", "New instruction on what to do when DDL is encountered in the VReplication stream. Possible values are IGNORE, STOP, EXEC, and EXEC_IGNORE")
	Workflow.AddCommand(WorkflowUpdate)
}
