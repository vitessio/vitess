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

package workflow

import (
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vt/topo/topoproto"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	// WorkflowUpdate makes a WorkflowUpdate gRPC call to a vtctld.
	workflowUpdate = &cobra.Command{
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

	resp, err := common.GetClient().WorkflowUpdate(common.GetCommandCtx(), req)
	if err != nil {
		return err
	}

	// Sort the inner TabletInfo slice for deterministic output.
	sort.Slice(resp.Details, func(i, j int) bool {
		return resp.Details[i].Tablet.String() < resp.Details[j].Tablet.String()
	})

	data, err := cli.MarshalJSONPretty(resp)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

func addWorkflowUpdateFlags(cmd *cobra.Command) {
	workflowUpdate.Flags().StringVarP(&workflowUpdateOptions.Workflow, "workflow", "w", "", "The workflow you want to update (required).")
	workflowUpdate.MarkFlagRequired("workflow")
	workflowUpdate.Flags().StringSliceVarP(&workflowUpdateOptions.Cells, "cells", "c", nil, "New Cell(s) or CellAlias(es) (comma-separated) to replicate from.")
	workflowUpdate.Flags().VarP((*topoproto.TabletTypeListFlag)(&workflowUpdateOptions.TabletTypes), "tablet-types", "t", "New source tablet types to replicate from (e.g. PRIMARY,REPLICA,RDONLY).")
	workflowUpdate.Flags().BoolVar(&workflowUpdateOptions.TabletTypesInPreferenceOrder, "tablet-types-in-order", true, "When performing source tablet selection, look for candidates in the type order as they are listed in the tablet-types flag.")
	workflowUpdate.Flags().StringVar(&workflowUpdateOptions.OnDDL, "on-ddl", "", "New instruction on what to do when DDL is encountered in the VReplication stream. Possible values are IGNORE, STOP, EXEC, and EXEC_IGNORE.")

}
