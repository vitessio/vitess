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

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	updateOptions = struct {
		Cells                        []string
		TabletTypes                  []topodatapb.TabletType
		TabletTypesInPreferenceOrder bool
		OnDDL                        string
	}{}

	// update makes a WorkflowUpdate gRPC call to a vtctld.
	update = &cobra.Command{
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
				for i, cell := range updateOptions.Cells { // Which only means trimming whitespace
					updateOptions.Cells[i] = strings.TrimSpace(cell)
				}
			} else {
				updateOptions.Cells = textutil.SimulatedNullStringSlice
			}
			if cmd.Flags().Lookup("tablet-types").Changed {
				if err := common.ParseTabletTypes(cmd); err != nil {
					return err
				}
				changes = true
			} else {
				updateOptions.TabletTypes = []topodatapb.TabletType{topodatapb.TabletType(textutil.SimulatedNullInt)}
			}
			if cmd.Flags().Lookup("on-ddl").Changed { // Validate the provided value
				changes = true
				if _, ok := binlogdatapb.OnDDLAction_value[strings.ToUpper(updateOptions.OnDDL)]; !ok {
					return fmt.Errorf("invalid on-ddl value: %s", updateOptions.OnDDL)
				}
			} // Simulated NULL will need to be handled in command
			if !changes {
				return fmt.Errorf("no configuration options specified to update")
			}
			return nil
		},
		RunE: commandUpdate,
	}
)

func commandUpdate(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	// We've already validated any provided value, if one WAS provided.
	// Now we need to do the mapping from the string representation to
	// the enum value.
	onddl := int32(textutil.SimulatedNullInt) // Simulated NULL when no value provided
	if val, ok := binlogdatapb.OnDDLAction_value[strings.ToUpper(updateOptions.OnDDL)]; ok {
		onddl = val
	}

	// Simulated NULL when no value is provided.
	tsp := tabletmanagerdatapb.TabletSelectionPreference_UNKNOWN
	if cmd.Flags().Lookup("tablet-types-in-order").Changed {
		if updateOptions.TabletTypesInPreferenceOrder {
			tsp = tabletmanagerdatapb.TabletSelectionPreference_INORDER
		} else {
			tsp = tabletmanagerdatapb.TabletSelectionPreference_ANY
		}
	}

	req := &vtctldatapb.WorkflowUpdateRequest{
		Keyspace: baseOptions.Keyspace,
		TabletRequest: &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
			Workflow:                  baseOptions.Workflow,
			Cells:                     updateOptions.Cells,
			TabletTypes:               updateOptions.TabletTypes,
			TabletSelectionPreference: tsp,
			OnDdl:                     binlogdatapb.OnDDLAction(onddl),
			Shards:                    baseOptions.Shards,
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
