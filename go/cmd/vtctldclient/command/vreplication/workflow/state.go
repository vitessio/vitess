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
	// start makes a WorfklowUpdate gRPC call to a vtctld.
	start = &cobra.Command{
		Use:                   "start",
		Short:                 "Start a VReplication workflow.",
		Example:               `vtctldclient --server localhost:15999 workflow --keyspace customer start --workflow commerce2customer`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Start"},
		Args:                  cobra.NoArgs,
		RunE:                  commandUpdateState,
	}

	// stop makes a WorfklowUpdate gRPC call to a vtctld.
	stop = &cobra.Command{
		Use:                   "stop",
		Short:                 "Stop a VReplication workflow.",
		Example:               `vtctldclient --server localhost:15999 workflow --keyspace customer stop --workflow commerce2customer`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Stop"},
		Args:                  cobra.NoArgs,
		RunE:                  commandUpdateState,
	}
)

func commandUpdateState(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	var state binlogdatapb.VReplicationWorkflowState
	switch strings.ToLower(cmd.Name()) {
	case "start":
		if err := common.CanRestartWorkflow(baseOptions.Keyspace, baseOptions.Workflow); err != nil {
			return err
		}
		state = binlogdatapb.VReplicationWorkflowState_Running
	case "stop":
		state = binlogdatapb.VReplicationWorkflowState_Stopped
	default:
		return fmt.Errorf("invalid workflow state: %s", args[0])
	}

	// The only thing we're updating is the state.
	req := &vtctldatapb.WorkflowUpdateRequest{
		Keyspace: baseOptions.Keyspace,
		TabletRequest: &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
			Workflow:    baseOptions.Workflow,
			Cells:       textutil.SimulatedNullStringSlice,
			TabletTypes: []topodatapb.TabletType{topodatapb.TabletType(textutil.SimulatedNullInt)},
			OnDdl:       binlogdatapb.OnDDLAction(textutil.SimulatedNullInt),
			State:       state,
			Shards:      baseOptions.Shards,
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
