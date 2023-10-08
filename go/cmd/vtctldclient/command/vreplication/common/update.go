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

package common

import (
	"fmt"
	"sort"
	"strings"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/textutil"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

func bridgeToWorkflow(cmd *cobra.Command, args []string) {
	workflowUpdateOptions.Workflow = BaseOptions.Workflow
	workflowOptions.Keyspace = BaseOptions.TargetKeyspace
}

var (
	workflowOptions = struct {
		Keyspace string
	}{}

	workflowUpdateOptions = struct {
		Workflow                     string
		Cells                        []string
		TabletTypes                  []topodatapb.TabletType
		TabletTypesInPreferenceOrder bool
		OnDDL                        string
	}{}
)

func GetStartCommand(opts *SubCommandsOpts) *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "start",
		Short:                 fmt.Sprintf("Start a %s workflow.", opts.SubCommand),
		Example:               fmt.Sprintf(`vtctldclient --server localhost:15999 %s --workflow %s --target-keyspace customer start`, opts.SubCommand, opts.Workflow),
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Start"},
		Args:                  cobra.NoArgs,
		PreRun:                bridgeToWorkflow,
		RunE:                  commandUpdateState,
	}
	return cmd
}

func GetStopCommand(opts *SubCommandsOpts) *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "stop",
		Short:                 fmt.Sprintf("Stop a %s workflow.", opts.SubCommand),
		Example:               fmt.Sprintf(`vtctldclient --server localhost:15999 %s --workflow %s --target-keyspace customer stop`, opts.SubCommand, opts.Workflow),
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Stop"},
		Args:                  cobra.NoArgs,
		PreRun:                bridgeToWorkflow,
		RunE:                  commandUpdateState,
	}
	return cmd
}

func getWorkflow(keyspace, workflow string) (*vtctldatapb.GetWorkflowsResponse, error) {
	resp, err := GetClient().GetWorkflows(GetCommandCtx(), &vtctldatapb.GetWorkflowsRequest{
		Keyspace: keyspace,
		Workflow: workflow,
	})
	if err != nil {
		return &vtctldatapb.GetWorkflowsResponse{}, err
	}
	return resp, nil
}

// CanRestartWorkflow validates that, for an atomic copy workflow, none of the streams are still in the copy phase.
// Since we copy all tables in a single snapshot, we cannot restart a workflow which broke before all tables were copied.
func CanRestartWorkflow(keyspace, workflow string) error {
	resp, err := getWorkflow(keyspace, workflow)
	if err != nil {
		return err
	}
	if len(resp.Workflows) == 0 {
		return fmt.Errorf("workflow %s not found", workflow)
	}
	if len(resp.Workflows) > 1 {
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "multiple results found for workflow %s", workflow)
	}
	wf := resp.Workflows[0]
	if wf.WorkflowSubType != binlogdatapb.VReplicationWorkflowSubType_AtomicCopy.String() {
		return nil
	}
	// If we're here, we have an atomic copy workflow.
	for _, shardStream := range wf.ShardStreams {
		for _, stream := range shardStream.Streams {
			if len(stream.CopyStates) > 0 {
				return fmt.Errorf("stream %d is still in the copy phase: can only start workflow %s if all streams have completed the copy phase", stream.Id, workflow)
			}
		}
	}
	return nil
}

func commandUpdateState(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	var state binlogdatapb.VReplicationWorkflowState
	switch strings.ToLower(cmd.Name()) {
	case "start":
		if err := CanRestartWorkflow(workflowOptions.Keyspace, workflowUpdateOptions.Workflow); err != nil {
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
		Keyspace: workflowOptions.Keyspace,
		TabletRequest: &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
			Workflow:    workflowUpdateOptions.Workflow,
			Cells:       textutil.SimulatedNullStringSlice,
			TabletTypes: []topodatapb.TabletType{topodatapb.TabletType(textutil.SimulatedNullInt)},
			OnDdl:       binlogdatapb.OnDDLAction(textutil.SimulatedNullInt),
			State:       state,
		},
	}

	resp, err := GetClient().WorkflowUpdate(GetCommandCtx(), req)
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
