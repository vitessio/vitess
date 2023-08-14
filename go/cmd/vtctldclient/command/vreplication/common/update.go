package common

import (
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/textutil"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

func bridgeToWorkflow(cmd *cobra.Command, args []string) {
	workflowUpdateOptions.Workflow = CommonVROptions.Workflow
	workflowOptions.Keyspace = CommonVROptions.TargetKeyspace
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

func commandUpdateState(cmd *cobra.Command, args []string) error {
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

	resp, err := GetClient().WorkflowUpdate(GetCommandCtx(), req)
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
