package common

import (
	"fmt"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

func GetStatusCommand(opts *SubCommandsOpts) *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "status",
		Short:                 fmt.Sprintf("Show the current status for a %s VReplication workflow.", opts.SubCommand),
		Example:               fmt.Sprintf(`vtctldclient --server localhost:15999 %s --workflow %s --target-keyspace customer status`, opts.SubCommand, opts.Workflow),
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Status", "progress", "Progress"},
		Args:                  cobra.NoArgs,
		RunE:                  commandStatus,
	}
	return cmd
}

func commandStatus(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	req := &vtctldatapb.WorkflowStatusRequest{
		Keyspace: CommonVROptions.TargetKeyspace,
		Workflow: CommonVROptions.Workflow,
	}
	resp, err := GetClient().WorkflowStatus(GetCommandCtx(), req)
	if err != nil {
		return err
	}

	if err = OutputStatusResponse(resp, "json"); err != nil {
		return err
	}

	return nil
}
