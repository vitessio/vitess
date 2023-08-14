package workflow

import (
	"fmt"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	common "vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	getWorkflowsOptions = struct {
		ShowAll bool
	}{}
	// GetWorkflows makes a GetWorkflows gRPC call to a vtctld.
	getWorkflows = &cobra.Command{
		Use:                   "GetWorkflows <keyspace>",
		Short:                 "Gets all vreplication workflows (Reshard, MoveTables, etc) in the given keyspace.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandGetWorkflows,
	}
)

func commandGetWorkflows(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	ks := cmd.Flags().Arg(0)

	resp, err := common.GetClient().GetWorkflows(common.GetCommandCtx(), &vtctldatapb.GetWorkflowsRequest{
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

func addGetWorkflowsFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVarP(&getWorkflowsOptions.ShowAll, "show-all", "a", false, "Show all workflows instead of just active workflows.")
}
