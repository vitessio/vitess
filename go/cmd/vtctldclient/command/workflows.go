package command

import (
	"fmt"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	// GetWorkflows makes a GetWorkflows gRPC call to a vtctld.
	GetWorkflows = &cobra.Command{
		Use:  "GetWorkflows <keyspace>",
		Args: cobra.ExactArgs(1),
		RunE: commandGetWorkflows,
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

func init() {
	GetWorkflows.Flags().BoolVarP(&getWorkflowsOptions.ShowAll, "show-all", "a", false, "Show all workflows instead of just active workflows")
	Root.AddCommand(GetWorkflows)
}
