package workflow

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	common "vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	// WorkflowList makes a GetWorkflows gRPC call to a vtctld.
	workflowList = &cobra.Command{
		Use:                   "list",
		Short:                 "List the VReplication workflows in the given keyspace.",
		Example:               `vtctldclient --server localhost:15999 workflow --keyspace customer list`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"List"},
		Args:                  cobra.NoArgs,
		RunE:                  commandWorkflowShow,
	}

	// WorkflowShow makes a GetWorkflows gRPC call to a vtctld.
	workflowShow = &cobra.Command{
		Use:                   "show",
		Short:                 "Show the details for a VReplication workflow.",
		Example:               `vtctldclient --server localhost:15999 workflow --keyspace customer show --workflow commerce2customer`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Show"},
		Args:                  cobra.NoArgs,
		RunE:                  commandWorkflowShow,
	}
)

func commandWorkflowShow(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	req := &vtctldatapb.GetWorkflowsRequest{
		Keyspace: workflowOptions.Keyspace,
		Workflow: workflowDeleteOptions.Workflow,
	}
	resp, err := common.GetClient().GetWorkflows(common.GetCommandCtx(), req)
	if err != nil {
		return err
	}

	var data []byte
	if strings.ToLower(cmd.Name()) == "list" {
		// We only want the names
		Names := make([]string, len(resp.Workflows))
		for i, wf := range resp.Workflows {
			Names[i] = wf.Name
		}
		data, err = cli.MarshalJSON(Names)
	} else {
		data, err = cli.MarshalJSON(resp)
	}
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", data)

	return nil
}

func addWorkflowShowFlags(cmd *cobra.Command) {
	workflowShow.Flags().StringVarP(&workflowDeleteOptions.Workflow, "workflow", "w", "", "The workflow you want the details for (required)")
	workflowShow.MarkFlagRequired("workflow")
}
