package workflow

import (
	"fmt"
	"sort"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	common "vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	workflowDeleteOptions = struct {
		Workflow         string
		KeepData         bool
		KeepRoutingRules bool
	}{}

	// WorkflowDelete makes a WorkflowDelete gRPC call to a vtctld.
	workflowDelete = &cobra.Command{
		Use:                   "delete",
		Short:                 "Delete a VReplication workflow.",
		Example:               `vtctldclient --server localhost:15999 workflow --keyspace customer delete --workflow commerce2customer`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Delete"},
		Args:                  cobra.NoArgs,
		RunE:                  commandWorkflowDelete,
	}
)

func commandWorkflowDelete(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	req := &vtctldatapb.WorkflowDeleteRequest{
		Keyspace:         workflowOptions.Keyspace,
		Workflow:         workflowDeleteOptions.Workflow,
		KeepData:         workflowDeleteOptions.KeepData,
		KeepRoutingRules: workflowDeleteOptions.KeepRoutingRules,
	}
	resp, err := common.GetClient().WorkflowDelete(common.GetCommandCtx(), req)
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

func addWorkflowDeleteFlags(cmd *cobra.Command) {
	workflowDelete.Flags().StringVarP(&workflowDeleteOptions.Workflow, "workflow", "w", "", "The workflow you want to delete (required)")
	workflowDelete.MarkFlagRequired("workflow")
	workflowDelete.Flags().BoolVar(&workflowDeleteOptions.KeepData, "keep-data", false, "Keep the partially copied table data from the workflow in the target keyspace")
	workflowDelete.Flags().BoolVar(&workflowDeleteOptions.KeepRoutingRules, "keep-routing-rules", false, "Keep the routing rules created for the workflow")

}
