package common

import (
	"fmt"
	"sort"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var cancelOptions = struct {
	KeepData         bool
	KeepRoutingRules bool
}{}

func GetCancelCommand(opts *SubCommandsOpts) *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "cancel",
		Short:                 fmt.Sprintf("Cancel a %s VReplication workflow.", opts.SubCommand),
		Example:               fmt.Sprintf(`vtctldclient --server localhost:15999 %s --workflow %s --target-keyspace customer cancel`, opts.SubCommand, opts.Workflow),
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Cancel"},
		Args:                  cobra.NoArgs,
		RunE:                  commandCancel,
	}
	return cmd
}

func commandCancel(cmd *cobra.Command, args []string) error {
	format, err := ParseAndValidateFormat(cmd, &CommonVROptions.VrCommonOptions)
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	req := &vtctldatapb.WorkflowDeleteRequest{
		Keyspace:         CommonVROptions.TargetKeyspace,
		Workflow:         CommonVROptions.Workflow,
		KeepData:         cancelOptions.KeepData,
		KeepRoutingRules: cancelOptions.KeepRoutingRules,
	}
	resp, err := GetClient().WorkflowDelete(GetCommandCtx(), req)
	if err != nil {
		return err
	}

	var output []byte
	if format == "json" {
		// Sort the inner TabletInfo slice for deterministic output.
		sort.Slice(resp.Details, func(i, j int) bool {
			return resp.Details[i].Tablet.String() < resp.Details[j].Tablet.String()
		})
		output, err = cli.MarshalJSONCompact(resp)
		if err != nil {
			return err
		}
	} else {
		output = []byte(resp.Summary + "\n")
	}
	fmt.Printf("%s\n", output)

	return nil
}
