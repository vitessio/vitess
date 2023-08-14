package common

import (
	"bytes"
	"fmt"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var completeOptions = struct {
	KeepData         bool
	KeepRoutingRules bool
	RenameTables     bool
	DryRun           bool
}{}

func GetCompleteCommand(opts *SubCommandsOpts) *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "complete",
		Short:                 "Complete a MoveTables VReplication workflow.",
		Example:               `vtctldclient --server localhost:15999 movetables --workflow commerce2customer --target-keyspace customer complete`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Complete"},
		Args:                  cobra.NoArgs,
		RunE:                  commandComplete,
	}
	return cmd
}

func commandComplete(cmd *cobra.Command, args []string) error {
	format, err := ParseAndValidateFormat(cmd, &CommonVROptions.VrCommonOptions)
	if err != nil {
		return err
	}
	cli.FinishedParsing(cmd)

	req := &vtctldatapb.MoveTablesCompleteRequest{
		Workflow:         CommonVROptions.Workflow,
		TargetKeyspace:   CommonVROptions.TargetKeyspace,
		KeepData:         completeOptions.KeepData,
		KeepRoutingRules: completeOptions.KeepRoutingRules,
		RenameTables:     completeOptions.RenameTables,
		DryRun:           completeOptions.DryRun,
	}
	resp, err := GetClient().MoveTablesComplete(GetCommandCtx(), req)
	if err != nil {
		return err
	}

	var output []byte
	if format == "json" {
		output, err = cli.MarshalJSONCompact(resp)
		if err != nil {
			return err
		}
	} else {
		tout := bytes.Buffer{}
		tout.WriteString(resp.Summary + "\n")
		if len(resp.DryRunResults) > 0 {
			tout.WriteString("\n")
			for _, r := range resp.DryRunResults {
				tout.WriteString(r + "\n")
			}
		}
		output = tout.Bytes()
	}
	fmt.Printf("%s\n", output)

	return nil
}
