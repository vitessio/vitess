package common

import (
	"bytes"
	"fmt"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var CompleteOptions = struct {
	KeepData         bool
	KeepRoutingRules bool
	RenameTables     bool
	DryRun           bool
	Shards           []string
}{}

func GetCompleteCommand(opts *SubCommandsOpts) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "complete",
		Short: fmt.Sprintf("Complete a %s VReplication workflow.", opts.SubCommand),
		Example: fmt.Sprintf(`vtctldclient --server localhost:15999 %s --workflow %s --target-keyspace customer complete`,
			opts.SubCommand, opts.Workflow),
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Complete"},
		Args:                  cobra.NoArgs,
		RunE:                  commandComplete,
	}
	return cmd
}

func commandComplete(cmd *cobra.Command, args []string) error {
	format, err := GetOutputFormat(cmd)
	if err != nil {
		return err
	}
	cli.FinishedParsing(cmd)

	req := &vtctldatapb.MoveTablesCompleteRequest{
		Workflow:         BaseOptions.Workflow,
		TargetKeyspace:   BaseOptions.TargetKeyspace,
		KeepData:         CompleteOptions.KeepData,
		KeepRoutingRules: CompleteOptions.KeepRoutingRules,
		RenameTables:     CompleteOptions.RenameTables,
		DryRun:           CompleteOptions.DryRun,
	}
	resp, err := GetClient().MoveTablesComplete(GetCommandCtx(), req)
	if err != nil {
		return err
	}

	var output []byte
	if format == "json" {
		output, err = cli.MarshalJSONPretty(resp)
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
	fmt.Println(string(output))

	return nil
}
