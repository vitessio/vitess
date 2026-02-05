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

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var ShowOptions = struct {
	IncludeLogs bool
	Shards      []string
}{}

func GetShowCommand(opts *SubCommandsOpts) *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "show",
		Short:                 fmt.Sprintf("Show the details for a %s VReplication workflow.", opts.SubCommand),
		Example:               fmt.Sprintf(`vtctldclient --server localhost:15999 %s --workflow %s --target-keyspace customer show`, opts.SubCommand, opts.Workflow),
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Show"},
		Args:                  cobra.NoArgs,
		RunE:                  commandShow,
	}
	cmd.Flags().BoolVar(&ShowOptions.IncludeLogs, "include-logs", true, "Include recent logs for the workflow.")
	return cmd
}

func commandShow(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	req := &vtctldatapb.GetWorkflowsRequest{
		Keyspace:    BaseOptions.TargetKeyspace,
		Workflow:    BaseOptions.Workflow,
		IncludeLogs: ShowOptions.IncludeLogs,
		Shards:      ShowOptions.Shards,
	}
	resp, err := GetClient().GetWorkflows(GetCommandCtx(), req)
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSONPretty(resp)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}
