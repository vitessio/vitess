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
	"sort"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var CancelOptions = struct {
	KeepData         bool
	KeepRoutingRules bool
	Shards           []string
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
	format, err := GetOutputFormat(cmd)
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	req := &vtctldatapb.WorkflowDeleteRequest{
		Keyspace:         BaseOptions.TargetKeyspace,
		Workflow:         BaseOptions.Workflow,
		KeepData:         CancelOptions.KeepData,
		KeepRoutingRules: CancelOptions.KeepRoutingRules,
		Shards:           CancelOptions.Shards,
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
		output, err = cli.MarshalJSONPretty(resp)
		if err != nil {
			return err
		}
	} else {
		output = []byte(resp.Summary + "\n")
	}
	fmt.Printf("%s\n", output)

	return nil
}
