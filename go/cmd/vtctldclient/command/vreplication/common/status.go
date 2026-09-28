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

var StatusOptions = struct {
	Shards []string
}{}

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
	format, err := GetOutputFormat(cmd)
	if err != nil {
		return err
	}
	cli.FinishedParsing(cmd)

	req := &vtctldatapb.WorkflowStatusRequest{
		Keyspace: BaseOptions.TargetKeyspace,
		Workflow: BaseOptions.Workflow,
		Shards:   StatusOptions.Shards,
	}
	resp, err := GetClient().WorkflowStatus(GetCommandCtx(), req)
	if err != nil {
		return err
	}

	if err = OutputStatusResponse(resp, format); err != nil {
		return err
	}

	return nil
}
