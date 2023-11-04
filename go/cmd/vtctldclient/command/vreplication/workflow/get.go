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

package workflow

import (
	"fmt"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"

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
		Keyspace:    ks,
		ActiveOnly:  !getWorkflowsOptions.ShowAll,
		IncludeLogs: workflowShowOptions.IncludeLogs,
	})

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
