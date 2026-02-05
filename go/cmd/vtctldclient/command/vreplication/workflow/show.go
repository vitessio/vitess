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
	"strings"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"

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
		RunE:                  commandShow,
	}

	// show makes a GetWorkflows gRPC call to a vtctld.
	show = &cobra.Command{
		Use:                   "show",
		Short:                 "Show the details for a VReplication workflow.",
		Example:               `vtctldclient --server localhost:15999 workflow --keyspace customer show --workflow commerce2customer`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Show"},
		Args:                  cobra.NoArgs,
		RunE:                  commandShow,
	}
)

func commandShow(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	req := &vtctldatapb.GetWorkflowsRequest{
		Keyspace:    baseOptions.Keyspace,
		Workflow:    baseOptions.Workflow,
		IncludeLogs: workflowShowOptions.IncludeLogs,
		Shards:      baseOptions.Shards,
	}
	resp, err := common.GetClient().GetWorkflows(common.GetCommandCtx(), req)
	if err != nil {
		return err
	}

	var data []byte
	if strings.ToLower(cmd.Name()) == "list" {
		// We only want the names.
		Names := make([]string, len(resp.Workflows))
		for i, wf := range resp.Workflows {
			Names[i] = wf.Name
		}
		data, err = cli.MarshalJSONPretty(Names)
	} else {
		data, err = cli.MarshalJSONPretty(resp)
	}
	if err != nil {
		return err
	}
	fmt.Println(string(data))

	return nil
}
