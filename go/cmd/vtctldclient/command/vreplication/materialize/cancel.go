/*
Copyright 2026 The Vitess Authors.

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

package materialize

import (
	"bytes"
	"errors"
	"fmt"
	"sort"

	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// Materialize does not share the generic cancel command because it needs a
// different keep-data default. A Materialize workflow's target tables are
// user data that the workflow populates but does not own, so cancelling must
// preserve them unless the caller explicitly asks otherwise. The generic
// command omits keep_data when the flag is not set, which the server resolves
// to false and then drops the target tables.
var cancelOptions = struct {
	KeepData bool
}{}

var cancel = &cobra.Command{
	Use:                   "cancel",
	Short:                 "Cancel a Materialize VReplication workflow.",
	Example:               `vtctldclient --server localhost:15999 Materialize --workflow product_sales --target-keyspace customer cancel`,
	DisableFlagsInUseLine: true,
	Aliases:               []string{"Cancel"},
	Args:                  cobra.NoArgs,
	RunE:                  commandCancel,
}

// buildCancelRequest always populates keep_data so the server never has to
// infer intent from an omitted field. Omission is what causes the target
// tables to be dropped, and Materialize has no way to express "keep" through
// the generic cancel command.
func buildCancelRequest() *vtctldatapb.WorkflowDeleteRequest {
	keepData := cancelOptions.KeepData
	return &vtctldatapb.WorkflowDeleteRequest{
		Keyspace: common.BaseOptions.TargetKeyspace,
		Workflow: common.BaseOptions.Workflow,
		KeepData: &keepData,
	}
}

func commandCancel(cmd *cobra.Command, args []string) error {
	format, err := common.GetOutputFormat(cmd)
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	req := buildCancelRequest()
	resp, err := common.GetClient().WorkflowDelete(common.GetCommandCtx(), req)
	if err != nil {
		if grpcerr, ok := status.FromError(err); ok && (grpcerr.Code() == codes.DeadlineExceeded) {
			return errors.New("Cancel action timed out. Please try again and the work will pick back up where it left off. Note that you can control the timeout using the --action-timeout flag.")
		}
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
		tout := bytes.Buffer{}
		common.AppendWarnings(&tout, resp.Warnings)
		tout.WriteString(resp.Summary)
		output = tout.Bytes()
	}
	fmt.Printf("%s\n", output)

	return nil
}
