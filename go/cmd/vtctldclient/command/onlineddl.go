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
package command

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/vtctl/schematools"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	OnlineDDL = &cobra.Command{
		Use:                   "OnlineDDL <cmd> <keyspace> [args]",
		DisableFlagsInUseLine: true,
		Args:                  cobra.MinimumNArgs(2),
	}
	OnlineDDLShow = &cobra.Command{
		Use:                   "show",
		DisableFlagsInUseLine: true,
		Args:                  cobra.RangeArgs(0, 1),
		RunE:                  commandOnlineDDLShow,
	}
)

var onlineDDLShowArgs = struct {
	JSON     bool
	OrderStr string
	Limit    uint64
	Skip     uint64
}{
	OrderStr: "ascending",
}

func commandOnlineDDLShow(cmd *cobra.Command, args []string) error {
	var order vtctldatapb.QueryOrdering
	switch strings.ToLower(onlineDDLShowArgs.OrderStr) {
	case "":
		order = vtctldatapb.QueryOrdering_NONE
	case "asc", "ascending":
		order = vtctldatapb.QueryOrdering_ASCENDING
	case "desc", "descending":
		order = vtctldatapb.QueryOrdering_DESCENDING
	default:
		return fmt.Errorf("invalid ordering %s (choices are 'asc', 'ascending', 'desc', 'descending')", onlineDDLShowArgs.OrderStr)
	}

	cli.FinishedParsing(cmd)

	req := &vtctldatapb.GetSchemaMigrationsRequest{
		Keyspace: cmd.Flags().Arg(0),
		Order:    order,
		Limit:    onlineDDLShowArgs.Limit,
		Skip:     onlineDDLShowArgs.Skip,
	}

	switch arg := cmd.Flags().Arg(1); arg {
	case "", "all":
	case "recent":
		req.Recent = protoutil.DurationToProto(7 * 24 * time.Hour)
	default:
		if status, err := schematools.ParseSchemaMigrationStatus(arg); err == nil {
			// Argument is a status name.
			req.Status = status
		} else if schema.IsOnlineDDLUUID(arg) {
			req.Uuid = arg
		} else {
			req.MigrationContext = arg
		}
	}

	resp, err := client.GetSchemaMigrations(commandCtx, req)
	if err != nil {
		return err
	}

	switch {
	case onlineDDLShowArgs.JSON:
		data, err := cli.MarshalJSON(resp)
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", data)
	default:
		// TODO: support tabular/textual format
	}
	return nil
}

func init() {
	OnlineDDLShow.Flags().BoolVar(&onlineDDLShowArgs.JSON, "json", false, "TODO")
	OnlineDDLShow.Flags().StringVar(&onlineDDLShowArgs.OrderStr, "order", "asc", "TODO")
	OnlineDDLShow.Flags().Uint64Var(&onlineDDLShowArgs.Limit, "limit", 0, "TODO")
	OnlineDDLShow.Flags().Uint64Var(&onlineDDLShowArgs.Skip, "skip", 0, "TODO")

	OnlineDDL.AddCommand(OnlineDDLShow)
	Root.AddCommand(OnlineDDL)
}
