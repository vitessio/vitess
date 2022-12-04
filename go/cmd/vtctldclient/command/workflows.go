/*
Copyright 2021 The Vitess Authors.

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

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	// GetWorkflows makes a GetWorkflows gRPC call to a vtctld.
	GetWorkflows = &cobra.Command{
		Use:                   "GetWorkflows <keyspace>",
		Short:                 "Gets all vreplication workflows (Reshard, MoveTables, etc) in the given keyspace.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandGetWorkflows,
	}

	// Materialize makes a Materialize gRPC call to a vtctld.
	Materialize = &cobra.Command{
		Use:                   "Materialize [--cells=<cells>] [--tablet_types=<source_tablet_types>] <json_spec>",
		Short:                 "Creates and starts a Materialize workflow based on the provided materialize spec.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandMaterialize,
		Example:               `Materialize '{"workflow": "aaa", "source_keyspace": "source", "target_keyspace": "target", "table_settings": [{"target_table": "customer", "source_expression": "select * from customer", "create_ddl": "copy"}]}'`,
	}
)

var getWorkflowsOptions = struct {
	ShowAll bool
}{}

func commandGetWorkflows(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	ks := cmd.Flags().Arg(0)

	resp, err := client.GetWorkflows(commandCtx, &vtctldatapb.GetWorkflowsRequest{
		Keyspace:   ks,
		ActiveOnly: !getWorkflowsOptions.ShowAll,
	})

	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

var materializeOptions = struct {
	Cells       string
	TabletTypes string
}{}

func commandMaterialize(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	materializeSpec := cmd.Flags().Arg(0)

	resp, err := client.Materialize(commandCtx, &vtctldatapb.MaterializeRequest{
		Cells:           materializeOptions.Cells,
		TabletTypes:     materializeOptions.TabletTypes,
		MaterializeSpec: materializeSpec,
	})

	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}
func init() {
	GetWorkflows.Flags().BoolVarP(&getWorkflowsOptions.ShowAll, "show-all", "a", false, "Show all workflows instead of just active workflows.")
	Root.AddCommand(GetWorkflows)

	Materialize.Flags().StringVarP(&materializeOptions.Cells, "cells", "c", "", "Choose tablets from these cells to source from")
	Materialize.Flags().StringVarP(&materializeOptions.TabletTypes, "tablet_types", "t", "", "Choose tablets of these types to source from")
	Root.AddCommand(Materialize)
}
