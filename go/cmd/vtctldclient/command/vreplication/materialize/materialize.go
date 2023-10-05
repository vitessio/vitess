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

package materialize

import (
	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"
)

var (
	// materialize is the base command for all actions related to Materialize.
	materialize = &cobra.Command{
		Use:                   "Materialize --workflow <workflow> --target-keyspace <keyspace> [command] [command-flags]",
		Short:                 "Perform commands related to materializing query results from the source keyspace into tables in the target keyspace.",
		DisableFlagsInUseLine: true,
		Aliases:               []string{"materialize"},
		Args:                  cobra.ExactArgs(1),
	}
)

func registerCommands(root *cobra.Command) {
	common.AddCommonFlags(materialize)
	root.AddCommand(materialize)

	common.AddCommonCreateFlags(create)
	create.Flags().StringVar(&createOptions.SourceKeyspace, "source-keyspace", "", "Keyspace where the tables are being moved from.")
	create.MarkFlagRequired("source-keyspace")
	create.Flags().Var(&createOptions.TableSettings, "table-settings", "A JSON array where each value must contain two key/value pairs. The first key is 'target_table' and it is the name of the table in the target-keyspace to store the results in. The second key is 'source_expression' and its value is the select to run against the source table. An optional k/v pair can be specified for 'create_ddl' which provides the DDL to create the target table if it does not exist.")
	create.MarkFlagRequired("table-settings")
	root.AddCommand(create)

	// Generic workflow commands.
	opts := &common.SubCommandsOpts{
		SubCommand: "Materialize",
		Workflow:   "product_sales",
	}
	materialize.AddCommand(common.GetCancelCommand(opts))
	materialize.AddCommand(common.GetShowCommand(opts))
	materialize.AddCommand(common.GetStartCommand(opts))
	materialize.AddCommand(common.GetStopCommand(opts))
}

func init() {
	common.RegisterCommandHandler("Materialize", registerCommands)
}
