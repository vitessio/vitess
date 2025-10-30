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
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"
	"vitess.io/vitess/go/mysql/config"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/utils"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	updateOptions = struct {
		AddReferenceTables []string
	}{}

	// base is the base command for all actions related to Materialize.
	base = &cobra.Command{
		Use:                   "Materialize --workflow <workflow> --target-keyspace <keyspace> [command] [command-flags]",
		Short:                 "Perform commands related to materializing query results from the source keyspace into tables in the target keyspace.",
		DisableFlagsInUseLine: true,
		Aliases:               []string{"materialize"},
		Args:                  cobra.ExactArgs(1),
	}

	// update is the command for updating existing materialize workflow.
	// This can be helpful if we plan to add other actions as well such as
	// removing tables from workflow.
	update = &cobra.Command{
		Use:     "update --add-tables='table1,table2'",
		Short:   "Update existing materialize workflow.",
		Aliases: []string{"Update"},
		Args:    cobra.NoArgs,
		RunE:    commandUpdate,
	}
)

func commandUpdate(cmd *cobra.Command, args []string) error {
	tableSettings := []*vtctldatapb.TableMaterializeSettings{}
	for _, table := range updateOptions.AddReferenceTables {
		tableSettings = append(tableSettings, &vtctldatapb.TableMaterializeSettings{
			TargetTable: table,
		})
	}

	_, err := common.GetClient().WorkflowAddTables(common.GetCommandCtx(), &vtctldatapb.WorkflowAddTablesRequest{
		Workflow:              common.BaseOptions.Workflow,
		Keyspace:              common.BaseOptions.TargetKeyspace,
		TableSettings:         tableSettings,
		MaterializationIntent: vtctldatapb.MaterializationIntent_REFERENCE,
	})

	if err != nil {
		return err
	}
	fmt.Printf("Table(s) %s added to the workflow %s. Use show to view the status.\n",
		strings.Join(updateOptions.AddReferenceTables, ", "), common.BaseOptions.Workflow)
	return nil
}

func registerCommands(root *cobra.Command) {
	common.AddCommonFlags(base)
	root.AddCommand(base)

	create.Flags().StringSliceVarP(&common.CreateOptions.Cells, "cells", "c", nil, "Cells and/or CellAliases to copy table data from.")
	create.Flags().Var((*topoproto.TabletTypeListFlag)(&common.CreateOptions.TabletTypes), "tablet-types", "Source tablet types to replicate table data from (e.g. PRIMARY,REPLICA,RDONLY).")
	create.Flags().BoolVar(&common.CreateOptions.TabletTypesInPreferenceOrder, "tablet-types-in-preference-order", true, "When performing source tablet selection, look for candidates in the type order as they are listed in the tablet-types flag.")
	create.Flags().StringVar(&createOptions.SourceKeyspace, "source-keyspace", "", "Keyspace where the tables queried in the 'source_expression' values within table-settings live.")
	create.MarkFlagRequired("source-keyspace")
	create.Flags().Var(&createOptions.TableSettings, "table-settings", "A JSON array defining what tables to materialize using what select statements. See the --help output for more details.")
	create.Flags().BoolVar(&common.CreateOptions.StopAfterCopy, "stop-after-copy", false, "Stop the workflow after it's finished copying the existing rows and before it starts replicating changes.")
	utils.SetFlagStringVar(create.Flags(), &common.CreateOptions.MySQLServerVersion, "mysql-server-version", config.DefaultMySQLVersion+"-Vitess", "Configure the MySQL version to use for example for the parser.")
	create.Flags().IntVar(&common.CreateOptions.TruncateUILen, "sql-max-length-ui", 512, "truncate queries in debug UIs to the given length (default 512)")
	create.Flags().IntVar(&common.CreateOptions.TruncateErrLen, "sql-max-length-errors", 0, "truncate queries in error logs to the given length (default unlimited)")
	create.Flags().StringSliceVarP(&common.CreateOptions.ReferenceTables, "reference-tables", "r", nil, "Used to specify the reference tables to materialize on every target shard.")
	base.AddCommand(create)

	update.Flags().StringSliceVar(&updateOptions.AddReferenceTables, "add-reference-tables", nil, "Used to specify the reference tables to be added to the existing workflow")
	update.MarkFlagRequired("add-reference-tables")
	base.AddCommand(update)

	// Generic workflow commands.
	opts := &common.SubCommandsOpts{
		SubCommand: "Materialize",
		Workflow:   "product_sales",
	}
	base.AddCommand(common.GetCancelCommand(opts))
	base.AddCommand(common.GetShowCommand(opts))
	base.AddCommand(common.GetStartCommand(opts))
	base.AddCommand(common.GetStopCommand(opts))
}

func init() {
	common.RegisterCommandHandler("Materialize", registerCommands)
}
