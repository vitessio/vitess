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
	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

var (
	// base is a parent command for Workflow commands.
	base = &cobra.Command{
		Use:                   "Workflow --keyspace <keyspace> [command] [command-flags]",
		Short:                 "Administer VReplication workflows (Reshard, MoveTables, etc) in the given keyspace.",
		DisableFlagsInUseLine: true,
		Aliases:               []string{"workflow"},
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandGetWorkflows,
	}
)

var (
	baseOptions = struct {
		Keyspace string
		Workflow string
		Shards   []string
	}{}

	workflowShowOptions = struct {
		IncludeLogs bool
	}{}
)

func registerCommands(root *cobra.Command) {
	base.PersistentFlags().StringVarP(&baseOptions.Keyspace, "keyspace", "k", "", "Keyspace context for the workflow.")
	base.MarkPersistentFlagRequired("keyspace")
	root.AddCommand(base)

	getWorkflows.Flags().BoolVar(&workflowShowOptions.IncludeLogs, "include-logs", true, "Include recent logs for the workflows.")
	getWorkflows.Flags().BoolVarP(&getWorkflowsOptions.ShowAll, "show-all", "a", false, "Show all workflows instead of just active workflows.")
	root.AddCommand(getWorkflows) // Yes this is supposed to be root as GetWorkflows is a top-level command.

	delete.Flags().StringVarP(&baseOptions.Workflow, "workflow", "w", "", "The workflow you want to delete.")
	delete.MarkFlagRequired("workflow")
	delete.Flags().BoolVar(&deleteOptions.KeepData, "keep-data", false, "Keep the partially copied table data from the workflow in the target keyspace.")
	delete.Flags().BoolVar(&deleteOptions.KeepRoutingRules, "keep-routing-rules", false, "Keep the routing rules created for the workflow.")
	common.AddShardSubsetFlag(delete, &baseOptions.Shards)
	base.AddCommand(delete)

	common.AddShardSubsetFlag(workflowList, &baseOptions.Shards)
	base.AddCommand(workflowList)

	show.Flags().StringVarP(&baseOptions.Workflow, "workflow", "w", "", "The workflow you want the details for.")
	show.MarkFlagRequired("workflow")
	show.Flags().BoolVar(&workflowShowOptions.IncludeLogs, "include-logs", true, "Include recent logs for the workflow.")
	common.AddShardSubsetFlag(show, &baseOptions.Shards)
	base.AddCommand(show)

	start.Flags().StringVarP(&baseOptions.Workflow, "workflow", "w", "", "The workflow you want to start.")
	start.MarkFlagRequired("workflow")
	common.AddShardSubsetFlag(start, &baseOptions.Shards)
	base.AddCommand(start)

	stop.Flags().StringVarP(&baseOptions.Workflow, "workflow", "w", "", "The workflow you want to stop.")
	stop.MarkFlagRequired("workflow")
	common.AddShardSubsetFlag(stop, &baseOptions.Shards)
	base.AddCommand(stop)

	update.Flags().StringVarP(&baseOptions.Workflow, "workflow", "w", "", "The workflow you want to update.")
	update.MarkFlagRequired("workflow")
	update.Flags().StringSliceVarP(&updateOptions.Cells, "cells", "c", nil, "New Cell(s) or CellAlias(es) (comma-separated) to replicate from.")
	update.Flags().VarP((*topoproto.TabletTypeListFlag)(&updateOptions.TabletTypes), "tablet-types", "t", "New source tablet types to replicate from (e.g. PRIMARY,REPLICA,RDONLY).")
	update.Flags().BoolVar(&updateOptions.TabletTypesInPreferenceOrder, "tablet-types-in-order", true, "When performing source tablet selection, look for candidates in the type order as they are listed in the tablet-types flag.")
	update.Flags().StringVar(&updateOptions.OnDDL, "on-ddl", "", "New instruction on what to do when DDL is encountered in the VReplication stream. Possible values are IGNORE, STOP, EXEC, and EXEC_IGNORE.")
	common.AddShardSubsetFlag(update, &baseOptions.Shards)
	base.AddCommand(update)
}

func init() {
	common.RegisterCommandHandler("Workflow", registerCommands)
}
