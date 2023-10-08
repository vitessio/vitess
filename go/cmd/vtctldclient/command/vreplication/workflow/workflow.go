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

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

var (

	// workflow is a parent command for Workflow* sub commands.
	workflow = &cobra.Command{
		Use:                   "Workflow --keyspace <keyspace> [command] [command-flags]",
		Short:                 "Administer VReplication workflows (Reshard, MoveTables, etc) in the given keyspace.",
		DisableFlagsInUseLine: true,
		Aliases:               []string{"workflow"},
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandGetWorkflows,
	}
)

var (
	workflowOptions = struct {
		Keyspace string
	}{}

	workflowUpdateOptions = struct {
		Workflow                     string
		Cells                        []string
		TabletTypes                  []topodatapb.TabletType
		TabletTypesInPreferenceOrder bool
		OnDDL                        string
	}{}

	workflowShowOptions = struct {
		IncludeLogs bool
	}{}
)

func RegisterWorkflowCommands(root *cobra.Command) {
	workflow.PersistentFlags().StringVarP(&workflowOptions.Keyspace, "keyspace", "k", "", "Keyspace context for the workflow (required).")
	workflow.MarkPersistentFlagRequired("keyspace")
	root.AddCommand(workflow)

	getWorkflows.Flags().BoolVar(&workflowShowOptions.IncludeLogs, "include-logs", true, "Include recent logs for the workflow.")
	getWorkflows.Flags().BoolVarP(&getWorkflowsOptions.ShowAll, "show-all", "a", false, "Show all workflows instead of just active workflows.")
	root.AddCommand(getWorkflows)

	workflowDelete.Flags().StringVarP(&workflowDeleteOptions.Workflow, "workflow", "w", "", "The workflow you want to delete (required).")
	workflowDelete.MarkFlagRequired("workflow")
	workflowDelete.Flags().BoolVar(&workflowDeleteOptions.KeepData, "keep-data", false, "Keep the partially copied table data from the workflow in the target keyspace.")
	workflowDelete.Flags().BoolVar(&workflowDeleteOptions.KeepRoutingRules, "keep-routing-rules", false, "Keep the routing rules created for the workflow.")
	workflow.AddCommand(workflowDelete)

	workflow.AddCommand(workflowList)

	workflowShow.Flags().StringVarP(&workflowDeleteOptions.Workflow, "workflow", "w", "", "The workflow you want the details for (required).")
	workflowShow.MarkFlagRequired("workflow")
	workflowShow.Flags().BoolVar(&workflowShowOptions.IncludeLogs, "include-logs", true, "Include recent logs for the workflow.")
	workflow.AddCommand(workflowShow)

	workflow.AddCommand(workflowStart)
	workflow.AddCommand(workflowStop)

	workflowUpdate.Flags().StringVarP(&workflowUpdateOptions.Workflow, "workflow", "w", "", "The workflow you want to update (required).")
	workflowUpdate.MarkFlagRequired("workflow")
	workflowUpdate.Flags().StringSliceVarP(&workflowUpdateOptions.Cells, "cells", "c", nil, "New Cell(s) or CellAlias(es) (comma-separated) to replicate from.")
	workflowUpdate.Flags().VarP((*topoproto.TabletTypeListFlag)(&workflowUpdateOptions.TabletTypes), "tablet-types", "t", "New source tablet types to replicate from (e.g. PRIMARY,REPLICA,RDONLY).")
	workflowUpdate.Flags().BoolVar(&workflowUpdateOptions.TabletTypesInPreferenceOrder, "tablet-types-in-order", true, "When performing source tablet selection, look for candidates in the type order as they are listed in the tablet-types flag.")
	workflowUpdate.Flags().StringVar(&workflowUpdateOptions.OnDDL, "on-ddl", "", "New instruction on what to do when DDL is encountered in the VReplication stream. Possible values are IGNORE, STOP, EXEC, and EXEC_IGNORE.")
	workflow.AddCommand(workflowUpdate)
}

func init() {
	common.RegisterCommandHandler("Workflow", RegisterWorkflowCommands)
}
