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
)

var (

	// workflow is a parent command for Workflow* sub commands.
	workflow = &cobra.Command{
		Use:   "Workflow --keyspace <keyspace> [command] [command-flags]",
		Short: "Administer VReplication workflows (Reshard, MoveTables, etc) in the given keyspace.",
		Long: `Workflow commands: List, Show, Start, Stop, Update, and Delete.
See the --help output for each command for more details.`,
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
)

func RegisterWorkflowCommands(root *cobra.Command) {
	workflow.PersistentFlags().StringVarP(&workflowOptions.Keyspace, "keyspace", "k", "", "Keyspace context for the workflow (required).")
	workflow.MarkPersistentFlagRequired("keyspace")
	root.AddCommand(workflow)

	addGetWorkflowsFlags(getWorkflows)
	root.AddCommand(getWorkflows)

	addWorkflowDeleteFlags(workflowDelete)
	workflow.AddCommand(workflowDelete)

	workflow.AddCommand(workflowList)

	addWorkflowShowFlags(workflowShow)
	workflow.AddCommand(workflowShow)

	workflow.AddCommand(workflowStart)
	workflow.AddCommand(workflowStop)

	addWorkflowUpdateFlags(workflowUpdate)
	workflow.AddCommand(workflowUpdate)
}

func init() {
	common.RegisterCommandHandler("Workflow", RegisterWorkflowCommands)
}
