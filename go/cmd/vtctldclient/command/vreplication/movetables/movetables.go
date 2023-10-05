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

package movetables

import (
	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"
)

var (
	// moveTables is the base command for all actions related to moveTables.
	moveTables = &cobra.Command{
		Use:                   "MoveTables --workflow <workflow> --target-keyspace <keyspace> [command] [command-flags]",
		Short:                 "Perform commands related to moving tables from a source keyspace to a target keyspace.",
		DisableFlagsInUseLine: true,
		Aliases:               []string{"movetables"},
		Args:                  cobra.ExactArgs(1),
	}
)

func registerMoveTablesCommands(root *cobra.Command) {
	common.AddCommonFlags(moveTables)
	root.AddCommand(moveTables)

	common.AddCommonCreateFlags(moveTablesCreate)
	moveTablesCreate.PersistentFlags().StringVar(&moveTablesCreateOptions.SourceKeyspace, "source-keyspace", "", "Keyspace where the tables are being moved from.")
	moveTablesCreate.MarkPersistentFlagRequired("source-keyspace")
	moveTablesCreate.Flags().StringSliceVar(&moveTablesCreateOptions.SourceShards, "source-shards", nil, "Source shards to copy data from when performing a partial moveTables (experimental).")
	moveTablesCreate.Flags().StringVar(&moveTablesCreateOptions.SourceTimeZone, "source-time-zone", "", "Specifying this causes any DATETIME fields to be converted from the given time zone into UTC.")
	moveTablesCreate.Flags().BoolVar(&moveTablesCreateOptions.AllTables, "all-tables", false, "Copy all tables from the source.")
	moveTablesCreate.Flags().StringSliceVar(&moveTablesCreateOptions.IncludeTables, "tables", nil, "Source tables to copy.")
	moveTablesCreate.Flags().StringSliceVar(&moveTablesCreateOptions.ExcludeTables, "exclude-tables", nil, "Source tables to exclude from copying.")
	moveTablesCreate.Flags().BoolVar(&moveTablesCreateOptions.NoRoutingRules, "no-routing-rules", false, "(Advanced) Do not create routing rules while creating the workflow. See the reference documentation for limitations if you use this flag.")
	moveTablesCreate.Flags().BoolVar(&moveTablesCreateOptions.AtomicCopy, "atomic-copy", false, "(EXPERIMENTAL) A single copy phase is run for all tables from the source. Use this, for example, if your source keyspace has tables which use foreign key constraints.")
	moveTables.AddCommand(moveTablesCreate)

	opts := &common.SubCommandsOpts{
		SubCommand: "MoveTables",
		Workflow:   "commerce2customer",
	}
	moveTables.AddCommand(common.GetShowCommand(opts))
	moveTables.AddCommand(common.GetStatusCommand(opts))

	moveTables.AddCommand(common.GetStartCommand(opts))
	moveTables.AddCommand(common.GetStopCommand(opts))

	switchTrafficCommand := common.GetSwitchTrafficCommand(opts)
	common.AddCommonSwitchTrafficFlags(switchTrafficCommand, true)
	moveTables.AddCommand(switchTrafficCommand)

	reverseTrafficCommand := common.GetReverseTrafficCommand(opts)
	common.AddCommonSwitchTrafficFlags(reverseTrafficCommand, false)
	moveTables.AddCommand(reverseTrafficCommand)

	complete := common.GetCompleteCommand(opts)
	complete.Flags().BoolVar(&common.CompleteOptions.KeepData, "keep-data", false, "Keep the original source table data that was copied by the MoveTables workflow")
	complete.Flags().BoolVar(&common.CompleteOptions.KeepRoutingRules, "keep-routing-rules", false, "Keep the routing rules in place that direct table traffic from the source keyspace to the target keyspace of the MoveTables workflow")
	complete.Flags().BoolVar(&common.CompleteOptions.RenameTables, "rename-tables", false, "Keep the original source table data that was copied by the MoveTables workflow, but rename each table to '_<tablename>_old'")
	complete.Flags().BoolVar(&common.CompleteOptions.DryRun, "dry-run", false, "Print the actions that would be taken and report any known errors that would have occurred")
	moveTables.AddCommand(complete)

	cancel := common.GetCancelCommand(opts)
	cancel.Flags().BoolVar(&common.CancelOptions.KeepData, "keep-data", false, "Keep the partially copied table data from the MoveTables workflow in the target keyspace")
	cancel.Flags().BoolVar(&common.CancelOptions.KeepRoutingRules, "keep-routing-rules", false, "Keep the routing rules created for the MoveTables workflow")
	moveTables.AddCommand(cancel)
}

func init() {
	common.RegisterCommandHandler("MoveTables", registerMoveTablesCommands)
}
