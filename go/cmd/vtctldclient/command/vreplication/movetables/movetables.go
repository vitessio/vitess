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
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"
	"vitess.io/vitess/go/vt/topo/topoproto"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// The default batch size to use when deleting tenant data
// if a multi-tenant migration is canceled or deleted.
const DefaultDeleteBatchSize = 1000

var (
	// base is the base command for all actions related to MoveTables.
	base = &cobra.Command{
		Use:                   "MoveTables --workflow <workflow> --target-keyspace <keyspace> [command] [command-flags]",
		Short:                 "Perform commands related to moving tables from a source keyspace to a target keyspace.",
		DisableFlagsInUseLine: true,
		Aliases:               []string{"movetables"},
		Args:                  cobra.ExactArgs(1),
	}
	shardedAutoIncHandlingStrOptions string
)

func registerCommands(root *cobra.Command) {
	common.AddCommonFlags(base)
	root.AddCommand(base)

	common.AddCommonCreateFlags(create)
	create.PersistentFlags().StringVar(&createOptions.SourceKeyspace, "source-keyspace", "", "Keyspace where the tables are being moved from.")
	create.MarkPersistentFlagRequired("source-keyspace")
	create.Flags().StringSliceVar(&createOptions.SourceShards, "source-shards", nil, "Source shards to copy data from when performing a partial MoveTables (experimental).")
	create.Flags().StringVar(&createOptions.SourceTimeZone, "source-time-zone", "", "Specifying this causes any DATETIME fields to be converted from the given time zone into UTC.")
	create.Flags().BoolVar(&createOptions.AllTables, "all-tables", false, "Copy all tables from the source.")
	create.Flags().StringSliceVar(&createOptions.IncludeTables, "tables", nil, "Source tables to copy.")
	create.Flags().StringSliceVar(&createOptions.ExcludeTables, "exclude-tables", nil, "Source tables to exclude from copying.")
	create.Flags().BoolVar(&createOptions.NoRoutingRules, "no-routing-rules", false, "(Advanced) Do not create routing rules while creating the workflow. See the reference documentation for limitations if you use this flag.")
	create.Flags().BoolVar(&createOptions.AtomicCopy, "atomic-copy", false, "(EXPERIMENTAL) A single copy phase is run for all tables from the source. Use this, for example, if your source keyspace has tables which use foreign key constraints.")
	create.Flags().StringVar(&createOptions.WorkflowOptions.TenantId, "tenant-id", "", "(EXPERIMENTAL: Multi-tenant migrations only) The tenant ID to use for the MoveTables workflow into a multi-tenant keyspace.")
	create.Flags().StringSliceVar(&createOptions.WorkflowOptions.Shards, "shards", nil, "(EXPERIMENTAL: Multi-tenant migrations only) Specify that vreplication streams should only be created on this subset of target shards. Warning: you should first ensure that all rows on the source route to the specified subset of target shards using your VIndex of choice or you could lose data during the migration.")
	create.Flags().StringVar(&createOptions.WorkflowOptions.GlobalKeyspace, "global-keyspace", "", "If specified, then attempt to create any global resources here such as sequence tables needed to replace auto_increment table clauses that are removed due to --sharded-auto-increment-handling=REPLACE. The value must be an unsharded keyspace that already exists.")
	create.Flags().StringVar(&createOptions.ShardedAutoIncrementHandlingStr, "sharded-auto-increment-handling", vtctldatapb.ShardedAutoIncrementHandling_REMOVE.String(),
		fmt.Sprintf("If moving the table(s) to a sharded keyspace, remove any MySQL auto_increment clauses when copying the schema to the target as sharded keyspaces should rely on either user/application generated values or Vitess sequences to ensure uniqueness. If REPLACE is specified then they are automatically replaced by Vitess sequence definitions. (options are: %s)",
			shardedAutoIncHandlingStrOptions))
	base.AddCommand(create)

	opts := &common.SubCommandsOpts{
		SubCommand: "MoveTables",
		Workflow:   "commerce2customer",
	}
	showCommand := common.GetShowCommand(opts)
	common.AddShardSubsetFlag(showCommand, &common.ShowOptions.Shards)
	base.AddCommand(showCommand)

	statusCommand := common.GetStatusCommand(opts)
	common.AddShardSubsetFlag(statusCommand, &common.StatusOptions.Shards)
	base.AddCommand(statusCommand)

	base.AddCommand(common.GetStartCommand(opts))
	base.AddCommand(common.GetStopCommand(opts))

	mirrorTrafficCommand := common.GetMirrorTrafficCommand(opts)
	mirrorTrafficCommand.Flags().Var((*topoproto.TabletTypeListFlag)(&common.MirrorTrafficOptions.TabletTypes), "tablet-types", "Tablet types to mirror traffic for.")
	mirrorTrafficCommand.Flags().Float32Var(&common.MirrorTrafficOptions.Percent, "percent", 1.0, "Percentage of traffic to mirror.")
	base.AddCommand(mirrorTrafficCommand)

	switchTrafficCommand := common.GetSwitchTrafficCommand(opts)
	common.AddCommonSwitchTrafficFlags(switchTrafficCommand, true)
	common.AddShardSubsetFlag(switchTrafficCommand, &common.SwitchTrafficOptions.Shards)
	base.AddCommand(switchTrafficCommand)

	reverseTrafficCommand := common.GetReverseTrafficCommand(opts)
	common.AddCommonSwitchTrafficFlags(reverseTrafficCommand, false)
	common.AddShardSubsetFlag(reverseTrafficCommand, &common.SwitchTrafficOptions.Shards)
	base.AddCommand(reverseTrafficCommand)

	complete := common.GetCompleteCommand(opts)
	complete.Flags().BoolVar(&common.CompleteOptions.KeepData, "keep-data", false, "Keep the original source table data that was copied by the MoveTables workflow.")
	complete.Flags().BoolVar(&common.CompleteOptions.KeepRoutingRules, "keep-routing-rules", false, "Keep the routing rules in place that direct table traffic from the source keyspace to the target keyspace of the MoveTables workflow.")
	complete.Flags().BoolVar(&common.CompleteOptions.RenameTables, "rename-tables", false, "Keep the original source table data that was copied by the MoveTables workflow, but rename each table to '_<tablename>_old'.")
	complete.Flags().BoolVar(&common.CompleteOptions.DryRun, "dry-run", false, "Print the actions that would be taken and report any known errors that would have occurred.")
	complete.Flags().BoolVar(&common.CompleteOptions.IgnoreSourceKeyspace, "ignore-source-keyspace", false, "WARNING: This option should only be used when absolutely necessary. Ignore the source keyspace as the workflow is completed and cleaned up. This allows the workflow to be completed if the source keyspace has been deleted or is not currently available.")
	common.AddShardSubsetFlag(complete, &common.CompleteOptions.Shards)
	base.AddCommand(complete)

	cancel := common.GetCancelCommand(opts)
	cancel.Flags().BoolVar(&common.CancelOptions.KeepData, "keep-data", false, "Keep the partially copied table data from the MoveTables workflow in the target keyspace.")
	cancel.Flags().BoolVar(&common.CancelOptions.KeepRoutingRules, "keep-routing-rules", false, "Keep the routing rules created for the MoveTables workflow.")
	cancel.Flags().Int64Var(&common.CancelOptions.DeleteBatchSize, "delete-batch-size", DefaultDeleteBatchSize, "When cleaning up the migrated data in tables moved as part of a multi-tenant workflow, delete the records in batches of this size.")
	cancel.Flags().BoolVar(&common.CancelOptions.IgnoreSourceKeyspace, "ignore-source-keyspace", false, "WARNING: This option should only be used when absolutely necessary. Ignore the source keyspace as the workflow is canceled and cleaned up. This allows the workflow to be canceled if the source keyspace has been deleted or is not currently available.")
	common.AddShardSubsetFlag(cancel, &common.CancelOptions.Shards)
	base.AddCommand(cancel)
}

func init() {
	common.RegisterCommandHandler("MoveTables", registerCommands)

	sb := strings.Builder{}
	strvals := make([]string, len(vtctldatapb.ShardedAutoIncrementHandling_name))
	for enumval, strval := range vtctldatapb.ShardedAutoIncrementHandling_name {
		strvals[enumval] = strval
	}
	for i, v := range strvals {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(v)
	}
	shardedAutoIncHandlingStrOptions = sb.String()
}
