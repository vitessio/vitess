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

package migrate

import (
	"fmt"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	// migrate is the base command for all actions related to the migrate command.
	migrate = &cobra.Command{
		Use:                   "Migrate --workflow <workflow> --target-keyspace <keyspace> [command] [command-flags]",
		Short:                 "Migrate is used to import data from an external cluster into the current cluster.",
		DisableFlagsInUseLine: true,
		Aliases:               []string{"migrate"},
		Args:                  cobra.ExactArgs(1),
	}
)

var createOptions = struct {
	MountName      string
	SourceKeyspace string
	AllTables      bool
	IncludeTables  []string
	ExcludeTables  []string
	SourceTimeZone string
	NoRoutingRules bool
}{}

var createCommand = &cobra.Command{
	Use:                   "create",
	Short:                 "Create and optionally run a Migrate VReplication workflow.",
	Example:               `vtctldclient --server localhost:15999 migrate --workflow import --target-keyspace customer create --source-keyspace commerce --mount-name ext1 --tablet-types replica`,
	SilenceUsage:          true,
	DisableFlagsInUseLine: true,
	Aliases:               []string{"Create"},
	Args:                  cobra.NoArgs,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		// Either specific tables or the all tables flags are required.
		if !cmd.Flags().Lookup("tables").Changed && !cmd.Flags().Lookup("all-tables").Changed {
			return fmt.Errorf("tables or all-tables are required to specify which tables to move")
		}
		if err := common.ParseAndValidateCreateOptions(cmd); err != nil {
			return err
		}
		return nil
	},
	RunE: commandCreate,
}

func commandCreate(cmd *cobra.Command, args []string) error {
	tsp := common.GetTabletSelectionPreference(cmd)
	cli.FinishedParsing(cmd)

	req := &vtctldatapb.MigrateCreateRequest{
		Workflow:                  common.BaseOptions.Workflow,
		TargetKeyspace:            common.BaseOptions.TargetKeyspace,
		SourceKeyspace:            createOptions.SourceKeyspace,
		MountName:                 createOptions.MountName,
		SourceTimeZone:            createOptions.SourceTimeZone,
		Cells:                     common.CreateOptions.Cells,
		TabletTypes:               common.CreateOptions.TabletTypes,
		TabletSelectionPreference: tsp,
		AllTables:                 createOptions.AllTables,
		IncludeTables:             createOptions.IncludeTables,
		ExcludeTables:             createOptions.ExcludeTables,
		OnDdl:                     common.CreateOptions.OnDDL,
		DeferSecondaryKeys:        common.CreateOptions.DeferSecondaryKeys,
		AutoStart:                 common.CreateOptions.AutoStart,
		StopAfterCopy:             common.CreateOptions.StopAfterCopy,
		NoRoutingRules:            createOptions.NoRoutingRules,
	}

	_, err := common.GetClient().MigrateCreate(common.GetCommandCtx(), req)
	if err != nil {
		return err
	}

	return nil
}

func addCreateFlags(cmd *cobra.Command) {
	common.AddCommonCreateFlags(cmd)
	cmd.Flags().StringVar(&createOptions.SourceKeyspace, "source-keyspace", "", "Keyspace where the tables are being moved from.")
	cmd.MarkFlagRequired("source-keyspace")
	cmd.Flags().StringVar(&createOptions.MountName, "mount-name", "", "Name external cluster is mounted as.")
	cmd.MarkFlagRequired("mount-name")
	cmd.Flags().StringVar(&createOptions.SourceTimeZone, "source-time-zone", "", "Specifying this causes any DATETIME fields to be converted from the given time zone into UTC.")
	cmd.Flags().BoolVar(&createOptions.AllTables, "all-tables", false, "Copy all tables from the source.")
	cmd.Flags().StringSliceVar(&createOptions.IncludeTables, "tables", nil, "Source tables to copy.")
	cmd.Flags().StringSliceVar(&createOptions.ExcludeTables, "exclude-tables", nil, "Source tables to exclude from copying.")
	cmd.Flags().BoolVar(&createOptions.NoRoutingRules, "no-routing-rules", false, "(Advanced) Do not create routing rules while creating the workflow. See the reference documentation for limitations if you use this flag.")

}

func registerCommands(root *cobra.Command) {
	common.AddCommonFlags(migrate)
	root.AddCommand(migrate)
	addCreateFlags(createCommand)
	migrate.AddCommand(createCommand)
	opts := &common.SubCommandsOpts{
		SubCommand: "Migrate",
		Workflow:   "import",
	}
	migrate.AddCommand(common.GetCompleteCommand(opts))
	migrate.AddCommand(common.GetCancelCommand(opts))
	migrate.AddCommand(common.GetShowCommand(opts))
	migrate.AddCommand(common.GetStatusCommand(opts))
}

func init() {
	common.RegisterCommandHandler("Migrate", registerCommands)
}
