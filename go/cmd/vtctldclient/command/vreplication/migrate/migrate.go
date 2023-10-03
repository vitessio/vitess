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
	"vitess.io/vitess/go/vt/topo/topoproto"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	// migrate is the base command for all actions related to the migrate command.
	migrate = &cobra.Command{
		Use:                   "Migrate [command] [command-flags]",
		Short:                 "",
		DisableFlagsInUseLine: true,
		Aliases:               []string{"migrate"},
		Args:                  cobra.ExactArgs(1),
	}
)

var createOptions = struct {
	MountName                    string
	SourceKeyspace               string
	AllTables                    bool
	IncludeTables                []string
	ExcludeTables                []string
	SourceTimeZone               string
	NoRoutingRules               bool
	Cells                        []string
	TabletTypes                  []topodatapb.TabletType
	TabletTypesInPreferenceOrder bool
	OnDDL                        string
	DeferSecondaryKeys           bool
	AutoStart                    bool
	StopAfterCopy                bool
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
		Cells:                     createOptions.Cells,
		TabletTypes:               createOptions.TabletTypes,
		TabletSelectionPreference: tsp,
		AllTables:                 createOptions.AllTables,
		IncludeTables:             createOptions.IncludeTables,
		ExcludeTables:             createOptions.ExcludeTables,
		OnDdl:                     createOptions.OnDDL,
		DeferSecondaryKeys:        createOptions.DeferSecondaryKeys,
		AutoStart:                 createOptions.AutoStart,
		StopAfterCopy:             createOptions.StopAfterCopy,
		NoRoutingRules:            createOptions.NoRoutingRules,
	}

	_, err := common.GetClient().MigrateCreate(common.GetCommandCtx(), req)
	if err != nil {
		return err
	}

	return nil
}

func addCreateFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVar(&createOptions.SourceKeyspace, "source-keyspace", "", "Keyspace where the tables are being moved from.")
	cmd.MarkPersistentFlagRequired("source-keyspace")
	cmd.Flags().StringVar(&createOptions.SourceTimeZone, "source-time-zone", "", "Specifying this causes any DATETIME fields to be converted from the given time zone into UTC.")
	cmd.Flags().BoolVar(&createOptions.AllTables, "all-tables", false, "Copy all tables from the source.")
	cmd.Flags().StringSliceVar(&createOptions.IncludeTables, "tables", nil, "Source tables to copy.")
	cmd.Flags().StringSliceVar(&createOptions.ExcludeTables, "exclude-tables", nil, "Source tables to exclude from copying.")
	cmd.Flags().BoolVar(&createOptions.NoRoutingRules, "no-routing-rules", false, "(Advanced) Do not create routing rules while creating the workflow. See the reference documentation for limitations if you use this flag.")

	cmd.Flags().StringVar(&createOptions.MountName, "mount-name", "", "Name external cluster is mounted as.")
	cmd.Flags().StringSliceVarP(&createOptions.Cells, "cells", "c", nil, "Cells and/or CellAliases to copy table data from.")
	cmd.Flags().Var((*topoproto.TabletTypeListFlag)(&createOptions.TabletTypes), "tablet-types", "Source tablet types to replicate table data from (e.g. PRIMARY,REPLICA,RDONLY).")
	cmd.Flags().BoolVar(&createOptions.TabletTypesInPreferenceOrder, "tablet-types-in-preference-order", true, "When performing source tablet selection, look for candidates in the type order as they are listed in the tablet-types flag.")
	onDDLDefault := binlogdatapb.OnDDLAction_IGNORE.String()
	cmd.Flags().StringVar(&createOptions.OnDDL, "on-ddl", onDDLDefault, "What to do when DDL is encountered in the VReplication stream. Possible values are IGNORE, STOP, EXEC, and EXEC_IGNORE.")
	cmd.Flags().BoolVar(&createOptions.DeferSecondaryKeys, "defer-secondary-keys", false, "Defer secondary index creation for a table until after it has been copied.")
	cmd.Flags().BoolVar(&createOptions.AutoStart, "auto-start", true, "Start the MoveTables workflow after creating it.")
	cmd.Flags().BoolVar(&createOptions.StopAfterCopy, "stop-after-copy", false, "Stop the MoveTables workflow after it's finished copying the existing rows and before it starts replicating changes.")
}

func registerMigrateCommands(root *cobra.Command) {
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
}

func init() {
	common.RegisterCommandHandler("Migrate", registerMigrateCommands)
}
