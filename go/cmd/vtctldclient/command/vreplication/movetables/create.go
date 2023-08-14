package movetables

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	common "vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	moveTablesCreateOptions = struct {
		SourceKeyspace      string
		SourceShards        []string
		ExternalClusterName string
		AllTables           bool
		IncludeTables       []string
		ExcludeTables       []string
		SourceTimeZone      string
		NoRoutingRules      bool
		AtomicCopy          bool
	}{}

	// moveTablesCreate makes a moveTablesCreate gRPC call to a vtctld.
	moveTablesCreate = &cobra.Command{
		Use:                   "create",
		Short:                 "Create and optionally run a moveTables VReplication workflow.",
		Example:               `vtctldclient --server localhost:15999 movetables --workflow commerce2customer --target-keyspace customer create --source-keyspace commerce --cells zone1 --cells zone2 --tablet-types replica`,
		SilenceUsage:          true,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Create"},
		Args:                  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			// Either specific tables or the all tables flags are required.
			if !cmd.Flags().Lookup("tables").Changed && !cmd.Flags().Lookup("all-tables").Changed {
				return fmt.Errorf("tables or all-tables are required to specify which tables to move")
			}
			if err := common.ParseAndValidateCreateOptions(cmd, &common.CommonVRCreateOptions.VrCreateCommonOptions); err != nil {
				return err
			}
			checkAtomicCopyOptions := func() error {
				var errors []string
				if !moveTablesCreateOptions.AtomicCopy {
					return nil
				}
				if !moveTablesCreateOptions.AllTables {
					errors = append(errors, "atomic copy requires --all-tables.")
				}
				if len(moveTablesCreateOptions.IncludeTables) > 0 || len(moveTablesCreateOptions.ExcludeTables) > 0 {
					errors = append(errors, "atomic copy does not support specifying tables.")
				}
				if len(errors) > 0 {
					errors = append(errors, "Found options incompatible with atomic copy:")
					return fmt.Errorf(strings.Join(errors, " "))
				}
				return nil
			}
			if err := checkAtomicCopyOptions(); err != nil {
				return err
			}
			return nil
		},
		RunE: commandMoveTablesCreate,
	}
)

func commandMoveTablesCreate(cmd *cobra.Command, args []string) error {

	cli.FinishedParsing(cmd)

	format, err := common.GetCommonOptions(cmd, &common.CommonVROptions.VrCommonOptions)
	if err != nil {
		return err
	}
	tsp := common.GetCreateOptions(cmd, &common.CommonVRCreateOptions.VrCreateCommonOptions)

	req := &vtctldatapb.MoveTablesCreateRequest{
		Workflow:                  common.CommonVROptions.Workflow,
		TargetKeyspace:            common.CommonVROptions.TargetKeyspace,
		SourceKeyspace:            moveTablesCreateOptions.SourceKeyspace,
		SourceShards:              moveTablesCreateOptions.SourceShards,
		SourceTimeZone:            moveTablesCreateOptions.SourceTimeZone,
		Cells:                     common.CommonVRCreateOptions.Cells,
		TabletTypes:               common.CommonVRCreateOptions.TabletTypes,
		TabletSelectionPreference: tsp,
		AllTables:                 moveTablesCreateOptions.AllTables,
		IncludeTables:             moveTablesCreateOptions.IncludeTables,
		ExcludeTables:             moveTablesCreateOptions.ExcludeTables,
		OnDdl:                     common.CommonVRCreateOptions.OnDDL,
		DeferSecondaryKeys:        common.CommonVRCreateOptions.DeferSecondaryKeys,
		AutoStart:                 common.CommonVRCreateOptions.AutoStart,
		StopAfterCopy:             common.CommonVRCreateOptions.StopAfterCopy,
		NoRoutingRules:            moveTablesCreateOptions.NoRoutingRules,
		AtomicCopy:                moveTablesCreateOptions.AtomicCopy,
	}

	resp, err := common.GetClient().MoveTablesCreate(common.GetCommandCtx(), req)
	if err != nil {
		return err
	}
	if err = common.OutputStatusResponse(resp, format); err != nil {
		return err
	}
	return nil
}

func registerCreateCommand(root *cobra.Command) {
	common.AddCommonCreateFlags(moveTablesCreate)
	moveTablesCreate.PersistentFlags().StringVar(&moveTablesCreateOptions.SourceKeyspace, "source-keyspace", "", "Keyspace where the tables are being moved from (required)")
	moveTablesCreate.MarkPersistentFlagRequired("source-keyspace")
	moveTablesCreate.Flags().StringSliceVar(&moveTablesCreateOptions.SourceShards, "source-shards", nil, "Source shards to copy data from when performing a partial moveTables (experimental)")
	moveTablesCreate.Flags().StringVar(&moveTablesCreateOptions.SourceTimeZone, "source-time-zone", "", "Specifying this causes any DATETIME fields to be converted from the given time zone into UTC")
	moveTablesCreate.Flags().BoolVar(&moveTablesCreateOptions.AllTables, "all-tables", false, "Copy all tables from the source")
	moveTablesCreate.Flags().StringSliceVar(&moveTablesCreateOptions.IncludeTables, "tables", nil, "Source tables to copy")
	moveTablesCreate.Flags().StringSliceVar(&moveTablesCreateOptions.ExcludeTables, "exclude-tables", nil, "Source tables to exclude from copying")
	moveTablesCreate.Flags().BoolVar(&moveTablesCreateOptions.NoRoutingRules, "no-routing-rules", false, "(Advanced) Do not create routing rules while creating the workflow. See the reference documentation for limitations if you use this flag.")
	moveTablesCreate.Flags().BoolVar(&moveTablesCreateOptions.AtomicCopy, "atomic-copy", false, "(EXPERIMENTAL) A single copy phase is run for all tables from the source. Use this, for example, if your source keyspace has tables which use foreign key constraints.")
	moveTables.AddCommand(moveTablesCreate)
}
