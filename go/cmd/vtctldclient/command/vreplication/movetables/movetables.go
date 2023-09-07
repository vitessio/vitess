package movetables

import (
	"github.com/spf13/cobra"

	common "vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"
)

var (
	// moveTables is the base command for all actions related to moveTables.
	moveTables = &cobra.Command{
		Use:   "MoveTables --workflow <workflow> --keyspace <keyspace> [command] [command-flags]",
		Short: "Perform commands related to moving tables from a source keyspace to a target keyspace.",
		Long: `moveTables commands: Create, Show, Status, SwitchTraffic, ReverseTraffic, Stop, Start, Cancel, and Delete.
See the --help output for each command for more details.`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"movetables"},
		Args:                  cobra.ExactArgs(1),
	}
)

func registerMoveTablesCommands(root *cobra.Command) {
	common.AddCommonFlags(moveTables)
	root.AddCommand(moveTables)

	registerCreateCommand(moveTables)
	opts := &common.SubCommandsOpts{
		SubCommand: "moveTables",
		Workflow:   "cust2cust",
	}
	moveTables.AddCommand(common.GetShowCommand(opts))
	moveTables.AddCommand(common.GetStatusCommand(opts))

	moveTables.AddCommand(common.GetStartCommand(opts))
	moveTables.AddCommand(common.GetStopCommand(opts))

	switchTrafficCommand := common.GetSwitchTrafficCommand(opts)
	//fixme: add flag to initialize sequences
	common.AddCommonSwitchTrafficFlags(switchTrafficCommand)
	moveTables.AddCommand(switchTrafficCommand)

	reverseTrafficCommand := common.GetReverseTrafficCommand(opts)
	common.AddCommonSwitchTrafficFlags(reverseTrafficCommand)
	moveTables.AddCommand(reverseTrafficCommand)

	moveTables.AddCommand(common.GetCompleteCommand(opts))
	moveTables.AddCommand(common.GetCancelCommand(opts))
}

func init() {
	common.RegisterCommandHandler("movetables", registerMoveTablesCommands)
}
