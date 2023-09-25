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
		Use:   "MoveTables --workflow <workflow> --keyspace <keyspace> [command] [command-flags]",
		Short: "Perform commands related to moving tables from a source keyspace to a target keyspace.",
		Long: `MoveTables commands: Create, Show, Status, SwitchTraffic, ReverseTraffic, Stop, Start, Cancel, and Delete.
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

	moveTables.AddCommand(common.GetCompleteCommand(opts))
	moveTables.AddCommand(common.GetCancelCommand(opts))
}

func init() {
	common.RegisterCommandHandler("MoveTables", registerMoveTablesCommands)
}
