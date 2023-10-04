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

package reshard

import (
	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"
)

var (
	// reshard is the base command for all actions related to reshard.
	reshard = &cobra.Command{
		Use:                   "Reshard --workflow <workflow> --target-keyspace <keyspace> [command] [command-flags]",
		Short:                 "Perform commands related to resharding a keyspace.",
		DisableFlagsInUseLine: true,
		Aliases:               []string{"reshard"},
		Args:                  cobra.ExactArgs(1),
	}
)

func registerReshardCommands(root *cobra.Command) {
	common.AddCommonFlags(reshard)
	root.AddCommand(reshard)

	registerCreateCommand(reshard)
	opts := &common.SubCommandsOpts{
		SubCommand: "Reshard",
		Workflow:   "cust2cust",
	}
	reshard.AddCommand(common.GetShowCommand(opts))
	reshard.AddCommand(common.GetStatusCommand(opts))

	reshard.AddCommand(common.GetStartCommand(opts))
	reshard.AddCommand(common.GetStopCommand(opts))

	switchTrafficCommand := common.GetSwitchTrafficCommand(opts)
	common.AddCommonSwitchTrafficFlags(switchTrafficCommand, false)
	reshard.AddCommand(switchTrafficCommand)

	reverseTrafficCommand := common.GetReverseTrafficCommand(opts)
	common.AddCommonSwitchTrafficFlags(reverseTrafficCommand, false)
	reshard.AddCommand(reverseTrafficCommand)

	reshard.AddCommand(common.GetCompleteCommand(opts))
	reshard.AddCommand(common.GetCancelCommand(opts))
}

func init() {
	common.RegisterCommandHandler("Reshard", registerReshardCommands)
}
