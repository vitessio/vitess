/*
Copyright 2026 The Vitess Authors.

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

package command

import (
	"fmt"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	UpdateGossipConfig = &cobra.Command{
		Use:                   "UpdateGossipConfig [--enable|--disable] [--phi-threshold=<float64>] [--ping-interval=<duration>] [--max-update-age=<duration>] <keyspace>",
		Short:                 "Update the gossip protocol configuration for all tablets in the given keyspace (across all cells)",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandUpdateGossipConfig,
	}

	updateGossipConfigOptions vtctldatapb.UpdateGossipConfigRequest
)

func commandUpdateGossipConfig(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	keyspace := cmd.Flags().Arg(0)
	updateGossipConfigOptions.Keyspace = keyspace

	resp, err := client.UpdateGossipConfig(commandCtx, &updateGossipConfigOptions)
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp)
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", data)
	return nil
}

func init() {
	UpdateGossipConfig.Flags().BoolVar(&updateGossipConfigOptions.Enable, "enable", false, "Enable gossip for this keyspace")
	UpdateGossipConfig.Flags().BoolVar(&updateGossipConfigOptions.Disable, "disable", false, "Disable gossip for this keyspace")
	UpdateGossipConfig.Flags().Float64Var(&updateGossipConfigOptions.PhiThreshold, "phi-threshold", 0, "Phi-accrual suspicion threshold (default 4, 0 = no change)")
	UpdateGossipConfig.Flags().StringVar(&updateGossipConfigOptions.PingInterval, "ping-interval", "", "Gossip exchange interval (default 1s)")
	UpdateGossipConfig.Flags().StringVar(&updateGossipConfigOptions.MaxUpdateAge, "max-update-age", "", "Max staleness before marking peer down (default 5s)")

	Root.AddCommand(UpdateGossipConfig)
}
