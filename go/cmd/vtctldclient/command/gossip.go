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

// UpdateGossipConfig is the CLI front for the UpdateGossipConfig RPC.
// Operators invoke it via vtctldclient to enable/disable gossip or
// tune its parameters for a keyspace — the RPC takes care of updating
// the Keyspace record and fanning the change out to each cell's
// SrvKeyspace so running vttablets and VTOrc pick it up live.
var UpdateGossipConfig = &cobra.Command{
	Use:                   "UpdateGossipConfig [--enable|--disable] [--phi-threshold=<float64>] [--ping-interval=<duration>] [--max-update-age=<duration>] <keyspace>",
	Short:                 "Update the gossip protocol configuration for all tablets in the given keyspace (across all cells)",
	DisableFlagsInUseLine: true,
	Args:                  cobra.ExactArgs(1),
	RunE:                  commandUpdateGossipConfig,
}

// commandUpdateGossipConfig marshals CLI flags into an
// UpdateGossipConfigRequest. Optional fields (Enabled, PhiThreshold)
// are only set in the request when the user explicitly passed the
// matching flag, so the server can distinguish "unset (leave as
// default/existing)" from an explicit value.
func commandUpdateGossipConfig(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	keyspace := cmd.Flags().Arg(0)
	pingInterval, err := cmd.Flags().GetString("ping-interval")
	if err != nil {
		return err
	}
	maxUpdateAge, err := cmd.Flags().GetString("max-update-age")
	if err != nil {
		return err
	}
	req := &vtctldatapb.UpdateGossipConfigRequest{
		Keyspace:     keyspace,
		PingInterval: pingInterval,
		MaxUpdateAge: maxUpdateAge,
	}
	switch {
	case cmd.Flags().Changed("enable"):
		enabled := true
		req.Enabled = &enabled
	case cmd.Flags().Changed("disable"):
		enabled := false
		req.Enabled = &enabled
	}
	if cmd.Flags().Changed("phi-threshold") {
		phiThreshold, err := cmd.Flags().GetFloat64("phi-threshold")
		if err != nil {
			return err
		}
		req.PhiThreshold = &phiThreshold
	}

	resp, err := client.UpdateGossipConfig(commandCtx, req)
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
	UpdateGossipConfig.Flags().Bool("enable", false, "Enable gossip for this keyspace")
	UpdateGossipConfig.Flags().Bool("disable", false, "Disable gossip for this keyspace")
	UpdateGossipConfig.MarkFlagsMutuallyExclusive("enable", "disable")
	UpdateGossipConfig.Flags().Float64("phi-threshold", 0, "Phi-accrual suspicion threshold. Must be > 0 when specified; omit to use the default (4) on create or preserve the existing value on update")
	UpdateGossipConfig.Flags().String("ping-interval", "", "Gossip exchange interval. Must be a positive duration when specified; omit to use the default (1s) on create or preserve the existing value on update")
	UpdateGossipConfig.Flags().String("max-update-age", "", "Max staleness before marking peer down. Must be a positive duration when specified; omit to use the default (5s) on create or preserve the existing value on update")

	Root.AddCommand(UpdateGossipConfig)
}
