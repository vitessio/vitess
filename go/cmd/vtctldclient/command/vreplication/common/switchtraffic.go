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

package common

import (
	"bytes"
	"fmt"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/protoutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/vtctl/workflow"
)

func GetSwitchTrafficCommand(opts *SubCommandsOpts) *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "SwitchTraffic",
		Short:                 fmt.Sprintf("Switch traffic for a %s VReplication workflow.", opts.SubCommand),
		Example:               fmt.Sprintf(`vtctldclient --server localhost:15999 %s --workflow %s --target-keyspace customer switchtraffic --tablet-types "replica,rdonly"`, opts.SubCommand, opts.Workflow),
		DisableFlagsInUseLine: true,
		Aliases:               []string{"switchtraffic"},
		Args:                  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			CommonSwitchTrafficOptions.Direction = workflow.DirectionForward
			if !cmd.Flags().Lookup("tablet-types").Changed {
				// We switch traffic for all tablet types if none are provided.
				CommonSwitchTrafficOptions.TabletTypes = []topodatapb.TabletType{
					topodatapb.TabletType_PRIMARY,
					topodatapb.TabletType_REPLICA,
					topodatapb.TabletType_RDONLY,
				}
			}
			return nil
		},
		RunE: commandSwitchTraffic,
	}
	return cmd
}

func GetReverseTrafficCommand(opts *SubCommandsOpts) *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "reversetraffic",
		Short:                 fmt.Sprintf("Reverse traffic for a %s VReplication workflow.", opts.SubCommand),
		Example:               fmt.Sprintf(`vtctldclient --server localhost:15999 %s --workflow %s --target-keyspace customer reversetraffic`, opts.SubCommand, opts.Workflow),
		DisableFlagsInUseLine: true,
		Aliases:               []string{"ReverseTraffic"},
		Args:                  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			CommonSwitchTrafficOptions.Direction = workflow.DirectionBackward
			if !cmd.Flags().Lookup("tablet-types").Changed {
				// We switch traffic for all tablet types if none are provided.
				CommonSwitchTrafficOptions.TabletTypes = []topodatapb.TabletType{
					topodatapb.TabletType_PRIMARY,
					topodatapb.TabletType_REPLICA,
					topodatapb.TabletType_RDONLY,
				}
			}
			return nil
		},
		RunE: commandSwitchTraffic,
	}
	return cmd
}

func commandSwitchTraffic(cmd *cobra.Command, args []string) error {
	format, err := ParseAndValidateFormat(cmd, &CommonVROptions.VrCommonOptions)
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	req := &vtctldatapb.WorkflowSwitchTrafficRequest{
		Keyspace:                  CommonVROptions.TargetKeyspace,
		Workflow:                  CommonVROptions.Workflow,
		TabletTypes:               CommonSwitchTrafficOptions.TabletTypes,
		MaxReplicationLagAllowed:  protoutil.DurationToProto(CommonSwitchTrafficOptions.MaxReplicationLagAllowed),
		Timeout:                   protoutil.DurationToProto(CommonSwitchTrafficOptions.Timeout),
		DryRun:                    CommonSwitchTrafficOptions.DryRun,
		EnableReverseReplication:  CommonSwitchTrafficOptions.EnableReverseReplication,
		InitializeTargetSequences: CommonSwitchTrafficOptions.InitializeTargetSequences,
		Direction:                 int32(CommonSwitchTrafficOptions.Direction),
	}
	resp, err := GetClient().WorkflowSwitchTraffic(GetCommandCtx(), req)
	if err != nil {
		return err
	}

	var output []byte
	if format == "json" {
		output, err = cli.MarshalJSONCompact(resp)
		if err != nil {
			return err
		}
	} else {
		tout := bytes.Buffer{}
		tout.WriteString(resp.Summary + "\n\n")
		if req.DryRun {
			for _, line := range resp.DryRunResults {
				tout.WriteString(line + "\n")
			}
		} else {
			tout.WriteString(fmt.Sprintf("Start State: %s\n", resp.StartState))
			tout.WriteString(fmt.Sprintf("Current State: %s\n", resp.CurrentState))
		}
		output = tout.Bytes()
	}
	fmt.Printf("%s\n", output)

	return nil
}
