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

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	reshardCreateOptions = struct {
		sourceShards   []string
		targetShards   []string
		skipSchemaCopy bool
	}{}

	// reshardCreate makes a ReshardCreate gRPC call to a vtctld.
	reshardCreate = &cobra.Command{
		Use:                   "create",
		Short:                 "Create and optionally run a Reshard VReplication workflow.",
		Example:               `vtctldclient --server localhost:15999 reshard --workflow customer2customer --target-keyspace customer create --source-shards="0" --target-shards="-80,80-" --cells zone1 --cells zone2 --tablet-types replica`,
		SilenceUsage:          true,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Create"},
		Args:                  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if err := common.ParseAndValidateCreateOptions(cmd); err != nil {
				return err
			}
			return nil
		},
		RunE: commandReshardCreate,
	}
)

func commandReshardCreate(cmd *cobra.Command, args []string) error {
	format, err := common.GetOutputFormat(cmd)
	if err != nil {
		return err
	}
	tsp := common.GetTabletSelectionPreference(cmd)
	cli.FinishedParsing(cmd)

	req := &vtctldatapb.ReshardCreateRequest{
		Workflow:                  common.BaseOptions.Workflow,
		Keyspace:                  common.BaseOptions.TargetKeyspace,
		TabletTypes:               common.CreateOptions.TabletTypes,
		TabletSelectionPreference: tsp,
		Cells:                     common.CreateOptions.Cells,
		OnDdl:                     common.CreateOptions.OnDDL,
		DeferSecondaryKeys:        common.CreateOptions.DeferSecondaryKeys,
		AutoStart:                 common.CreateOptions.AutoStart,
		StopAfterCopy:             common.CreateOptions.StopAfterCopy,
		SourceShards:              reshardCreateOptions.sourceShards,
		TargetShards:              reshardCreateOptions.targetShards,
		SkipSchemaCopy:            reshardCreateOptions.skipSchemaCopy,
	}
	resp, err := common.GetClient().ReshardCreate(common.GetCommandCtx(), req)
	if err != nil {
		return err
	}
	if err = common.OutputStatusResponse(resp, format); err != nil {
		return err
	}
	return nil
}

func registerCreateCommand(root *cobra.Command) {
	common.AddCommonCreateFlags(reshardCreate)
	reshardCreate.Flags().StringSliceVar(&reshardCreateOptions.sourceShards, "source-shards", nil, "Source shards.")
	reshardCreate.Flags().StringSliceVar(&reshardCreateOptions.targetShards, "target-shards", nil, "Target shards.")
	reshardCreate.Flags().BoolVar(&reshardCreateOptions.skipSchemaCopy, "skip-schema-copy", false, "Skip copying the schema from the source shards to the target shards.")
	root.AddCommand(reshardCreate)
}
