/*
Copyright 2022 The Vitess Authors.

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
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/json2"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	// ApplyShardRoutingRules makes an ApplyShardRoutingRules gRPC call to a vtctld.
	ApplyShardRoutingRules = &cobra.Command{
		Use:                   "ApplyShardRoutingRules {--rules RULES | --rules-file RULES_FILE} [--cells=c1,c2,...] [--skip-rebuild] [--dry-run]",
		Short:                 "Applies the provided shard routing rules.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.NoArgs,
		RunE:                  commandApplyShardRoutingRules,
	}
	// GetShardRoutingRules makes a GetShardRoutingRules gRPC call to a vtctld.
	GetShardRoutingRules = &cobra.Command{
		Use:   "GetShardRoutingRules",
		Short: "Displays the currently active shard routing rules as a JSON document.",
		Long: `Displays the currently active shard routing rules as a JSON document.

See the documentation on shard level migrations[1] for more information.

[1]: https://vitess.io/docs/reference/vreplication/shardlevelmigrations/`,
		DisableFlagsInUseLine: true,
		Args:                  cobra.NoArgs,
		RunE:                  commandGetShardRoutingRules,
	}
)

var applyShardRoutingRulesOptions = struct {
	Rules         string
	RulesFilePath string
	Cells         []string
	SkipRebuild   bool
	DryRun        bool
}{}

func commandApplyShardRoutingRules(cmd *cobra.Command, args []string) error {
	if applyShardRoutingRulesOptions.Rules != "" && applyShardRoutingRulesOptions.RulesFilePath != "" {
		return fmt.Errorf("cannot pass both --rules (=%s) and --rules-file (=%s)", applyShardRoutingRulesOptions.Rules, applyShardRoutingRulesOptions.RulesFilePath)
	}

	if applyShardRoutingRulesOptions.Rules == "" && applyShardRoutingRulesOptions.RulesFilePath == "" {
		return errors.New("must pass exactly one of --rules or --rules-file")
	}

	cli.FinishedParsing(cmd)

	var rulesBytes []byte
	if applyShardRoutingRulesOptions.RulesFilePath != "" {
		data, err := os.ReadFile(applyShardRoutingRulesOptions.RulesFilePath)
		if err != nil {
			return err
		}

		rulesBytes = data
	} else {
		rulesBytes = []byte(applyShardRoutingRulesOptions.Rules)
	}

	srr := &vschemapb.ShardRoutingRules{}
	if err := json2.UnmarshalPB(rulesBytes, srr); err != nil {
		return err
	}
	// Round-trip so when we display the result it's readable.
	data, err := cli.MarshalJSON(srr)
	if err != nil {
		return err
	}

	if applyShardRoutingRulesOptions.DryRun {
		fmt.Printf("[DRY RUN] Would have saved new ShardRoutingRules object:\n%s\n", data)

		if applyRoutingRulesOptions.SkipRebuild {
			fmt.Println("[DRY RUN] Would not have rebuilt VSchema graph, would have required operator to run RebuildVSchemaGraph for changes to take effect.")
		} else {
			fmt.Print("[DRY RUN] Would have rebuilt the VSchema graph")
			if len(applyRoutingRulesOptions.Cells) == 0 {
				fmt.Print(" in all cells\n")
			} else {
				fmt.Printf(" in the following cells: %s.\n", strings.Join(applyShardRoutingRulesOptions.Cells, ", "))
			}
		}

		return nil
	}

	_, err = client.ApplyShardRoutingRules(commandCtx, &vtctldatapb.ApplyShardRoutingRulesRequest{
		ShardRoutingRules: srr,
		SkipRebuild:       applyShardRoutingRulesOptions.SkipRebuild,
		RebuildCells:      applyShardRoutingRulesOptions.Cells,
	})
	if err != nil {
		return err
	}

	fmt.Printf("New ShardRoutingRules object:\n%s\nIf this is not what you expected, check the input data (as JSON parsing will skip unexpected fields).\n", data)

	if applyRoutingRulesOptions.SkipRebuild {
		fmt.Println("Skipping rebuild of VSchema graph as requested, you will need to run RebuildVSchemaGraph for the changes to take effect.")
	}

	return nil
}

func commandGetShardRoutingRules(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	resp, err := client.GetShardRoutingRules(commandCtx, &vtctldatapb.GetShardRoutingRulesRequest{})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.ShardRoutingRules)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

func init() {
	ApplyShardRoutingRules.Flags().StringVarP(&applyShardRoutingRulesOptions.Rules, "rules", "r", "", "Shard routing rules, specified as a string")
	ApplyShardRoutingRules.Flags().StringVarP(&applyShardRoutingRulesOptions.RulesFilePath, "rules-file", "f", "", "Path to a file containing shard routing rules specified as JSON")
	ApplyShardRoutingRules.Flags().StringSliceVarP(&applyShardRoutingRulesOptions.Cells, "cells", "c", nil, "Limit the VSchema graph rebuilding to the specified cells. Ignored if --skip-rebuild is specified.")
	ApplyShardRoutingRules.Flags().BoolVar(&applyShardRoutingRulesOptions.SkipRebuild, "skip-rebuild", false, "Skip rebuilding the SrvVSchema objects.")
	ApplyShardRoutingRules.Flags().BoolVarP(&applyShardRoutingRulesOptions.DryRun, "dry-run", "d", false, "Validate the specified shard routing rules and note actions that would be taken, but do not actually apply the rules to the topo.")
	Root.AddCommand(ApplyShardRoutingRules)

	Root.AddCommand(GetShardRoutingRules)
}
