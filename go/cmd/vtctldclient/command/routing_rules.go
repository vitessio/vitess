/*
Copyright 2021 The Vitess Authors.

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
	// ApplyRoutingRules makes an ApplyRoutingRules gRPC call to a vtctld.
	ApplyRoutingRules = &cobra.Command{
		Use:                   "ApplyRoutingRules {--rules RULES | --rules-file RULES_FILE} [--cells=c1,c2,...] [--skip-rebuild] [--dry-run]",
		Short:                 "Applies the VSchema routing rules.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.NoArgs,
		RunE:                  commandApplyRoutingRules,
	}
	// GetRoutingRules makes a GetRoutingRules gRPC call to a vtctld.
	GetRoutingRules = &cobra.Command{
		Use:                   "GetRoutingRules",
		Short:                 "Displays the VSchema routing rules.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.NoArgs,
		RunE:                  commandGetRoutingRules,
	}
)

var applyRoutingRulesOptions = struct {
	Rules         string
	RulesFilePath string
	Cells         []string
	SkipRebuild   bool
	DryRun        bool
}{}

func commandApplyRoutingRules(cmd *cobra.Command, args []string) error {
	if applyRoutingRulesOptions.Rules != "" && applyRoutingRulesOptions.RulesFilePath != "" {
		return fmt.Errorf("cannot pass both --rules (=%s) and --rules-file (=%s)", applyRoutingRulesOptions.Rules, applyRoutingRulesOptions.RulesFilePath)
	}

	if applyRoutingRulesOptions.Rules == "" && applyRoutingRulesOptions.RulesFilePath == "" {
		return errors.New("must pass exactly one of --rules or --rules-file")
	}

	cli.FinishedParsing(cmd)

	var rulesBytes []byte
	if applyRoutingRulesOptions.RulesFilePath != "" {
		data, err := os.ReadFile(applyRoutingRulesOptions.RulesFilePath)
		if err != nil {
			return err
		}

		rulesBytes = data
	} else {
		rulesBytes = []byte(applyRoutingRulesOptions.Rules)
	}

	rr := &vschemapb.RoutingRules{}
	if err := json2.UnmarshalPB(rulesBytes, rr); err != nil {
		return err
	}

	// Round-trip so when we display the result it's readable.
	data, err := cli.MarshalJSON(rr)
	if err != nil {
		return err
	}

	if applyRoutingRulesOptions.DryRun {
		fmt.Printf("[DRY RUN] Would have saved new RoutingRules object:\n%s\n", data)

		if applyRoutingRulesOptions.SkipRebuild {
			fmt.Println("[DRY RUN] Would not have rebuilt VSchema graph, would have required operator to run RebuildVSchemaGraph for changes to take effect")
		} else {
			fmt.Print("[DRY RUN] Would have rebuilt the VSchema graph")
			if len(applyRoutingRulesOptions.Cells) == 0 {
				fmt.Print(" in all cells\n")
			} else {
				fmt.Printf(" in the following cells: %s.\n", strings.Join(applyRoutingRulesOptions.Cells, ", "))
			}
		}

		return nil
	}

	_, err = client.ApplyRoutingRules(commandCtx, &vtctldatapb.ApplyRoutingRulesRequest{
		RoutingRules: rr,
		SkipRebuild:  applyRoutingRulesOptions.SkipRebuild,
		RebuildCells: applyRoutingRulesOptions.Cells,
	})
	if err != nil {
		return err
	}

	fmt.Printf("New RoutingRules object:\n%s\nIf this is not what you expected, check the input data (as JSON parsing will skip unexpected fields).\n", data)

	if applyRoutingRulesOptions.SkipRebuild {
		fmt.Println("Skipping rebuild of VSchema graph, will need to run RebuildVSchemaGraph for changes to take effect.")
	}

	return nil
}

func commandGetRoutingRules(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	resp, err := client.GetRoutingRules(commandCtx, &vtctldatapb.GetRoutingRulesRequest{})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.RoutingRules)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

func init() {
	ApplyRoutingRules.Flags().StringVarP(&applyRoutingRulesOptions.Rules, "rules", "r", "", "Routing rules, specified as a string.")
	ApplyRoutingRules.Flags().StringVarP(&applyRoutingRulesOptions.RulesFilePath, "rules-file", "f", "", "Path to a file containing routing rules specified as JSON.")
	ApplyRoutingRules.Flags().StringSliceVarP(&applyRoutingRulesOptions.Cells, "cells", "c", nil, "Limit the VSchema graph rebuilding to the specified cells. Ignored if --skip-rebuild is specified.")
	ApplyRoutingRules.Flags().BoolVar(&applyRoutingRulesOptions.SkipRebuild, "skip-rebuild", false, "Skip rebuilding the SrvVSchema objects.")
	ApplyRoutingRules.Flags().BoolVarP(&applyRoutingRulesOptions.DryRun, "dry-run", "d", false, "Load the specified routing rules as a validation step, but do not actually apply the rules to the topo.")
	Root.AddCommand(ApplyRoutingRules)

	Root.AddCommand(GetRoutingRules)
}
