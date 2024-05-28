/*
Copyright 2024 The Vitess Authors.

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
	// ApplyMirrorRules makes an ApplyMirrorRules gRPC call to a vtctld.
	ApplyMirrorRules = &cobra.Command{
		Use:                   "ApplyMirrorRules {--rules RULES | --rules-file RULES_FILE} [--cells=c1,c2,...] [--skip-rebuild] [--dry-run]",
		Short:                 "Applies the VSchema mirror rules.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.NoArgs,
		RunE:                  commandApplyMirrorRules,
	}
	// GetMirrorRules makes a GetMirrorRules gRPC call to a vtctld.
	GetMirrorRules = &cobra.Command{
		Use:                   "GetMirrorRules",
		Short:                 "Displays the VSchema mirror rules.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.NoArgs,
		RunE:                  commandGetMirrorRules,
	}
)

var applyMirrorRulesOptions = struct {
	Rules         string
	RulesFilePath string
	Cells         []string
	SkipRebuild   bool
	DryRun        bool
}{}

func commandApplyMirrorRules(cmd *cobra.Command, args []string) error {
	if applyMirrorRulesOptions.Rules != "" && applyMirrorRulesOptions.RulesFilePath != "" {
		return fmt.Errorf("cannot pass both --rules (=%s) and --rules-file (=%s)", applyMirrorRulesOptions.Rules, applyMirrorRulesOptions.RulesFilePath)
	}

	if applyMirrorRulesOptions.Rules == "" && applyMirrorRulesOptions.RulesFilePath == "" {
		return errors.New("must pass exactly one of --rules or --rules-file")
	}

	cli.FinishedParsing(cmd)

	var rulesBytes []byte
	if applyMirrorRulesOptions.RulesFilePath != "" {
		data, err := os.ReadFile(applyMirrorRulesOptions.RulesFilePath)
		if err != nil {
			return err
		}

		rulesBytes = data
	} else {
		rulesBytes = []byte(applyMirrorRulesOptions.Rules)
	}

	mr := &vschemapb.MirrorRules{}
	if err := json2.Unmarshal(rulesBytes, &mr); err != nil {
		return err
	}

	// Round-trip so when we display the result it's readable.
	data, err := cli.MarshalJSON(mr)
	if err != nil {
		return err
	}

	if applyMirrorRulesOptions.DryRun {
		fmt.Printf("[DRY RUN] Would have saved new MirrorRules object:\n%s\n", data)

		if applyMirrorRulesOptions.SkipRebuild {
			fmt.Println("[DRY RUN] Would not have rebuilt VSchema graph, would have required operator to run RebuildVSchemaGraph for changes to take effect")
		} else {
			fmt.Print("[DRY RUN] Would have rebuilt the VSchema graph")
			if len(applyMirrorRulesOptions.Cells) == 0 {
				fmt.Print(" in all cells\n")
			} else {
				fmt.Printf(" in the following cells: %s.\n", strings.Join(applyMirrorRulesOptions.Cells, ", "))
			}
		}

		return nil
	}

	_, err = client.ApplyMirrorRules(commandCtx, &vtctldatapb.ApplyMirrorRulesRequest{
		MirrorRules:  mr,
		SkipRebuild:  applyMirrorRulesOptions.SkipRebuild,
		RebuildCells: applyMirrorRulesOptions.Cells,
	})
	if err != nil {
		return err
	}

	fmt.Printf("New MirrorRules object:\n%s\nIf this is not what you expected, check the input data (as JSON parsing will skip unexpected fields).\n", data)

	if applyMirrorRulesOptions.SkipRebuild {
		fmt.Println("Skipping rebuild of VSchema graph, will need to run RebuildVSchemaGraph for changes to take effect.")
	}

	return nil
}

func commandGetMirrorRules(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	resp, err := client.GetMirrorRules(commandCtx, &vtctldatapb.GetMirrorRulesRequest{})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.MirrorRules)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

func init() {
	ApplyMirrorRules.Flags().StringVarP(&applyMirrorRulesOptions.Rules, "rules", "r", "", "Mirror rules, specified as a string.")
	ApplyMirrorRules.Flags().StringVarP(&applyMirrorRulesOptions.RulesFilePath, "rules-file", "f", "", "Path to a file containing mirror rules specified as JSON.")
	ApplyMirrorRules.Flags().StringSliceVarP(&applyMirrorRulesOptions.Cells, "cells", "c", nil, "Limit the VSchema graph rebuilding to the specified cells. Ignored if --skip-rebuild is specified.")
	ApplyMirrorRules.Flags().BoolVar(&applyMirrorRulesOptions.SkipRebuild, "skip-rebuild", false, "Skip rebuilding the SrvVSchema objects.")
	ApplyMirrorRules.Flags().BoolVarP(&applyMirrorRulesOptions.DryRun, "dry-run", "d", false, "Load the specified mirror rules as a validation step, but do not actually apply the rules to the topo.")
	Root.AddCommand(ApplyMirrorRules)

	Root.AddCommand(GetMirrorRules)
}
