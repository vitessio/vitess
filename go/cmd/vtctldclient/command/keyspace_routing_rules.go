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
	// ApplyKeyspaceRoutingRules makes an ApplyKeyspaceRoutingRules gRPC call to a vtctld.
	ApplyKeyspaceRoutingRules = &cobra.Command{
		Use:                   "ApplyKeyspaceRoutingRules {--rules RULES | --rules-file RULES_FILE} [--cells=c1,c2,...] [--skip-rebuild] [--dry-run]",
		Short:                 "Applies the provided keyspace routing rules.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.NoArgs,
		PreRunE:               validateApplyKeyspaceRoutingRulesOptions,
		RunE:                  commandApplyKeyspaceRoutingRules,
	}
	// GetKeyspaceRoutingRules makes a GetKeyspaceRoutingRules gRPC call to a vtctld.
	GetKeyspaceRoutingRules = &cobra.Command{
		Use:                   "GetKeyspaceRoutingRules",
		Short:                 "Displays the currently active keyspace routing rules.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.NoArgs,
		RunE:                  commandGetKeyspaceRoutingRules,
	}
)

func validateApplyKeyspaceRoutingRulesOptions(cmd *cobra.Command, args []string) error {
	opts := applyKeyspaceRoutingRulesOptions
	if (opts.Rules != "" && opts.RulesFilePath != "") || (opts.Rules == "" && opts.RulesFilePath == "") {
		return errors.New("must pass exactly one of --rules or --rules-file")
	}
	return nil
}

var applyKeyspaceRoutingRulesOptions = struct {
	Rules         string
	RulesFilePath string
	Cells         []string
	SkipRebuild   bool
	DryRun        bool
}{}

func commandApplyKeyspaceRoutingRules(cmd *cobra.Command, args []string) error {
	opts := applyKeyspaceRoutingRulesOptions
	cli.FinishedParsing(cmd)
	var rulesBytes []byte
	if opts.RulesFilePath != "" {
		data, err := os.ReadFile(opts.RulesFilePath)
		if err != nil {
			return err
		}
		rulesBytes = data
	} else {
		rulesBytes = []byte(opts.Rules)
	}

	krr := &vschemapb.KeyspaceRoutingRules{}
	if err := json2.UnmarshalPB(rulesBytes, krr); err != nil {
		return err
	}

	if opts.DryRun {
		// Round-trip so that when we display the result it's readable.
		data, err := cli.MarshalJSON(krr)
		if err != nil {
			return err
		}

		fmt.Printf("[DRY RUN] Would have saved new KeyspaceRoutingRules object:\n%s\n", data)

		if opts.SkipRebuild {
			fmt.Println("[DRY RUN] Would not have rebuilt VSchema graph, would have required operator to run RebuildVSchemaGraph for changes to take effect.")
		} else {
			fmt.Print("[DRY RUN] Would have rebuilt the VSchema graph")
			if len(opts.Cells) == 0 {
				fmt.Print(" in all cells\n")
			} else {
				fmt.Printf(" in the following cells: %s.\n", strings.Join(applyKeyspaceRoutingRulesOptions.Cells, ", "))
			}
		}
		return nil
	}

	resp, err := client.ApplyKeyspaceRoutingRules(commandCtx, &vtctldatapb.ApplyKeyspaceRoutingRulesRequest{
		KeyspaceRoutingRules: krr,
		SkipRebuild:          opts.SkipRebuild,
		RebuildCells:         opts.Cells,
	})
	if err != nil {
		return err
	}

	respJSON, err := cli.MarshalJSON(resp)
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", respJSON)
	return nil
}

func commandGetKeyspaceRoutingRules(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	resp, err := client.GetKeyspaceRoutingRules(commandCtx, &vtctldatapb.GetKeyspaceRoutingRulesRequest{})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.KeyspaceRoutingRules)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

func init() {
	ApplyKeyspaceRoutingRules.Flags().StringVarP(&applyKeyspaceRoutingRulesOptions.Rules, "rules", "r", "", "Keyspace routing rules, specified as a string")
	ApplyKeyspaceRoutingRules.Flags().StringVarP(&applyKeyspaceRoutingRulesOptions.RulesFilePath, "rules-file", "f", "", "Path to a file containing keyspace routing rules specified as JSON")
	ApplyKeyspaceRoutingRules.Flags().StringSliceVarP(&applyKeyspaceRoutingRulesOptions.Cells, "cells", "c", nil, "Limit the VSchema graph rebuilding to the specified cells. Ignored if --skip-rebuild is specified.")
	ApplyKeyspaceRoutingRules.Flags().BoolVar(&applyKeyspaceRoutingRulesOptions.SkipRebuild, "skip-rebuild", false, "Skip rebuilding the SrvVSchema objects.")
	ApplyKeyspaceRoutingRules.Flags().BoolVarP(&applyKeyspaceRoutingRulesOptions.DryRun, "dry-run", "d", false, "Validate the specified keyspace routing rules and note actions that would be taken, but do not actually apply the rules to the topo.")
	Root.AddCommand(ApplyKeyspaceRoutingRules)
	Root.AddCommand(GetKeyspaceRoutingRules)
}
