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

package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/rulesctl/common"
)

func Remove() *cobra.Command {
	var removeOptName string
	var removeOptDryRun bool

	removeCmd := &cobra.Command{
		Use:   "remove-rule",
		Short: "Removes a named rule from the config file",
		Args:  cobra.NoArgs,
	}

	removeCmd.Flags().StringVarP(
		&removeOptName,
		"name", "n",
		"",
		"The named rule to remove (required)")
	removeCmd.Flags().BoolVarP(
		&removeOptDryRun,
		"dry-run", "d",
		false,
		"Instead of writing the config file back print the result to stdout")
	removeCmd.MarkFlagRequired("name")

	removeCmd.Run = func(cmd *cobra.Command, args []string) {
		rules := common.GetRules(configFile)
		if deleted := rules.Delete(removeOptName); deleted == nil {
			fmt.Printf("No rule found: '%v'", removeOptName)
			return
		}

		if removeOptDryRun {
			common.MustPrintJSON(rules)
		} else {
			common.MustWriteJSON(rules, configFile)
		}
	}

	return removeCmd
}
