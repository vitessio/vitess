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
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

func Validate() *cobra.Command {
	validateCmd := &cobra.Command{
		Use:   "validate",
		Short: "Validate the rules in the config file without applying them",
		Long: "Validate parses every rule in the config file using the same logic vttablet " +
			"uses when loading it and reports all invalid rules, so it can gate CI or a " +
			"config rollout. It exits non-zero if any rule is invalid.",
		Args: cobra.NoArgs,
	}

	validateCmd.Run = func(cmd *cobra.Command, args []string) {
		numRules, errs := validateRulesFile(configFile)
		if len(errs) == 0 {
			fmt.Printf("%d rule(s) in %q are valid\n", numRules, configFile)
			return
		}
		for _, err := range errs {
			fmt.Fprintf(os.Stderr, "%v\n", err)
		}
		log.Fatalf("%d of %d rule(s) in %q are invalid", len(errs), numRules, configFile)
	}

	return validateCmd
}

// validateRulesFile parses every rule in the file individually — with the
// same BuildQueryRule logic vttablet uses when loading it — and returns all
// per-rule errors, rather than stopping at the first one the way
// rules.UnmarshalJSON does.
func validateRulesFile(path string) (numRules int, errs []error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, []error{err}
	}

	var rulesInfo []map[string]any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&rulesInfo); err != nil {
		return 0, []error{fmt.Errorf("not a valid JSON list of rules: %v", err)}
	}

	for i, ruleInfo := range rulesInfo {
		if _, err := rules.BuildQueryRule(ruleInfo); err != nil {
			name := "?"
			if n, ok := ruleInfo["Name"].(string); ok {
				name = n
			}
			errs = append(errs, fmt.Errorf("rule %d (Name: %s): %v", i, name, err))
		}
	}
	return len(rulesInfo), errs
}
