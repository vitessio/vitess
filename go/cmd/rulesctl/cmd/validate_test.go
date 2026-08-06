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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateCommand(t *testing.T) {
	cmd := Validate()
	require.NotNil(t, cmd)
	require.Equal(t, "validate", cmd.Name())
}

func TestValidateRulesFile(t *testing.T) {
	t.Run("valid file", func(t *testing.T) {
		numRules, errs := validateRulesFile("./testdata/rules.json")
		require.Empty(t, errs)
		require.Equal(t, 1, numRules)
	})

	t.Run("invalid rules are all reported", func(t *testing.T) {
		numRules, errs := validateRulesFile("./testdata/invalid-rules.json")
		require.Equal(t, 3, numRules)
		require.Len(t, errs, 2)
		require.ErrorContains(t, errs[0], "bad_plan")
		require.ErrorContains(t, errs[1], "bad_attribute")
	})

	t.Run("missing file", func(t *testing.T) {
		_, errs := validateRulesFile("./testdata/does-not-exist.json")
		require.Len(t, errs, 1)
	})

	t.Run("not a JSON list", func(t *testing.T) {
		_, errs := validateRulesFile("./testdata/not-a-list.json")
		require.Len(t, errs, 1)
		require.ErrorContains(t, errs[0], "not a valid JSON list of rules")
	})
}
