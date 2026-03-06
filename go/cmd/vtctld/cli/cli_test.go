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

package cli

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/utils"
)

// TestExecute exercises the cobra Execute() path (PreRunE → RunE) with different
// os.Args so we know the CLI layer behaves as expected.
func TestExecute(t *testing.T) {
	t.Parallel()

	// Save and restore os.Args so this test doesn't leak into others.
	args := append([]string{}, os.Args...)
	t.Cleanup(func() {
		os.Args = append([]string{}, args...)
	})

	Main.SetGlobalNormalizationFunc(utils.NormalizeUnderscoresToDashes)

	t.Run("help succeeds", func(t *testing.T) {
		os.Args = []string{"vtctld", "--help"}
		err := Main.Execute()
		require.NoError(t, err) // help should always succeed
	})

	t.Run("unknown flag returns error", func(t *testing.T) {
		os.Args = []string{"vtctld", "--unknown-flag"}
		err := Main.Execute()
		assert.Error(t, err) // cobra should reject unknown flags
	})
}

// TestMainFlagRegistration checks that the flags we care about are actually
// registered on Main (from init + servenv + plugins). If any of these are
// missing, the binary would be broken at runtime.
func TestMainFlagRegistration(t *testing.T) {
	require.NotNil(t, Main.Flags().Lookup("port"), "servenv port flag should be on Main")
	require.NotNil(t, Main.Flags().Lookup("bind-address"), "servenv bind-address should be on Main")
	require.NotNil(t, Main.Flags().Lookup("grpc-port"), "servenv grpc-port flag should be on Main")
	require.NotNil(t, Main.Flags().Lookup("service-map"), "service-map flag should be on Main")
}

// TestMainCommandMetadata makes sure the cobra command is wired the way we
// expect: correct use string, help text, no positional args, and run hooks set.
func TestMainCommandMetadata(t *testing.T) {
	require.Equal(t, "vtctld", Main.Use)
	require.Contains(t, Main.Short, "cluster management")
	require.Contains(t, Main.Long, "vtctld")
	require.NotNil(t, Main.Args)
	// Main should accept no positional args and reject extras.
	require.NoError(t, Main.Args(Main, []string{}))
	require.Error(t, Main.Args(Main, []string{"extra"}))
	require.NotNil(t, Main.PreRunE)
	require.NotNil(t, Main.RunE)
	require.NotEmpty(t, Main.Version)
}
