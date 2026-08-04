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

package common

import (
	"bytes"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestOptionalBoolFromFlag(t *testing.T) {
	t.Run("returns nil when flag was not provided", func(t *testing.T) {
		cmd := &cobra.Command{Use: "test"}
		cmd.Flags().Bool("keep-data", false, "")

		require.Nil(t, OptionalBoolFromFlag(cmd, "keep-data", false))
	})

	t.Run("returns false when flag was explicitly set false", func(t *testing.T) {
		cmd := &cobra.Command{Use: "test"}
		cmd.Flags().Bool("keep-data", true, "")
		require.NoError(t, cmd.Flags().Set("keep-data", "false"))

		got := OptionalBoolFromFlag(cmd, "keep-data", false)
		require.NotNil(t, got)
		require.False(t, *got)
	})

	t.Run("returns true when flag was explicitly set true", func(t *testing.T) {
		cmd := &cobra.Command{Use: "test"}
		cmd.Flags().Bool("keep-data", false, "")
		require.NoError(t, cmd.Flags().Set("keep-data", "true"))

		got := OptionalBoolFromFlag(cmd, "keep-data", true)
		require.NotNil(t, got)
		require.True(t, *got)
	})
}

func TestAppendWarnings(t *testing.T) {
	var buf bytes.Buffer

	AppendWarnings(&buf, []string{"first warning", "second warning"})

	require.Equal(t, "WARNING: first warning\nWARNING: second warning\n\n", buf.String())
}
