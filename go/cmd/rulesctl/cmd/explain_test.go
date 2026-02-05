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

package cmd

import (
	"io"
	"os"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestExplainWithQueryPlanArguement(t *testing.T) {
	explainCmd := Explain()

	require.NotNil(t, explainCmd)
	require.Equal(t, "explain", explainCmd.Name())

	originalStdOut := os.Stdout
	defer func() {
		os.Stdout = originalStdOut
	}()
	// Redirect stdout to a buffer
	r, w, _ := os.Pipe()
	os.Stdout = w

	explainCmd.Run(&cobra.Command{}, []string{"query-plans"})

	err := w.Close()
	require.NoError(t, err)
	got, err := io.ReadAll(r)
	require.NoError(t, err)

	expected := "Query Plans!"
	require.Contains(t, string(got), expected)
}

func TestExplainWithRandomArguement(t *testing.T) {
	explainCmd := Explain()

	require.NotNil(t, explainCmd)
	require.Equal(t, "explain", explainCmd.Name())

	// Redirect stdout to a buffer
	originalStdOut := os.Stdout
	defer func() {
		os.Stdout = originalStdOut
	}()
	// Redirect stdout to a buffer
	r, w, _ := os.Pipe()
	os.Stdout = w

	explainCmd.Run(&cobra.Command{}, []string{"random"})

	err := w.Close()
	require.NoError(t, err)
	got, err := io.ReadAll(r)
	require.NoError(t, err)

	expected := "I don't know anything about"
	require.Contains(t, string(got), expected)
}
