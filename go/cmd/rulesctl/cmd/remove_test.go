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

func TestRemoveOld(t *testing.T) {
	removeCmd := Remove()

	require.NotNil(t, removeCmd)
	require.Equal(t, "remove-rule", removeCmd.Name())

	originalStdOut := os.Stdout
	defer func() {
		os.Stdout = originalStdOut
	}()
	// Redirect stdout to a buffer
	r, w, _ := os.Pipe()
	os.Stdout = w

	configFile = "../common/testdata/rules.json"
	removeCmd.Run(&cobra.Command{}, []string{""})

	err := w.Close()
	require.NoError(t, err)
	got, err := io.ReadAll(r)
	require.NoError(t, err)

	expected := "No rule found:"
	require.Contains(t, string(got), expected)
}

func TestRemove(t *testing.T) {
	cmd := Remove()
	require.NotNil(t, cmd)
	require.Equal(t, "remove-rule", cmd.Name())
	configFile = "./testdata/rules.json"
	defer func() {
		_ = os.WriteFile(configFile, []byte(`[
    {
        "Description": "Some value",
        "Name": "Name"
    }
]
`), 0777)
	}()

	tests := []struct {
		name           string
		args           []string
		expectedOutput string
	}{
		{
			name:           "No args",
			expectedOutput: "No rule found: ''",
		},
		{
			name:           "Dry run and name both set",
			args:           []string{"--dry-run=true", "--name=Name"},
			expectedOutput: "[]\n",
		},
		{
			name:           "Dry run not set name set",
			args:           []string{"--dry-run=false", "--name=Name"},
			expectedOutput: "No rule found: 'Name'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			originalStdOut := os.Stdout
			defer func() {
				os.Stdout = originalStdOut
			}()
			// Redirect stdout to a buffer
			r, w, _ := os.Pipe()
			os.Stdout = w

			if tt.args != nil {
				cmd.SetArgs(tt.args)
				err := cmd.Execute()
				require.NoError(t, err)
			}
			cmd.Run(&cobra.Command{}, []string{})

			err := w.Close()
			require.NoError(t, err)
			got, err := io.ReadAll(r)
			require.NoError(t, err)

			require.Contains(t, string(got), tt.expectedOutput)
		})
	}
}
