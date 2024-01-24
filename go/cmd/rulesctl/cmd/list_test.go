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

func TestList(t *testing.T) {
	cmd := List()
	require.NotNil(t, cmd)
	require.Equal(t, "list", cmd.Name())
	configFile = "./testdata/rules.json"

	tests := []struct {
		name           string
		args           []string
		expectedOutput string
	}{
		{
			name:           "No args",
			expectedOutput: "[\n  {\n    \"Description\": \"Some value\",\n    \"Name\": \"Name\",\n    \"Action\": \"FAIL\"\n  }\n]\n",
		},
		{
			name:           "Name only",
			args:           []string{"--names-only=true"},
			expectedOutput: "[\n  \"Name\"\n]\n",
		},
		{
			name:           "Name flag set",
			args:           []string{"--name=Name"},
			expectedOutput: "\"Name\"\n",
		},
		{
			name:           "Random name in name flag",
			args:           []string{"--name=Random"},
			expectedOutput: "\"\"\n",
		},
		{
			name:           "Random name in name flag and names-only false",
			args:           []string{"--name=Random", "--names-only=false"},
			expectedOutput: "null\n",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if tt.args != nil {
				cmd.SetArgs(tt.args)
				err := cmd.Execute()
				require.NoError(t, err)
			}

			// Redirect stdout to a buffer
			rescueStdout := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w

			cmd.Run(&cobra.Command{}, []string{})

			w.Close()
			got, _ := io.ReadAll(r)
			os.Stdout = rescueStdout

			require.Contains(t, string(got), tt.expectedOutput)
		})
	}
}
