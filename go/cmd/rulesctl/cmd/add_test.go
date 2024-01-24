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

func TestAdd(t *testing.T) {
	cmd := Add()
	require.NotNil(t, cmd)
	require.Equal(t, "add-rule", cmd.Name())
	configFile = "./testdata/rules.json"

	tests := []struct {
		name           string
		args           []string
		expectedOutput string
	}{
		{
			name:           "Action fail",
			args:           []string{"--dry-run=true", "--name=Rule", `--description="New rules that will be added to the file"`, "--action=fail", "--plan=Select"},
			expectedOutput: "[\n  {\n    \"Description\": \"Some value\",\n    \"Name\": \"Name\",\n    \"Action\": \"FAIL\"\n  },\n  {\n    \"Description\": \"\\\"New rules that will be added to the file\\\"\",\n    \"Name\": \"Rule\",\n    \"Plans\": [\n      \"Select\"\n    ],\n    \"Action\": \"FAIL\"\n  }\n]\n",
		},
		{
			name:           "Action fail_retry",
			args:           []string{"--dry-run=true", "--name=Rule", `--description="New rules that will be added to the file"`, "--action=fail_retry", "--plan=Select"},
			expectedOutput: "[\n  {\n    \"Description\": \"Some value\",\n    \"Name\": \"Name\",\n    \"Action\": \"FAIL\"\n  },\n  {\n    \"Description\": \"\\\"New rules that will be added to the file\\\"\",\n    \"Name\": \"Rule\",\n    \"Plans\": [\n      \"Select\",\n      \"Select\"\n    ],\n    \"Action\": \"FAIL_RETRY\"\n  }\n]\n",
		},
		{
			name:           "Action continue with query",
			args:           []string{"--dry-run=true", "--name=Rule", `--description="New rules that will be added to the file"`, "--action=continue", "--plan=Select", "--query=secret", "--leading-comment=None", "--trailing-comment=Yoho", "--table=Temp"},
			expectedOutput: "[\n  {\n    \"Description\": \"Some value\",\n    \"Name\": \"Name\",\n    \"Action\": \"FAIL\"\n  },\n  {\n    \"Description\": \"\\\"New rules that will be added to the file\\\"\",\n    \"Name\": \"Rule\",\n    \"Query\": \"secret\",\n    \"LeadingComment\": \"None\",\n    \"TrailingComment\": \"Yoho\",\n    \"Plans\": [\n      \"Select\",\n      \"Select\",\n      \"Select\"\n    ],\n    \"TableNames\": [\n      \"Temp\"\n    ]\n  }\n]\n",
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
