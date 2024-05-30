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
			name: "Action fail",
			args: []string{"--dry-run=true", "--name=Rule", `--description="New rules that will be added to the file"`, "--action=fail", "--plan=Select"},
			expectedOutput: `[
  {
    "Description": "Some value",
    "Name": "Name",
    "Action": "FAIL"
  },
  {
    "Description": "\"New rules that will be added to the file\"",
    "Name": "Rule",
    "Plans": [
      "Select"
    ],
    "Action": "FAIL"
  }
]
`,
		},
		{
			name: "Action fail_retry",
			args: []string{"--dry-run=true", "--name=Rule", `--description="New rules that will be added to the file"`, "--action=fail_retry", "--plan=Select"},
			expectedOutput: `[
  {
    "Description": "Some value",
    "Name": "Name",
    "Action": "FAIL"
  },
  {
    "Description": "\"New rules that will be added to the file\"",
    "Name": "Rule",
    "Plans": [
      "Select",
      "Select"
    ],
    "Action": "FAIL_RETRY"
  }
]
`,
		},
		{
			name: "Action continue with query",
			args: []string{"--dry-run=true", "--name=Rule", `--description="New rules that will be added to the file"`, "--action=continue", "--plan=Select", "--query=secret", "--leading-comment=None", "--trailing-comment=Yoho", "--table=Temp"},
			expectedOutput: `[
  {
    "Description": "Some value",
    "Name": "Name",
    "Action": "FAIL"
  },
  {
    "Description": "\"New rules that will be added to the file\"",
    "Name": "Rule",
    "Query": "secret",
    "LeadingComment": "None",
    "TrailingComment": "Yoho",
    "Plans": [
      "Select",
      "Select",
      "Select"
    ],
    "TableNames": [
      "Temp"
    ]
  }
]
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.args != nil {
				cmd.SetArgs(tt.args)
				err := cmd.Execute()
				require.NoError(t, err)
			}

			originalStdOut := os.Stdout
			defer func() {
				os.Stdout = originalStdOut
			}()
			// Redirect stdout to a buffer
			r, w, _ := os.Pipe()
			os.Stdout = w

			cmd.Run(&cobra.Command{}, []string{})

			err := w.Close()
			require.NoError(t, err)
			got, err := io.ReadAll(r)
			require.NoError(t, err)
			require.EqualValues(t, tt.expectedOutput, string(got))
		})
	}
}
