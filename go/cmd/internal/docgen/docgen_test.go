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

package docgen

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestGenerateMarkdownTree(t *testing.T) {
	tests := []struct {
		name      string
		dir       string
		cmd       *cobra.Command
		expectErr bool
	}{
		{
			name:      "Empty dir",
			dir:       "",
			cmd:       &cobra.Command{},
			expectErr: true,
		},
		{
			name:      "current dir",
			dir:       "./",
			cmd:       &cobra.Command{},
			expectErr: false,
		},
		{
			name:      "Permission denied",
			dir:       "/root",
			cmd:       &cobra.Command{},
			expectErr: true,
		},
		{
			name:      "Not a directory error",
			dir:       "./docgen.go",
			cmd:       &cobra.Command{},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := GenerateMarkdownTree(tt.cmd, tt.dir)
			if !tt.expectErr {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestRestructure(t *testing.T) {
	rootCmd := &cobra.Command{
		Use: "root-command",
	}
	cmd := &cobra.Command{
		Use: "random",
	}
	rootCmd.AddCommand(cmd)
	cmds := []*cobra.Command{rootCmd}

	tests := []struct {
		name      string
		rootDir   string
		dir       string
		cmds      []*cobra.Command
		expectErr bool
	}{
		{
			name: "Empty commands",
			cmds: []*cobra.Command{},
		},
		{
			name:      "Non-empty commands",
			rootDir:   "../",
			dir:       "./",
			cmds:      cmds,
			expectErr: true,
		},
		{
			name:      "No subcommands",
			rootDir:   "../",
			dir:       "./",
			cmds:      []*cobra.Command{{Use: "help"}, {Use: "test-cmd"}},
			expectErr: true,
		},
		{
			name:      "No subcommands with rootDir and dir unset",
			cmds:      []*cobra.Command{{Use: "random"}},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := restructure(tt.rootDir, tt.dir, tt.name, tt.cmds)
			if !tt.expectErr {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestLinkHandler(t *testing.T) {
	tests := []struct {
		name        string
		fileName    string
		expectedStr string
	}{
		{
			name:        "Normal value",
			fileName:    "Some_value",
			expectedStr: "./some_value/",
		},
		{
			name:        "Abnormal value",
			fileName:    `./.jash13_24`,
			expectedStr: "../",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			str := linkHandler(tt.fileName)
			require.Equal(t, tt.expectedStr, str)
		})
	}
}

func TestNewParentLinkSedCommand(t *testing.T) {
	tests := []struct {
		name           string
		parentDir      string
		fileName       string
		expectedOutput string
	}{
		{
			name:           "Empty values",
			expectedOutput: "sed -i  -e s:(.//):(../):i ",
		},
		{
			name:           "Normal value",
			parentDir:      "./",
			fileName:       "Some_value",
			expectedOutput: "sed -i  -e s:(././/):(../):i Some_value",
		},
		{
			name:           "Abnormal value",
			parentDir:      "/root",
			fileName:       `./.jash13_24`,
			expectedOutput: "sed -i  -e s:(.//root/):(../):i ./.jash13_24",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := newParentLinkSedCommand(tt.parentDir, tt.fileName)
			// We only check for suffix because the sed command's actual path may differ on different machines.
			require.True(t, strings.HasSuffix(cmd.String(), tt.expectedOutput))
		})
	}
}

func TestGetCommitID(t *testing.T) {
	// This function should return an error when the reference is not in the
	// git tree.
	_, err := getCommitID("invalid ref")
	require.Error(t, err)
}
