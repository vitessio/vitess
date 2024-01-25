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

package command

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestPosition(t *testing.T) {
	cmd := Position
	require.NotNil(t, cmd)
	require.Equal(t, "position", cmd.Name())

	tests := []struct {
		name        string
		args        []string
		expectedErr string
	}{
		{
			name:        "empty args",
			args:        []string{},
			expectedErr: "accepts 3 arg(s), received 0",
		},
		{
			name: "non-empty args",
			args: []string{"equal", "at_least", "append"},
		},
		{
			name:        "non-empty args",
			args:        []string{"temp", "at_least", "append"},
			expectedErr: "invalid operation temp (choices are 'equal', 'at_least', 'append')",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cmd.Args(&cobra.Command{}, tt.args)
			if tt.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.expectedErr)
			}
		})
	}
}

func TestPositionRun(t *testing.T) {
	cmd := Position
	require.NotNil(t, cmd)
	require.Equal(t, "position", cmd.Name())

	tests := []struct {
		name        string
		args        []string
		expectedErr string
	}{
		{
			name:        "parse error",
			args:        []string{"equal", "at_least", "append"},
			expectedErr: "parse error: unknown GTIDSet flavor \"\"",
		},
		{
			name: "only equal",
			args: []string{"equal", "", ""},
		},
		{
			name:        "only equal with error",
			args:        []string{"equal", "", "error"},
			expectedErr: "parse error: unknown GTIDSet flavor \"\"",
		},
		{
			name: "only append",
			args: []string{"append", "", ""},
		},
		{
			name:        "only append with error",
			args:        []string{"append", "", "error"},
			expectedErr: "parse error: unknown GTID flavor \"\"",
		},
		{
			name: "only at_least",
			args: []string{"at_least", "", ""},
		},
		{
			name:        "only at_least with error",
			args:        []string{"at_least", "", "error"},
			expectedErr: "parse error: unknown GTIDSet flavor \"\"",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cmd.RunE(&cobra.Command{}, tt.args)
			if tt.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.Equal(t, tt.expectedErr, err.Error())
			}
		})
	}
}
