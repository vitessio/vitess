/*
Copyright 2025 The Vitess Authors.

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
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func captureStdout(f func()) string {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	f()

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	return buf.String()
}

func TestPosition(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		expectedOutput string
		expectedError  string
	}{
		{
			name:           "equal - same positions",
			args:           []string{"equal", "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5", "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
			expectedOutput: "true\n",
		},
		{
			name:           "equal - different positions",
			args:           []string{"equal", "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5", "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6"},
			expectedOutput: "false\n",
		},
		{
			name:           "equal - different server uuids",
			args:           []string{"equal", "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5", "MySQL56/4e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
			expectedOutput: "false\n",
		},
		{
			name:           "at_least - pos1 ahead of pos2",
			args:           []string{"at_least", "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-10", "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
			expectedOutput: "true\n",
		},
		{
			name:           "at_least - pos1 behind pos2",
			args:           []string{"at_least", "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5", "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-10"},
			expectedOutput: "false\n",
		},
		{
			name:           "at_least - equal positions",
			args:           []string{"at_least", "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5", "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
			expectedOutput: "true\n",
		},
		{
			name:           "append - valid gtid",
			args:           []string{"append", "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5", "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:6"},
			expectedOutput: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6\n",
		},
		{
			name:           "append - gtid from different server",
			args:           []string{"append", "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5", "MySQL56/4e11fa47-71ca-11e1-9e33-c80aa9429562:1"},
			expectedOutput: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5,4e11fa47-71ca-11e1-9e33-c80aa9429562:1\n",
		},
		{
			name:           "append - to position with multiple GTIDs",
			args:           []string{"append", "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5,4e11fa47-71ca-11e1-9e33-c80aa9429562:1-3", "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:6"},
			expectedOutput: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6,4e11fa47-71ca-11e1-9e33-c80aa9429562:1-3\n",
		},
		{
			name:          "equal - invalid pos1",
			args:          []string{"equal", "invalid-position", "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
			expectedError: "parse error",
		},
		{
			name:          "equal - invalid pos2",
			args:          []string{"equal", "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5", "invalid-position"},
			expectedError: "parse error",
		},
		{
			name:          "at_least - invalid pos1",
			args:          []string{"at_least", "invalid-position", "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
			expectedError: "parse error",
		},
		{
			name:          "at_least - invalid pos2",
			args:          []string{"at_least", "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5", "invalid-position"},
			expectedError: "parse error",
		},
		{
			name:          "append - invalid position",
			args:          []string{"append", "invalid-position", "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:6"},
			expectedError: "parse error",
		},
		{
			name:          "append - invalid gtid",
			args:          []string{"append", "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5", "invalid-gtid"},
			expectedError: "parse error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var output string
			var err error

			if tt.expectedError == "" {
				output = captureStdout(func() {
					err = commandPosition(Position, tt.args)
				})
			} else {
				err = commandPosition(Position, tt.args)
			}

			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedOutput, output)
			}
		})
	}
}

func TestPositionCommand_ArgsValidation(t *testing.T) {
	tests := []struct {
		name          string
		args          []string
		expectedError string
	}{
		{
			name:          "invalid operation",
			args:          []string{"invalid_op", "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5", "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
			expectedError: "invalid operation invalid_op",
		},
		{
			name:          "no args",
			args:          []string{},
			expectedError: "accepts 3 arg(s), received 0",
		},
		{
			name:          "one arg",
			args:          []string{"equal"},
			expectedError: "accepts 3 arg(s), received 1",
		},
		{
			name:          "two args",
			args:          []string{"equal", "pos1"},
			expectedError: "accepts 3 arg(s), received 2",
		},
		{
			name:          "four args",
			args:          []string{"equal", "pos1", "pos2", "extra"},
			expectedError: "accepts 3 arg(s), received 4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Position.Args(Position, tt.args)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedError)
		})
	}
}
