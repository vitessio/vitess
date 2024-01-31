/*
Copyright 2019 The Vitess Authors.

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

package env

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVtDataRoot(t *testing.T) {
	envVar := "VTDATAROOT"
	oldEnvVar := os.Getenv(envVar)

	if oldEnvVar != "" {
		os.Setenv(envVar, "")
	}

	defer os.Setenv(envVar, oldEnvVar)

	root := VtDataRoot()
	if root != DefaultVtDataRoot {
		t.Errorf("When VTDATAROOT is not set, the default value should be %v, not %v.", DefaultVtDataRoot, root)
	}

	passed := "/tmp"
	os.Setenv(envVar, passed)
	root = VtDataRoot()
	if root != passed {
		t.Errorf("The value of VtDataRoot should be %v, not %v.", passed, root)
	}
}

func TestVtRoot(t *testing.T) {
	tests := []struct {
		name          string
		newVal        string
		args          []string
		expectedRoot  string
		expectedError string
	}{
		{
			name:         "empty env",
			expectedRoot: "/usr/local/vitess",
		},
		{
			name:         "root path already defined",
			newVal:       "test-vtroot",
			expectedRoot: "test-vtroot",
		},
		{
			name:         "path unset and args modified",
			args:         []string{"bin/arg1"},
			expectedRoot: "vitess/go/vt/env",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			envVar := "VTROOT"
			oldEnvVar := os.Getenv(envVar)
			oldArgs := os.Args

			defer func() {
				os.Setenv(envVar, oldEnvVar)
				os.Args = oldArgs
			}()

			os.Setenv(envVar, tt.newVal)
			if tt.args != nil {
				os.Args = tt.args
			}

			root, err := VtRoot()
			if tt.expectedError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.expectedError)
			}

			require.Contains(t, root, tt.expectedRoot)
		})
	}
}

func TestVtMysqlBaseDir(t *testing.T) {
	tests := []struct {
		name            string
		newMySqlRoot    string
		newMySqlBaseDir string
		expectedRoot    string
		expectedError   string
	}{
		{
			name: "empty env",
		},
		{
			name:            "only base dir set",
			newMySqlBaseDir: "test-base-dir",
			expectedRoot:    "test-base-dir",
		},
		{
			name:         "root path already defined",
			newMySqlRoot: "test-vtroot",
			expectedRoot: "test-vtroot",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			envVar := "VT_MYSQL_BASEDIR"
			envMySqlRoot := "VT_MYSQL_ROOT"
			oldEnvVar := os.Getenv(envVar)
			oldMySqlRoot := os.Getenv(envMySqlRoot)

			defer func() {
				os.Setenv(envVar, oldEnvVar)
				os.Setenv(envMySqlRoot, oldMySqlRoot)
			}()

			os.Setenv(envVar, tt.newMySqlBaseDir)
			os.Setenv(envMySqlRoot, tt.newMySqlRoot)

			root, err := VtMysqlBaseDir()
			if tt.expectedError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.expectedError)
			}

			require.Contains(t, root, tt.expectedRoot)
		})
	}
}
