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
	"fmt"
	"os"
	"path/filepath"
	"slices"
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

func TestVtMysqlRoot(t *testing.T) {
	envVar := "VT_MYSQL_ROOT"
	originalMySQLRoot := os.Getenv(envVar)
	defer os.Setenv(envVar, originalMySQLRoot)
	originalPATH := os.Getenv("PATH")
	defer os.Setenv("PATH", originalPATH)

	testcases := []struct {
		name   string
		envVal string
	}{
		{
			name:   "env var set",
			envVal: "/home/mysql/binaries",
		},
		{
			name: "env var unset",
		},
		{ // Second call allows us to verify that we're not adding to the PATH multiple times.
			name: "env var unset again",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			os.Setenv(envVar, tc.envVal)
			defer os.Setenv(envVar, "")

			path, err := VtMysqlRoot()
			if tc.envVal != "" {
				require.Equal(t, tc.envVal, path)
				require.NoError(t, err)
			}
			// We don't require a nil error as the test env may not have MySQL installed.
		})
	}

	// Confirm the PATH value now after all test runs.
	currentPATH := os.Getenv("PATH")
	if slices.Contains(filepath.SplitList(originalPATH), sbinPath) {
		// The PATH already had /usr/sbin and we should not have changed it.
		require.Equal(t, originalPATH, currentPATH)
	} else {
		// We should have prepended it exactly once.
		require.Equal(t, fmt.Sprintf("%s:%s", sbinPath, originalPATH), currentPATH)
	}
}
