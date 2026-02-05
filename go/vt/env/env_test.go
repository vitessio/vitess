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
	"path/filepath"
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

	// The test directory is used to create our fake mysqld binary.
	testDir := t.TempDir() // This is automatically cleaned up
	createExecutable := func(path string) error {
		fullPath := testDir + path
		err := os.MkdirAll(filepath.Dir(fullPath), 0755)
		require.NoError(t, err)
		return os.WriteFile(fullPath, []byte("test"), 0755)
	}

	type testcase struct {
		name              string
		preFunc           func() error
		vtMysqlRootEnvVal string
		pathEnvVal        string
		expect            string // The return value we expect from VtMysqlRoot()
		expectErr         string
	}
	testcases := []testcase{
		{
			name:              "VT_MYSQL_ROOT set",
			vtMysqlRootEnvVal: "/home/mysql/binaries",
		},
		{
			name: "VT_MYSQL_ROOT empty; PATH set without /usr/sbin",
			pathEnvVal: testDir + filepath.Dir(mysqldSbinPath) +
				":/usr/bin:/sbin:/bin:/usr/local/bin:/usr/local/sbin:/home/mysql/binaries",
			preFunc: func() error {
				return createExecutable(mysqldSbinPath)
			},
			expect: testDir + "/usr",
		},
	}

	// If /usr/sbin/mysqld exists, confirm that we find it even
	// when /usr/sbin is not in the PATH.
	_, err := os.Stat(mysqldSbinPath)
	if err == nil {
		t.Logf("Found %s, confirming auto detection behavior", mysqldSbinPath)
		testcases = append(testcases, testcase{
			name:   "VT_MYSQL_ROOT empty; PATH empty; mysqld in /usr/sbin",
			expect: "/usr",
		})
	} else {
		testcases = append(testcases, testcase{ // Error expected
			name:      "VT_MYSQL_ROOT empty; PATH empty; mysqld not in /usr/sbin",
			expectErr: errMysqldNotFound.Error(),
		})
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.preFunc != nil {
				err := tc.preFunc()
				require.NoError(t, err)
			}
			os.Setenv(envVar, tc.vtMysqlRootEnvVal)
			os.Setenv("PATH", tc.pathEnvVal)
			path, err := VtMysqlRoot()
			if tc.expectErr != "" {
				require.EqualError(t, err, tc.expectErr)
			} else {
				require.NoError(t, err)
			}
			if tc.vtMysqlRootEnvVal != "" {
				// This should always be returned.
				tc.expect = tc.vtMysqlRootEnvVal
			}
			require.Equal(t, tc.expect, path)
		})
	}
}
