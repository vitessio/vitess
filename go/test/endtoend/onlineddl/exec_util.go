/*
Copyright 2021 The Vitess Authors.

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

package onlineddl

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

// CreateTempScript creates a script in the temporary directory with given content
func CreateTempScript(t *testing.T, content string) (fileName string) {
	f, err := os.CreateTemp("", "onlineddl-test-")
	require.NoError(t, err)

	_, err = f.WriteString(content)
	require.NoError(t, err)
	err = f.Close()
	require.NoError(t, err)

	return f.Name()
}

// MysqlClientExecFile runs a file through the mysql client
func MysqlClientExecFile(t *testing.T, mysqlParams *mysql.ConnParams, testDataPath, testName string, fileName string) (output string) {
	t.Helper()

	bashPath, err := exec.LookPath("bash")
	require.NoError(t, err)
	mysqlPath, err := exec.LookPath("mysql")
	require.NoError(t, err)

	filePath := fileName
	if !filepath.IsAbs(fileName) {
		filePath, _ = filepath.Abs(path.Join(testDataPath, testName, fileName))
	}
	bashCommand := fmt.Sprintf(`%s -u%s --socket=%s --database=%s -s -s < %s 2> /tmp/error.log`, mysqlPath, mysqlParams.Uname, mysqlParams.UnixSocket, mysqlParams.DbName, filePath)
	cmd, err := exec.Command(
		bashPath,
		"-c",
		bashCommand,
	).Output()

	require.NoError(t, err)
	return string(cmd)
}
