/*
Copyright 2026 The Vitess Authors.

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

package main

import (
	"bytes"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const exitTestEnv = "TEST_VTCTLCLIENT_EXIT"

// ansiEscape matches the color codes in the text log format used in tests.
var ansiEscape = regexp.MustCompile("\x1b\\[[0-9;]*m")

// TestActionFailedLogsActionAndError checks the record logged before
// vtctlclient exits when a command fails. vtctlclient exits, so it runs in a
// child process. Flag parsing sets up the text log format used in tests.
func TestActionFailedLogsActionAndError(t *testing.T) {
	if os.Getenv(exitTestEnv) == t.Name() {
		os.Args = []string{"vtctlclient", "--server", "localhost:1", "--action-timeout", "1s", "GetKeyspace", "ks"}
		main()
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$")
	cmd.Env = append(os.Environ(), exitTestEnv+"="+t.Name())
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()

	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	assert.Equal(t, 1, exitErr.ExitCode())

	var line string
	for l := range strings.SplitSeq(ansiEscape.ReplaceAllString(stderr.String(), ""), "\n") {
		if strings.Contains(l, " action failed ") {
			line = l
		}
	}
	require.NotEmpty(t, line, stderr.String())
	assert.Contains(t, line, " ERR ")
	assert.Contains(t, line, " action=GetKeyspace ")
	assert.Contains(t, line, ` error="cannot execute remote command: `)
}
