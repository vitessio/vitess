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

package cli

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
)

const exitTestEnv = "TEST_VTORC_EXIT"

// TestRunLogsConfigAndStartError checks the configuration record and the
// record logged before vtorc exits when discovery can't start, here because
// --cell is not set. The exit runs in a child process.
func TestRunLogsConfigAndStartError(t *testing.T) {
	if os.Getenv(exitTestEnv) == t.Name() {
		log.SwapLogger(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
		run(Main, nil)
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

	records := make(map[string]map[string]any)
	for line := range strings.SplitSeq(strings.TrimSpace(stderr.String()), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) == nil {
			records[record["msg"].(string)] = record
		}
	}

	record := records["Running with Configuration"]
	require.NotNil(t, record, stderr.String())
	assert.Equal(t, "INFO", record["level"])
	assert.Contains(t, record["config"], "instance-poll-time:")

	record = records["Failed to start vtorc"]
	require.NotNil(t, record, stderr.String())
	assert.Equal(t, "ERROR", record["level"])
	assert.Contains(t, record["error"], "--cell is required")
	assert.Equal(t, "FAILED_PRECONDITION", record["error_code"])
}
