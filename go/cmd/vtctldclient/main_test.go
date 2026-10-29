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

// TestMainLogsCommandError checks the record logged before vtctldclient exits when
// its command fails, here on an unknown flag. The exit runs in a child process.
func TestMainLogsCommandError(t *testing.T) {
	if os.Getenv("TEST_MAIN_EXIT") == t.Name() {
		log.SwapLogger(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
		os.Args = []string{"vtctldclient", "--no-such-flag"}
		main()
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$")
	cmd.Env = append(os.Environ(), "TEST_MAIN_EXIT="+t.Name())
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()

	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	assert.Equal(t, 1, exitErr.ExitCode())

	lines := strings.Split(strings.TrimSpace(stderr.String()), "\n")
	var record map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[len(lines)-1]), &record), stderr.String())
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, "command failed", record["msg"])
	assert.Equal(t, "unknown flag: --no-such-flag", record["error"])
	assert.Equal(t, "UNKNOWN", record["error_code"])
}
