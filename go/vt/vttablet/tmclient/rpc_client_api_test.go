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

package tmclient

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

const exitTestEnv = "TEST_TMCLIENT_EXIT"

// exitRecord runs the calling test in a subprocess with exitTestEnv set, and
// returns the last record it logged before exiting with status 1. The test
// checks exitTestEnv and calls the code that exits.
func exitRecord(t *testing.T) map[string]any {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$")
	cmd.Env = append(os.Environ(), exitTestEnv+"="+t.Name())
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()

	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	assert.Equal(t, 1, exitErr.ExitCode())

	lines := strings.Split(strings.TrimSpace(stderr.String()), "\n")
	var record map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[len(lines)-1]), &record), stderr.String())
	return record
}

func inExitSubprocess(t *testing.T) bool {
	if os.Getenv(exitTestEnv) != t.Name() {
		return false
	}
	log.SwapLogger(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	return true
}

func TestNewTabletManagerClientUnregisteredLogsProtocol(t *testing.T) {
	if inExitSubprocess(t) {
		tabletManagerProtocol = "unregistered"
		NewTabletManagerClient()
		return
	}

	record := exitRecord(t)
	assert.Equal(t, "No TabletManagerProtocol registered with name", record["msg"])
	assert.Equal(t, "unregistered", record["protocol"])
}
