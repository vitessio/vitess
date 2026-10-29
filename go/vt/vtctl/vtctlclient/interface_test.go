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

package vtctlclient

import (
	"bytes"
	"context"
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

const exitTestEnv = "TEST_VTCTLCLIENT_EXIT"

// TestRegisterFactoryDuplicateLogsProtocol checks the record logged before
// the process exits on a second registration of a protocol. The exit is run
// in a child process.
func TestRegisterFactoryDuplicateLogsProtocol(t *testing.T) {
	if os.Getenv(exitTestEnv) == t.Name() {
		log.SwapLogger(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
		factory := func(ctx context.Context, addr string) (VtctlClient, error) { return nil, nil }
		RegisterFactory("duplicate-test", factory)
		RegisterFactory("duplicate-test", factory)
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

	lines := strings.Split(strings.TrimSpace(stderr.String()), "\n")
	var record map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[len(lines)-1]), &record), stderr.String())
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, "RegisterFactory: factory already exists", record["msg"])
	assert.Equal(t, "duplicate-test", record["protocol"])
}
