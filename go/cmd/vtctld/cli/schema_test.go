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
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtenv"
)

const exitTestEnv = "TEST_VTCTLD_EXIT"

// TestInitSchemaLogsUnknownController checks the record logged before vtctld
// exits when the schema change controller doesn't exist. The exit runs in a
// child process.
func TestInitSchemaLogsUnknownController(t *testing.T) {
	if os.Getenv(exitTestEnv) == t.Name() {
		log.SwapLogger(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
		schemaChangeDir = t.TempDir()
		schemaChangeController = "no_such_controller"
		initSchema(t.Context())
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
	assert.Equal(t, "unable to get a controller factory", record["msg"])
	assert.Contains(t, record["error"], "no_such_controller")
}

// TestInitSchemaLogsSchemaChangeFailed checks the record logged when a schema
// change from the schema change dir fails, here because its keyspace doesn't
// exist: the error and its code are attributes. The schema change timer keeps
// running, so the check runs in a child process.
func TestInitSchemaLogsSchemaChangeFailed(t *testing.T) {
	if os.Getenv(exitTestEnv) == t.Name() {
		schemaChangeDir = t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(schemaChangeDir, "ks", "input"), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(schemaChangeDir, "ks", "input", "1.sql"), []byte("create table t (id int)"), 0o644))
		schemaChangeController = "local"
		schemaChangeCheckInterval = 10 * time.Millisecond
		ts = memorytopo.NewServer(t.Context(), "zone1")
		env = vtenv.NewTestEnv()

		logs := logtest.Capture(t, slog.LevelInfo)
		initSchema(t.Context())
		record := logs.Wait(t, "Schema change failed")
		line, err := json.Marshal(record)
		require.NoError(t, err)
		fmt.Println("RECORD " + string(line))
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$")
	cmd.Env = append(os.Environ(), exitTestEnv+"="+t.Name())
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))

	var record map[string]any
	for line := range strings.SplitSeq(string(out), "\n") {
		if rest, ok := strings.CutPrefix(line, "RECORD "); ok {
			require.NoError(t, json.Unmarshal([]byte(rest), &record))
		}
	}
	require.NotNil(t, record, string(out))
	assert.Equal(t, "ERROR", record["level"])
	assert.Contains(t, record["error"], "ks")
	assert.Equal(t, "UNKNOWN", record["error_code"])
}
