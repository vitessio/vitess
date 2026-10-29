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
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtenv"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

// TestVSchemaPersistenceLogs checks the records logged when a keyspace's
// vschema is persisted, loaded back, and fails to be written.
func TestVSchemaPersistenceLogs(t *testing.T) {
	oldDir, oldKeyspaces, oldEnv := vschemaPersistenceDir, tpb.Keyspaces, env
	t.Cleanup(func() {
		vschemaPersistenceDir, tpb.Keyspaces, env = oldDir, oldKeyspaces, oldEnv
	})
	dir := t.TempDir()
	vschemaPersistenceDir = dir
	tpb.Keyspaces = []*vttestpb.Keyspace{{Name: "ks"}}
	env = vtenv.NewTestEnv()
	srvVSchema := &vschemapb.SrvVSchema{Keyspaces: map[string]*vschemapb.Keyspace{"ks": {}}}

	logs := logtest.Capture(t, slog.LevelInfo)
	persistNewSrvVSchema(srvVSchema)
	record := logs.Record(t, "Persisted keyspace to dir")
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, "ks", record["keyspace"])
	assert.Equal(t, dir, record["dir"])

	ctx := t.Context()
	loadKeyspacesFromDir(ctx, dir, memorytopo.NewServer(ctx, "cell1"))
	record = logs.Record(t, "Loaded keyspace from path")
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, "ks", record["keyspace"])
	assert.Equal(t, filepath.Join(dir, "ks.json"), record["path"])

	logs.Reset()
	vschemaPersistenceDir = filepath.Join(dir, "missing")
	persistNewSrvVSchema(srvVSchema)
	record = logs.Record(t, "Error writing keyspace file")
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, "ks", record["keyspace"])
	assert.Equal(t, filepath.Join(dir, "missing", "ks.json"), record["path"])
	assert.Contains(t, record["error"], "no such file or directory")
}

// TestLoadKeyspacesFromDirLogsInvalidKeyspace checks the record logged before
// vtcombo exits on a keyspace file that doesn't build: the keyspace, the error
// and its code. The exit runs in a child process.
func TestLoadKeyspacesFromDirLogsInvalidKeyspace(t *testing.T) {
	if dir := os.Getenv("TEST_VTCOMBO_EXIT"); dir != "" {
		log.SwapLogger(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
		tpb.Keyspaces = []*vttestpb.Keyspace{{Name: "ks"}}
		env = vtenv.NewTestEnv()
		ctx := t.Context()
		loadKeyspacesFromDir(ctx, dir, memorytopo.NewServer(ctx, "cell1"))
		return
	}

	dir := t.TempDir()
	ksJSON := `{"sharded": true, "tables": {"t": {"column_vindexes": [{"column": "id", "name": "no_such_vindex"}]}}}`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ks.json"), []byte(ksJSON), 0o644))

	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$")
	cmd.Env = append(os.Environ(), "TEST_VTCOMBO_EXIT="+dir)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()

	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	assert.Equal(t, 1, exitErr.ExitCode())

	var record map[string]any
	for line := range strings.SplitSeq(stderr.String(), "\n") {
		if strings.Contains(line, `"msg":"Invalid keyspace definition"`) {
			require.NoError(t, json.Unmarshal([]byte(line), &record))
		}
	}
	require.NotNil(t, record, stderr.String())
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, "ks", record["keyspace"])
	assert.Equal(t, "vindex no_such_vindex not found for table t", record["error"])
	assert.Equal(t, "NOT_FOUND", record["error_code"])
}
