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
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

func TestPersistKeyspace_WritesFile(t *testing.T) {
	dir := t.TempDir()
	ks := &vschemapb.Keyspace{Sharded: true}

	require.NoError(t, persistKeyspace(dir, "ks1", ks))

	final := filepath.Join(dir, "ks1.json")
	data, err := os.ReadFile(final)
	require.NoError(t, err)

	var got vschemapb.Keyspace
	require.NoError(t, json.Unmarshal(data, &got))
	assert.True(t, got.Sharded)

	// CreateTemp produces 0o600; persistKeyspace must restore 0o644 so we
	// don't silently tighten permissions versus the prior os.WriteFile call.
	info, err := os.Stat(final)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o644), info.Mode().Perm())
}

func TestPersistKeyspace_ReplacesExistingFile(t *testing.T) {
	dir := t.TempDir()
	final := filepath.Join(dir, "ks1.json")

	require.NoError(t, os.WriteFile(final, []byte(`{"sharded": false}`), 0o644))
	require.NoError(t, persistKeyspace(dir, "ks1", &vschemapb.Keyspace{Sharded: true}))

	data, err := os.ReadFile(final)
	require.NoError(t, err)

	var got vschemapb.Keyspace
	require.NoError(t, json.Unmarshal(data, &got))
	assert.True(t, got.Sharded, "existing file should have been replaced with the new content")
}

// TestPersistKeyspace_NoTempLeftover guards the cleanup path: after a
// successful write, only the final file is present in the directory — no
// stray ks1.*.tmp files. This matters because vtcombo can persist many times
// over a long-lived process, and unbounded temp-file accumulation would
// eventually exhaust inodes.
func TestPersistKeyspace_NoTempLeftover(t *testing.T) {
	dir := t.TempDir()

	for range 5 {
		require.NoError(t, persistKeyspace(dir, "ks1", &vschemapb.Keyspace{Sharded: true}))
	}

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	assert.Equal(t, []string{"ks1.json"}, names, "only the final file should remain")
}

// TestPersistKeyspace_ExistingFilePreservedOnFailure is the core property:
// if the write fails, the previous file content is still intact. Without
// the atomic rename, os.WriteFile would truncate the destination first and
// leave an empty file behind on a kill between truncate and write.
//
// We force a failure by making the directory non-writable so CreateTemp
// fails. Skipped when running as root since root ignores 0o555.
func TestPersistKeyspace_ExistingFilePreservedOnFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory write permissions")
	}

	dir := t.TempDir()
	final := filepath.Join(dir, "ks1.json")
	original := []byte(`{"sharded": false}`)
	require.NoError(t, os.WriteFile(final, original, 0o644))

	require.NoError(t, os.Chmod(dir, 0o555))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	err := persistKeyspace(dir, "ks1", &vschemapb.Keyspace{Sharded: true})
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "creating temp file"), "got: %v", err)

	got, err := os.ReadFile(final)
	require.NoError(t, err)
	assert.Equal(t, original, got, "original file content should be preserved when write fails")
}
