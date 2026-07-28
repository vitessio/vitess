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
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

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

// TestVschemaPersisterFlush_WritesCurrentTopoState covers the shutdown path:
// persisting a vschema change is asynchronous, so a change that reached the
// topo shortly before shutdown may never have been handed to the watcher. The
// flush at shutdown has to write it from the topo, otherwise the next startup
// comes back with a stale vschema.
func TestVschemaPersisterFlush_WritesCurrentTopoState(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "cell1")
	t.Cleanup(func() { ts.Close() })

	saveVSchema(t, ctx, ts, "cell1", "ks1", true)

	dir := t.TempDir()
	p := &vschemaPersister{dir: dir}

	// No watcher is running here: flush alone has to get the vschema on disk.
	p.flush(ctx, ts, "cell1")

	assert.True(t, readPersistedKeyspace(t, dir, "ks1").Sharded)
}

// TestVschemaPersisterFlush_SealsLaterUpdates guards the ordering between the
// shutdown flush and the watcher goroutine. The watcher can still be holding
// updates it has not consumed yet, and those are necessarily no newer than what
// the flush read from the topo, so they must not replace it on disk.
func TestVschemaPersisterFlush_SealsLaterUpdates(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "cell1")
	t.Cleanup(func() { ts.Close() })

	saveVSchema(t, ctx, ts, "cell1", "ks1", true)

	dir := t.TempDir()
	p := &vschemaPersister{dir: dir}
	p.flush(ctx, ts, "cell1")

	p.persistNewSrvVSchema(&vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{"ks1": {Sharded: false}},
	})

	assert.True(t, readPersistedKeyspace(t, dir, "ks1").Sharded, "an update delivered after the shutdown flush should have been dropped")
}

// TestVschemaPersisterFlush_LeavesFileOnTopoError checks that a failed flush
// leaves the previously persisted vschema alone, so an unreadable topo at
// shutdown cannot cost us the file we already had.
func TestVschemaPersisterFlush_LeavesFileOnTopoError(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "cell1")
	t.Cleanup(func() { ts.Close() })

	dir := t.TempDir()
	final := filepath.Join(dir, "ks1.json")
	original := []byte(`{"sharded": true}`)
	require.NoError(t, os.WriteFile(final, original, 0o644))

	p := &vschemaPersister{dir: dir}
	p.flush(ctx, ts, "nosuchcell")

	got, err := os.ReadFile(final)
	require.NoError(t, err)
	assert.Equal(t, original, got)
	assert.False(t, p.sealed, "a failed flush must not seal the persister")
}

func saveVSchema(t *testing.T, ctx context.Context, ts *topo.Server, cell, ksName string, sharded bool) {
	t.Helper()
	require.NoError(t, ts.UpdateSrvVSchema(ctx, cell, &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{ksName: {Sharded: sharded}},
	}))
}

func readPersistedKeyspace(t *testing.T, dir, ksName string) *vschemapb.Keyspace {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, ksName+".json"))
	require.NoError(t, err)

	ks := &vschemapb.Keyspace{}
	require.NoError(t, json.Unmarshal(data, ks))
	return ks
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
