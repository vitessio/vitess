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
	"syscall"
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

	// A newly persisted file should carry the same mode os.WriteFile would have
	// given it, rather than os.CreateTemp's 0o600 or a mode that ignores the
	// umask. Comparing against a reference file keeps this independent of
	// whatever umask the test runs under.
	reference := filepath.Join(dir, "reference")
	require.NoError(t, os.WriteFile(reference, []byte("{}"), 0o644))
	referenceInfo, err := os.Stat(reference)
	require.NoError(t, err)

	info, err := os.Stat(final)
	require.NoError(t, err)
	assert.Equal(t, referenceInfo.Mode().Perm(), info.Mode().Perm())
}

// TestPersistKeyspace_PreservesExistingFileMode covers replacing a file whose
// mode an operator has tightened. The rename swaps in a new inode, so without
// carrying the mode over, each vschema update would widen the file back up.
func TestPersistKeyspace_PreservesExistingFileMode(t *testing.T) {
	dir := t.TempDir()
	final := filepath.Join(dir, "ks1.json")

	require.NoError(t, os.WriteFile(final, []byte(`{"sharded": false}`), 0o600))
	require.NoError(t, os.Chmod(final, 0o600))

	require.NoError(t, persistKeyspace(dir, "ks1", &vschemapb.Keyspace{Sharded: true}))

	info, err := os.Stat(final)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	assert.True(t, readPersistedKeyspace(t, dir, "ks1").Sharded, "the new content should still have been written")
}

// TestPersistKeyspace_NewFileRespectsUmask pins the create path to the umask,
// so a restrictive umask is not overridden by a hardcoded mode.
func TestPersistKeyspace_NewFileRespectsUmask(t *testing.T) {
	previous := syscall.Umask(0o077)
	t.Cleanup(func() { syscall.Umask(previous) })

	dir := t.TempDir()
	require.NoError(t, persistKeyspace(dir, "ks1", &vschemapb.Keyspace{Sharded: true}))

	info, err := os.Stat(filepath.Join(dir, "ks1.json"))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
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
// successful write, only the final file is present in the directory — the
// temp file the content was staged in is gone.
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

// TestPersistKeyspace_ReplacesStaleTempLeftover covers a leftover temp file
// from a kill between create and rename. The temp name is deterministic, so
// the next write of the same keyspace has to replace the leftover — including
// its mode, which may have been chmodded over for a destination that no longer
// exists and would otherwise stick, since O_CREATE leaves the mode of an
// existing file alone.
func TestPersistKeyspace_ReplacesStaleTempLeftover(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".ks1.json.tmp"), []byte("garbage"), 0o600))

	require.NoError(t, persistKeyspace(dir, "ks1", &vschemapb.Keyspace{Sharded: true}))

	assert.True(t, readPersistedKeyspace(t, dir, "ks1").Sharded)

	// The mode must come from the create (0o644 under the test's umask, same
	// as a reference os.WriteFile), not from the leftover's 0o600.
	reference := filepath.Join(dir, "reference")
	require.NoError(t, os.WriteFile(reference, []byte("{}"), 0o644))
	referenceInfo, err := os.Stat(reference)
	require.NoError(t, err)

	info, err := os.Stat(filepath.Join(dir, "ks1.json"))
	require.NoError(t, err)
	assert.Equal(t, referenceInfo.Mode().Perm(), info.Mode().Perm())

	_, err = os.Stat(filepath.Join(dir, ".ks1.json.tmp"))
	assert.True(t, os.IsNotExist(err), "the leftover temp file should be gone")
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

// TestVschemaPersisterFlush_SealsWhenSomeKeyspacesFail covers a flush that got
// the authoritative vschema out of the topo but could only write part of it.
// Keyspaces are written one file at a time, and the ordering the seal enforces
// holds per file: an update the watcher is still holding is older than the
// snapshot either way, so it must not replace a keyspace the flush did write.
func TestVschemaPersisterFlush_SealsWhenSomeKeyspacesFail(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "cell1")
	t.Cleanup(func() { ts.Close() })

	require.NoError(t, ts.UpdateSrvVSchema(ctx, "cell1", &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"ks1": {Sharded: true},
			"ks2": {Sharded: true},
		},
	}))

	dir := t.TempDir()
	// A directory where ks2's file belongs fails the rename, and only that one.
	require.NoError(t, os.Mkdir(filepath.Join(dir, "ks2.json"), 0o755))

	p := &vschemaPersister{dir: dir}
	p.flush(ctx, ts, "cell1")

	require.True(t, readPersistedKeyspace(t, dir, "ks1").Sharded, "ks1 should have been written")
	assert.True(t, p.sealed)

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
// We force a failure by making the directory non-writable so creating the
// temp file fails. Skipped when running as root since root ignores 0o555.
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
	require.ErrorContains(t, err, "creating temp file")

	got, err := os.ReadFile(final)
	require.NoError(t, err)
	assert.Equal(t, original, got, "original file content should be preserved when write fails")
}
