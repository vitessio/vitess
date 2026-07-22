/*
Copyright 2023 The Vitess Authors.

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

// Package mysqlctl_test is the blackbox tests for package mysqlctl.
package mysqlctl

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/fileutil"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/os2"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstats"
	"vitess.io/vitess/go/vt/mysqlctl/filebackupstorage"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func TestGetIncrementalFromPosGTIDSet(t *testing.T) {
	tcases := []struct {
		incrementalFromPos string
		gtidSet            string
		expctError         bool
	}{
		{
			"MySQL56/16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615",
			"16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615",
			false,
		},
		{
			"16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615",
			"16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615",
			false,
		},
		{
			"MySQL56/16b1039f-22b6-11ed-b765-0a43f95f28a3",
			"",
			true,
		},
		{
			"MySQL56/invalid",
			"",
			true,
		},
		{
			"16b1039f-22b6-11ed-b765-0a43f95f28a3",
			"",
			true,
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.incrementalFromPos, func(t *testing.T) {
			gtidSet, err := getIncrementalFromPosGTIDSet(tcase.incrementalFromPos)
			if tcase.expctError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tcase.gtidSet, gtidSet.String())
			}
		})
	}
}

func TestFileEntryFullPath(t *testing.T) {
	cnf := &Mycnf{
		DataDir:               "/vt/data",
		InnodbDataHomeDir:     "/vt/innodb-data",
		InnodbLogGroupHomeDir: "/vt/innodb-log",
		BinLogPath:            "/vt/binlogs/mysql-bin",
	}

	tests := []struct {
		name      string
		entry     FileEntry
		wantPath  string
		wantError error
	}{
		{
			name:     "valid relative path in DataDir",
			entry:    FileEntry{Base: backupData, Name: "mydb/table1.ibd"},
			wantPath: "/vt/data/mydb/table1.ibd",
		},
		{
			name:     "valid relative path in InnodbDataHomeDir",
			entry:    FileEntry{Base: backupInnodbDataHomeDir, Name: "ibdata1"},
			wantPath: "/vt/innodb-data/ibdata1",
		},
		{
			name:     "valid relative path in InnodbLogGroupHomeDir",
			entry:    FileEntry{Base: backupInnodbLogGroupHomeDir, Name: "ib_logfile0"},
			wantPath: "/vt/innodb-log/ib_logfile0",
		},
		{
			name:     "valid relative path in BinlogDir",
			entry:    FileEntry{Base: backupBinlogDir, Name: "mysql-bin.000001"},
			wantPath: "/vt/binlogs/mysql-bin.000001",
		},
		{
			name:     "valid path with ParentPath",
			entry:    FileEntry{Base: backupData, Name: "mydb/table1.ibd", ParentPath: "/tmp/restore"},
			wantPath: "/tmp/restore/vt/data/mydb/table1.ibd",
		},
		{
			name:      "path traversal escapes base directory",
			entry:     FileEntry{Base: backupData, Name: "../../etc/passwd"},
			wantError: fileutil.ErrInvalidJoinedPath,
		},
		{
			name:      "path traversal with deeper nesting",
			entry:     FileEntry{Base: backupData, Name: "mydb/../../../etc/shadow"},
			wantError: fileutil.ErrInvalidJoinedPath,
		},
		{
			name:      "path traversal to root",
			entry:     FileEntry{Base: backupData, Name: "../../../../../etc/crontab"},
			wantError: fileutil.ErrInvalidJoinedPath,
		},
		{
			name:      "path traversal escapes ParentPath",
			entry:     FileEntry{Base: backupData, Name: "../../../../etc/passwd", ParentPath: "/tmp/restore"},
			wantError: fileutil.ErrInvalidJoinedPath,
		},
		{
			name:     "relative path with dot-dot that stays within base",
			entry:    FileEntry{Base: backupData, Name: "mydb/../mydb/table1.ibd"},
			wantPath: "/vt/data/mydb/table1.ibd",
		},
	}

	// Test unknown base separately since it returns a different error type.
	t.Run("unknown base", func(t *testing.T) {
		entry := FileEntry{Base: "unknown", Name: "file"}
		_, err := entry.fullPath(cnf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown base")
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.entry.fullPath(cnf)
			if tt.wantError != nil {
				require.ErrorIs(t, err, tt.wantError)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantPath, got)
			}
		})
	}
}

func TestParseBackupStorageName(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantFeIdx  int
		wantChkIdx int
		wantErr    bool
	}{
		{name: "whole file", input: "5", wantFeIdx: 5, wantChkIdx: -1},
		{name: "chunk", input: "5-2", wantFeIdx: 5, wantChkIdx: 2},
		{name: "zero index", input: "0-0", wantFeIdx: 0, wantChkIdx: 0},
		{name: "large indices", input: "123-456", wantFeIdx: 123, wantChkIdx: 456},
		{name: "invalid file index", input: "abc", wantErr: true},
		{name: "invalid chunk index", input: "5-abc", wantErr: true},
		{name: "invalid file index with chunk", input: "abc-2", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			feIdx, chkIdx, err := parseBackupName(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantFeIdx, feIdx)
			assert.Equal(t, tt.wantChkIdx, chkIdx)
		})
	}
}

func TestComputeFileChunks(t *testing.T) {
	tests := []struct {
		name       string
		fileIndex  int
		fileSize   int64
		chunkSize  int64
		wantChunks []FileChunk
	}{
		{
			name:      "exact multiple",
			fileIndex: 3,
			fileSize:  100,
			chunkSize: 50,
			wantChunks: []FileChunk{
				{StorageName: "3-0", Offset: 0, Size: 50},
				{StorageName: "3-1", Offset: 50, Size: 50},
			},
		},
		{
			name:      "last chunk is smaller",
			fileIndex: 0,
			fileSize:  70,
			chunkSize: 30,
			wantChunks: []FileChunk{
				{StorageName: "0-0", Offset: 0, Size: 30},
				{StorageName: "0-1", Offset: 30, Size: 30},
				{StorageName: "0-2", Offset: 60, Size: 10},
			},
		},
		{
			name:      "file smaller than chunk size",
			fileIndex: 5,
			fileSize:  10,
			chunkSize: 100,
			wantChunks: []FileChunk{
				{StorageName: "5-0", Offset: 0, Size: 10},
			},
		},
		{
			name:      "single byte file",
			fileIndex: 0,
			fileSize:  1,
			chunkSize: 1024,
			wantChunks: []FileChunk{
				{StorageName: "0-0", Offset: 0, Size: 1},
			},
		},
		{
			name:      "max int64 chunk size",
			fileIndex: 0,
			fileSize:  100,
			chunkSize: math.MaxInt64,
			wantChunks: []FileChunk{
				{StorageName: "0-0", Offset: 0, Size: 100},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeFileChunks(tt.fileIndex, tt.fileSize, tt.chunkSize)
			require.Len(t, got, len(tt.wantChunks))
			for i, want := range tt.wantChunks {
				assert.Equal(t, want.StorageName, got[i].StorageName)
				assert.Equal(t, want.Offset, got[i].Offset)
				assert.Equal(t, want.Size, got[i].Size)
			}
		})
	}
}

func TestOffsetWriter(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "offsetwriter")
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })

	// Pre-size the file.
	require.NoError(t, f.Truncate(100))

	// Write "hello" at offset 10.
	ow := &offsetWriter{f: f, offset: 10}
	n, err := ow.Write([]byte("hello"))
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, int64(15), ow.offset)

	// Write "world" at offset 50.
	ow2 := &offsetWriter{f: f, offset: 50}
	n, err = ow2.Write([]byte("world"))
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, int64(55), ow2.offset)

	// Verify contents.
	buf := make([]byte, 100)
	_, err = f.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), buf[10:15])
	assert.Equal(t, []byte("world"), buf[50:55])
	// Gaps should be zero-filled.
	assert.Equal(t, make([]byte, 10), buf[0:10])
	assert.Equal(t, make([]byte, 35), buf[15:50])
}

// crc32Hash computes the CRC32 IEEE hash of data, matching what backupPipe.HashString() produces.
func crc32Hash(data []byte) string {
	h := crc32.NewIEEE()
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

// TestRestoreChunkedRetryPreservesData verifies that when a chunk fails during
// restore and is retried, the previously-restored chunks are not destroyed.
// This guards against the bug where os2.Create (which truncates) is used on retry,
// wiping the already-restored data.
func TestRestoreChunkedRetryPreservesData(t *testing.T) {
	tmpDir := t.TempDir()
	cnf := &Mycnf{DataDir: tmpDir}

	// Simulate a file split into 3 chunks of 10 bytes each.
	chunk0Data := []byte("AAAAAAAAAA")
	chunk1Data := []byte("BBBBBBBBBB")
	chunk2Data := []byte("CCCCCCCCCC")

	fes := []FileEntry{
		{
			Base: backupData,
			Name: "testfile.ibd",
			Chunks: []FileChunk{
				{StorageName: "0-0", Offset: 0, Size: 10, Hash: crc32Hash(chunk0Data)},
				{StorageName: "0-1", Offset: 10, Size: 10, Hash: crc32Hash(chunk1Data)},
				{StorageName: "0-2", Offset: 20, Size: 10, Hash: crc32Hash(chunk2Data)},
			},
		},
	}

	// Chunk "0-1" fails on first attempt.
	var attempted sync.Map
	bh := &FakeBackupHandle{
		ReadFileReturnF: func(_ context.Context, filename string) (io.ReadCloser, error) {
			count := 0
			if v, ok := attempted.Load(filename); ok {
				count = v.(int)
			}
			count++
			attempted.Store(filename, count)

			if filename == "0-1" && count == 1 {
				return nil, errors.New("simulated read failure")
			}
			files := map[string][]byte{
				"0-0": chunk0Data,
				"0-1": chunk1Data,
				"0-2": chunk2Data,
			}
			data, ok := files[filename]
			if !ok {
				return nil, fmt.Errorf("file %s not found", filename)
			}
			return io.NopCloser(bytes.NewReader(data)), nil
		},
	}

	bm := builtinBackupManifest{SkipCompress: true}
	params := RestoreParams{
		Cnf:         cnf,
		Logger:      logutil.NewMemoryLogger(),
		Stats:       backupstats.NoStats(),
		Concurrency: 1,
	}

	be := &BuiltinBackupEngine{}

	// Pre-create and pre-size destination files, as restoreFiles() would.
	require.NoError(t, createChunkedDestinations(t.Context(), fes, cnf, "", logutil.NewConsoleLogger()))

	// First pass: chunk 0-1 will fail.
	err := be.restoreFileEntries(t.Context(), fes, bh, bm, params, "")
	require.Error(t, err)
	require.Len(t, bh.GetFailedFiles(), 1)

	// Verify chunks 0 and 2 were written correctly.
	filePath := tmpDir + "/testfile.ibd"
	content, err := os.ReadFile(filePath)
	require.NoError(t, err)
	assert.Equal(t, chunk0Data, content[0:10], "chunk 0 should be intact after first pass")
	assert.Equal(t, chunk2Data, content[20:30], "chunk 2 should be intact after first pass")

	// Retry: build newFEs with only the failed chunk, as restoreFiles would.
	failedFiles := bh.GetFailedFiles()
	require.Len(t, failedFiles, 1)

	newFEs := make([]FileEntry, len(fes))
	for _, file := range failedFiles {
		feIdx, chunkIdx, parseErr := parseBackupName(file)
		require.NoError(t, parseErr)
		oldFe := fes[feIdx]
		if newFEs[feIdx].Name == "" {
			newFEs[feIdx] = FileEntry{
				Base:       oldFe.Base,
				Name:       oldFe.Name,
				ParentPath: oldFe.ParentPath,
				RetryCount: 1,
			}
		}
		newFEs[feIdx].Chunks = append(newFEs[feIdx].Chunks, oldFe.Chunks[chunkIdx])
		bh.ResetErrorForFile(file)
	}

	// Second pass: retry the failed chunk.
	err = be.restoreFileEntries(t.Context(), newFEs, bh, bm, params, "")
	require.NoError(t, err)

	// Verify ALL chunks are correct after retry.
	content, err = os.ReadFile(filePath)
	require.NoError(t, err)
	assert.Equal(t, chunk0Data, content[0:10], "chunk 0 should still be intact after retry")
	assert.Equal(t, chunk1Data, content[10:20], "chunk 1 should be restored after retry")
	assert.Equal(t, chunk2Data, content[20:30], "chunk 2 should still be intact after retry")
}

func TestRestoreFileEntriesSetupErrIsFatal(t *testing.T) {
	tmpDir := t.TempDir()
	cnf := &Mycnf{DataDir: tmpDir}

	fes := []FileEntry{
		{
			Base: backupData,
			Name: "good.ibd",
		},
		{
			Base: "invalid-base",
			Name: "bad.ibd",
			Chunks: []FileChunk{
				{StorageName: "1-0", Offset: 0, Size: 10},
			},
		},
	}

	bh := &FakeBackupHandle{
		ReadFileReturnF: func(_ context.Context, filename string) (io.ReadCloser, error) {
			return nil, errors.New("simulated read failure")
		},
	}

	bm := builtinBackupManifest{SkipCompress: true}
	params := RestoreParams{
		Cnf:         cnf,
		Logger:      logutil.NewMemoryLogger(),
		Stats:       backupstats.NoStats(),
		Concurrency: 1,
	}

	be := &BuiltinBackupEngine{}
	err := be.restoreFileEntries(t.Context(), fes, bh, bm, params, "")

	require.Error(t, err)
	assert.ErrorIs(t, err, errRestoreFatal)
}

func TestRestoreCloseErrorIsFatal(t *testing.T) {
	tmpDir := t.TempDir()
	cnf := &Mycnf{DataDir: tmpDir}

	chunk0Data := []byte("AAAAAAAAAA")
	chunk1Data := []byte("BBBBBBBBBB")

	fes := []FileEntry{
		{
			Base: backupData,
			Name: "testfile.ibd",
			Chunks: []FileChunk{
				{StorageName: "0-0", Offset: 0, Size: 10, Hash: crc32Hash(chunk0Data)},
				{StorageName: "0-1", Offset: 10, Size: 10, Hash: crc32Hash(chunk1Data)},
			},
		},
	}

	bh := &FakeBackupHandle{
		ReadFileReturnF: func(_ context.Context, filename string) (io.ReadCloser, error) {
			if filename == "0-1" {
				return nil, errors.New("simulated read failure")
			}
			files := map[string][]byte{
				"0-0": chunk0Data,
				"0-1": chunk1Data,
			}
			return io.NopCloser(bytes.NewReader(files[filename])), nil
		},
	}

	oldMaxRetries := maxFileCloseRetries
	oldOpenFile := openFile
	t.Cleanup(func() {
		maxFileCloseRetries = oldMaxRetries
		openFile = oldOpenFile
	})

	maxFileCloseRetries = 0
	openFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
		f, err := os.OpenFile(name, flag, perm)
		if err != nil {
			return nil, err
		}
		f.Close()
		return f, nil
	}

	bm := builtinBackupManifest{SkipCompress: true}
	params := RestoreParams{
		Cnf:         cnf,
		Logger:      logutil.NewMemoryLogger(),
		Stats:       backupstats.NoStats(),
		Concurrency: 1,
	}

	be := &BuiltinBackupEngine{}
	require.NoError(t, createChunkedDestinations(t.Context(), fes, cnf, "", logutil.NewConsoleLogger()))

	err := be.restoreFileEntries(t.Context(), fes, bh, bm, params, "")
	require.Error(t, err)
	assert.ErrorIs(t, err, errRestoreFatal)
}

func TestExecuteBackupRejectsOversizedChunkSize(t *testing.T) {
	oldThreshold := backupFileChunkThreshold
	oldSize := backupFileChunkSize
	t.Cleanup(func() {
		backupFileChunkThreshold = oldThreshold
		backupFileChunkSize = oldSize
	})

	backupFileChunkThreshold = 1024
	backupFileChunkSize = math.MaxInt64 + 1

	be := &BuiltinBackupEngine{}
	_, err := be.ExecuteBackup(t.Context(), BackupParams{Logger: logutil.NewMemoryLogger()}, nil)
	require.ErrorContains(t, err, "exceeds maximum allowed value")
}

func TestShouldDrainForBackupBuiltIn(t *testing.T) {
	be := &BuiltinBackupEngine{}

	assert.True(t, be.ShouldDrainForBackup(&tabletmanagerdatapb.BackupRequest{}))
	assert.False(t, be.ShouldDrainForBackup(&tabletmanagerdatapb.BackupRequest{IncrementalFromPos: "auto"}))
	assert.False(t, be.ShouldDrainForBackup(&tabletmanagerdatapb.BackupRequest{IncrementalFromPos: "99ca8ed4-399c-11ee-861b-0a43f95f28a3:1-197"}))
	assert.False(t, be.ShouldDrainForBackup(&tabletmanagerdatapb.BackupRequest{IncrementalFromPos: "MySQL56/99ca8ed4-399c-11ee-861b-0a43f95f28a3:1-197"}))
}

type nopWriteCloser struct{}

func (nopWriteCloser) Write(p []byte) (int, error) { return len(p), nil }
func (nopWriteCloser) Close() error                { return nil }

func TestBackupFilesDoesNotCallEndBackup(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := path.Join(tmpDir, "datadir", "test1")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))
	require.NoError(t, os.MkdirAll(path.Join(tmpDir, "innodb"), 0o755))
	require.NoError(t, os.MkdirAll(path.Join(tmpDir, "log"), 0o755))

	for _, name := range []string{"0.ibd", "1.ibd"} {
		f, err := os2.Create(path.Join(dataDir, name))
		require.NoError(t, err)
		_, err = f.WriteString("test data for backup")
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}

	var addFileCalls sync.Map
	bh := &FakeBackupHandle{
		AddFileReturnF: func(filename string) FakeBackupHandleAddFileReturn {
			count := 0
			if v, ok := addFileCalls.Load(filename); ok {
				count = v.(int)
			}
			count++
			addFileCalls.Store(filename, count)

			// Fail file "0" on first attempt to trigger a retry.
			if filename == "0" && count == 1 {
				return FakeBackupHandleAddFileReturn{WriteCloser: nil, Err: errors.New("simulated transient error")}
			}
			return FakeBackupHandleAddFileReturn{WriteCloser: nopWriteCloser{}}
		},
	}

	be := &BuiltinBackupEngine{}
	err := be.backupFiles(
		t.Context(),
		BackupParams{
			Cnf: &Mycnf{
				InnodbDataHomeDir:     path.Join(tmpDir, "innodb"),
				InnodbLogGroupHomeDir: path.Join(tmpDir, "log"),
				DataDir:               path.Join(tmpDir, "datadir"),
			},
			Logger:      logutil.NewMemoryLogger(),
			Stats:       backupstats.NoStats(),
			Concurrency: 1,
		},
		bh,
		replication.Position{},
		replication.Position{},
		replication.Position{},
		"",
		nil,
		"",
		"",
		nil,
	)
	require.NoError(t, err)

	assert.Empty(t, bh.EndBackupCalls, "backupFiles must not call EndBackup; only the top-level Backup() caller should")
	assert.Positive(t, bh.WaitCalls, "backupFiles must call Wait() to flush pending async operations")
}

// TestBackupRestoreWithManyChunks exercises backup and restore with a high
// chunk count (256 chunks) by overriding minBackupFileChunkSize below the
// production floor. This validates that chunking works correctly under heavy
// fan-out without requiring the large file sizes that the 4MiB minimum would
// impose.
func TestBackupRestoreWithManyChunks(t *testing.T) {
	ctx := context.Background()

	const (
		fileSize  = 16 * 1024 * 1024 // 16MiB
		chunkSize = 64 * 1024        // 64KiB → 256 chunks
	)

	// Override the minimum chunk size so we can use 64KiB chunks without
	// hitting the 4MiB production floor validation in ExecuteBackup.
	oldThreshold := backupFileChunkThreshold
	oldSize := backupFileChunkSize
	oldMin := minBackupFileChunkSize
	t.Cleanup(func() {
		backupFileChunkThreshold = oldThreshold
		backupFileChunkSize = oldSize
		minBackupFileChunkSize = oldMin
	})
	backupFileChunkThreshold = chunkSize
	backupFileChunkSize = chunkSize
	minBackupFileChunkSize = chunkSize

	// Create a single 16MiB file filled with random (incompressible) data.
	backupRoot := t.TempDir()
	filebackupstorage.FileBackupStorageRoot = backupRoot
	require.NoError(t, os.MkdirAll(path.Join(backupRoot, "innodb"), 0o755))
	require.NoError(t, os.MkdirAll(path.Join(backupRoot, "log"), 0o755))
	dataDir := path.Join(backupRoot, "datadir", "test1")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))

	filePath := path.Join(dataDir, "0.ibd")
	f, err := os2.Create(filePath)
	require.NoError(t, err)
	_, err = io.CopyN(f, rand.Reader, fileSize)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Checksum the original file before backup.
	originalData, err := os.ReadFile(filePath)
	require.NoError(t, err)
	originalChecksum := crc32.ChecksumIEEE(originalData)

	// Set up topo — required by ExecuteBackup for MANIFEST metadata.
	keyspace, shard := "mykeyspace", "-"
	ts := memorytopo.NewServer(ctx, "cell1")
	t.Cleanup(ts.Close)

	require.NoError(t, ts.CreateKeyspace(ctx, keyspace, &topodata.Keyspace{}))
	require.NoError(t, ts.CreateShard(ctx, keyspace, shard))
	tablet := topo.NewTablet(100, "cell1", "mykeyspace-00-80-0100")
	tablet.Keyspace = keyspace
	tablet.Shard = shard
	require.NoError(t, ts.CreateTablet(ctx, tablet))
	_, err = ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
		si.PrimaryAlias = &topodata.TabletAlias{Uid: 100, Cell: "cell1"}
		now := time.Now()
		si.PrimaryTermStartTime = &vttime.Time{Seconds: now.Unix(), Nanoseconds: int32(now.Nanosecond())}
		return nil
	})
	require.NoError(t, err)

	oldDeadline := BuiltinBackupMysqldTimeout
	BuiltinBackupMysqldTimeout = time.Second
	t.Cleanup(func() { BuiltinBackupMysqldTimeout = oldDeadline })

	// Backup: should split the 16MiB file into 256 chunks of 64KiB each.
	be := &BuiltinBackupEngine{}
	bh := filebackupstorage.NewBackupHandle(nil, "", "", false)

	fakedb := fakesqldb.New(t)
	t.Cleanup(fakedb.Close)
	mysqld := NewFakeMysqlDaemon(fakedb)
	t.Cleanup(mysqld.Close)
	mysqld.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "START REPLICA"}

	backupResult, err := be.ExecuteBackup(ctx, BackupParams{
		Logger: logutil.NewMemoryLogger(),
		Mysqld: mysqld,
		Cnf: &Mycnf{
			InnodbDataHomeDir:     path.Join(backupRoot, "innodb"),
			InnodbLogGroupHomeDir: path.Join(backupRoot, "log"),
			DataDir:               path.Join(backupRoot, "datadir"),
		},
		Stats:                backupstats.NoStats(),
		Concurrency:          4,
		HookExtraEnv:         map[string]string{},
		TopoServer:           ts,
		Keyspace:             keyspace,
		Shard:                shard,
		MysqlShutdownTimeout: time.Minute,
	}, bh)

	require.NoError(t, err)
	require.Equal(t, BackupUsable, backupResult)

	// Restore: read back all 256 chunks and reassemble into the original file.
	restoreBh := filebackupstorage.NewBackupHandle(nil, "", "", true)
	fakedb2 := fakesqldb.New(t)
	t.Cleanup(fakedb2.Close)
	mysqld2 := NewFakeMysqlDaemon(fakedb2)
	t.Cleanup(mysqld2.Close)
	mysqld2.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "START REPLICA"}

	bm, err := be.ExecuteRestore(ctx, RestoreParams{
		Cnf: &Mycnf{
			InnodbDataHomeDir:     path.Join(backupRoot, "innodb"),
			InnodbLogGroupHomeDir: path.Join(backupRoot, "log"),
			DataDir:               path.Join(backupRoot, "datadir"),
			BinLogPath:            path.Join(backupRoot, "binlog"),
			RelayLogPath:          path.Join(backupRoot, "relaylog"),
			RelayLogIndexPath:     path.Join(backupRoot, "relaylogindex"),
			RelayLogInfoPath:      path.Join(backupRoot, "relayloginfo"),
		},
		Logger:               logutil.NewMemoryLogger(),
		Mysqld:               mysqld2,
		Concurrency:          4,
		HookExtraEnv:         map[string]string{},
		DeleteBeforeRestore:  false,
		DbName:               "test",
		Keyspace:             "test",
		Shard:                "-",
		StartTime:            time.Now(),
		RestoreToPos:         replication.Position{},
		RestoreToTimestamp:   time.Time{},
		DryRun:               false,
		Stats:                backupstats.NoStats(),
		MysqlShutdownTimeout: time.Minute,
	}, restoreBh)

	require.NoError(t, err)
	require.NotNil(t, bm)

	// Verify restored file matches the original.
	restoredData, err := os.ReadFile(filePath)
	require.NoError(t, err)
	assert.Equal(t, originalChecksum, crc32.ChecksumIEEE(restoredData))
}

func TestCreateChunkedDestinationsRejectsBadStorageName(t *testing.T) {
	tmpDir := t.TempDir()
	cnf := &Mycnf{DataDir: tmpDir}

	fes := []FileEntry{
		{
			Base: backupData,
			Name: "testfile.ibd",
			Chunks: []FileChunk{
				{StorageName: "0-0", Offset: 0, Size: 10},
				{StorageName: "wrong-name", Offset: 10, Size: 10},
			},
		},
	}

	err := createChunkedDestinations(t.Context(), fes, cnf, "", logutil.NewConsoleLogger())
	require.ErrorContains(t, err, "unexpected storage name")
	require.ErrorContains(t, err, `got "wrong-name"`)
	require.ErrorContains(t, err, `expected "0-1"`)
}

func TestRestoreFileChunkRejectsDecompressedSizeMismatch(t *testing.T) {
	tmpDir := t.TempDir()
	cnf := &Mycnf{DataDir: tmpDir}

	chunkData := []byte("AAAAAAAAAA")

	fes := []FileEntry{
		{
			Base: backupData,
			Name: "testfile.ibd",
			Chunks: []FileChunk{
				{StorageName: "0-0", Offset: 0, Size: 20, Hash: crc32Hash(chunkData)},
			},
		},
	}

	bh := &FakeBackupHandle{
		ReadFileReturnF: func(_ context.Context, filename string) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(chunkData)), nil
		},
	}

	bm := builtinBackupManifest{SkipCompress: true}
	params := RestoreParams{
		Cnf:         cnf,
		Logger:      logutil.NewMemoryLogger(),
		Stats:       backupstats.NoStats(),
		Concurrency: 1,
	}

	be := &BuiltinBackupEngine{}
	require.NoError(t, createChunkedDestinations(t.Context(), fes, cnf, "", logutil.NewConsoleLogger()))

	err := be.restoreFileEntries(t.Context(), fes, bh, bm, params, "")
	require.ErrorContains(t, err, "decompressed size mismatch")
	require.ErrorContains(t, err, "wrote 10 bytes, expected 20")
}

func TestCreateChunkedDestinationsRejectsGap(t *testing.T) {
	tmpDir := t.TempDir()
	cnf := &Mycnf{DataDir: tmpDir}

	fes := []FileEntry{
		{
			Base: backupData,
			Name: "testfile.ibd",
			Chunks: []FileChunk{
				{StorageName: "0-0", Offset: 0, Size: 10},
				{StorageName: "0-1", Offset: 20, Size: 10},
			},
		},
	}

	err := createChunkedDestinations(t.Context(), fes, cnf, "", logutil.NewConsoleLogger())
	require.ErrorContains(t, err, "gap/overlap")
	require.ErrorContains(t, err, "expected offset 10, got 20")
}

func TestCreateChunkedDestinationsRejectsOverlap(t *testing.T) {
	tmpDir := t.TempDir()
	cnf := &Mycnf{DataDir: tmpDir}

	fes := []FileEntry{
		{
			Base: backupData,
			Name: "testfile.ibd",
			Chunks: []FileChunk{
				{StorageName: "0-0", Offset: 0, Size: 15},
				{StorageName: "0-1", Offset: 10, Size: 10},
			},
		},
	}

	err := createChunkedDestinations(t.Context(), fes, cnf, "", logutil.NewConsoleLogger())
	require.ErrorContains(t, err, "gap/overlap")
	require.ErrorContains(t, err, "expected offset 15, got 10")
}

// TestCreateChunkedDestinationsAcceptsOutOfOrderChunks verifies that chunks listed
// out of offset order in the MANIFEST are accepted as long as the file is complete
// (contiguous coverage with no gaps or overlaps and correct StorageNames).
func TestCreateChunkedDestinationsAcceptsOutOfOrderChunks(t *testing.T) {
	tmpDir := t.TempDir()
	cnf := &Mycnf{DataDir: tmpDir}

	fes := []FileEntry{
		{
			Base: backupData,
			Name: "testfile.ibd",
			Chunks: []FileChunk{
				{StorageName: "0-1", Offset: 10, Size: 10},
				{StorageName: "0-0", Offset: 0, Size: 10},
			},
		},
	}

	err := createChunkedDestinations(t.Context(), fes, cnf, "", logutil.NewConsoleLogger())
	require.NoError(t, err)
}

func TestHasErrorCode(t *testing.T) {
	precondition := vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "fatal error")
	internal := vterrors.Errorf(vtrpcpb.Code_INTERNAL, "transient error")

	tests := []struct {
		name     string
		err      error
		code     vtrpcpb.Code
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			code:     vtrpcpb.Code_FAILED_PRECONDITION,
			expected: false,
		},
		{
			name:     "single error with matching code",
			err:      precondition,
			code:     vtrpcpb.Code_FAILED_PRECONDITION,
			expected: true,
		},
		{
			name:     "single error with non-matching code",
			err:      internal,
			code:     vtrpcpb.Code_FAILED_PRECONDITION,
			expected: false,
		},
		{
			name:     "joined error with matching code in second position",
			err:      errors.Join(internal, precondition),
			code:     vtrpcpb.Code_FAILED_PRECONDITION,
			expected: true,
		},
		{
			name:     "joined error with no matching code",
			err:      errors.Join(internal, errors.New("plain error")),
			code:     vtrpcpb.Code_FAILED_PRECONDITION,
			expected: false,
		},
		{
			name:     "nested join with matching code",
			err:      errors.Join(errors.New("outer"), errors.Join(internal, precondition)),
			code:     vtrpcpb.Code_FAILED_PRECONDITION,
			expected: true,
		},
		{
			name:     "wrapped error with matching code",
			err:      vterrors.Wrapf(precondition, "context"),
			code:     vtrpcpb.Code_FAILED_PRECONDITION,
			expected: true,
		},
		{
			name:     "wrapped joined error with matching code",
			err:      vterrors.Wrapf(errors.Join(internal, precondition), "context"),
			code:     vtrpcpb.Code_FAILED_PRECONDITION,
			expected: true,
		},
		{
			name:     "fmt wrapped joined error with matching code",
			err:      fmt.Errorf("outer: %w", errors.Join(internal, precondition)),
			code:     vtrpcpb.Code_FAILED_PRECONDITION,
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, hasErrorCode(tc.err, tc.code))
		})
	}
}
