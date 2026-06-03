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
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/fileutil"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstats"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
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
				assert.NoError(t, err)
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
	require.NoError(t, createChunkedDestinations(fes, cnf, ""))

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

func TestBackupWorkItemsEndBackupErrIsFatal(t *testing.T) {
	bh := &FakeBackupHandle{
		EndBackupReturn: errors.New("simulated EndBackup failure"),
	}

	be := &BuiltinBackupEngine{}
	fes := []FileEntry{}
	params := BackupParams{
		Logger:      logutil.NewMemoryLogger(),
		Concurrency: 1,
	}

	err := be.backupWorkItems(t.Context(), nil, fes, bh, params)
	require.Error(t, err)
	assert.ErrorIs(t, err, errBackupFatal)
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
