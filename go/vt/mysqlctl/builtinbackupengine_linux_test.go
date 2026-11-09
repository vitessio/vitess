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

package mysqlctl

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstats"
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func TestDirectIOWriter(t *testing.T) {
	const bufSize = 64 * 1024

	tests := []struct {
		name   string
		offset int64
		size   int
	}{
		{name: "multiple of the buffer", offset: 0, size: 3 * bufSize},
		{name: "unaligned tail", offset: 0, size: 2*bufSize + 3*directIOAlignment + 123},
		{name: "smaller than the alignment", offset: 0, size: 100},
		{name: "aligned non-zero offset with unaligned tail", offset: 1024 * 1024, size: bufSize + 4567},
	}
	for _, tt := range tests {
		for _, viaReadFrom := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/readFrom=%v", tt.name, viaReadFrom), func(t *testing.T) {
				data := make([]byte, tt.size)
				_, err := rand.Read(data)
				require.NoError(t, err)

				dir := t.TempDir()
				requireDirectIO(t, dir)
				p := path.Join(dir, "file")
				require.NoError(t, os.WriteFile(p, nil, 0o644))
				require.NoError(t, os.Truncate(p, tt.offset+int64(tt.size)))

				f, err := openForDirectWrite(p)
				require.NoError(t, err)
				require.True(t, directIOEnabled(t, f))

				w := newDirectIOWriter(f, &offsetWriter{f: f, offset: tt.offset}, bufSize)
				if viaReadFrom {
					n, err := w.ReadFrom(bytes.NewReader(data))
					require.NoError(t, err)
					require.EqualValues(t, tt.size, n)
				} else {
					// Odd-sized writes so the buffer fills up mid-write.
					for rest := data; len(rest) > 0; {
						n := min(len(rest), 10000)
						written, err := w.Write(rest[:n])
						require.NoError(t, err)
						require.Equal(t, n, written)
						rest = rest[n:]
					}
				}
				require.NoError(t, w.Flush())
				require.NoError(t, f.Close())

				got, err := os.ReadFile(p)
				require.NoError(t, err)
				assert.Equal(t, make([]byte, tt.offset), got[:tt.offset], "bytes before the offset must be untouched")
				assert.True(t, bytes.Equal(data, got[tt.offset:]), "written data does not match")
			})
		}
	}
}

func TestDirectIOWriterDisablesDirectIOOnlyForUnalignedTail(t *testing.T) {
	dir := t.TempDir()
	requireDirectIO(t, dir)
	p := path.Join(dir, "file")
	require.NoError(t, os.WriteFile(p, nil, 0o644))
	require.NoError(t, os.Truncate(p, 2*directIOAlignment))

	f, err := openForDirectWrite(p)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, f.Close()) })

	w := newDirectIOWriter(f, &offsetWriter{f: f}, directIOAlignment)
	_, err = w.Write(make([]byte, directIOAlignment))
	require.NoError(t, err)
	require.NoError(t, w.Flush())
	assert.True(t, directIOEnabled(t, f), "aligned data should not disable O_DIRECT")

	_, err = w.Write(make([]byte, 10))
	require.NoError(t, err)
	require.NoError(t, w.Flush())
	assert.False(t, directIOEnabled(t, f), "an unaligned tail must be written with O_DIRECT disabled")
}

func TestRestoreChunksWriteMode(t *testing.T) {
	const fileSize = 4*64*1024 + 1000

	tests := []struct {
		name        string
		mode        string
		chunkSize   int64
		concurrency int
		compress    bool
		// expectDirect is whether the chunks should be written with O_DIRECT.
		expectDirect bool
	}{
		{
			name:        "buffered",
			mode:        restoreWriteModeBuffered,
			chunkSize:   64 * 1024,
			concurrency: 1,
		},
		{
			name:         "direct",
			mode:         restoreWriteModeDirect,
			chunkSize:    64 * 1024,
			concurrency:  1,
			expectDirect: true,
		},
		{
			name:         "direct with zstd compressed chunks",
			mode:         restoreWriteModeDirect,
			chunkSize:    64 * 1024,
			concurrency:  1,
			compress:     true,
			expectDirect: true,
		},
		{
			name:         "direct with chunks of the same file restored concurrently",
			mode:         restoreWriteModeDirect,
			chunkSize:    64 * 1024,
			concurrency:  4,
			expectDirect: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cnf := &Mycnf{DataDir: t.TempDir()}
			if tt.mode == restoreWriteModeDirect {
				requireDirectIO(t, cnf.DataDir)
				requireFallocate(t, cnf.DataDir)
			}
			setRestoreWriteMode(t, tt.mode)

			data := make([]byte, fileSize)
			_, err := rand.Read(data)
			require.NoError(t, err)

			stored := map[string][]byte{}
			fe := FileEntry{Base: backupData, Name: "vt_test/t1.ibd"}
			for _, c := range computeFileChunks(0, fileSize, tt.chunkSize) {
				chunkData := data[c.Offset : c.Offset+c.Size]
				if tt.compress {
					chunkData = compressForTest(t, ZstdCompressor, chunkData)
				}
				c.Hash = crc32Hash(chunkData)
				stored[c.StorageName] = chunkData
				fe.Chunks = append(fe.Chunks, c)
			}
			fes := []FileEntry{fe}
			bm := builtinBackupManifest{SkipCompress: !tt.compress}
			if tt.compress {
				bm.CompressionEngine = ZstdCompressor
			}

			// Records, for every destination opened for writing, whether O_DIRECT was used.
			var mu sync.Mutex
			var directOpens []bool
			oldOpenFile := openFile
			t.Cleanup(func() { openFile = oldOpenFile })
			openFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
				f, err := os.OpenFile(name, flag, perm)
				if err == nil {
					mu.Lock()
					directOpens = append(directOpens, flag&unix.O_DIRECT != 0)
					mu.Unlock()
				}
				return f, err
			}

			bh := &FakeBackupHandle{
				ReadFileReturnF: func(_ context.Context, filename string) (io.ReadCloser, error) {
					return io.NopCloser(bytes.NewReader(stored[filename])), nil
				},
			}
			params := RestoreParams{
				Cnf:         cnf,
				Logger:      logutil.NewMemoryLogger(),
				Stats:       backupstats.NoStats(),
				Concurrency: tt.concurrency,
			}

			require.NoError(t, createChunkedDestinations(t.Context(), fes, cnf, "", tt.mode == restoreWriteModeDirect, params.Logger))
			// Only count the opens made to restore the chunks.
			directOpens = nil
			be := &BuiltinBackupEngine{}
			require.NoError(t, be.restoreFileEntries(t.Context(), fes, bh, bm, params, ""))

			got, err := os.ReadFile(path.Join(cnf.DataDir, fe.Name))
			require.NoError(t, err)
			assert.True(t, bytes.Equal(data, got), "restored file does not match")

			var directOpenCount int
			for _, direct := range directOpens {
				if direct {
					directOpenCount++
				}
			}
			assert.Len(t, directOpens, len(fes[0].Chunks), "every chunk opens its destination once")
			if tt.expectDirect {
				assert.Equal(t, len(fes[0].Chunks), directOpenCount, "every chunk must be opened with O_DIRECT")
			} else {
				assert.Zero(t, directOpenCount, "no chunk may be opened with O_DIRECT")
			}
		})
	}
}

func TestCreateChunkedDestinationsSizing(t *testing.T) {
	const fileSize = 3*1024*1024 + 123

	tests := []struct {
		name            string
		mode            string
		fallocateErr    error
		expectAllocated bool
		expectErr       string
		// expectFailedPrecondition is whether the error must not be retried.
		expectFailedPrecondition bool
	}{
		{
			name: "buffered mode does not preallocate the file",
			mode: restoreWriteModeBuffered,
		},
		{
			name:            "direct mode preallocates the file",
			mode:            restoreWriteModeDirect,
			expectAllocated: true,
		},
		{
			name:                     "direct mode fails when fallocate is unsupported",
			mode:                     restoreWriteModeDirect,
			fallocateErr:             vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "fallocate is not supported"),
			expectErr:                "fallocate is not supported",
			expectFailedPrecondition: true,
		},
		{
			name:         "direct mode fails on other fallocate errors",
			mode:         restoreWriteModeDirect,
			fallocateErr: syscall.ENOSPC,
			expectErr:    "no space left on device",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cnf := &Mycnf{DataDir: t.TempDir()}
			if tt.expectAllocated {
				requireFallocate(t, cnf.DataDir)
			}
			setRestoreWriteMode(t, tt.mode)
			// Counts the preallocations, and fails them with fallocateErr when it is set.
			var fallocateCalls int
			oldFallocate := fallocate
			t.Cleanup(func() { fallocate = oldFallocate })
			fallocate = func(f *os.File, size int64) error {
				fallocateCalls++
				if tt.fallocateErr != nil {
					return tt.fallocateErr
				}
				return oldFallocate(f, size)
			}

			fes := []FileEntry{{Base: backupData, Name: "vt_test/t1.ibd", Chunks: computeFileChunks(0, fileSize, 1024*1024)}}
			err := createChunkedDestinations(t.Context(), fes, cnf, "", tt.mode == restoreWriteModeDirect, logutil.NewMemoryLogger())
			if tt.expectErr != "" {
				require.ErrorContains(t, err, tt.expectErr)
				if tt.expectFailedPrecondition {
					require.ErrorContains(t, err, "--builtinbackup-restore-write-mode=buffered", "the error must say how to restore the backup")
					assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err), "the restore must not be retried")
				}
				return
			}
			require.NoError(t, err)

			fi, err := os.Stat(path.Join(cnf.DataDir, fes[0].Name))
			require.NoError(t, err)
			assert.EqualValues(t, fileSize, fi.Size())
			if !tt.expectAllocated {
				// Whether a truncated file takes up blocks depends on the filesystem, so this only
				// checks that the file wasn't preallocated.
				assert.Zero(t, fallocateCalls, "the file must not be preallocated")
				return
			}
			assert.Equal(t, 1, fallocateCalls, "the file must be preallocated once")
			allocated := fi.Sys().(*syscall.Stat_t).Blocks * 512
			assert.GreaterOrEqual(t, allocated, int64(fileSize), "file should be preallocated")
		})
	}
}

// TestCheckDirectRestore checks the requirements of direct write mode that a restore checks
// before it changes anything, and that each root directory of the chunked files is probed once,
// with a scratch file that is removed.
func TestCheckDirectRestore(t *testing.T) {
	const fileSize = 3*1024*1024 + 123

	tests := []struct {
		name          string
		chunkSize     int64
		rejectODirect bool
		fallocateErr  error
		expectErr     string
	}{
		{
			name:      "aligned chunks on a filesystem that supports O_DIRECT and fallocate",
			chunkSize: 1024 * 1024,
		},
		{
			name:      "unaligned chunk offsets",
			chunkSize: 6000,
			expectErr: "starts at offset 6000, which is not a multiple of 4096",
		},
		{
			name:          "O_DIRECT is rejected",
			chunkSize:     1024 * 1024,
			rejectODirect: true,
			expectErr:     "O_DIRECT is not supported",
		},
		{
			name:         "fallocate is unsupported",
			chunkSize:    1024 * 1024,
			fallocateErr: vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "fallocate is not supported"),
			expectErr:    "fallocate is not supported",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			if tt.expectErr == "" || tt.fallocateErr != nil {
				requireDirectIO(t, root)
			}
			if tt.expectErr == "" {
				requireFallocate(t, root)
			}
			// The directories don't exist yet, as on a tablet that is restored for the first time.
			cnf := &Mycnf{DataDir: path.Join(root, "data"), InnodbDataHomeDir: path.Join(root, "innodb")}

			var directOpens atomic.Int32
			oldOpenFile, oldFallocate := openFile, fallocate
			t.Cleanup(func() { openFile, fallocate = oldOpenFile, oldFallocate })
			openFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
				if flag&unix.O_DIRECT != 0 {
					directOpens.Add(1)
					if tt.rejectODirect {
						return nil, &os.PathError{Op: "open", Path: name, Err: syscall.EINVAL}
					}
				}
				return oldOpenFile(name, flag, perm)
			}
			if tt.fallocateErr != nil {
				fallocate = func(*os.File, int64) error { return tt.fallocateErr }
			}

			// Two chunked files under the data directory, one under the InnoDB data directory, and a
			// file that isn't chunked, so isn't restored with O_DIRECT.
			fes := []FileEntry{
				{Base: backupData, Name: "vt_test/t1.ibd", Chunks: computeFileChunks(0, fileSize, tt.chunkSize)},
				{Base: backupData, Name: "vt_test/t2.ibd", Chunks: computeFileChunks(1, fileSize, tt.chunkSize)},
				{Base: backupInnodbDataHomeDir, Name: "ibdata1", Chunks: computeFileChunks(2, fileSize, tt.chunkSize)},
				{Base: backupInnodbLogGroupHomeDir, Name: "ib_logfile0"},
			}
			err := checkDirectRestore(cnf, fes)

			for _, dir := range []string{cnf.DataDir, cnf.InnodbDataHomeDir} {
				entries, _ := os.ReadDir(dir)
				assert.Empty(t, entries, "no scratch file may be left in %v", dir)
			}
			if tt.expectErr != "" {
				require.ErrorContains(t, err, tt.expectErr)
				require.ErrorContains(t, err, "--builtinbackup-restore-write-mode=buffered", "the error must say how to restore the backup")
				assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err), "the restore must not be retried")
				return
			}
			require.NoError(t, err)
			assert.EqualValues(t, 2, directOpens.Load(), "each root directory must be probed once")
			assert.DirExists(t, cnf.DataDir)
			assert.DirExists(t, cnf.InnodbDataHomeDir)
			assert.NoDirExists(t, path.Join(cnf.DataDir, "vt_test"), "only the root directories may be created")
		})
	}
}

// TestBackupRestoreWithManyChunksDirect runs the backup and restore of TestBackupRestoreWithManyChunks
// through ExecuteBackup and ExecuteRestore with --builtinbackup-restore-write-mode=direct, and checks
// that every chunk is written with O_DIRECT.
func TestBackupRestoreWithManyChunksDirect(t *testing.T) {
	backupRoot := t.TempDir()
	requireDirectIO(t, backupRoot)
	requireFallocate(t, backupRoot)
	setRestoreWriteMode(t, restoreWriteModeDirect)

	// Only the restore opens files through openFile in this test: the destinations of the chunks.
	var directOpens, bufferedOpens atomic.Int32
	oldOpenFile := openFile
	t.Cleanup(func() { openFile = oldOpenFile })
	openFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
		if flag&unix.O_DIRECT != 0 {
			directOpens.Add(1)
		} else if flag&(os.O_WRONLY|os.O_RDWR) != 0 {
			bufferedOpens.Add(1)
		}
		return oldOpenFile(name, flag, perm)
	}

	backupRestoreWithManyChunks(t, backupRoot)

	assert.Zero(t, bufferedOpens.Load(), "no chunk may be written without O_DIRECT")
	assert.GreaterOrEqual(t, directOpens.Load(), int32(256), "each of the 256 chunks must be written with O_DIRECT")
}

// TestExecuteRestoreDirectChecksBeforeRemovingData checks that a restore that can't be done in
// direct write mode fails before it marks the restore as in progress, shuts down mysqld and
// removes the existing data.
func TestExecuteRestoreDirectChecksBeforeRemovingData(t *testing.T) {
	backupRoot := t.TempDir()
	filePath, originalChecksum := backupWithManyChunks(t, backupRoot)

	setRestoreWriteMode(t, restoreWriteModeDirect)
	oldOpenFile := openFile
	t.Cleanup(func() { openFile = oldOpenFile })
	openFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
		if flag&unix.O_DIRECT != 0 {
			return nil, &os.PathError{Op: "open", Path: name, Err: syscall.EINVAL}
		}
		return oldOpenFile(name, flag, perm)
	}

	_, err := restoreWithManyChunks(t, backupRoot)
	require.ErrorContains(t, err, "O_DIRECT is not supported")
	assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err), "the restore must not be retried")

	data, err := os.ReadFile(filePath)
	require.NoError(t, err, "the existing data must not be removed")
	assert.Equal(t, originalChecksum, crc32.ChecksumIEEE(data), "the existing data must not be changed")
	assert.NoFileExists(t, path.Join(backupRoot, RestoreState), "the restore must not be marked as in progress")
}

// TestRestoreIncrementalBackupBuffered checks that the chunks of an incremental backup are written
// buffered even with --builtinbackup-restore-write-mode=direct. Its binlogs are written to a temporary
// directory and read back once, so O_DIRECT gains nothing there, and that directory may not support it.
func TestRestoreIncrementalBackupBuffered(t *testing.T) {
	const fileSize = 4*64*1024 + 1000
	setRestoreWriteMode(t, restoreWriteModeDirect)
	oldRestorePath := builtinIncrementalRestorePath
	t.Cleanup(func() { builtinIncrementalRestorePath = oldRestorePath })
	builtinIncrementalRestorePath = t.TempDir()

	// O_DIRECT is rejected, so that the restore fails if it tries to use it.
	var directOpens, fallocateCalls atomic.Int32
	oldOpenFile, oldFallocate := openFile, fallocate
	t.Cleanup(func() { openFile, fallocate = oldOpenFile, oldFallocate })
	openFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
		if flag&unix.O_DIRECT != 0 {
			directOpens.Add(1)
			return nil, &os.PathError{Op: "open", Path: name, Err: syscall.EINVAL}
		}
		return oldOpenFile(name, flag, perm)
	}
	fallocate = func(f *os.File, size int64) error {
		fallocateCalls.Add(1)
		return oldFallocate(f, size)
	}

	data := make([]byte, fileSize)
	_, err := rand.Read(data)
	require.NoError(t, err)
	stored := map[string][]byte{}
	fe := FileEntry{Base: backupBinlogDir, Name: "vt-bin.000002"}
	for _, c := range computeFileChunks(0, fileSize, 64*1024) {
		chunkData := data[c.Offset : c.Offset+c.Size]
		c.Hash = crc32Hash(chunkData)
		stored[c.StorageName] = chunkData
		fe.Chunks = append(fe.Chunks, c)
	}
	bm := builtinBackupManifest{
		Incremental:  true,
		FileEntries:  []FileEntry{fe},
		SkipCompress: true,
	}
	bh := &FakeBackupHandle{
		ReadFileReturnF: func(_ context.Context, filename string) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(stored[filename])), nil
		},
	}
	cnf := &Mycnf{BinLogPath: path.Join(t.TempDir(), "binlogs", "vt-bin")}
	params := RestoreParams{
		Cnf:         cnf,
		Logger:      logutil.NewMemoryLogger(),
		Stats:       backupstats.NoStats(),
		Concurrency: 2,
	}

	be := &BuiltinBackupEngine{}
	createdDir, err := be.restoreFiles(t.Context(), params, bh, bm)
	require.NoError(t, err)

	fe.ParentPath = createdDir
	restoredPath, err := fe.fullPath(cnf)
	require.NoError(t, err)
	got, err := os.ReadFile(restoredPath)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(data, got), "restored file does not match")
	assert.Zero(t, directOpens.Load(), "an incremental backup must not be restored with O_DIRECT")
	assert.Zero(t, fallocateCalls.Load(), "an incremental backup must not be preallocated")
}

func directIOEnabled(t *testing.T, f *os.File) bool {
	t.Helper()
	flags, err := unix.FcntlInt(f.Fd(), unix.F_GETFL, 0)
	require.NoError(t, err)
	return flags&unix.O_DIRECT != 0
}

// requireDirectIO skips the test when the filesystem of dir does not support O_DIRECT.
func requireDirectIO(t *testing.T, dir string) {
	t.Helper()
	p := path.Join(dir, "direct-io-probe")
	require.NoError(t, os.WriteFile(p, nil, 0o644))
	t.Cleanup(func() { assert.NoError(t, os.Remove(p)) })

	f, err := openForDirectWrite(p)
	if vterrors.Code(err) == vtrpcpb.Code_UNIMPLEMENTED {
		t.Skipf("O_DIRECT is not supported in %s: %v", dir, err)
	}
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

// requireFallocate skips the test when the filesystem of dir does not support fallocate.
func requireFallocate(t *testing.T, dir string) {
	t.Helper()
	f, err := os.CreateTemp(dir, "fallocate-probe")
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, f.Close())
		assert.NoError(t, os.Remove(f.Name()))
	})

	err = fallocateFile(f, directIOAlignment)
	if vterrors.Code(err) == vtrpcpb.Code_UNIMPLEMENTED {
		t.Skipf("fallocate is not supported in %s: %v", dir, err)
	}
	require.NoError(t, err)
}

func compressForTest(t *testing.T, engine string, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	c, err := newBuiltinCompressor(engine, &buf, logutil.NewMemoryLogger())
	require.NoError(t, err)
	_, err = c.Write(data)
	require.NoError(t, err)
	require.NoError(t, c.Close())
	return buf.Bytes()
}
