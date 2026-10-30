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
	"io"
	"os"
	"path"
	"sync"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstats"
	"vitess.io/vitess/go/vt/vterrors"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
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
				defer f.Close()
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
	defer f.Close()

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
		name          string
		mode          string
		chunkSize     int64
		concurrency   int
		compress      bool
		rejectODirect bool
		// expectedDirect returns whether a chunk should be written with O_DIRECT.
		expectedDirect func(chunk FileChunk) bool
		// expectedWarning is a warning that must be logged exactly once. When empty, no warnings are expected.
		expectedWarning string
	}{
		{
			name:           "buffered",
			mode:           restoreWriteModeBuffered,
			chunkSize:      64 * 1024,
			concurrency:    1,
			expectedDirect: func(FileChunk) bool { return false },
		},
		{
			name:           "direct",
			mode:           restoreWriteModeDirect,
			chunkSize:      64 * 1024,
			concurrency:    1,
			expectedDirect: func(FileChunk) bool { return true },
		},
		{
			name:           "direct with zstd compressed chunks",
			mode:           restoreWriteModeDirect,
			chunkSize:      64 * 1024,
			concurrency:    1,
			compress:       true,
			expectedDirect: func(FileChunk) bool { return true },
		},
		{
			name:           "direct with chunks of the same file restored concurrently",
			mode:           restoreWriteModeDirect,
			chunkSize:      64 * 1024,
			concurrency:    4,
			expectedDirect: func(FileChunk) bool { return true },
		},
		{
			name:            "direct falls back to buffered for unaligned chunk offsets",
			mode:            restoreWriteModeDirect,
			chunkSize:       6000,
			concurrency:     1,
			expectedDirect:  func(c FileChunk) bool { return c.Offset%directIOAlignment == 0 },
			expectedWarning: "don't start at an O_DIRECT-aligned offset",
		},
		{
			name:            "direct falls back to buffered when O_DIRECT is rejected",
			mode:            restoreWriteModeDirect,
			chunkSize:       64 * 1024,
			concurrency:     1,
			rejectODirect:   true,
			expectedDirect:  func(FileChunk) bool { return false },
			expectedWarning: "Can't use O_DIRECT for chunked file",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cnf := &Mycnf{DataDir: t.TempDir()}
			if tt.mode == restoreWriteModeDirect && !tt.rejectODirect {
				requireDirectIO(t, cnf.DataDir)
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
				if flag&unix.O_DIRECT != 0 && tt.rejectODirect {
					return nil, &os.PathError{Op: "open", Path: name, Err: syscall.EINVAL}
				}
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
			logger := logutil.NewMemoryLogger()
			params := RestoreParams{
				Cnf:         cnf,
				Logger:      logger,
				Stats:       backupstats.NoStats(),
				Concurrency: tt.concurrency,
			}

			require.NoError(t, createChunkedDestinations(t.Context(), fes, cnf, "", params.Logger))
			// Only count the opens made to restore the chunks.
			directOpens = nil
			be := &BuiltinBackupEngine{}
			require.NoError(t, be.restoreFileEntries(t.Context(), fes, bh, bm, params, ""))

			got, err := os.ReadFile(path.Join(cnf.DataDir, fe.Name))
			require.NoError(t, err)
			assert.True(t, bytes.Equal(data, got), "restored file does not match")

			var expectedDirectOpens, directOpenCount int
			for _, c := range fes[0].Chunks {
				if tt.expectedDirect(c) {
					expectedDirectOpens++
				}
			}
			for _, direct := range directOpens {
				if direct {
					directOpenCount++
				}
			}
			assert.Len(t, directOpens, len(fes[0].Chunks), "every chunk opens its destination once")
			assert.Equal(t, expectedDirectOpens, directOpenCount, "chunks opened with O_DIRECT")

			warnings := warningsLogged(logger)
			if tt.expectedWarning == "" {
				assert.Empty(t, warnings)
			} else {
				require.Len(t, warnings, 1, "the fallback should be logged once per file, not once per chunk")
				assert.Contains(t, warnings[0], tt.expectedWarning)
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
	}{
		{
			name: "buffered mode leaves the file sparse",
			mode: restoreWriteModeBuffered,
		},
		{
			name:            "direct mode preallocates the file",
			mode:            restoreWriteModeDirect,
			expectAllocated: true,
		},
		{
			name:         "direct mode falls back to a sparse file when fallocate is unsupported",
			mode:         restoreWriteModeDirect,
			fallocateErr: vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "test"),
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
			if tt.fallocateErr != nil {
				oldFallocate := fallocate
				t.Cleanup(func() { fallocate = oldFallocate })
				fallocate = func(*os.File, int64) error { return tt.fallocateErr }
			}

			fes := []FileEntry{{Base: backupData, Name: "vt_test/t1.ibd", Chunks: computeFileChunks(0, fileSize, 1024*1024)}}
			err := createChunkedDestinations(t.Context(), fes, cnf, "", logutil.NewMemoryLogger())
			if tt.expectErr != "" {
				assert.ErrorContains(t, err, tt.expectErr)
				return
			}
			require.NoError(t, err)

			fi, err := os.Stat(path.Join(cnf.DataDir, fes[0].Name))
			require.NoError(t, err)
			assert.EqualValues(t, fileSize, fi.Size())
			allocated := fi.Sys().(*syscall.Stat_t).Blocks * 512
			if tt.expectAllocated {
				assert.GreaterOrEqual(t, allocated, int64(fileSize), "file should be preallocated")
			} else {
				assert.Less(t, allocated, int64(fileSize), "file should be sparse")
			}
		})
	}
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
	defer os.Remove(p)

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
	defer os.Remove(f.Name())
	defer f.Close()

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

func warningsLogged(logger *logutil.MemoryLogger) []string {
	var warnings []string
	for _, e := range logger.LogEvents() {
		if e.Level == logutilpb.Level_WARNING {
			warnings = append(warnings, e.Value)
		}
	}
	return warnings
}
