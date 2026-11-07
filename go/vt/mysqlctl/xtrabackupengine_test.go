/*
Copyright 2019 The Vitess Authors.

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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/ioutil"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstats"
	"vitess.io/vitess/go/vt/vterrors"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

type (
	// ctxAwareCloser simulates a backend (e.g. GCS) whose Close() blocks on
	// upload completion and respects context cancellation.
	ctxAwareCloser struct {
		ctx context.Context
	}
)

func (c ctxAwareCloser) Write(p []byte) (int, error) { return len(p), nil }
func (c ctxAwareCloser) Close() error {
	<-c.ctx.Done()
	return c.ctx.Err()
}

func TestFindReplicationPosition(t *testing.T) {
	input := `MySQL binlog position: filename 'vt-0476396352-bin.000005', position '310088991', GTID of the last change '145e508e-ae54-11e9-8ce6-46824dd1815e:1-3,
	1e51f8be-ae54-11e9-a7c6-4280a041109b:1-3,
	47b59de1-b368-11e9-b48b-624401d35560:1-152981,
	557def0a-b368-11e9-84ed-f6fffd91cc57:1-3,
	599ef589-ae55-11e9-9688-ca1f44501925:1-14857169,
	b9ce485d-b36b-11e9-9b17-2a6e0a6011f4:1-371262'
	MySQL replica binlog position: master host '10.128.0.43', purge list '145e508e-ae54-11e9-8ce6-46824dd1815e:1-3, 1e51f8be-ae54-11e9-a7c6-4280a041109b:1-3, 47b59de1-b368-11e9-b48b-624401d35560:1-152981, 557def0a-b368-11e9-84ed-f6fffd91cc57:1-3, 599ef589-ae55-11e9-9688-ca1f44501925:1-14857169, b9ce485d-b36b-11e9-9b17-2a6e0a6011f4:1-371262', channel name: ''
	
	190809 00:15:44 [00] Streaming <STDOUT>
	190809 00:15:44 [00]        ...done
	190809 00:15:44 [00] Streaming <STDOUT>
	190809 00:15:44 [00]        ...done
	xtrabackup: Transaction log of lsn (405344842034) to (406364859653) was copied.
	190809 00:16:14 completed OK!`
	want := "145e508e-ae54-11e9-8ce6-46824dd1815e:1-3,1e51f8be-ae54-11e9-a7c6-4280a041109b:1-3,47b59de1-b368-11e9-b48b-624401d35560:1-152981,557def0a-b368-11e9-84ed-f6fffd91cc57:1-3,599ef589-ae55-11e9-9688-ca1f44501925:1-14857169,b9ce485d-b36b-11e9-9b17-2a6e0a6011f4:1-371262"

	pos, err := findReplicationPosition(input, "MySQL56", logutil.NewConsoleLogger())
	require.NoError(t, err)
	assert.Equal(t, want, pos.String())
}

func TestFindReplicationPositionFromXtrabackupInfo(t *testing.T) {
	input := `tool_version = 8.0.35-30
	binlog_pos = filename 'vt-0476396352-bin.000005', position '310088991', GTID of the last change '145e508e-ae54-11e9-8ce6-46824dd1815e:1-3,
	1e51f8be-ae54-11e9-a7c6-4280a041109b:1-3,
	47b59de1-b368-11e9-b48b-624401d35560:1-152981,
	557def0a-b368-11e9-84ed-f6fffd91cc57:1-3,
	599ef589-ae55-11e9-9688-ca1f44501925:1-14857169,
	b9ce485d-b36b-11e9-9b17-2a6e0a6011f4:1-371262'
	format = xbstream
	`
	want := "145e508e-ae54-11e9-8ce6-46824dd1815e:1-3,1e51f8be-ae54-11e9-a7c6-4280a041109b:1-3,47b59de1-b368-11e9-b48b-624401d35560:1-152981,557def0a-b368-11e9-84ed-f6fffd91cc57:1-3,599ef589-ae55-11e9-9688-ca1f44501925:1-14857169,b9ce485d-b36b-11e9-9b17-2a6e0a6011f4:1-371262"

	tmp, err := os.MkdirTemp(t.TempDir(), "test")
	require.NoError(t, err)

	f, err := os.Create(path.Join(tmp, xtrabackupInfoFile))
	require.NoError(t, err)
	_, err = f.WriteString(input)
	assert.NoError(t, err)
	assert.NoError(t, f.Close())

	pos, err := findReplicationPositionFromXtrabackupInfo(tmp, "MySQL56", logutil.NewConsoleLogger())
	require.NoError(t, err)
	assert.Equal(t, want, pos.String())
}

func TestFindReplicationPositionNoMatchFromXtrabackupInfo(t *testing.T) {
	// Make sure failure to find a match triggers an error.
	input := `nothing`

	_, err := findReplicationPositionFromXtrabackupInfo(input, "MySQL56", logutil.NewConsoleLogger())
	assert.Error(t, err)
}

func TestFindReplicationPositionEmptyMatchFromXtrabackupInfo(t *testing.T) {
	// Make sure failure to find a match triggers an error.
	input := `GTID of the last change '
	
	'`

	_, err := findReplicationPositionFromXtrabackupInfo(input, "MySQL56", logutil.NewConsoleLogger())
	assert.Error(t, err)
}

func TestStripeRoundTrip(t *testing.T) {
	// Generate some random input data.
	dataSize := int64(1000000)
	input := make([]byte, dataSize)
	rand.Read(input)

	test := func(blockSize int64, stripes int) {
		// Write it out striped across some buffers.
		buffers := make([]bytes.Buffer, stripes)
		readers := []io.Reader{}
		writers := []io.Writer{}
		for i := range buffers {
			readers = append(readers, &buffers[i])
			writers = append(writers, &buffers[i])
		}
		copyToStripes(writers, bytes.NewReader(input), blockSize)

		// Read it back and merge.
		outBuf := &bytes.Buffer{}
		written, err := io.Copy(outBuf, stripeReader(readers, blockSize))
		require.NoError(t, err)
		assert.Equal(t, dataSize, written)

		output := outBuf.Bytes()
		assert.Equal(t, input, output)
	}

	// Test block size that evenly divides data size.
	test(1000, 10)
	// Test block size that doesn't evenly divide data size.
	test(3000, 10)
	// Test stripe count that doesn't evenly divide data size.
	test(1000, 30)
	// Test block size and stripe count that don't evenly divide data size.
	test(6000, 7)
}

func TestShouldDrainForBackupXtrabackup(t *testing.T) {
	be := &XtrabackupEngine{}

	// Test default behavior (should not drain)
	originalValue := xtrabackupShouldDrain
	defer func() { xtrabackupShouldDrain = originalValue }()

	xtrabackupShouldDrain = false
	assert.False(t, be.ShouldDrainForBackup(nil))
	assert.False(t, be.ShouldDrainForBackup(&tabletmanagerdatapb.BackupRequest{}))

	// Test configurable behavior (should drain when flag is set)
	xtrabackupShouldDrain = true
	assert.True(t, be.ShouldDrainForBackup(nil))
	assert.True(t, be.ShouldDrainForBackup(&tabletmanagerdatapb.BackupRequest{}))
}

// TestCloseBackupFilesDoesNotCancelContextOnSuccess guards against a
// regression where closeBackupFiles cancelled ctx as soon as all files
// finished closing. On backends like S3 and Ceph, that ctx is also used by a
// background upload goroutine that Close() does not wait for, so cancelling
// it right after Close() returns aborts an otherwise-successful, still
// in-flight upload. A successful close must only stop the watchdog, never
// cancel the context itself — that's left for bh.Wait()/EndBackup() later.
func TestCloseBackupFilesDoesNotCancelContextOnSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	logger := logutil.NewMemoryLogger()

	destFiles := []io.WriteCloser{nopWriteCloser{}, nopWriteCloser{}}
	var finalErr error

	done := make(chan struct{})
	go func() {
		defer close(done)
		// Use a generous, CI-safe watchdog timeout. The nop closers return
		// immediately, so a successful close stops the watchdog long before
		// this fires. A large timeout keeps a preempted CI worker from
		// spuriously tripping the watchdog and cancelling ctx.
		closeBackupFiles(ctx, cancel, 30*time.Second, destFiles, "backup", len(destFiles), logger, &finalErr)
	}()

	// closeBackupFiles stops the watchdog before returning, so once it has
	// returned the watchdog can no longer fire. Synchronizing on completion
	// (rather than sleeping past a wall-clock window) makes the assertions
	// below deterministic: with a 30s watchdog timeout, the timer cannot have
	// fired during this sub-second test. Poll non-blockingly so a regression
	// that hangs closeBackupFiles fails at the deadline instead of blocking on
	// <-done indefinitely.
	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 30*time.Second, 10*time.Millisecond)

	require.NoError(t, finalErr)

	// A successful close must leave ctx untouched for the still-in-flight
	// upload, and must not have logged a watchdog timeout.
	assert.NoError(t, ctx.Err())
	assert.NotContains(t, logger.String(), "Timed out waiting for Close()")
}

// TestCloseBackupFilesCancelsOnRealTimeout guards the other direction: if
// Close() genuinely hangs past the timeout, the watchdog must still log and
// cancel ctx so a stuck Close() (e.g. GCS's synchronous upload-on-Close) can
// abort instead of hanging forever.
func TestCloseBackupFilesCancelsOnRealTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	logger := logutil.NewMemoryLogger()

	destFiles := []io.WriteCloser{ctxAwareCloser{ctx: ctx}}
	var finalErr error

	done := make(chan struct{})
	go func() {
		defer close(done)
		closeBackupFiles(ctx, cancel, 50*time.Millisecond, destFiles, "backup", len(destFiles), logger, &finalErr)
	}()

	// The ctxAwareCloser blocks until the watchdog cancels ctx, so this path
	// is deterministic in outcome — it only needs a generous, CI-safe deadline
	// for how long we wait. A resource-starved runner can pause the goroutine
	// for multiple seconds before the 50ms watchdog fires, so use 30s. Poll
	// non-blockingly so a regression that hangs closeBackupFiles fails the test
	// at the deadline instead of blocking on <-done indefinitely.
	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 30*time.Second, 10*time.Millisecond)

	require.ErrorIs(t, finalErr, context.Canceled)

	assert.Contains(t, logger.String(), "Timed out waiting for Close()")
	assert.Error(t, ctx.Err())
}

// TestXtrabackupExecuteBackupVerifiesPosition runs a full backup with a fake
// xtrabackup binary, and checks that the engine verifies the position its
// MANIFEST records before it writes the MANIFEST, and writes none when that
// check fails.
func TestXtrabackupExecuteBackupVerifiesPosition(t *testing.T) {
	const (
		serverUUID = "16b1039f-22b6-11ed-b765-0a43f95f28a3"
		gtids      = serverUUID + ":1-10"
	)

	// The fake xtrabackup writes xtrabackup_info into --target-dir, where the
	// engine reads the position, and streams a few bytes as the backup.
	binDir := t.TempDir()
	script := fmt.Sprintf(`#!/bin/bash
for arg in "$@"; do
  case "$arg" in --target-dir=*) dir="${arg#--target-dir=}" ;; esac
done
echo "binlog_pos = filename 'vt-bin.000001', position '4', GTID of the last change '%s'" > "$dir/xtrabackup_info"
echo "backup data"
`, gtids)
	require.NoError(t, os.WriteFile(path.Join(binDir, xtrabackupBinaryName), []byte(script), 0o755))
	oldEnginePath, oldUser := xtrabackupEnginePath, xtrabackupUser
	xtrabackupEnginePath, xtrabackupUser = binDir, "backup"
	t.Cleanup(func() { xtrabackupEnginePath, xtrabackupUser = oldEnginePath, oldUser })

	fakedb := fakesqldb.New(t)
	t.Cleanup(fakedb.Close)
	fakedb.AddQuery("SELECT @@global.gtid_executed", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("@@global.gtid_executed", "varchar"), gtids))
	fakedb.AddQuery("SELECT @@global.server_uuid", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("@@global.server_uuid", "varchar"), serverUUID))
	mysqld := NewFakeMysqlDaemon(fakedb)
	t.Cleanup(mysqld.Close)
	mysqld.Version = "8.0.40"

	root := t.TempDir()
	for _, dir := range []string{"innodb", "log", "datadir", "tmp"} {
		require.NoError(t, os.MkdirAll(path.Join(root, dir), 0o755))
	}
	cnf := &Mycnf{
		InnodbDataHomeDir:     path.Join(root, "innodb"),
		InnodbLogGroupHomeDir: path.Join(root, "log"),
		DataDir:               path.Join(root, "datadir"),
		TmpDir:                path.Join(root, "tmp"),
	}

	for _, tc := range []struct {
		name      string
		verifyErr error
	}{
		{"MANIFEST written after the check passes", nil},
		{"no MANIFEST when the check fails", vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "errant GTIDs")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bh := &FakeBackupHandle{
				AddFileReturnF: func(string) FakeBackupHandleAddFileReturn {
					return FakeBackupHandleAddFileReturn{WriteCloser: ioutil.NewBytesBufferWriter()}
				},
			}
			var verified []replication.Position
			params := BackupParams{
				Cnf:    cnf,
				Mysqld: mysqld,
				Logger: logutil.NewMemoryLogger(),
				Stats:  backupstats.NoStats(),
				VerifyPosition: func(ctx context.Context, pos replication.Position) error {
					verified = append(verified, pos)
					return tc.verifyErr
				},
			}

			result, err := (&XtrabackupEngine{}).ExecuteBackup(t.Context(), params, bh)

			require.Len(t, verified, 1)
			assert.Equal(t, gtids, verified[0].String())
			var added []string
			for _, call := range bh.AddFileCalls {
				added = append(added, call.Filename)
			}
			if tc.verifyErr == nil {
				require.NoError(t, err)
				assert.Equal(t, BackupUsable, result)
				assert.Contains(t, added, backupManifestFileName)
			} else {
				require.ErrorContains(t, err, "errant GTIDs")
				assert.Equal(t, BackupUnusable, result)
				assert.NotContains(t, added, backupManifestFileName)
			}
		})
	}
}
