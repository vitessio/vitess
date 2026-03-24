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
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/fileutil"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	sqltypes "vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstats"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
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

func TestShouldDrainForBackupBuiltIn(t *testing.T) {
	be := &BuiltinBackupEngine{}

	assert.True(t, be.ShouldDrainForBackup(&tabletmanagerdatapb.BackupRequest{}))
	assert.False(t, be.ShouldDrainForBackup(&tabletmanagerdatapb.BackupRequest{IncrementalFromPos: "auto"}))
	assert.False(t, be.ShouldDrainForBackup(&tabletmanagerdatapb.BackupRequest{IncrementalFromPos: "99ca8ed4-399c-11ee-861b-0a43f95f28a3:1-197"}))
	assert.False(t, be.ShouldDrainForBackup(&tabletmanagerdatapb.BackupRequest{IncrementalFromPos: "MySQL56/99ca8ed4-399c-11ee-861b-0a43f95f28a3:1-197"}))
}

// nopWriteCloser wraps io.Discard as a WriteCloser, used in backup tests.
type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

// setupRebuildGTIDMysqld creates a FakeMysqlDaemon configured for the primary
// path of executeFullBackup (no replication, super_read_only already on).
func setupRebuildGTIDMysqld(t *testing.T) *FakeMysqlDaemon {
	t.Helper()
	sqldb := fakesqldb.New(t)
	t.Cleanup(sqldb.Close)
	mysqld := NewFakeMysqlDaemon(sqldb)
	mysqld.ReplicationStatusError = mysql.ErrNotReplica
	mysqld.SuperReadOnly.Store(true)
	return mysqld
}

// setupRebuildGTIDCnf creates a temp directory structure for the backup cnf.
func setupRebuildGTIDCnf(t *testing.T) *Mycnf {
	t.Helper()
	root := t.TempDir()
	for _, d := range []string{"innodb_data", "innodb_log", "data"} {
		require.NoError(t, os.MkdirAll(filepath.Join(root, d), os.ModePerm))
	}
	return &Mycnf{
		InnodbDataHomeDir:     filepath.Join(root, "innodb_data"),
		InnodbLogGroupHomeDir: filepath.Join(root, "innodb_log"),
		DataDir:               filepath.Join(root, "data"),
	}
}

func TestExecuteFullBackupRebuildGTIDExecuted(t *testing.T) {
	origFlag := builtinBackupRebuildGTIDExecuted
	origProgress := builtinBackupProgress
	t.Cleanup(func() {
		builtinBackupRebuildGTIDExecuted = origFlag
		builtinBackupProgress = origProgress
	})
	builtinBackupProgress = 1 * time.Millisecond

	const alterQuery = "ALTER TABLE mysql.gtid_executed ENGINE=InnoDB"

	be := &BuiltinBackupEngine{}

	makeParams := func(mysqld *FakeMysqlDaemon, cnf *Mycnf) BackupParams {
		return BackupParams{
			Cnf:                  cnf,
			Logger:               logutil.NewMemoryLogger(),
			Mysqld:               mysqld,
			Concurrency:          1,
			HookExtraEnv:         map[string]string{},
			BackupTime:           time.Now(),
			Stats:                backupstats.NoStats(),
			MysqlShutdownTimeout: 10 * time.Second,
		}
	}

	makeBH := func() *FakeBackupHandle {
		return &FakeBackupHandle{
			AddFileReturnF: func(string) FakeBackupHandleAddFileReturn {
				return FakeBackupHandleAddFileReturn{WriteCloser: nopWriteCloser{io.Discard}}
			},
		}
	}

	t.Run("flag disabled - ALTER TABLE not called", func(t *testing.T) {
		builtinBackupRebuildGTIDExecuted = false
		mysqld := setupRebuildGTIDMysqld(t)
		alterCalled := false
		mysqld.FetchSuperQueryCallback = func(query string) (*sqltypes.Result, error) {
			if query == alterQuery {
				alterCalled = true
			}
			return &sqltypes.Result{}, nil
		}

		params := makeParams(mysqld, setupRebuildGTIDCnf(t))
		be.executeFullBackup(context.Background(), params, makeBH()) //nolint:errcheck
		assert.False(t, alterCalled, "ALTER TABLE should not be called when flag is disabled")
	})

	t.Run("flag enabled - ALTER TABLE called", func(t *testing.T) {
		builtinBackupRebuildGTIDExecuted = true
		mysqld := setupRebuildGTIDMysqld(t)
		alterCalled := false
		mysqld.FetchSuperQueryCallback = func(query string) (*sqltypes.Result, error) {
			if query == alterQuery {
				alterCalled = true
			}
			return &sqltypes.Result{}, nil
		}

		params := makeParams(mysqld, setupRebuildGTIDCnf(t))
		be.executeFullBackup(context.Background(), params, makeBH()) //nolint:errcheck
		assert.True(t, alterCalled, "ALTER TABLE should be called when flag is enabled")
	})

	t.Run("flag enabled - ALTER TABLE failure aborts backup", func(t *testing.T) {
		builtinBackupRebuildGTIDExecuted = true
		mysqld := setupRebuildGTIDMysqld(t)
		mysqld.FetchSuperQueryCallback = func(query string) (*sqltypes.Result, error) {
			if query == alterQuery {
				return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "disk full")
			}
			return &sqltypes.Result{}, nil
		}

		params := makeParams(mysqld, setupRebuildGTIDCnf(t))
		result, err := be.executeFullBackup(context.Background(), params, &FakeBackupHandle{})
		require.Error(t, err)
		assert.Equal(t, BackupUnusable, result)
		assert.ErrorContains(t, err, "failed to rebuild mysql.gtid_executed")
	})

	// Ensure the backup handle is not used (i.e. backup was aborted before backupFiles).
	t.Run("flag enabled - ALTER TABLE failure does not write any files", func(t *testing.T) {
		builtinBackupRebuildGTIDExecuted = true
		mysqld := setupRebuildGTIDMysqld(t)
		mysqld.FetchSuperQueryCallback = func(query string) (*sqltypes.Result, error) {
			if query == alterQuery {
				return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "disk full")
			}
			return &sqltypes.Result{}, nil
		}

		bh := &FakeBackupHandle{}
		params := makeParams(mysqld, setupRebuildGTIDCnf(t))
		be.executeFullBackup(context.Background(), params, bh) //nolint:errcheck
		assert.Empty(t, bh.AddFileCalls, "no files should be written when ALTER TABLE fails")
	})
}
