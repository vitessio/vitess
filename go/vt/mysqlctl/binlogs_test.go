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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func TestBinlogLocation(t *testing.T) {
	tests := []struct {
		name          string
		cnf           Mycnf
		wantBasename  string
		wantIndexPath string
		wantErr       bool
	}{
		{
			name: "absolute log-bin", cnf: Mycnf{DataDir: "/vt/data", BinLogPath: "/vt/bin-logs/vt-0000000101-bin"},
			wantBasename: "/vt/bin-logs/vt-0000000101-bin", wantIndexPath: "/vt/bin-logs/vt-0000000101-bin.index",
		},
		{
			name: "relative log-bin", cnf: Mycnf{DataDir: "/vt/data", BinLogPath: "mysql-bin"},
			wantBasename: "/vt/data/mysql-bin", wantIndexPath: "/vt/data/mysql-bin.index",
		},
		{
			name: "log-bin extension is dropped", cnf: Mycnf{DataDir: "/vt/data", BinLogPath: "/vt/bin-logs/src-bin.log"},
			wantBasename: "/vt/bin-logs/src-bin", wantIndexPath: "/vt/bin-logs/src-bin.index",
		},
		{
			name: "absolute index without extension", cnf: Mycnf{DataDir: "/vt/data", BinLogPath: "/vt/b/x", BinLogIndexPath: "/vt/idx/custom"},
			wantBasename: "/vt/b/x", wantIndexPath: "/vt/idx/custom.index",
		},
		{
			name: "relative index with extension", cnf: Mycnf{DataDir: "/vt/data", BinLogPath: "/vt/b/x", BinLogIndexPath: "custom.idx"},
			wantBasename: "/vt/b/x", wantIndexPath: "/vt/data/custom.idx",
		},
		{name: "empty log-bin", cnf: Mycnf{DataDir: "/vt/data"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			basename, indexPath, err := binlogLocation(&tt.cnf)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantBasename, basename)
			assert.Equal(t, tt.wantIndexPath, indexPath)
		})
	}
}

func TestBinlogFilesForFullBackup(t *testing.T) {
	dir := t.TempDir()
	cnf := &Mycnf{DataDir: dir, BinLogPath: filepath.Join(dir, "src-bin")}
	mtime := time.Unix(1700000000, 0).UTC()
	for _, seq := range []string{"000001", "000002"} {
		p := filepath.Join(dir, "src-bin."+seq)
		require.NoError(t, os.WriteFile(p, []byte("x"), 0o644))
		require.NoError(t, os.Chtimes(p, mtime, mtime))
	}
	index := filepath.Join(dir, "src-bin.000001") + "\n./src-bin.000002\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "src-bin.index"), []byte(index), 0o644))

	fes, err := binlogFilesForFullBackup(cnf)
	require.NoError(t, err)
	assert.Equal(t, []FileEntry{
		{Base: backupBinlogStripped, Name: "binlog.000001", ModTime: mtime},
		{Base: backupBinlogStripped, Name: "binlog.000002", ModTime: mtime},
	}, fes)

	t.Run("missing file", func(t *testing.T) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "src-bin.index"), []byte("./src-bin.000003\n"), 0o644))
		_, err := binlogFilesForFullBackup(cnf)
		require.ErrorContains(t, err, "src-bin.000003")
	})
	t.Run("file outside the basename", func(t *testing.T) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "other-bin.000001"), []byte("x"), 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "src-bin.index"), []byte("./other-bin.000001\n"), 0o644))
		_, err := binlogFilesForFullBackup(cnf)
		require.ErrorContains(t, err, "other-bin.000001")
	})
}

func TestCheckBinlogsBackupable(t *testing.T) {
	tests := []struct {
		name string
		rows []string // Variable_name|Value, as SHOW GLOBAL VARIABLES returns them
		// binLogPath overrides the cnf's log-bin, which matches what mysqld reports.
		binLogPath string
		noLogBin   bool
		noIndex    bool
		wantErr    string
	}{
		{name: "with replica updates", rows: []string{"log_bin|ON", "log_replica_updates|ON", "log_slave_updates|ON"}},
		{name: "binlog off", rows: []string{"log_bin|OFF", "log_replica_updates|ON", "log_slave_updates|ON"}, wantErr: "log_bin"},
		{name: "without replica updates", rows: []string{"log_bin|ON", "log_replica_updates|OFF", "log_slave_updates|OFF"}, wantErr: "log_replica_updates is off"},
		// MySQL 5.7 has only log_slave_updates.
		{name: "MySQL 5.7 with replica updates", rows: []string{"log_bin|ON", "log_slave_updates|ON"}},
		{name: "MySQL 5.7 without replica updates", rows: []string{"log_bin|ON", "log_slave_updates|OFF"}, wantErr: "log_replica_updates is off"},
		// The binlogs are found through cnf once mysqld stops, so cnf must describe
		// them before it does.
		{name: "log-bin unset", rows: []string{"log_bin|ON", "log_replica_updates|ON"}, noLogBin: true, wantErr: "log-bin is not set"},
		{name: "log-bin differs from mysqld's", rows: []string{"log_bin|ON", "log_replica_updates|ON"}, binLogPath: "other-bin", wantErr: "log_bin_basename"},
		{name: "index unreadable", rows: []string{"log_bin|ON", "log_replica_updates|ON"}, noIndex: true, wantErr: "cannot read binlog index"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakedb := fakesqldb.New(t)
			t.Cleanup(fakedb.Close)
			mysqld := NewFakeMysqlDaemon(fakedb)
			t.Cleanup(mysqld.Close)
			dir := t.TempDir()
			src := filepath.Join(dir, "src-bin")
			require.NoError(t, os.WriteFile(src+".000001", []byte("x"), 0o644))
			if !tt.noIndex {
				require.NoError(t, os.WriteFile(src+".index", []byte(src+".000001\n"), 0o644))
			}
			mysqld.FetchSuperQueryMap = map[string]*sqltypes.Result{
				binlogBackupVarsQuery: sqltypes.MakeTestResult(
					sqltypes.MakeTestFields("Variable_name|Value", "varchar|varchar"),
					tt.rows...),
				binlogLocationQuery: sqltypes.MakeTestResult(
					sqltypes.MakeTestFields("log_bin_basename|log_bin_index", "varchar|varchar"),
					src+"|"+src+".index"),
			}
			cnf := &Mycnf{DataDir: filepath.Join(dir, "data"), BinLogPath: src}
			if tt.binLogPath != "" {
				cnf.BinLogPath = filepath.Join(dir, tt.binLogPath)
			}
			if tt.noLogBin {
				cnf.BinLogPath = ""
			}
			err := checkBinlogsBackupable(t.Context(), mysqld, cnf)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
			assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
		})
	}
}

func TestValidateIncludeBinlogs(t *testing.T) {
	require.NoError(t, ValidateIncludeBinlogs(BackupParams{BackupEngine: builtinBackupEngineName}))
	require.NoError(t, ValidateIncludeBinlogs(BackupParams{BackupEngine: "xtrabackup"}), "no-op without IncludeBinlogs")

	err := ValidateIncludeBinlogs(BackupParams{IncludeBinlogs: true, BackupEngine: "xtrabackup"})
	assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
	err = ValidateIncludeBinlogs(BackupParams{IncludeBinlogs: true, BackupEngine: builtinBackupEngineName, IncrementalFromPos: "auto"})
	assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
	require.NoError(t, ValidateIncludeBinlogs(BackupParams{IncludeBinlogs: true, BackupEngine: builtinBackupEngineName}))
}

func TestFinishBinlogRestoreRejectsMissingModTime(t *testing.T) {
	dir := t.TempDir()
	cnf := &Mycnf{DataDir: dir, BinLogPath: filepath.Join(dir, "dst-bin")}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "dst-bin.000001"), []byte("x"), 0o644))
	err := finishBinlogRestore(cnf, []FileEntry{{Base: backupBinlogStripped, Name: "binlog.000001"}})
	assert.Equal(t, vtrpcpb.Code_INVALID_ARGUMENT, vterrors.Code(err))
}

func TestVerifyRestoredBinlogs(t *testing.T) {
	dir := t.TempDir()
	cnf := &Mycnf{DataDir: filepath.Join(dir, "data"), BinLogPath: filepath.Join(dir, "bin-logs/dst-bin")}
	customIndexCnf := &Mycnf{DataDir: filepath.Join(dir, "data"), BinLogPath: filepath.Join(dir, "bin-logs/dst-bin"), BinLogIndexPath: "custom.idx"}
	relativeCnf := &Mycnf{DataDir: filepath.Join(dir, "data"), BinLogPath: "sub/dst-bin"}
	// The binlogs are listed from the index the restore wrote, so the check isn't
	// limited by the row cap on SHOW BINARY LOGS, which the fake doesn't answer.
	for _, index := range []string{"bin-logs/dst-bin.index", "data/custom.idx", "data/sub/dst-bin.index"} {
		p := filepath.Join(dir, index)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		binlogs := filepath.Join(dir, "bin-logs/dst-bin.000001") + "\n" + filepath.Join(dir, "bin-logs/dst-bin.000002") + "\n"
		require.NoError(t, os.WriteFile(p, []byte(binlogs), 0o644))
	}
	pos, err := replication.DecodePosition("MySQL56/16b1039f-22b6-11ed-b765-0a43f95f28a3:1-10")
	require.NoError(t, err)
	other, err := replication.DecodePosition("MySQL56/16b1039f-22b6-11ed-b765-0a43f95f28a3:1-9")
	require.NoError(t, err)

	tests := []struct {
		name string
		cnf  *Mycnf
		// basename and index are mysqld's reported paths, relative to dir unless
		// reportedAsIs.
		basename     string
		index        string
		reportedAsIs bool
		executed     replication.Position
		// unreadable is a binlog that SHOW BINLOG EVENTS fails on, as with a missing key.
		unreadable string
		wantErr    string
		wantCode   vtrpcpb.Code
	}{
		{name: "match", cnf: cnf, basename: "bin-logs/dst-bin", index: "bin-logs/dst-bin.index", executed: pos},
		{name: "custom index extension is reported as .index", cnf: customIndexCnf, basename: "bin-logs/dst-bin", index: "data/custom.index", executed: pos},
		// mysqld reports a log-bin in a relative subdirectory as given.
		{name: "relative subdirectory is reported relative", cnf: relativeCnf, basename: "sub/dst-bin", index: "sub/dst-bin.index", reportedAsIs: true, executed: pos},
		{name: "basename differs", cnf: cnf, basename: "data/binlog", index: "bin-logs/dst-bin.index", executed: pos, wantErr: "log_bin_basename"},
		{name: "index differs", cnf: cnf, basename: "bin-logs/dst-bin", index: "data/binlog.index", executed: pos, wantErr: "log_bin_index"},
		{name: "gtid_executed differs", cnf: cnf, basename: "bin-logs/dst-bin", index: "bin-logs/dst-bin.index", executed: other, wantErr: "gtid_executed"},
		{name: "binlog unreadable", cnf: cnf, basename: "bin-logs/dst-bin", index: "bin-logs/dst-bin.index", executed: pos, unreadable: "dst-bin.000002", wantErr: "cannot read restored binlog dst-bin.000002", wantCode: vtrpcpb.Code_FAILED_PRECONDITION},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakedb := fakesqldb.New(t)
			t.Cleanup(fakedb.Close)
			mysqld := NewFakeMysqlDaemon(fakedb)
			t.Cleanup(mysqld.Close)
			mysqld.CurrentPrimaryPosition = tt.executed
			basename, index := filepath.Join(dir, tt.basename), filepath.Join(dir, tt.index)
			if tt.reportedAsIs {
				basename, index = tt.basename, tt.index
			}
			mysqld.FetchSuperQueryMap = map[string]*sqltypes.Result{
				binlogLocationQuery: sqltypes.MakeTestResult(
					sqltypes.MakeTestFields("log_bin_basename|log_bin_index", "varchar|varchar"),
					basename+"|"+index),
			}
			for _, name := range []string{"dst-bin.000001", "dst-bin.000002"} {
				if name != tt.unreadable {
					mysqld.FetchSuperQueryMap["SHOW BINLOG EVENTS IN '"+name+"' LIMIT 1"] = &sqltypes.Result{}
				}
			}
			// No ExpectedExecuteSuperQueryList: any RESET or SET GLOBAL gtid_purged fails the test.
			err := verifyRestoredBinlogs(t.Context(), mysqld, tt.cnf, pos)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
			wantCode := vtrpcpb.Code_INTERNAL
			if tt.wantCode != vtrpcpb.Code_OK {
				wantCode = tt.wantCode
			}
			assert.Equal(t, wantCode, vterrors.Code(err))
		})
	}
}

func TestEnsureRestoredGTIDPurgedMatchesManifestKeepsBinlogs(t *testing.T) {
	dir := t.TempDir()
	cnf := &Mycnf{DataDir: filepath.Join(dir, "data"), BinLogPath: filepath.Join(dir, "dst-bin")}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "dst-bin.index"), nil, 0o644))
	pos, err := replication.DecodePosition("MySQL56/16b1039f-22b6-11ed-b765-0a43f95f28a3:1-10")
	require.NoError(t, err)
	other, err := replication.DecodePosition("MySQL56/16b1039f-22b6-11ed-b765-0a43f95f28a3:1-9")
	require.NoError(t, err)

	fakedb := fakesqldb.New(t)
	t.Cleanup(fakedb.Close)
	mysqld := NewFakeMysqlDaemon(fakedb)
	t.Cleanup(mysqld.Close)
	mysqld.CurrentPrimaryPosition = pos
	// The fake's gtid_purged is empty, which differs from pos; a call to
	// SetReplicationPosition would fail because it expects a different position.
	mysqld.SetReplicationPositionPos = other
	mysqld.FetchSuperQueryMap = map[string]*sqltypes.Result{
		binlogLocationQuery: sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("log_bin_basename|log_bin_index", "varchar|varchar"),
			filepath.Join(dir, "dst-bin")+"|"+filepath.Join(dir, "dst-bin.index")),
	}
	params := &RestoreParams{Cnf: cnf, Mysqld: mysqld, Logger: logutil.NewMemoryLogger()}
	manifest := &BackupManifest{Position: pos, IncludesBinlogs: true}

	require.NoError(t, ensureRestoredGTIDPurgedMatchesManifest(t.Context(), manifest, params))

	// Without binlogs in the backup, the same state is repaired through SetReplicationPosition.
	manifest.IncludesBinlogs = false
	require.Error(t, ensureRestoredGTIDPurgedMatchesManifest(t.Context(), manifest, params))
}
