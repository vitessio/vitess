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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstats"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
)

// TestBackupExecutesBackupWithScopedParams tests that Backup passes
// a Scope()-ed stats to backupengine ExecuteBackup.
func TestBackupExecutesBackupWithScopedParams(t *testing.T) {
	env, closer := createFakeBackupRestoreEnv(t)
	defer closer()

	require.Nil(t, Backup(env.ctx, env.backupParams), env.logger.Events)

	require.Equal(t, 1, len(env.backupEngine.ExecuteBackupCalls))
	executeBackupParams := env.backupEngine.ExecuteBackupCalls[0].BackupParams
	var executeBackupStats *backupstats.FakeStats
	for _, sr := range env.stats.ScopeReturns {
		if sr == executeBackupParams.Stats {
			executeBackupStats = sr.(*backupstats.FakeStats)
		}
	}
	require.Contains(t, executeBackupStats.ScopeV, backupstats.ScopeComponent)
	require.Equal(t, backupstats.BackupEngine.String(), executeBackupStats.ScopeV[backupstats.ScopeComponent])
	require.Contains(t, executeBackupStats.ScopeV, backupstats.ScopeImplementation)
	require.Equal(t, "Fake", executeBackupStats.ScopeV[backupstats.ScopeImplementation])
}

// TestBackupNoStats tests that if BackupParams.Stats is nil, then Backup will
// pass non-nil Stats to sub-components.
func TestBackupNoStats(t *testing.T) {
	env, closer := createFakeBackupRestoreEnv(t)
	defer closer()

	env.setStats(nil)

	require.Nil(t, Backup(env.ctx, env.backupParams), env.logger.Events)

	// It parameterizes the backup storage with nop stats.
	require.Equal(t, 1, len(env.backupStorage.WithParamsCalls))
	require.Equal(t, backupstats.NoStats(), env.backupStorage.WithParamsCalls[0].Stats)
}

// TestBackupParameterizesBackupStorageWithScopedStats tests that Backup passes
// a Scope()-ed stats to BackupStorage.WithParams.
func TestBackupParameterizesBackupStorageWithScopedStats(t *testing.T) {
	env, closer := createFakeBackupRestoreEnv(t)
	defer closer()

	require.Nil(t, Backup(env.ctx, env.backupParams), env.logger.Events)

	require.Equal(t, 1, len(env.backupStorage.WithParamsCalls))
	var storageStats *backupstats.FakeStats
	for _, sr := range env.stats.ScopeReturns {
		if sr == env.backupStorage.WithParamsCalls[0].Stats {
			storageStats = sr.(*backupstats.FakeStats)
		}
	}
	require.Contains(t, storageStats.ScopeV, backupstats.ScopeComponent)
	require.Equal(t, backupstats.BackupStorage.String(), storageStats.ScopeV[backupstats.ScopeComponent])
	require.Contains(t, storageStats.ScopeV, backupstats.ScopeImplementation)
	require.Equal(t, "Fake", storageStats.ScopeV[backupstats.ScopeImplementation])
}

// TestBackupEmitsStats tests that Backup emits stats.
func TestBackupEmitsStats(t *testing.T) {
	env, closer := createFakeBackupRestoreEnv(t)
	defer closer()

	// Force ExecuteBackup to take time so we can test stats emission.
	env.backupEngine.ExecuteBackupDuration = 1001 * time.Millisecond

	require.Nil(t, Backup(env.ctx, env.backupParams), env.logger.Events)

	require.NotZero(t, backupstats.DeprecatedBackupDurationS.Get())
	require.Equal(t, 0, len(env.stats.TimedIncrementCalls))
	require.Equal(t, 0, len(env.stats.ScopeV))
}

// TestBackupTriesToParameterizeBackupStorage tests that Backup tries to pass
// backupstorage.Params to backupstorage, but only if it responds to
// backupstorage.WithParams.
func TestBackupTriesToParameterizeBackupStorage(t *testing.T) {
	env, closer := createFakeBackupRestoreEnv(t)
	defer closer()

	require.Nil(t, Backup(env.ctx, env.backupParams), env.logger.Events)

	require.Equal(t, 1, len(env.backupStorage.WithParamsCalls))
	require.Equal(t, env.logger, env.backupStorage.WithParamsCalls[0].Logger)
	var scopedStats backupstats.Stats
	for _, sr := range env.stats.ScopeReturns {
		if sr != env.backupStorage.WithParamsCalls[0].Stats {
			continue
		}
		if scopedStats != nil {
			require.Fail(t, "backupstorage stats matches multiple scoped stats produced by parent stats")
		}
		scopedStats = sr
	}
	require.NotNil(t, scopedStats)
}

func TestFindFilesToBackupWithoutRedoLog(t *testing.T) {
	root := t.TempDir()

	// Initialize the fake mysql root directories
	innodbDataDir := path.Join(root, "innodb_data")
	innodbLogDir := path.Join(root, "innodb_log")
	dataDir := path.Join(root, "data")
	dataDbDir := path.Join(dataDir, "vt_db")
	extraDir := path.Join(dataDir, "extra_dir")
	outsideDbDir := path.Join(root, "outside_db")
	rocksdbDir := path.Join(dataDir, ".rocksdb")
	sdiOnlyDir := path.Join(dataDir, "sdi_dir")
	for _, s := range []string{innodbDataDir, innodbLogDir, dataDbDir, extraDir, outsideDbDir, rocksdbDir, sdiOnlyDir} {
		if err := os.MkdirAll(s, os.ModePerm); err != nil {
			t.Fatalf("failed to create directory %v: %v", s, err)
		}
	}

	innodbLogFile := "innodb_log_1"

	if err := os.WriteFile(path.Join(innodbDataDir, "innodb_data_1"), []byte("innodb data 1 contents"), os.ModePerm); err != nil {
		t.Fatalf("failed to write file innodb_data_1: %v", err)
	}
	if err := os.WriteFile(path.Join(innodbLogDir, innodbLogFile), []byte("innodb log 1 contents"), os.ModePerm); err != nil {
		t.Fatalf("failed to write file %s: %v", innodbLogFile, err)
	}
	if err := os.WriteFile(path.Join(dataDbDir, "db.opt"), []byte("db opt file"), os.ModePerm); err != nil {
		t.Fatalf("failed to write file db.opt: %v", err)
	}
	if err := os.WriteFile(path.Join(extraDir, "extra.stuff"), []byte("extra file"), os.ModePerm); err != nil {
		t.Fatalf("failed to write file extra.stuff: %v", err)
	}
	if err := os.WriteFile(path.Join(outsideDbDir, "table1.frm"), []byte("frm file"), os.ModePerm); err != nil {
		t.Fatalf("failed to write file table1.opt: %v", err)
	}
	if err := os.Symlink(outsideDbDir, path.Join(dataDir, "vt_symlink")); err != nil {
		t.Fatalf("failed to symlink vt_symlink: %v", err)
	}
	if err := os.WriteFile(path.Join(rocksdbDir, "000011.sst"), []byte("rocksdb file"), os.ModePerm); err != nil {
		t.Fatalf("failed to write file 000011.sst: %v", err)
	}
	if err := os.WriteFile(path.Join(sdiOnlyDir, "table1.sdi"), []byte("sdi file"), os.ModePerm); err != nil {
		t.Fatalf("failed to write file table1.sdi: %v", err)
	}

	cnf := &Mycnf{
		InnodbDataHomeDir:     innodbDataDir,
		InnodbLogGroupHomeDir: innodbLogDir,
		DataDir:               dataDir,
	}

	result, totalSize, err := findFilesToBackup(cnf)
	if err != nil {
		t.Fatalf("findFilesToBackup failed: %v", err)
	}
	sort.Sort(forTest(result))
	t.Logf("findFilesToBackup returned: %v", result)
	expected := []FileEntry{
		{
			Base: "Data",
			Name: ".rocksdb/000011.sst",
		},
		{
			Base: "Data",
			Name: "sdi_dir/table1.sdi",
		},
		{
			Base: "Data",
			Name: "vt_db/db.opt",
		},
		{
			Base: "Data",
			Name: "vt_symlink/table1.frm",
		},
		{
			Base: "InnoDBData",
			Name: "innodb_data_1",
		},
		{
			Base: "InnoDBLog",
			Name: innodbLogFile,
		},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Fatalf("got wrong list of FileEntry %v, expected %v", result, expected)
	}
	if totalSize <= 0 {
		t.Fatalf("backup size should be > 0, got %v", totalSize)
	}
}

func TestFindFilesToBackupWithRedoLog(t *testing.T) {
	root := t.TempDir()

	// Initialize the fake mysql root directories
	innodbDataDir := path.Join(root, "innodb_data")
	innodbLogDir := path.Join(root, "innodb_log")
	dataDir := path.Join(root, "data")
	dataDbDir := path.Join(dataDir, "vt_db")
	extraDir := path.Join(dataDir, "extra_dir")
	outsideDbDir := path.Join(root, "outside_db")
	rocksdbDir := path.Join(dataDir, ".rocksdb")
	sdiOnlyDir := path.Join(dataDir, "sdi_dir")
	for _, s := range []string{innodbDataDir, innodbLogDir, dataDbDir, extraDir, outsideDbDir, rocksdbDir, sdiOnlyDir} {
		if err := os.MkdirAll(s, os.ModePerm); err != nil {
			t.Fatalf("failed to create directory %v: %v", s, err)
		}
	}

	cnf := &Mycnf{
		InnodbDataHomeDir:     innodbDataDir,
		InnodbLogGroupHomeDir: innodbLogDir,
		DataDir:               dataDir,
	}

	os.Mkdir(path.Join(innodbLogDir, mysql.DynamicRedoLogSubdir), os.ModePerm)
	innodbLogFile := path.Join(mysql.DynamicRedoLogSubdir, "#ib_redo1")

	if err := os.WriteFile(path.Join(innodbDataDir, "innodb_data_1"), []byte("innodb data 1 contents"), os.ModePerm); err != nil {
		t.Fatalf("failed to write file innodb_data_1: %v", err)
	}
	if err := os.WriteFile(path.Join(innodbLogDir, innodbLogFile), []byte("innodb log 1 contents"), os.ModePerm); err != nil {
		t.Fatalf("failed to write file %s: %v", innodbLogFile, err)
	}
	if err := os.WriteFile(path.Join(dataDbDir, "db.opt"), []byte("db opt file"), os.ModePerm); err != nil {
		t.Fatalf("failed to write file db.opt: %v", err)
	}
	if err := os.WriteFile(path.Join(extraDir, "extra.stuff"), []byte("extra file"), os.ModePerm); err != nil {
		t.Fatalf("failed to write file extra.stuff: %v", err)
	}
	if err := os.WriteFile(path.Join(outsideDbDir, "table1.frm"), []byte("frm file"), os.ModePerm); err != nil {
		t.Fatalf("failed to write file table1.opt: %v", err)
	}
	if err := os.Symlink(outsideDbDir, path.Join(dataDir, "vt_symlink")); err != nil {
		t.Fatalf("failed to symlink vt_symlink: %v", err)
	}
	if err := os.WriteFile(path.Join(rocksdbDir, "000011.sst"), []byte("rocksdb file"), os.ModePerm); err != nil {
		t.Fatalf("failed to write file 000011.sst: %v", err)
	}
	if err := os.WriteFile(path.Join(sdiOnlyDir, "table1.sdi"), []byte("sdi file"), os.ModePerm); err != nil {
		t.Fatalf("failed to write file table1.sdi: %v", err)
	}

	result, totalSize, err := findFilesToBackup(cnf)
	if err != nil {
		t.Fatalf("findFilesToBackup failed: %v", err)
	}
	sort.Sort(forTest(result))
	t.Logf("findFilesToBackup returned: %v", result)
	expected := []FileEntry{
		{
			Base: "Data",
			Name: ".rocksdb/000011.sst",
		},
		{
			Base: "Data",
			Name: "sdi_dir/table1.sdi",
		},
		{
			Base: "Data",
			Name: "vt_db/db.opt",
		},
		{
			Base: "Data",
			Name: "vt_symlink/table1.frm",
		},
		{
			Base: "InnoDBData",
			Name: "innodb_data_1",
		},
		{
			Base: "InnoDBLog",
			Name: innodbLogFile,
		},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Fatalf("got wrong list of FileEntry %v, expected %v", result, expected)
	}
	if totalSize <= 0 {
		t.Fatalf("backup size should be > 0, got %v", totalSize)
	}
}

// TestRestoreEmitsStats tests that Restore emits stats.
func TestRestoreEmitsStats(t *testing.T) {
	env, closer := createFakeBackupRestoreEnv(t)
	defer closer()

	// Force ExecuteRestore to take time so we can test stats emission.
	env.backupEngine.ExecuteRestoreDuration = 1001 * time.Millisecond

	_, err := Restore(env.ctx, env.restoreParams)
	require.Nil(t, err, env.logger.Events)

	require.NotZero(t, backupstats.DeprecatedRestoreDurationS.Get())
	require.Equal(t, 0, len(env.stats.TimedIncrementCalls))
	require.Equal(t, 0, len(env.stats.ScopeV))
}

// TestRestoreExecutesRestoreWithScopedParams tests that Restore passes
// a Scope()-ed stats to backupengine ExecuteRestore.
func TestRestoreExecutesRestoreWithScopedParams(t *testing.T) {
	env, closer := createFakeBackupRestoreEnv(t)
	defer closer()

	_, err := Restore(env.ctx, env.restoreParams)
	require.Nil(t, err, env.logger.Events)

	require.Equal(t, 1, len(env.backupEngine.ExecuteRestoreCalls))
	executeRestoreParams := env.backupEngine.ExecuteRestoreCalls[0].RestoreParams
	var executeRestoreStats *backupstats.FakeStats
	for _, sr := range env.stats.ScopeReturns {
		if sr == executeRestoreParams.Stats {
			executeRestoreStats = sr.(*backupstats.FakeStats)
		}
	}
	require.Contains(t, executeRestoreStats.ScopeV, backupstats.ScopeComponent)
	require.Equal(t, backupstats.BackupEngine.String(), executeRestoreStats.ScopeV[backupstats.ScopeComponent])
	require.Contains(t, executeRestoreStats.ScopeV, backupstats.ScopeImplementation)
	require.Equal(t, "Fake", executeRestoreStats.ScopeV[backupstats.ScopeImplementation])
}

// TestRestoreNoStats tests that if RestoreParams.Stats is nil, then Restore will
// pass non-nil Stats to sub-components.
func TestRestoreNoStats(t *testing.T) {
	env, closer := createFakeBackupRestoreEnv(t)
	defer closer()

	env.setStats(nil)

	_, err := Restore(env.ctx, env.restoreParams)
	require.Nil(t, err, env.logger.Events)

	// It parameterizes the backup storage with nop stats.
	require.Equal(t, 1, len(env.backupStorage.WithParamsCalls))
	require.Equal(t, backupstats.NoStats(), env.backupStorage.WithParamsCalls[0].Stats)
}

// TestRestoreParameterizesBackupStorageWithScopedStats tests that Restore passes
// a Scope()-ed stats to BackupStorage.WithParams.
func TestRestoreParameterizesBackupStorageWithScopedStats(t *testing.T) {
	env, closer := createFakeBackupRestoreEnv(t)
	defer closer()

	_, err := Restore(env.ctx, env.restoreParams)
	require.Nil(t, err, env.logger.Events)

	require.Equal(t, 1, len(env.backupStorage.WithParamsCalls))
	var storageStats *backupstats.FakeStats
	for _, sr := range env.stats.ScopeReturns {
		if sr == env.backupStorage.WithParamsCalls[0].Stats {
			storageStats = sr.(*backupstats.FakeStats)
		}
	}
	require.Contains(t, storageStats.ScopeV, backupstats.ScopeComponent)
	require.Equal(t, backupstats.BackupStorage.String(), storageStats.ScopeV[backupstats.ScopeComponent])
	require.Contains(t, storageStats.ScopeV, backupstats.ScopeImplementation)
	require.Equal(t, "Fake", storageStats.ScopeV[backupstats.ScopeImplementation])
}

// TestRestoreTriesToParameterizeBackupStorage tests that Restore tries to pass
// backupstorage.Params to backupstorage, but only if it responds to
// backupstorage.WithParams.
func TestRestoreTriesToParameterizeBackupStorage(t *testing.T) {
	env, closer := createFakeBackupRestoreEnv(t)
	defer closer()

	_, err := Restore(env.ctx, env.restoreParams)
	require.Nil(t, err, env.logger.Events)

	require.Equal(t, 1, len(env.backupStorage.WithParamsCalls))
	require.Equal(t, env.logger, env.backupStorage.WithParamsCalls[0].Logger)
	var scopedStats backupstats.Stats
	for _, sr := range env.stats.ScopeReturns {
		if sr != env.backupStorage.WithParamsCalls[0].Stats {
			continue
		}
		if scopedStats != nil {
			require.Fail(t, "backupstorage stats matches multiple scoped stats produced by parent stats")
		}
		scopedStats = sr
	}
	require.NotNil(t, scopedStats)
}

// TestRestoreManifestMySQLVersionValidation tests that Restore tries to validate
// the MySQL version and safe upgrade attribute.
func TestRestoreManifestMySQLVersionValidation(t *testing.T) {
	testCases := []struct {
		fromVersion, toVersion string
		upgradeSafe            bool
		wantErr                bool
	}{
		{
			fromVersion: "mysqld  Ver 5.6.42",
			toVersion:   "mysqld  Ver 5.7.40",
			upgradeSafe: false,
			wantErr:     true,
		},
		{
			fromVersion: "mysqld  Ver 5.6.42",
			toVersion:   "mysqld  Ver 5.7.40",
			upgradeSafe: true,
			wantErr:     false,
		},
		{
			fromVersion: "mysqld  Ver 5.7.42",
			toVersion:   "mysqld  Ver 8.0.32",
			upgradeSafe: true,
			wantErr:     false,
		},
		{
			fromVersion: "mysqld  Ver 5.7.42",
			toVersion:   "mysqld  Ver 8.0.32",
			upgradeSafe: false,
			wantErr:     true,
		},
		{
			fromVersion: "mysqld  Ver 5.7.42",
			toVersion:   "mysqld  Ver 8.0.32",
			upgradeSafe: true,
			wantErr:     false,
		},
		{
			fromVersion: "mysqld  Ver 8.0.32",
			toVersion:   "mysqld  Ver 8.0.32",
			upgradeSafe: false,
			wantErr:     false,
		},
		{
			fromVersion: "mysqld  Ver 8.0.32",
			toVersion:   "mysqld  Ver 8.0.32",
			upgradeSafe: true,
			wantErr:     false,
		},
		{
			fromVersion: "mysqld  Ver 8.0.32",
			toVersion:   "mysqld  Ver 8.0.31",
			upgradeSafe: false,
			wantErr:     true,
		},
		{
			fromVersion: "mysqld  Ver 8.0.32",
			toVersion:   "mysqld  Ver 8.0.31",
			upgradeSafe: true,
			wantErr:     true,
		},
		{
			fromVersion: "mysqld  Ver 8.0.32",
			toVersion:   "mysqld  Ver 8.0.33",
			upgradeSafe: false,
			wantErr:     true,
		},
		{
			fromVersion: "mysqld  Ver 8.0.32",
			toVersion:   "mysqld  Ver 8.0.33",
			upgradeSafe: true,
			wantErr:     false,
		},
		{
			fromVersion: "",
			toVersion:   "mysqld  Ver 8.0.33",
			upgradeSafe: false,
			wantErr:     false,
		},
		{
			fromVersion: "",
			toVersion:   "mysqld  Ver 8.0.33",
			upgradeSafe: true,
			wantErr:     false,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s->%s upgradeSafe=%t", tc.fromVersion, tc.toVersion, tc.upgradeSafe), func(t *testing.T) {
			env, closer := createFakeBackupRestoreEnv(t)
			defer closer()
			env.mysqld.Version = tc.toVersion

			manifest := BackupManifest{
				BackupTime:   time.Now().Add(-1 * time.Hour).Format(time.RFC3339),
				BackupMethod: "fake",
				Keyspace:     "test",
				Shard:        "-",
				MySQLVersion: tc.fromVersion,
				UpgradeSafe:  tc.upgradeSafe,
			}

			manifestBytes, err := json.Marshal(manifest)
			require.Nil(t, err)

			env.backupEngine.ExecuteRestoreReturn = FakeBackupEngineExecuteRestoreReturn{&manifest, nil}
			env.backupStorage.ListBackupsReturn = FakeBackupStorageListBackupsReturn{
				BackupHandles: []backupstorage.BackupHandle{
					&FakeBackupHandle{
						ReadFileReturnF: func(context.Context, string) (io.ReadCloser, error) {
							return io.NopCloser(bytes.NewBuffer(manifestBytes)), nil
						},
					},
				},
			}

			_, err = Restore(env.ctx, env.restoreParams)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}

}

type forTest []FileEntry

func (f forTest) Len() int           { return len(f) }
func (f forTest) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }
func (f forTest) Less(i, j int) bool { return f[i].Base+f[i].Name < f[j].Base+f[j].Name }

type fakeBackupRestoreEnv struct {
	backupEngine  *FakeBackupEngine
	backupParams  BackupParams
	backupStorage *FakeBackupStorage
	ctx           context.Context
	logger        *logutil.MemoryLogger
	restoreParams RestoreParams
	mysqld        *FakeMysqlDaemon
	stats         *backupstats.FakeStats
}

func createFakeBackupRestoreEnv(t *testing.T) (*fakeBackupRestoreEnv, func()) {
	ctx := context.Background()
	logger := logutil.NewMemoryLogger()

	sqldb := fakesqldb.New(t)
	sqldb.SetNeverFail(true)
	mysqld := NewFakeMysqlDaemon(sqldb)
	require.Nil(t, mysqld.Shutdown(ctx, nil, false))
	defer mysqld.Close()

	dirName, err := os.MkdirTemp("", "vt_backup_test")
	require.Nil(t, err)

	cnf := &Mycnf{
		DataDir: dirName,
	}

	stats := backupstats.NewFakeStats()

	backupParams := BackupParams{
		Cnf:                cnf,
		Logger:             logger,
		Mysqld:             mysqld,
		Concurrency:        1,
		HookExtraEnv:       map[string]string{},
		TopoServer:         nil,
		Keyspace:           "test",
		Shard:              "-",
		BackupTime:         time.Now(),
		IncrementalFromPos: "",
		Stats:              stats,
	}

	restoreParams := RestoreParams{
		Cnf:                 cnf,
		Logger:              logger,
		Mysqld:              mysqld,
		Concurrency:         1,
		HookExtraEnv:        map[string]string{},
		DeleteBeforeRestore: false,
		DbName:              "test",
		Keyspace:            "test",
		Shard:               "-",
		StartTime:           time.Now(),
		RestoreToPos:        mysql.Position{},
		DryRun:              false,
		Stats:               stats,
	}

	manifest := BackupManifest{
		BackupTime:   time.Now().Add(-1 * time.Hour).Format(time.RFC3339),
		BackupMethod: "fake",
		Keyspace:     "test",
		Shard:        "-",
		MySQLVersion: "8.0.32",
	}

	manifestBytes, err := json.Marshal(manifest)
	require.Nil(t, err)

	testBackupEngine := FakeBackupEngine{}
	testBackupEngine.ExecuteRestoreReturn = FakeBackupEngineExecuteRestoreReturn{&manifest, nil}

	previousBackupEngineImplementation := backupEngineImplementation
	BackupRestoreEngineMap["fake"] = &testBackupEngine
	backupEngineImplementation = "fake"

	testBackupStorage := FakeBackupStorage{}
	testBackupStorage.ListBackupsReturn = FakeBackupStorageListBackupsReturn{
		BackupHandles: []backupstorage.BackupHandle{
			&FakeBackupHandle{
				ReadFileReturnF: func(context.Context, string) (io.ReadCloser, error) {
					return io.NopCloser(bytes.NewBuffer(manifestBytes)), nil
				},
			},
		},
	}
	testBackupStorage.StartBackupReturn = FakeBackupStorageStartBackupReturn{&FakeBackupHandle{}, nil}
	testBackupStorage.WithParamsReturn = &testBackupStorage

	backupstorage.BackupStorageMap["fake"] = &testBackupStorage
	previousBackupStorageImplementation := backupstorage.BackupStorageImplementation
	backupstorage.BackupStorageImplementation = "fake"

	closer := func() {
		backupstats.DeprecatedBackupDurationS.Reset()
		backupstats.DeprecatedRestoreDurationS.Reset()

		delete(BackupRestoreEngineMap, "fake")
		backupEngineImplementation = previousBackupEngineImplementation

		delete(backupstorage.BackupStorageMap, "fake")
		backupstorage.BackupStorageImplementation = previousBackupStorageImplementation
	}

	return &fakeBackupRestoreEnv{
		backupEngine:  &testBackupEngine,
		backupParams:  backupParams,
		backupStorage: &testBackupStorage,
		ctx:           ctx,
		logger:        logger,
		mysqld:        mysqld,
		restoreParams: restoreParams,
		stats:         stats,
	}, closer
}

func (fbe *fakeBackupRestoreEnv) setStats(stats *backupstats.FakeStats) {
	fbe.backupParams.Stats = nil
	fbe.restoreParams.Stats = nil
	fbe.stats = nil
}
