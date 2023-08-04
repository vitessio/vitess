/*
Copyright 2022 The Vitess Authors.

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
package mysqlctl_test

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/backupstats"
	"vitess.io/vitess/go/vt/mysqlctl/filebackupstorage"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func setBuiltinBackupMysqldDeadline(t time.Duration) time.Duration {
	old := mysqlctl.BuiltinBackupMysqldTimeout
	mysqlctl.BuiltinBackupMysqldTimeout = t

	return old
}

func createBackupDir(root string, dirs ...string) error {
	for _, dir := range dirs {
		if err := os.MkdirAll(path.Join(root, dir), 0755); err != nil {
			return err
		}
	}

	return nil
}

func createBackupFiles(root string, fileCount int, ext string) error {
	for i := 0; i < fileCount; i++ {
		f, err := os.Create(path.Join(root, fmt.Sprintf("%d.%s", i, ext)))
		if err != nil {
			return err
		}
		if _, err := f.Write([]byte("hello, world!")); err != nil {
			return err
		}
		defer f.Close()
	}

	return nil
}

func TestExecuteBackup(t *testing.T) {
	// Set up local backup directory
	backupRoot := "testdata/builtinbackup_test"
	filebackupstorage.FileBackupStorageRoot = backupRoot
	require.NoError(t, createBackupDir(backupRoot, "innodb", "log", "datadir"))
	dataDir := path.Join(backupRoot, "datadir")
	// Add some files under data directory to force backup to actually backup files.
	require.NoError(t, createBackupDir(dataDir, "test1"))
	require.NoError(t, createBackupDir(dataDir, "test2"))
	require.NoError(t, createBackupFiles(path.Join(dataDir, "test1"), 2, "ibd"))
	require.NoError(t, createBackupFiles(path.Join(dataDir, "test2"), 2, "ibd"))
	defer os.RemoveAll(backupRoot)

	ctx := context.Background()

	needIt, err := needInnoDBRedoLogSubdir()
	require.NoError(t, err)
	if needIt {
		fpath := path.Join("log", mysql.DynamicRedoLogSubdir)
		if err := createBackupDir(backupRoot, fpath); err != nil {
			require.Failf(t, err.Error(), "failed to create directory: %s", fpath)
		}
	}

	// Set up topo
	keyspace, shard := "mykeyspace", "-80"
	ts := memorytopo.NewServer("cell1")
	defer ts.Close()

	require.NoError(t, ts.CreateKeyspace(ctx, keyspace, &topodata.Keyspace{}))
	require.NoError(t, ts.CreateShard(ctx, keyspace, shard))

	tablet := topo.NewTablet(100, "cell1", "mykeyspace-00-80-0100")
	tablet.Keyspace = keyspace
	tablet.Shard = shard

	require.NoError(t, ts.CreateTablet(ctx, tablet))

	_, err = ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
		si.PrimaryAlias = &topodata.TabletAlias{Uid: 100, Cell: "cell1"}

		now := time.Now()
		si.PrimaryTermStartTime = &vttime.Time{Seconds: int64(now.Second()), Nanoseconds: int32(now.Nanosecond())}

		return nil
	})
	require.NoError(t, err)

	be := &mysqlctl.BuiltinBackupEngine{}

	// Configure a tight deadline to force a timeout
	oldDeadline := setBuiltinBackupMysqldDeadline(time.Second)
	defer setBuiltinBackupMysqldDeadline(oldDeadline)

	bh := filebackupstorage.NewBackupHandle(nil, "", "", false)

	// Spin up a fake daemon to be used in backups. It needs to be allowed to receive:
	//  "STOP SLAVE", "START SLAVE", in that order.
	mysqld := mysqlctl.NewFakeMysqlDaemon(fakesqldb.New(t))
	mysqld.ExpectedExecuteSuperQueryList = []string{"STOP SLAVE", "START SLAVE"}
	// mysqld.ShutdownTime = time.Minute

	fakeStats := backupstats.NewFakeStats()

	ok, err := be.ExecuteBackup(ctx, mysqlctl.BackupParams{
		Logger: logutil.NewConsoleLogger(),
		Mysqld: mysqld,
		Cnf: &mysqlctl.Mycnf{
			InnodbDataHomeDir:     path.Join(backupRoot, "innodb"),
			InnodbLogGroupHomeDir: path.Join(backupRoot, "log"),
			DataDir:               path.Join(backupRoot, "datadir"),
		},
		Concurrency:  2,
		HookExtraEnv: map[string]string{},
		TopoServer:   ts,
		Keyspace:     keyspace,
		Shard:        shard,
		Stats:        fakeStats,
	}, bh)

	require.NoError(t, err)
	assert.True(t, ok)

	var destinationCloseStats int
	var destinationOpenStats int
	var destinationWriteStats int
	var sourceCloseStats int
	var sourceOpenStats int
	var sourceReadStats int

	for _, sr := range fakeStats.ScopeReturns {
		sfs := sr.(*backupstats.FakeStats)
		switch sfs.ScopeV[backupstats.ScopeOperation] {
		case "Destination:Close":
			destinationCloseStats++
			require.Len(t, sfs.TimedIncrementCalls, 1)
		case "Destination:Open":
			destinationOpenStats++
			require.Len(t, sfs.TimedIncrementCalls, 1)
		case "Destination:Write":
			destinationWriteStats++
			require.GreaterOrEqual(t, len(sfs.TimedIncrementBytesCalls), 1)
		case "Source:Close":
			sourceCloseStats++
			require.Len(t, sfs.TimedIncrementCalls, 1)
		case "Source:Open":
			sourceOpenStats++
			require.Len(t, sfs.TimedIncrementCalls, 1)
		case "Source:Read":
			sourceReadStats++
			require.GreaterOrEqual(t, len(sfs.TimedIncrementBytesCalls), 1)
		}
	}

	require.Equal(t, 4, destinationCloseStats)
	require.Equal(t, 4, destinationOpenStats)
	require.Equal(t, 4, destinationWriteStats)
	require.Equal(t, 4, sourceCloseStats)
	require.Equal(t, 4, sourceOpenStats)
	require.Equal(t, 4, sourceReadStats)

	mysqld.ExpectedExecuteSuperQueryCurrent = 0 // resest the index of what queries we've run
	mysqld.ShutdownTime = time.Minute           // reminder that shutdownDeadline is 1s

	ok, err = be.ExecuteBackup(ctx, mysqlctl.BackupParams{
		Logger: logutil.NewConsoleLogger(),
		Mysqld: mysqld,
		Cnf: &mysqlctl.Mycnf{
			InnodbDataHomeDir:     path.Join(backupRoot, "innodb"),
			InnodbLogGroupHomeDir: path.Join(backupRoot, "log"),
			DataDir:               path.Join(backupRoot, "datadir"),
		},
		HookExtraEnv: map[string]string{},
		TopoServer:   ts,
		Keyspace:     keyspace,
		Shard:        shard,
	}, bh)

	assert.Error(t, err)
	assert.False(t, ok)
}

func TestExecuteBackupWithSafeUpgrade(t *testing.T) {
	// Set up local backup directory
	backupRoot := "testdata/builtinbackup_test"
	filebackupstorage.FileBackupStorageRoot = backupRoot
	require.NoError(t, createBackupDir(backupRoot, "innodb", "log", "datadir"))
	dataDir := path.Join(backupRoot, "datadir")
	// Add some files under data directory to force backup to actually backup files.
	require.NoError(t, createBackupDir(dataDir, "test1"))
	require.NoError(t, createBackupDir(dataDir, "test2"))
	require.NoError(t, createBackupFiles(path.Join(dataDir, "test1"), 2, "ibd"))
	require.NoError(t, createBackupFiles(path.Join(dataDir, "test2"), 2, "ibd"))
	defer os.RemoveAll(backupRoot)

	ctx := context.Background()

	needIt, err := needInnoDBRedoLogSubdir()
	require.NoError(t, err)
	if needIt {
		fpath := path.Join("log", mysql.DynamicRedoLogSubdir)
		if err := createBackupDir(backupRoot, fpath); err != nil {
			require.Failf(t, err.Error(), "failed to create directory: %s", fpath)
		}
	}

	// Set up topo
	keyspace, shard := "mykeyspace", "-80"
	ts := memorytopo.NewServer("cell1")
	defer ts.Close()

	require.NoError(t, ts.CreateKeyspace(ctx, keyspace, &topodata.Keyspace{}))
	require.NoError(t, ts.CreateShard(ctx, keyspace, shard))

	tablet := topo.NewTablet(100, "cell1", "mykeyspace-00-80-0100")
	tablet.Keyspace = keyspace
	tablet.Shard = shard

	require.NoError(t, ts.CreateTablet(ctx, tablet))

	_, err = ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
		si.PrimaryAlias = &topodata.TabletAlias{Uid: 100, Cell: "cell1"}

		now := time.Now()
		si.PrimaryTermStartTime = &vttime.Time{Seconds: int64(now.Second()), Nanoseconds: int32(now.Nanosecond())}

		return nil
	})
	require.NoError(t, err)

	be := &mysqlctl.BuiltinBackupEngine{}

	// Configure a tight deadline to force a timeout
	oldDeadline := setBuiltinBackupMysqldDeadline(time.Second)
	defer setBuiltinBackupMysqldDeadline(oldDeadline)

	bh := filebackupstorage.NewBackupHandle(nil, "", "", false)

	// Spin up a fake daemon to be used in backups. It needs to be allowed to receive:
	//  "STOP SLAVE", "START SLAVE", in that order.
	// It also needs to be allowed to receive the query to disable the innodb_fast_shutdown flag.
	mysqld := mysqlctl.NewFakeMysqlDaemon(fakesqldb.New(t))
	mysqld.ExpectedExecuteSuperQueryList = []string{"STOP SLAVE", "START SLAVE"}
	mysqld.FetchSuperQueryMap = map[string]*sqltypes.Result{
		"SET GLOBAL innodb_fast_shutdown=0": {},
	}

	ok, err := be.ExecuteBackup(ctx, mysqlctl.BackupParams{
		Logger: logutil.NewConsoleLogger(),
		Mysqld: mysqld,
		Cnf: &mysqlctl.Mycnf{
			InnodbDataHomeDir:     path.Join(backupRoot, "innodb"),
			InnodbLogGroupHomeDir: path.Join(backupRoot, "log"),
			DataDir:               path.Join(backupRoot, "datadir"),
		},
		Concurrency: 2,
		TopoServer:  ts,
		Keyspace:    keyspace,
		Shard:       shard,
		Stats:       backupstats.NewFakeStats(),
		UpgradeSafe: true,
	}, bh)

	require.NoError(t, err)
	assert.True(t, ok)
}

// TestExecuteBackupWithCanceledContext tests the ability of the backup function to gracefully handle cases where errors
// occur due to various reasons, such as context time cancel. The process should not panic in these situations.
func TestExecuteBackupWithCanceledContext(t *testing.T) {
	// Set up local backup directory
	id := fmt.Sprintf("%d", time.Now().UnixNano())
	backupRoot := fmt.Sprintf("testdata/builtinbackup_test_%s", id)
	filebackupstorage.FileBackupStorageRoot = backupRoot
	require.NoError(t, createBackupDir(backupRoot, "innodb", "log", "datadir"))
	dataDir := path.Join(backupRoot, "datadir")
	// Add some files under data directory to force backup to execute semaphore acquire inside
	// backupFiles() method (https://github.com/vitessio/vitess/blob/main/go/vt/mysqlctl/builtinbackupengine.go#L483).
	require.NoError(t, createBackupDir(dataDir, "test1"))
	require.NoError(t, createBackupDir(dataDir, "test2"))
	require.NoError(t, createBackupFiles(path.Join(dataDir, "test1"), 2, "ibd"))
	require.NoError(t, createBackupFiles(path.Join(dataDir, "test2"), 2, "ibd"))
	defer os.RemoveAll(backupRoot)

	// Cancel the context deliberately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	needIt, err := needInnoDBRedoLogSubdir()
	require.NoError(t, err)
	if needIt {
		fpath := path.Join("log", mysql.DynamicRedoLogSubdir)
		if err := createBackupDir(backupRoot, fpath); err != nil {
			require.Failf(t, err.Error(), "failed to create directory: %s", fpath)
		}
	}

	// Set up topo
	keyspace, shard := "mykeyspace", "-80"
	ts := memorytopo.NewServer("cell1")
	defer ts.Close()

	require.NoError(t, ts.CreateKeyspace(ctx, keyspace, &topodata.Keyspace{}))
	require.NoError(t, ts.CreateShard(ctx, keyspace, shard))

	tablet := topo.NewTablet(100, "cell1", "mykeyspace-00-80-0100")
	tablet.Keyspace = keyspace
	tablet.Shard = shard

	require.NoError(t, ts.CreateTablet(ctx, tablet))

	_, err = ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
		si.PrimaryAlias = &topodata.TabletAlias{Uid: 100, Cell: "cell1"}

		now := time.Now()
		si.PrimaryTermStartTime = &vttime.Time{Seconds: int64(now.Second()), Nanoseconds: int32(now.Nanosecond())}

		return nil
	})
	require.NoError(t, err)

	be := &mysqlctl.BuiltinBackupEngine{}
	bh := filebackupstorage.NewBackupHandle(nil, "", "", false)
	// Spin up a fake daemon to be used in backups. It needs to be allowed to receive:
	// "STOP SLAVE", "START SLAVE", in that order.
	mysqld := mysqlctl.NewFakeMysqlDaemon(fakesqldb.New(t))
	mysqld.ExpectedExecuteSuperQueryList = []string{"STOP SLAVE", "START SLAVE"}

	ok, err := be.ExecuteBackup(ctx, mysqlctl.BackupParams{
		Logger: logutil.NewConsoleLogger(),
		Mysqld: mysqld,
		Cnf: &mysqlctl.Mycnf{
			InnodbDataHomeDir:     path.Join(backupRoot, "innodb"),
			InnodbLogGroupHomeDir: path.Join(backupRoot, "log"),
			DataDir:               path.Join(backupRoot, "datadir"),
		},
		Stats:        backupstats.NewFakeStats(),
		Concurrency:  2,
		HookExtraEnv: map[string]string{},
		TopoServer:   ts,
		Keyspace:     keyspace,
		Shard:        shard,
	}, bh)

	require.Error(t, err)
	// all four files will fail
	require.ErrorContains(t, err, "context canceled;context canceled;context canceled;context canceled")
	assert.False(t, ok)
}

// TestExecuteRestoreWithCanceledContext tests the ability of the restore function to gracefully handle cases where errors
// occur due to various reasons, such as context timed-out. The process should not panic in these situations.
func TestExecuteRestoreWithTimedOutContext(t *testing.T) {
	// Set up local backup directory
	id := fmt.Sprintf("%d", time.Now().UnixNano())
	backupRoot := fmt.Sprintf("testdata/builtinbackup_test_%s", id)
	filebackupstorage.FileBackupStorageRoot = backupRoot
	require.NoError(t, createBackupDir(backupRoot, "innodb", "log", "datadir"))
	dataDir := path.Join(backupRoot, "datadir")
	// Add some files under data directory to force backup to execute semaphore acquire inside
	// backupFiles() method (https://github.com/vitessio/vitess/blob/main/go/vt/mysqlctl/builtinbackupengine.go#L483).
	require.NoError(t, createBackupDir(dataDir, "test1"))
	require.NoError(t, createBackupDir(dataDir, "test2"))
	require.NoError(t, createBackupFiles(path.Join(dataDir, "test1"), 2, "ibd"))
	require.NoError(t, createBackupFiles(path.Join(dataDir, "test2"), 2, "ibd"))
	defer os.RemoveAll(backupRoot)

	ctx := context.Background()
	needIt, err := needInnoDBRedoLogSubdir()
	require.NoError(t, err)
	if needIt {
		fpath := path.Join("log", mysql.DynamicRedoLogSubdir)
		if err := createBackupDir(backupRoot, fpath); err != nil {
			require.Failf(t, err.Error(), "failed to create directory: %s", fpath)
		}
	}

	// Set up topo
	keyspace, shard := "mykeyspace", "-80"
	ts := memorytopo.NewServer("cell1")
	defer ts.Close()

	require.NoError(t, ts.CreateKeyspace(ctx, keyspace, &topodata.Keyspace{}))
	require.NoError(t, ts.CreateShard(ctx, keyspace, shard))

	tablet := topo.NewTablet(100, "cell1", "mykeyspace-00-80-0100")
	tablet.Keyspace = keyspace
	tablet.Shard = shard

	require.NoError(t, ts.CreateTablet(ctx, tablet))

	_, err = ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
		si.PrimaryAlias = &topodata.TabletAlias{Uid: 100, Cell: "cell1"}

		now := time.Now()
		si.PrimaryTermStartTime = &vttime.Time{Seconds: int64(now.Second()), Nanoseconds: int32(now.Nanosecond())}

		return nil
	})
	require.NoError(t, err)

	be := &mysqlctl.BuiltinBackupEngine{}
	bh := filebackupstorage.NewBackupHandle(nil, "", "", false)
	// Spin up a fake daemon to be used in backups. It needs to be allowed to receive:
	// "STOP SLAVE", "START SLAVE", in that order.
	mysqld := mysqlctl.NewFakeMysqlDaemon(fakesqldb.New(t))
	mysqld.ExpectedExecuteSuperQueryList = []string{"STOP SLAVE", "START SLAVE"}

	ok, err := be.ExecuteBackup(ctx, mysqlctl.BackupParams{
		Logger: logutil.NewConsoleLogger(),
		Mysqld: mysqld,
		Cnf: &mysqlctl.Mycnf{
			InnodbDataHomeDir:     path.Join(backupRoot, "innodb"),
			InnodbLogGroupHomeDir: path.Join(backupRoot, "log"),
			DataDir:               path.Join(backupRoot, "datadir"),
		},
		Stats:        backupstats.NewFakeStats(),
		Concurrency:  2,
		HookExtraEnv: map[string]string{},
		TopoServer:   ts,
		Keyspace:     keyspace,
		Shard:        shard,
	}, bh)

	require.NoError(t, err)
	assert.True(t, ok)

	// Now try to restore the above backup.
	bh = filebackupstorage.NewBackupHandle(nil, "", "", true)
	mysqld = mysqlctl.NewFakeMysqlDaemon(fakesqldb.New(t))
	mysqld.ExpectedExecuteSuperQueryList = []string{"STOP SLAVE", "START SLAVE"}

	fakeStats := backupstats.NewFakeStats()

	restoreParams := mysqlctl.RestoreParams{
		Cnf: &mysqlctl.Mycnf{
			InnodbDataHomeDir:     path.Join(backupRoot, "innodb"),
			InnodbLogGroupHomeDir: path.Join(backupRoot, "log"),
			DataDir:               path.Join(backupRoot, "datadir"),
			BinLogPath:            path.Join(backupRoot, "binlog"),
			RelayLogPath:          path.Join(backupRoot, "relaylog"),
			RelayLogIndexPath:     path.Join(backupRoot, "relaylogindex"),
			RelayLogInfoPath:      path.Join(backupRoot, "relayloginfo"),
		},
		Logger:              logutil.NewConsoleLogger(),
		Mysqld:              mysqld,
		Concurrency:         2,
		HookExtraEnv:        map[string]string{},
		DeleteBeforeRestore: false,
		DbName:              "test",
		Keyspace:            "test",
		Shard:               "-",
		StartTime:           time.Now(),
		RestoreToPos:        mysql.Position{},
		RestoreToTimestamp:  time.Time{},
		DryRun:              false,
		Stats:               fakeStats,
	}

	// Successful restore.
	bm, err := be.ExecuteRestore(ctx, restoreParams, bh)
	assert.NoError(t, err)
	assert.NotNil(t, bm)

	var destinationCloseStats int
	var destinationOpenStats int
	var destinationWriteStats int
	var sourceCloseStats int
	var sourceOpenStats int
	var sourceReadStats int

	for _, sr := range fakeStats.ScopeReturns {
		sfs := sr.(*backupstats.FakeStats)
		switch sfs.ScopeV[backupstats.ScopeOperation] {
		case "Destination:Close":
			destinationCloseStats++
			require.Len(t, sfs.TimedIncrementCalls, 1)
		case "Destination:Open":
			destinationOpenStats++
			require.Len(t, sfs.TimedIncrementCalls, 1)
		case "Destination:Write":
			destinationWriteStats++
			require.GreaterOrEqual(t, len(sfs.TimedIncrementBytesCalls), 1)
		case "Source:Close":
			sourceCloseStats++
			require.Len(t, sfs.TimedIncrementCalls, 1)
		case "Source:Open":
			sourceOpenStats++
			require.Len(t, sfs.TimedIncrementCalls, 1)
		case "Source:Read":
			sourceReadStats++
			require.GreaterOrEqual(t, len(sfs.TimedIncrementBytesCalls), 1)
		}
	}

	require.Equal(t, 4, destinationCloseStats)
	require.Equal(t, 4, destinationOpenStats)
	require.Equal(t, 4, destinationWriteStats)
	require.Equal(t, 4, sourceCloseStats)
	require.Equal(t, 4, sourceOpenStats)
	require.Equal(t, 4, sourceReadStats)

	// Restore using timed-out context
	mysqld = mysqlctl.NewFakeMysqlDaemon(fakesqldb.New(t))
	mysqld.ExpectedExecuteSuperQueryList = []string{"STOP SLAVE", "START SLAVE"}
	restoreParams.Mysqld = mysqld
	timedOutCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	// Let the context time out.
	time.Sleep(1 * time.Second)
	bm, err = be.ExecuteRestore(timedOutCtx, restoreParams, bh)
	// ExecuteRestore should fail.
	assert.Error(t, err)
	assert.Nil(t, bm)
	// error message can contain any combination of "context deadline exceeded" or "context canceled"
	if !strings.Contains(err.Error(), "context canceled") && !strings.Contains(err.Error(), "context deadline exceeded") {
		assert.Fail(t, "Test should fail with either `context canceled` or `context deadline exceeded`")
	}
}

// needInnoDBRedoLogSubdir indicates whether we need to create a redo log subdirectory.
// Starting with MySQL 8.0.30, the InnoDB redo logs are stored in a subdirectory of the
// <innodb_log_group_home_dir> (<datadir>/. by default) called "#innodb_redo". See:
//
//	https://dev.mysql.com/doc/refman/8.0/en/innodb-redo-log.html#innodb-modifying-redo-log-capacity
func needInnoDBRedoLogSubdir() (needIt bool, err error) {
	mysqldVersionStr, err := mysqlctl.GetVersionString()
	if err != nil {
		return needIt, err
	}
	_, sv, err := mysqlctl.ParseVersionString(mysqldVersionStr)
	if err != nil {
		return needIt, err
	}
	versionStr := fmt.Sprintf("%d.%d.%d", sv.Major, sv.Minor, sv.Patch)
	_, capableOf, _ := mysql.GetFlavor(versionStr, nil)
	if capableOf == nil {
		return needIt, fmt.Errorf("cannot determine database flavor details for version %s", versionStr)
	}
	return capableOf(mysql.DynamicRedoLogCapacityFlavorCapability)
}
