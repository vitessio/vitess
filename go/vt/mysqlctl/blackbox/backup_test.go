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

// Package blackbox is the blackbox tests for package mysqlctl.
package blackbox

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/utils"

	"vitess.io/vitess/go/mysql/replication"

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

func TestExecuteBackup(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

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

	needIt, err := NeedInnoDBRedoLogSubdir()
	require.NoError(t, err)
	if needIt {
		fpath := path.Join("log", mysql.DynamicRedoLogSubdir)
		if err := createBackupDir(backupRoot, fpath); err != nil {
			require.Failf(t, err.Error(), "failed to create directory: %s", fpath)
		}
	}

	// Set up topo
	keyspace, shard := "mykeyspace", "-80"
	ts := memorytopo.NewServer(ctx, "cell1")
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
	oldDeadline := SetBuiltinBackupMysqldDeadline(time.Second)
	defer SetBuiltinBackupMysqldDeadline(oldDeadline)

	bh := filebackupstorage.NewBackupHandle(nil, "", "", false)

	// Spin up a fake daemon to be used in backups. It needs to be allowed to receive:
	//  "STOP REPLICA", "START REPLICA", in that order.
	fakedb := fakesqldb.New(t)
	defer fakedb.Close()
	mysqld := mysqlctl.NewFakeMysqlDaemon(fakedb)
	defer mysqld.Close()
	mysqld.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "START REPLICA"}
	// mysqld.ShutdownTime = time.Minute

	fakeStats := backupstats.NewFakeStats()

	backupResult, err := be.ExecuteBackup(ctx, mysqlctl.BackupParams{
		Logger: logutil.NewConsoleLogger(),
		Mysqld: mysqld,
		Cnf: &mysqlctl.Mycnf{
			InnodbDataHomeDir:     path.Join(backupRoot, "innodb"),
			InnodbLogGroupHomeDir: path.Join(backupRoot, "log"),
			DataDir:               path.Join(backupRoot, "datadir"),
		},
		Concurrency:          2,
		HookExtraEnv:         map[string]string{},
		TopoServer:           ts,
		Keyspace:             keyspace,
		Shard:                shard,
		Stats:                fakeStats,
		MysqlShutdownTimeout: MysqlShutdownTimeout,
	}, bh)

	require.NoError(t, err)
	assert.Equal(t, mysqlctl.BackupUsable, backupResult)

	var destinationCloseStats int
	var destinationOpenStats int
	var destinationWriteStats int
	var sourceCloseStats int
	var sourceOpenStats int
	var sourceReadStats int

	for _, sr := range fakeStats.ScopeReturns {
		switch sr.ScopeV[backupstats.ScopeOperation] {
		case "Destination:Close":
			destinationCloseStats++
			require.Len(t, sr.TimedIncrementCalls, 1)
		case "Destination:Open":
			destinationOpenStats++
			require.Len(t, sr.TimedIncrementCalls, 1)
		case "Destination:Write":
			destinationWriteStats++
			require.GreaterOrEqual(t, len(sr.TimedIncrementBytesCalls), 1)
		case "Source:Close":
			sourceCloseStats++
			require.Len(t, sr.TimedIncrementCalls, 1)
		case "Source:Open":
			sourceOpenStats++
			require.Len(t, sr.TimedIncrementCalls, 1)
		case "Source:Read":
			sourceReadStats++
			require.GreaterOrEqual(t, len(sr.TimedIncrementBytesCalls), 1)
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

	backupResult, err = be.ExecuteBackup(ctx, mysqlctl.BackupParams{
		Logger: logutil.NewConsoleLogger(),
		Mysqld: mysqld,
		Cnf: &mysqlctl.Mycnf{
			InnodbDataHomeDir:     path.Join(backupRoot, "innodb"),
			InnodbLogGroupHomeDir: path.Join(backupRoot, "log"),
			DataDir:               path.Join(backupRoot, "datadir"),
		},
		HookExtraEnv:         map[string]string{},
		TopoServer:           ts,
		Keyspace:             keyspace,
		Shard:                shard,
		MysqlShutdownTimeout: MysqlShutdownTimeout,
	}, bh)

	assert.Error(t, err)
	assert.Equal(t, mysqlctl.BackupUnusable, backupResult)
}

func TestExecuteBackupWithSafeUpgrade(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

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

	needIt, err := NeedInnoDBRedoLogSubdir()
	require.NoError(t, err)
	if needIt {
		fpath := path.Join("log", mysql.DynamicRedoLogSubdir)
		if err := createBackupDir(backupRoot, fpath); err != nil {
			require.Failf(t, err.Error(), "failed to create directory: %s", fpath)
		}
	}

	// Set up topo
	keyspace, shard := "mykeyspace", "-80"
	ts := memorytopo.NewServer(ctx, "cell1")
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
	oldDeadline := SetBuiltinBackupMysqldDeadline(time.Second)
	defer SetBuiltinBackupMysqldDeadline(oldDeadline)

	bh := filebackupstorage.NewBackupHandle(nil, "", "", false)

	// Spin up a fake daemon to be used in backups. It needs to be allowed to receive:
	//  "STOP REPLICA", "START REPLICA", in that order.
	// It also needs to be allowed to receive the query to disable the innodb_fast_shutdown flag.
	fakedb := fakesqldb.New(t)
	defer fakedb.Close()
	mysqld := mysqlctl.NewFakeMysqlDaemon(fakedb)
	defer mysqld.Close()
	mysqld.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "START REPLICA"}
	mysqld.FetchSuperQueryMap = map[string]*sqltypes.Result{
		"SET GLOBAL innodb_fast_shutdown=0": {},
	}

	backupResult, err := be.ExecuteBackup(ctx, mysqlctl.BackupParams{
		Logger: logutil.NewConsoleLogger(),
		Mysqld: mysqld,
		Cnf: &mysqlctl.Mycnf{
			InnodbDataHomeDir:     path.Join(backupRoot, "innodb"),
			InnodbLogGroupHomeDir: path.Join(backupRoot, "log"),
			DataDir:               path.Join(backupRoot, "datadir"),
		},
		Concurrency:          2,
		TopoServer:           ts,
		Keyspace:             keyspace,
		Shard:                shard,
		Stats:                backupstats.NewFakeStats(),
		UpgradeSafe:          true,
		MysqlShutdownTimeout: MysqlShutdownTimeout,
	}, bh)

	require.NoError(t, err)
	assert.Equal(t, mysqlctl.BackupUsable, backupResult)
}

// TestExecuteBackupWithCanceledContext tests the ability of the backup function to gracefully handle cases where errors
// occur due to various reasons, such as context time cancel. The process should not panic in these situations.
func TestExecuteBackupWithCanceledContext(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	// Set up local backup directory
	id := strconv.FormatInt(time.Now().UnixNano(), 10)
	backupRoot := "testdata/builtinbackup_test_" + id
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

	needIt, err := NeedInnoDBRedoLogSubdir()
	require.NoError(t, err)
	if needIt {
		fpath := path.Join("log", mysql.DynamicRedoLogSubdir)
		if err := createBackupDir(backupRoot, fpath); err != nil {
			require.Failf(t, err.Error(), "failed to create directory: %s", fpath)
		}
	}

	// Set up topo
	keyspace, shard := "mykeyspace", "-80"
	ts := memorytopo.NewServer(ctx, "cell1")
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
	// "STOP REPLICA", "START REPLICA", in that order.
	fakedb := fakesqldb.New(t)
	defer fakedb.Close()
	mysqld := mysqlctl.NewFakeMysqlDaemon(fakedb)
	defer mysqld.Close()
	mysqld.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "START REPLICA"}

	// Cancel the context deliberately
	cancelledCtx, cancelCtx := context.WithCancel(context.Background())
	cancelCtx()

	backupResult, err := be.ExecuteBackup(cancelledCtx, mysqlctl.BackupParams{
		Logger: logutil.NewConsoleLogger(),
		Mysqld: mysqld,
		Cnf: &mysqlctl.Mycnf{
			InnodbDataHomeDir:     path.Join(backupRoot, "innodb"),
			InnodbLogGroupHomeDir: path.Join(backupRoot, "log"),
			DataDir:               path.Join(backupRoot, "datadir"),
		},
		Stats:                backupstats.NewFakeStats(),
		Concurrency:          2,
		HookExtraEnv:         map[string]string{},
		TopoServer:           ts,
		Keyspace:             keyspace,
		Shard:                shard,
		MysqlShutdownTimeout: MysqlShutdownTimeout,
	}, bh)

	require.Error(t, err)
	// all four files will fail
	require.ErrorContains(t, err, "context canceled; context canceled; context canceled; context canceled")
	assert.Equal(t, mysqlctl.BackupUnusable, backupResult)
}

// TestExecuteRestoreWithCanceledContext tests the ability of the restore function to gracefully handle cases where errors
// occur due to various reasons, such as context timed-out. The process should not panic in these situations.
func TestExecuteRestoreWithTimedOutContext(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	// Set up local backup directory
	id := strconv.FormatInt(time.Now().UnixNano(), 10)
	backupRoot := "testdata/builtinbackup_test_" + id
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

	needIt, err := NeedInnoDBRedoLogSubdir()
	require.NoError(t, err)
	if needIt {
		fpath := path.Join("log", mysql.DynamicRedoLogSubdir)
		if err := createBackupDir(backupRoot, fpath); err != nil {
			require.Failf(t, err.Error(), "failed to create directory: %s", fpath)
		}
	}

	// Set up topo
	keyspace, shard := "mykeyspace", "-80"
	ts := memorytopo.NewServer(ctx, "cell1")
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
	// "STOP REPLICA", "START REPLICA", in that order.
	fakedb := fakesqldb.New(t)
	defer fakedb.Close()
	mysqld := mysqlctl.NewFakeMysqlDaemon(fakedb)
	defer mysqld.Close()
	mysqld.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "START REPLICA"}

	backupResult, err := be.ExecuteBackup(ctx, mysqlctl.BackupParams{
		Logger: logutil.NewConsoleLogger(),
		Mysqld: mysqld,
		Cnf: &mysqlctl.Mycnf{
			InnodbDataHomeDir:     path.Join(backupRoot, "innodb"),
			InnodbLogGroupHomeDir: path.Join(backupRoot, "log"),
			DataDir:               path.Join(backupRoot, "datadir"),
		},
		Stats:                backupstats.NewFakeStats(),
		Concurrency:          2,
		HookExtraEnv:         map[string]string{},
		TopoServer:           ts,
		Keyspace:             keyspace,
		Shard:                shard,
		MysqlShutdownTimeout: MysqlShutdownTimeout,
	}, bh)

	require.NoError(t, err)
	assert.Equal(t, mysqlctl.BackupUsable, backupResult)

	// Now try to restore the above backup.
	bh = filebackupstorage.NewBackupHandle(nil, "", "", true)
	fakedb = fakesqldb.New(t)
	defer fakedb.Close()
	mysqld = mysqlctl.NewFakeMysqlDaemon(fakedb)
	defer mysqld.Close()
	mysqld.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "START REPLICA"}

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
		Logger:               logutil.NewConsoleLogger(),
		Mysqld:               mysqld,
		Concurrency:          2,
		HookExtraEnv:         map[string]string{},
		DeleteBeforeRestore:  false,
		DbName:               "test",
		Keyspace:             "test",
		Shard:                "-",
		StartTime:            time.Now(),
		RestoreToPos:         replication.Position{},
		RestoreToTimestamp:   time.Time{},
		DryRun:               false,
		Stats:                fakeStats,
		MysqlShutdownTimeout: MysqlShutdownTimeout,
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
		switch sr.ScopeV[backupstats.ScopeOperation] {
		case "Destination:Close":
			destinationCloseStats++
			require.Len(t, sr.TimedIncrementCalls, 1)
		case "Destination:Open":
			destinationOpenStats++
			require.Len(t, sr.TimedIncrementCalls, 1)
		case "Destination:Write":
			destinationWriteStats++
			require.GreaterOrEqual(t, len(sr.TimedIncrementBytesCalls), 1)
		case "Source:Close":
			sourceCloseStats++
			require.Len(t, sr.TimedIncrementCalls, 1)
		case "Source:Open":
			sourceOpenStats++
			require.Len(t, sr.TimedIncrementCalls, 1)
		case "Source:Read":
			sourceReadStats++
			require.GreaterOrEqual(t, len(sr.TimedIncrementBytesCalls), 1)
		}
	}

	require.Equal(t, 4, destinationCloseStats)
	require.Equal(t, 4, destinationOpenStats)
	require.Equal(t, 4, destinationWriteStats)
	require.Equal(t, 4, sourceCloseStats)
	require.Equal(t, 4, sourceOpenStats)
	require.Equal(t, 4, sourceReadStats)

	// Restore using timed-out context
	fakedb = fakesqldb.New(t)
	defer fakedb.Close()
	mysqld = mysqlctl.NewFakeMysqlDaemon(fakedb)
	defer mysqld.Close()
	mysqld.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "START REPLICA"}
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

type rwCloseFailFirstCall struct {
	*bytes.Buffer
	firstDone bool
}

func (w *rwCloseFailFirstCall) Write(p []byte) (n int, err error) {
	if w.firstDone {
		return w.Buffer.Write(p)
	}
	w.firstDone = true
	return 0, errors.New("failing first write")
}

func (w *rwCloseFailFirstCall) Read(p []byte) (n int, err error) {
	if w.firstDone {
		return w.Buffer.Read(p)
	}
	w.firstDone = true
	return 0, errors.New("failing first read")
}

func (w *rwCloseFailFirstCall) Close() error {
	return nil
}

func newWriteCloseFailFirstWrite(firstWriteDone bool) *rwCloseFailFirstCall {
	return &rwCloseFailFirstCall{
		Buffer:    bytes.NewBuffer(nil),
		firstDone: firstWriteDone,
	}
}

func TestExecuteBackupFailToWriteEachFileOnlyOnce(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	backupRoot, keyspace, shard, ts := SetupCluster(ctx, t, 2, 2)

	bufferPerFiles := make(map[string]*rwCloseFailFirstCall)
	be := &mysqlctl.BuiltinBackupEngine{}
	bh := &mysqlctl.FakeBackupHandle{}
	bh.AddFileReturnF = func(filename string) mysqlctl.FakeBackupHandleAddFileReturn {
		// This mimics what happens with the other BackupHandles where doing AddFile will either truncate or override
		// any existing data if the same filename already exists.
		_, isRetry := bufferPerFiles[filename]
		newBuffer := newWriteCloseFailFirstWrite(isRetry)
		bufferPerFiles[filename] = newBuffer
		return mysqlctl.FakeBackupHandleAddFileReturn{WriteCloser: newBuffer}
	}

	// Spin up a fake daemon to be used in backups. It needs to be allowed to receive:
	// "STOP REPLICA", "START REPLICA", in that order.
	fakedb := fakesqldb.New(t)
	defer fakedb.Close()
	mysqld := mysqlctl.NewFakeMysqlDaemon(fakedb)
	defer mysqld.Close()
	mysqld.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "START REPLICA"}

	logger := logutil.NewMemoryLogger()
	ctx, cancel := context.WithCancel(ctx)
	backupResult, err := be.ExecuteBackup(ctx, mysqlctl.BackupParams{
		Logger: logger,
		Mysqld: mysqld,
		Cnf: &mysqlctl.Mycnf{
			InnodbDataHomeDir:     path.Join(backupRoot, "innodb"),
			InnodbLogGroupHomeDir: path.Join(backupRoot, "log"),
			DataDir:               path.Join(backupRoot, "datadir"),
		},
		Stats:                backupstats.NewFakeStats(),
		Concurrency:          1,
		HookExtraEnv:         map[string]string{},
		TopoServer:           ts,
		Keyspace:             keyspace,
		Shard:                shard,
		MysqlShutdownTimeout: MysqlShutdownTimeout,
	}, bh)
	cancel()

	expectedLogs := []string{
		"Backing up file: test1/0.ibd (attempt 1/2)",
		"Backing up file: test1/1.ibd (attempt 1/2)",
		"Backing up file: test2/0.ibd (attempt 1/2)",
		"Backing up file: test2/1.ibd (attempt 1/2)",

		"Backing up file: test1/0.ibd (attempt 2/2)",
		"Backing up file: test1/1.ibd (attempt 2/2)",
		"Backing up file: test2/0.ibd (attempt 2/2)",
		"Backing up file: test2/1.ibd (attempt 2/2)",

		"Backing up file MANIFEST (attempt 1/2)",
		"Failed backing up MANIFEST (attempt 1/2)",
		"Backing up file MANIFEST (attempt 2/2)",
		"Completed backing up MANIFEST (attempt 2/2)",
	}

	// Sleep just long enough for everything to complete.
	// It's not flaky, the race detector detects a race when there isn't,
	// the machine is just too slow to propagate the ctxCancel() to all goroutines.
	time.Sleep(2 * time.Second)
	AssertLogs(t, expectedLogs, logger)

	require.NoError(t, err)
	require.Equal(t, mysqlctl.BackupUsable, backupResult)
}

func TestExecuteBackupFailToWriteFileTwice(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	backupRoot, keyspace, shard, ts := SetupCluster(ctx, t, 1, 1)

	bufferPerFiles := make(map[string]*rwCloseFailFirstCall)
	be := &mysqlctl.BuiltinBackupEngine{}
	bh := &mysqlctl.FakeBackupHandle{}
	bh.AddFileReturnF = func(filename string) mysqlctl.FakeBackupHandleAddFileReturn {
		newBuffer := newWriteCloseFailFirstWrite(false)
		bufferPerFiles[filename] = newBuffer

		return mysqlctl.FakeBackupHandleAddFileReturn{WriteCloser: newBuffer}
	}

	// Spin up a fake daemon to be used in backups. It needs to be allowed to receive:
	// "STOP REPLICA", "START REPLICA", in that order.
	fakedb := fakesqldb.New(t)
	defer fakedb.Close()
	mysqld := mysqlctl.NewFakeMysqlDaemon(fakedb)
	defer mysqld.Close()
	mysqld.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "START REPLICA"}

	logger := logutil.NewMemoryLogger()
	fakeStats := backupstats.NewFakeStats()
	ctx, cancel := context.WithCancel(ctx)
	backupResult, err := be.ExecuteBackup(ctx, mysqlctl.BackupParams{
		Logger: logger,
		Mysqld: mysqld,
		Cnf: &mysqlctl.Mycnf{
			InnodbDataHomeDir:     path.Join(backupRoot, "innodb"),
			InnodbLogGroupHomeDir: path.Join(backupRoot, "log"),
			DataDir:               path.Join(backupRoot, "datadir"),
		},
		Stats:                fakeStats,
		Concurrency:          1,
		HookExtraEnv:         map[string]string{},
		TopoServer:           ts,
		Keyspace:             keyspace,
		Shard:                shard,
		MysqlShutdownTimeout: MysqlShutdownTimeout,
	}, bh)
	cancel()

	// Sleep just long enough for everything to complete.
	// It's not flaky, the race detector detects a race when there isn't,
	// the machine is just too slow to propagate the ctxCancel() to all goroutines.
	time.Sleep(2 * time.Second)

	expectedLogs := []string{
		"Backing up file: test1/0.ibd (attempt 1/2)",
		"Backing up file: test1/0.ibd (attempt 2/2)",
	}
	AssertLogs(t, expectedLogs, logger)

	ss := GetStats(fakeStats)
	require.Equal(t, 2, ss.DestinationCloseStats)
	require.Equal(t, 2, ss.DestinationOpenStats)
	require.Equal(t, 2, ss.DestinationWriteStats)
	require.Equal(t, 2, ss.SourceCloseStats)
	require.Equal(t, 2, ss.SourceOpenStats)
	require.Equal(t, 2, ss.SourceReadStats)
	require.ErrorContains(t, err, "failing first write")
	require.Equal(t, mysqlctl.BackupUnusable, backupResult)
}

func TestExecuteRestoreFailToReadEachFileOnlyOnce(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	backupRoot, keyspace, shard, ts := SetupCluster(ctx, t, 2, 2)

	be := &mysqlctl.BuiltinBackupEngine{}
	bufferPerFiles := make(map[string]*rwCloseFailFirstCall)
	bh := &mysqlctl.FakeBackupHandle{}
	bh.AddFileReturnF = func(filename string) mysqlctl.FakeBackupHandleAddFileReturn {
		// let's never make it fail for now
		newBuffer := newWriteCloseFailFirstWrite(true)
		bufferPerFiles[filename] = newBuffer
		return mysqlctl.FakeBackupHandleAddFileReturn{WriteCloser: newBuffer}
	}

	// Spin up a fake daemon to be used in backups. It needs to be allowed to receive:
	// "STOP REPLICA", "START REPLICA", in that order.
	fakedb := fakesqldb.New(t)
	defer fakedb.Close()
	mysqld := mysqlctl.NewFakeMysqlDaemon(fakedb)
	defer mysqld.Close()
	mysqld.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "START REPLICA"}

	backupResult, err := be.ExecuteBackup(ctx, mysqlctl.BackupParams{
		Logger: logutil.NewConsoleLogger(),
		Mysqld: mysqld,
		Cnf: &mysqlctl.Mycnf{
			InnodbDataHomeDir:     path.Join(backupRoot, "innodb"),
			InnodbLogGroupHomeDir: path.Join(backupRoot, "log"),
			DataDir:               path.Join(backupRoot, "datadir"),
		},
		Stats:                backupstats.NewFakeStats(),
		Concurrency:          1,
		HookExtraEnv:         map[string]string{},
		TopoServer:           ts,
		Keyspace:             keyspace,
		Shard:                shard,
		MysqlShutdownTimeout: MysqlShutdownTimeout,
	}, bh)

	require.NoError(t, err)
	require.Equal(t, mysqlctl.BackupUsable, backupResult)

	// let's mark each file in the buffer as if it is their first read
	for key := range bufferPerFiles {
		bufferPerFiles[key].firstDone = false
	}

	// Now try to restore the above backup.
	fakeBh := &mysqlctl.FakeBackupHandle{}
	fakeBh.ReadFileReturnF = func(ctx context.Context, filename string) (io.ReadCloser, error) {
		return bufferPerFiles[filename], nil
	}

	fakedb = fakesqldb.New(t)
	defer fakedb.Close()
	mysqld = mysqlctl.NewFakeMysqlDaemon(fakedb)
	defer mysqld.Close()
	mysqld.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "START REPLICA"}

	fakeStats := backupstats.NewFakeStats()
	logger := logutil.NewMemoryLogger()

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
		Logger:               logger,
		Mysqld:               mysqld,
		Concurrency:          1,
		HookExtraEnv:         map[string]string{},
		DeleteBeforeRestore:  false,
		DbName:               "test",
		Keyspace:             "test",
		Shard:                "-",
		StartTime:            time.Now(),
		RestoreToPos:         replication.Position{},
		RestoreToTimestamp:   time.Time{},
		DryRun:               false,
		Stats:                fakeStats,
		MysqlShutdownTimeout: MysqlShutdownTimeout,
	}

	// Successful restore.
	bm, err := be.ExecuteRestore(ctx, restoreParams, fakeBh)
	assert.NoError(t, err)
	assert.NotNil(t, bm)

	ss := GetStats(fakeStats)
	require.Equal(t, 8, ss.DestinationCloseStats)
	require.Equal(t, 8, ss.DestinationOpenStats)
	require.Equal(t, 4, ss.DestinationWriteStats)
	require.Equal(t, 8, ss.SourceCloseStats)
	require.Equal(t, 8, ss.SourceOpenStats)
	require.Equal(t, 8, ss.SourceReadStats)
}

func TestExecuteRestoreFailToReadEachFileTwice(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	backupRoot, keyspace, shard, ts := SetupCluster(ctx, t, 2, 2)

	be := &mysqlctl.BuiltinBackupEngine{}
	bufferPerFiles := make(map[string]*rwCloseFailFirstCall)
	bh := &mysqlctl.FakeBackupHandle{}
	bh.AddFileReturnF = func(filename string) mysqlctl.FakeBackupHandleAddFileReturn {
		// let's never make it fail for now
		newBuffer := newWriteCloseFailFirstWrite(true)
		bufferPerFiles[filename] = newBuffer
		return mysqlctl.FakeBackupHandleAddFileReturn{WriteCloser: newBuffer}
	}

	// Spin up a fake daemon to be used in backups. It needs to be allowed to receive:
	// "STOP REPLICA", "START REPLICA", in that order.
	fakedb := fakesqldb.New(t)
	defer fakedb.Close()
	mysqld := mysqlctl.NewFakeMysqlDaemon(fakedb)
	defer mysqld.Close()
	mysqld.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "START REPLICA"}

	backupResult, err := be.ExecuteBackup(ctx, mysqlctl.BackupParams{
		Logger: logutil.NewConsoleLogger(),
		Mysqld: mysqld,
		Cnf: &mysqlctl.Mycnf{
			InnodbDataHomeDir:     path.Join(backupRoot, "innodb"),
			InnodbLogGroupHomeDir: path.Join(backupRoot, "log"),
			DataDir:               path.Join(backupRoot, "datadir"),
		},
		Stats:                backupstats.NewFakeStats(),
		Concurrency:          1,
		HookExtraEnv:         map[string]string{},
		TopoServer:           ts,
		Keyspace:             keyspace,
		Shard:                shard,
		MysqlShutdownTimeout: MysqlShutdownTimeout,
	}, bh)

	require.NoError(t, err)
	require.Equal(t, mysqlctl.BackupUsable, backupResult)

	// Now try to restore the above backup.
	fakeBh := &mysqlctl.FakeBackupHandle{}
	fakeBh.ReadFileReturnF = func(ctx context.Context, filename string) (io.ReadCloser, error) {
		// always make it fail, expect if it is the MANIFEST file, otherwise we won't start restoring the other files
		buffer := bufferPerFiles[filename]
		if filename != "MANIFEST" {
			buffer.firstDone = false
		}
		return buffer, nil
	}

	fakedb = fakesqldb.New(t)
	defer fakedb.Close()
	mysqld = mysqlctl.NewFakeMysqlDaemon(fakedb)
	defer mysqld.Close()
	mysqld.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "START REPLICA"}

	fakeStats := backupstats.NewFakeStats()
	logger := logutil.NewMemoryLogger()

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
		Logger:               logger,
		Mysqld:               mysqld,
		Concurrency:          1,
		HookExtraEnv:         map[string]string{},
		DeleteBeforeRestore:  false,
		DbName:               "test",
		Keyspace:             "test",
		Shard:                "-",
		StartTime:            time.Now(),
		RestoreToPos:         replication.Position{},
		RestoreToTimestamp:   time.Time{},
		DryRun:               false,
		Stats:                fakeStats,
		MysqlShutdownTimeout: MysqlShutdownTimeout,
	}

	// Successful restore.
	bm, err := be.ExecuteRestore(ctx, restoreParams, fakeBh)
	assert.ErrorContains(t, err, "failing first read")
	assert.Nil(t, bm)

	ss := GetStats(fakeStats)
	require.Equal(t, 5, ss.DestinationCloseStats)
	require.Equal(t, 5, ss.DestinationOpenStats)
	require.Equal(t, 0, ss.DestinationWriteStats)
	require.Equal(t, 5, ss.SourceCloseStats)
	require.Equal(t, 5, ss.SourceOpenStats)
	require.Equal(t, 5, ss.SourceReadStats)
}
