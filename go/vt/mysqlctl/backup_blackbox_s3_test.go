/*
Copyright 2024 The Vitess Authors.

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

package mysqlctl_test

import (
	"context"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/replication"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/backupstats"
	"vitess.io/vitess/go/vt/mysqlctl/s3backupstorage"
)

/*
	The tests in this file are meant to only be run locally for now. This allows us to avoid putting
	AWS secrets in GitHub Actions. In order to run this test locally you must have an AWS S3 bucket
	and set the proper environment variables, the full list of variables is available in checkEnvForS3.
*/

func checkEnvForS3(t *testing.T) {
	envRequired := []string{
		"AWS_ACCESS_KEY_ID",
		"AWS_SECRET_ACCESS_KEY",
		"AWS_SESSION_TOKEN",
		"AWS_BUCKET",
		"AWS_ENDPOINT",
		"AWS_REGION",
	}

	var missing []string
	for _, s := range envRequired {
		if os.Getenv(s) == "" {
			missing = append(missing, s)
		}
	}
	if len(missing) > 0 {
		t.Skipf("missing AWS secrets to run this test: please set: %s", strings.Join(missing, ", "))
	}
}

func TestExecuteBackupS3FailEachFileOnce(t *testing.T) {
	checkEnvForS3(t)
	s3backupstorage.InitFlag(s3backupstorage.FakeConfig{
		Region:    os.Getenv("AWS_REGION"),
		Endpoint:  os.Getenv("AWS_ENDPOINT"),
		Bucket:    os.Getenv("AWS_BUCKET"),
		ForcePath: true,
	})

	ctx := context.Background()
	backupRoot, keyspace, shard, ts := setupCluster(ctx, t, 2, 2)

	be := &mysqlctl.BuiltinBackupEngine{}

	// Configure a tight deadline to force a timeout
	oldDeadline := setBuiltinBackupMysqldDeadline(time.Second)
	defer setBuiltinBackupMysqldDeadline(oldDeadline)

	fakeStats := backupstats.NewFakeStats()
	logger := logutil.NewMemoryLogger()

	bh, err := s3backupstorage.NewFakeS3BackupHandle(ctx, t.Name(), time.Now().Format(mysqlctl.BackupTimestampFormat), logger, fakeStats)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, bh.AbortBackup(ctx))
	})
	// Modify the fake S3 storage to always fail when trying to write a file for the first time
	bh.AddFileReturnF = s3backupstorage.FailFirstWrite

	// Spin up a fake daemon to be used in backups. It needs to be allowed to receive:
	//  "STOP REPLICA", "START REPLICA", in that order.
	fakedb := fakesqldb.New(t)
	defer fakedb.Close()
	mysqld := mysqlctl.NewFakeMysqlDaemon(fakedb)
	defer mysqld.Close()
	mysqld.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "START REPLICA"}

	backupResult, err := be.ExecuteBackup(ctx, mysqlctl.BackupParams{
		Logger: logger,
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
		MysqlShutdownTimeout: mysqlShutdownTimeout,
	}, bh)

	require.NoError(t, err)
	require.Equal(t, mysqlctl.BackupUsable, backupResult)

	ss := getStats(fakeStats)

	// Even though we have 4 files, we expect '8' for all the values below as we re-do every file once.
	require.Equal(t, 8, ss.destinationCloseStats)
	require.Equal(t, 8, ss.destinationOpenStats)
	require.Equal(t, 8, ss.destinationWriteStats)
	require.Equal(t, 8, ss.sourceCloseStats)
	require.Equal(t, 8, ss.sourceOpenStats)
	require.Equal(t, 8, ss.sourceReadStats)
}

func TestExecuteBackupS3FailEachFileTwice(t *testing.T) {
	checkEnvForS3(t)
	s3backupstorage.InitFlag(s3backupstorage.FakeConfig{
		Region:    os.Getenv("AWS_REGION"),
		Endpoint:  os.Getenv("AWS_ENDPOINT"),
		Bucket:    os.Getenv("AWS_BUCKET"),
		ForcePath: true,
	})

	ctx := context.Background()
	backupRoot, keyspace, shard, ts := setupCluster(ctx, t, 2, 2)

	be := &mysqlctl.BuiltinBackupEngine{}

	// Configure a tight deadline to force a timeout
	oldDeadline := setBuiltinBackupMysqldDeadline(time.Second)
	defer setBuiltinBackupMysqldDeadline(oldDeadline)

	fakeStats := backupstats.NewFakeStats()
	logger := logutil.NewMemoryLogger()

	bh, err := s3backupstorage.NewFakeS3BackupHandle(ctx, t.Name(), time.Now().Format(mysqlctl.BackupTimestampFormat), logger, fakeStats)
	require.NoError(t, err)
	t.Cleanup(func() {
		// If the code works as expected by this test, no files will be created on S3 and AbortBackup will
		// fail, for this reason, let's not check the error return.
		// We still call AbortBackup anyway in the event that the code is not behaving as expected and some
		// files were created by mistakes, we delete them.
		_ = bh.AbortBackup(ctx)
	})
	// Modify the fake S3 storage to always fail when trying to write a file for the first time
	bh.AddFileReturnF = s3backupstorage.FailAllWrites

	// Spin up a fake daemon to be used in backups. It needs to be allowed to receive:
	//  "STOP REPLICA", "START REPLICA", in that order.
	fakedb := fakesqldb.New(t)
	defer fakedb.Close()
	mysqld := mysqlctl.NewFakeMysqlDaemon(fakedb)
	defer mysqld.Close()
	mysqld.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "START REPLICA"}

	backupResult, err := be.ExecuteBackup(ctx, mysqlctl.BackupParams{
		Logger: logger,
		Mysqld: mysqld,
		Cnf: &mysqlctl.Mycnf{
			InnodbDataHomeDir:     path.Join(backupRoot, "innodb"),
			InnodbLogGroupHomeDir: path.Join(backupRoot, "log"),
			DataDir:               path.Join(backupRoot, "datadir"),
		},
		Concurrency:          1,
		HookExtraEnv:         map[string]string{},
		TopoServer:           ts,
		Keyspace:             keyspace,
		Shard:                shard,
		Stats:                fakeStats,
		MysqlShutdownTimeout: mysqlShutdownTimeout,
	}, bh)

	require.Error(t, err)
	require.Equal(t, mysqlctl.BackupUnusable, backupResult)

	ss := getStats(fakeStats)

	// All stats here must be equal to 5, we have four files, we go each of them, they all fail.
	// The logic decides to retry each file once, we retry the first failed file, it fails again
	// but since it has reached the limit of retries, the backup will fail anyway, thus we don't
	// retry the other 3 files.
	require.Equal(t, 5, ss.destinationCloseStats)
	require.Equal(t, 5, ss.destinationOpenStats)
	require.Equal(t, 5, ss.destinationWriteStats)
	require.Equal(t, 5, ss.sourceCloseStats)
	require.Equal(t, 5, ss.sourceOpenStats)
	require.Equal(t, 5, ss.sourceReadStats)
}

func TestExecuteRestoreS3FailEachFileOnce(t *testing.T) {
	checkEnvForS3(t)
	s3backupstorage.InitFlag(s3backupstorage.FakeConfig{
		Region:    os.Getenv("AWS_REGION"),
		Endpoint:  os.Getenv("AWS_ENDPOINT"),
		Bucket:    os.Getenv("AWS_BUCKET"),
		ForcePath: true,
	})

	ctx := context.Background()
	backupRoot, keyspace, shard, ts := setupCluster(ctx, t, 2, 2)

	fakeStats := backupstats.NewFakeStats()
	logger := logutil.NewMemoryLogger()

	be := &mysqlctl.BuiltinBackupEngine{}
	dirName := time.Now().Format(mysqlctl.BackupTimestampFormat)
	name := t.Name() + "-" + strconv.Itoa(int(time.Now().Unix()))
	bh, err := s3backupstorage.NewFakeS3BackupHandle(ctx, name, dirName, logger, fakeStats)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, bh.AbortBackup(ctx))
	})

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
		MysqlShutdownTimeout: mysqlShutdownTimeout,
	}, bh)

	require.NoError(t, err)
	require.Equal(t, mysqlctl.BackupUsable, backupResult)

	restoreBh, err := s3backupstorage.NewFakeS3RestoreHandle(ctx, name, logger, fakeStats)
	require.NoError(t, err)
	restoreBh.ReadFileReturnF = s3backupstorage.FailFirstRead

	fakedb = fakesqldb.New(t)
	defer fakedb.Close()
	mysqld = mysqlctl.NewFakeMysqlDaemon(fakedb)
	defer mysqld.Close()
	mysqld.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "START REPLICA"}

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
		MysqlShutdownTimeout: mysqlShutdownTimeout,
	}

	// Successful restore.
	bm, err := be.ExecuteRestore(ctx, restoreParams, restoreBh)
	assert.NoError(t, err)
	assert.NotNil(t, bm)

	ss := getStats(fakeStats)
	require.Equal(t, 8, ss.destinationCloseStats)
	require.Equal(t, 8, ss.destinationOpenStats)
	require.Equal(t, 4, ss.destinationWriteStats) // 4, because on the first attempt, we fail to read before writing to the filesystem
	require.Equal(t, 8, ss.sourceCloseStats)
	require.Equal(t, 8, ss.sourceOpenStats)
	require.Equal(t, 8, ss.sourceReadStats)
}

func TestExecuteRestoreS3FailEachFileTwice(t *testing.T) {
	checkEnvForS3(t)
	s3backupstorage.InitFlag(s3backupstorage.FakeConfig{
		Region:    os.Getenv("AWS_REGION"),
		Endpoint:  os.Getenv("AWS_ENDPOINT"),
		Bucket:    os.Getenv("AWS_BUCKET"),
		ForcePath: true,
	})

	ctx := context.Background()
	backupRoot, keyspace, shard, ts := setupCluster(ctx, t, 2, 2)

	fakeStats := backupstats.NewFakeStats()
	logger := logutil.NewMemoryLogger()

	be := &mysqlctl.BuiltinBackupEngine{}
	dirName := time.Now().Format(mysqlctl.BackupTimestampFormat)
	name := t.Name() + "-" + strconv.Itoa(int(time.Now().Unix()))
	bh, err := s3backupstorage.NewFakeS3BackupHandle(ctx, name, dirName, logger, fakeStats)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, bh.AbortBackup(ctx))
	})

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
		MysqlShutdownTimeout: mysqlShutdownTimeout,
	}, bh)

	require.NoError(t, err)
	require.Equal(t, mysqlctl.BackupUsable, backupResult)

	restoreBh, err := s3backupstorage.NewFakeS3RestoreHandle(ctx, name, logger, fakeStats)
	require.NoError(t, err)
	restoreBh.ReadFileReturnF = s3backupstorage.FailAllReadExpectManifest

	fakedb = fakesqldb.New(t)
	defer fakedb.Close()
	mysqld = mysqlctl.NewFakeMysqlDaemon(fakedb)
	defer mysqld.Close()
	mysqld.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "START REPLICA"}

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
		MysqlShutdownTimeout: mysqlShutdownTimeout,
	}

	// Successful restore.
	_, err = be.ExecuteRestore(ctx, restoreParams, restoreBh)
	assert.ErrorContains(t, err, "failing read")

	ss := getStats(fakeStats)
	// Everything except destination writes must be equal to 5:
	// +1 for every file on the first attempt (= 4), and +1 for the first file we try for the second time.
	// Since we fail early as soon as a second-attempt-file fails, we won't see a value above 5.
	require.Equal(t, 5, ss.destinationCloseStats)
	require.Equal(t, 5, ss.destinationOpenStats)
	require.Equal(t, 0, ss.destinationWriteStats) // 0, because on the both attempts, we fail to read before writing to the filesystem
	require.Equal(t, 5, ss.sourceCloseStats)
	require.Equal(t, 5, ss.sourceOpenStats)
	require.Equal(t, 5, ss.sourceReadStats)
}
