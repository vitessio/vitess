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

package integration

import (
	"context"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"log"

	"github.com/minio/minio-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/backupstats"
	"vitess.io/vitess/go/vt/mysqlctl/blackbox"
	"vitess.io/vitess/go/vt/mysqlctl/s3backupstorage"
)

/*
	These tests use Minio to emulate AWS S3. It allows us to run the tests on
	GitHub Actions without having the security burden of carrying out AWS secrets
	in our GitHub repo.

	Minio is almost a drop-in replacement for AWS S3, if you want to run these
	tests against a true AWS S3 Bucket, you can do so by not running the TestMain
	and setting the 'AWS_*' environment variable to your own values.
*/

func TestMain(m *testing.M) {
	f := func() int {
		minioPath, err := exec.LookPath("minio")
		if err != nil {
			log.Fatalf("minio binary not found: %v", err)
		}

		dataDir, err := os.MkdirTemp("", "")
		if err != nil {
			log.Fatalf("could not create temporary directory: %v", err)
		}
		err = os.MkdirAll(dataDir, 0755)
		if err != nil {
			log.Fatalf("failed to create MinIO data directory: %v", err)
		}

		cmd := exec.Command(minioPath, "server", dataDir, "--console-address", ":9001")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		err = cmd.Start()
		if err != nil {
			log.Fatalf("failed to start MinIO: %v", err)
		}
		defer func() {
			cmd.Process.Kill()
		}()

		// Local MinIO credentials
		accessKey := "minioadmin"
		secretKey := "minioadmin"
		minioEndpoint := "http://localhost:9000"
		bucketName := "test-bucket"
		region := "us-east-1"

		client, err := minio.New("localhost:9000", accessKey, secretKey, false)
		if err != nil {
			log.Fatalf("failed to create MinIO client: %v", err)
		}
		waitForMinio(client)

		err = client.MakeBucket(bucketName, region)
		if err != nil {
			log.Fatalf("failed to create test bucket: %v", err)
		}

		// Same env variables that are used between AWS S3 and Minio
		os.Setenv("AWS_ACCESS_KEY_ID", accessKey)
		os.Setenv("AWS_SECRET_ACCESS_KEY", secretKey)
		os.Setenv("AWS_BUCKET", bucketName)
		os.Setenv("AWS_ENDPOINT", minioEndpoint)
		os.Setenv("AWS_REGION", region)

		return m.Run()
	}

	os.Exit(f())
}

func waitForMinio(client *minio.Client) {
	for i := 0; i < 60; i++ {
		_, err := client.ListBuckets()
		if err == nil {
			return
		}
		time.Sleep(1 * time.Second)
	}
	log.Fatalf("MinIO server did not become ready in time")
}

func checkEnvForS3(t *testing.T) {
	envRequired := []string{
		"AWS_ACCESS_KEY_ID",
		"AWS_SECRET_ACCESS_KEY",
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
	backupRoot, keyspace, shard, ts := blackbox.SetupCluster(ctx, t, 2, 2)

	be := &mysqlctl.BuiltinBackupEngine{}

	// Configure a tight deadline to force a timeout
	oldDeadline := blackbox.SetBuiltinBackupMysqldDeadline(time.Second)
	defer blackbox.SetBuiltinBackupMysqldDeadline(oldDeadline)

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
		MysqlShutdownTimeout: blackbox.MysqlShutdownTimeout,
	}, bh)

	require.NoError(t, err)
	require.Equal(t, mysqlctl.BackupUsable, backupResult)

	ss := blackbox.GetStats(fakeStats)

	// Even though we have 4 files, we expect '8' for all the values below as we re-do every file once.
	require.Equal(t, 8, ss.DestinationCloseStats)
	require.Equal(t, 8, ss.DestinationOpenStats)
	require.Equal(t, 8, ss.DestinationWriteStats)
	require.Equal(t, 8, ss.SourceCloseStats)
	require.Equal(t, 8, ss.SourceOpenStats)
	require.Equal(t, 8, ss.SourceReadStats)
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
	backupRoot, keyspace, shard, ts := blackbox.SetupCluster(ctx, t, 2, 2)

	be := &mysqlctl.BuiltinBackupEngine{}

	// Configure a tight deadline to force a timeout
	oldDeadline := blackbox.SetBuiltinBackupMysqldDeadline(time.Second)
	defer blackbox.SetBuiltinBackupMysqldDeadline(oldDeadline)

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
		MysqlShutdownTimeout: blackbox.MysqlShutdownTimeout,
	}, bh)

	require.Error(t, err)
	require.Equal(t, mysqlctl.BackupUnusable, backupResult)

	ss := blackbox.GetStats(fakeStats)

	// All stats here must be equal to 5, we have four files, we go each of them, they all fail.
	// The logic decides to retry each file once, we retry the first failed file, it fails again
	// but since it has reached the limit of retries, the backup will fail anyway, thus we don't
	// retry the other 3 files.
	require.Equal(t, 5, ss.DestinationCloseStats)
	require.Equal(t, 5, ss.DestinationOpenStats)
	require.Equal(t, 5, ss.DestinationWriteStats)
	require.Equal(t, 5, ss.SourceCloseStats)
	require.Equal(t, 5, ss.SourceOpenStats)
	require.Equal(t, 5, ss.SourceReadStats)
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
	backupRoot, keyspace, shard, ts := blackbox.SetupCluster(ctx, t, 2, 2)

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
		MysqlShutdownTimeout: blackbox.MysqlShutdownTimeout,
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
		MysqlShutdownTimeout: blackbox.MysqlShutdownTimeout,
	}

	// Successful restore.
	bm, err := be.ExecuteRestore(ctx, restoreParams, restoreBh)
	assert.NoError(t, err)
	assert.NotNil(t, bm)

	ss := blackbox.GetStats(fakeStats)
	require.Equal(t, 8, ss.DestinationCloseStats)
	require.Equal(t, 8, ss.DestinationOpenStats)
	require.Equal(t, 4, ss.DestinationWriteStats) // 4, because on the first attempt, we fail to read before writing to the filesystem
	require.Equal(t, 8, ss.SourceCloseStats)
	require.Equal(t, 8, ss.SourceOpenStats)
	require.Equal(t, 8, ss.SourceReadStats)
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
	backupRoot, keyspace, shard, ts := blackbox.SetupCluster(ctx, t, 2, 2)

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
		MysqlShutdownTimeout: blackbox.MysqlShutdownTimeout,
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
		MysqlShutdownTimeout: blackbox.MysqlShutdownTimeout,
	}

	// Successful restore.
	_, err = be.ExecuteRestore(ctx, restoreParams, restoreBh)
	assert.ErrorContains(t, err, "failing read")

	ss := blackbox.GetStats(fakeStats)
	// Everything except destination writes must be equal to 5:
	// +1 for every file on the first attempt (= 4), and +1 for the first file we try for the second time.
	// Since we fail early as soon as a second-attempt-file fails, we won't see a value above 5.
	require.Equal(t, 5, ss.DestinationCloseStats)
	require.Equal(t, 5, ss.DestinationOpenStats)
	require.Equal(t, 0, ss.DestinationWriteStats) // 0, because on the both attempts, we fail to read before writing to the filesystem
	require.Equal(t, 5, ss.SourceCloseStats)
	require.Equal(t, 5, ss.SourceOpenStats)
	require.Equal(t, 5, ss.SourceReadStats)
}
