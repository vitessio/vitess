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

package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"vitess.io/vitess/go/vitesst"

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
	tests against a true AWS S3 Bucket, you can do so by not running the test setup
	and setting the 'AWS_*' environment variables to your own values.

	This package and file are named 'endtoend', but it's more an integration test.
	However, we don't want our CI infra to mistake this for a regular unit-test,
	hence the rename to 'endtoend'.
*/

const (
	minioImage       = "minio/minio:latest"
	minioAPIPort     = "9000/tcp"
	minioConsolePort = "9001/tcp"
)

func setup(t *testing.T) {
	t.Helper()
	ctx := t.Context()

	// Local MinIO credentials
	accessKey := "minioadmin"
	secretKey := "minioadmin"
	bucketName := "test-bucket"
	region := "us-east-1"

	ctr, err := testcontainers.Run(
		ctx, minioImage,
		testcontainers.WithCmd("server", "/data", "--console-address", ":9001"),
		testcontainers.WithEnv(map[string]string{
			"MINIO_ROOT_USER":     accessKey,
			"MINIO_ROOT_PASSWORD": secretKey,
		}),
		testcontainers.WithExposedPorts(minioAPIPort, minioConsolePort),
		testcontainers.WithWaitStrategyAndDeadline(
			2*time.Minute,
			wait.ForHTTP("/minio/health/live").
				WithPort(minioAPIPort).
				WithStartupTimeout(2*time.Minute).
				WithPollInterval(time.Second),
		),
	)
	t.Cleanup(func() {
		if ctr == nil {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
		defer cancel()
		if cleanupErr := ctr.Terminate(cleanupCtx); cleanupErr != nil {
			t.Logf("MinIO teardown: %v", cleanupErr)
		}
	})
	require.NoError(t, err)

	host, err := ctr.Host(ctx)
	require.NoError(t, err)
	port, err := ctr.MappedPort(ctx, minioAPIPort)
	require.NoError(t, err)

	minioAddress := net.JoinHostPort(host, port.Port())
	minioEndpoint := "http://" + minioAddress

	client, err := minio.New(minioAddress, accessKey, secretKey, false)
	require.NoError(t, err)
	require.NoError(t, waitForMinio(ctx, client))
	require.NoError(t, client.MakeBucket(bucketName, region))

	// Same env variables that are used between AWS S3 and Minio
	t.Setenv("AWS_ACCESS_KEY_ID", accessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", secretKey)
	t.Setenv("AWS_BUCKET", bucketName)
	t.Setenv("AWS_ENDPOINT", minioEndpoint)
	t.Setenv("AWS_REGION", region)

	mysqlRoot, err := setupMysqlRoot(t)
	require.NoError(t, err)
	t.Setenv("VT_MYSQL_ROOT", mysqlRoot)
}

// setupMysqlRoot returns a directory whose bin/mysqld reports the MySQL version
// of the Vitess image. mysqlctl reads that version to decide how a backup lays
// out the InnoDB redo log directory.
func setupMysqlRoot(t *testing.T) (string, error) {
	version, err := imageMysqldVersion(t)
	if err != nil {
		return "", err
	}

	root, err := os.MkdirTemp(t.TempDir(), "vt_mysql_root")
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(path.Join(root, "bin"), 0o755); err != nil {
		return "", err
	}

	versionFile := path.Join(root, "version.txt")
	if err := os.WriteFile(versionFile, []byte(version+"\n"), 0o644); err != nil {
		return "", err
	}
	mysqld := "#!/bin/sh\ncat " + versionFile + "\n"
	if err := os.WriteFile(path.Join(root, "bin", "mysqld"), []byte(mysqld), 0o755); err != nil {
		return "", err
	}
	return root, nil
}

// imageMysqldVersion runs "mysqld --version" inside the Vitess image and returns
// the version line it prints.
func imageMysqldVersion(t *testing.T) (string, error) {
	t.Helper()
	ctx := t.Context()
	ctr, err := testcontainers.Run(
		ctx, vitesst.Image("8.0"),
		testcontainers.WithEntrypoint("mysqld"),
		testcontainers.WithCmd("--version"),
		testcontainers.WithWaitStrategy(wait.ForExit().WithExitTimeout(time.Minute)),
	)
	t.Cleanup(func() {
		if ctr == nil {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
		defer cancel()
		if cleanupErr := ctr.Terminate(cleanupCtx); cleanupErr != nil {
			t.Logf("mysqld version image teardown: %v", cleanupErr)
		}
	})
	if err != nil {
		return "", err
	}

	rc, err := ctr.Logs(ctx)
	if err != nil {
		return "", err
	}
	defer rc.Close()

	output, err := io.ReadAll(rc)
	if err != nil {
		return "", err
	}

	for line := range strings.SplitSeq(string(output), "\n") {
		if idx := strings.Index(line, "mysqld"); idx >= 0 && strings.Contains(line, " Ver ") {
			return strings.TrimSpace(line[idx:]), nil
		}
	}
	return "", fmt.Errorf("could not find the mysqld version in: %s", output)
}

func waitForMinio(ctx context.Context, client *minio.Client) error {
	for range 60 {
		_, err := client.ListBuckets()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return errors.New("MinIO server did not become ready in time")
}

func checkEnvForS3(t *testing.T) {
	// We never want to skip the tests if we are running on CI.
	// We will always run these tests on CI with the test setup and Minio.
	// There should not be a need to skip the tests due to missing ENV vars.
	if os.Getenv("GITHUB_ACTIONS") != "" {
		return
	}

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

type backupTestConfig struct {
	concurrency       int
	addFileReturnFn   func(s3 *s3backupstorage.S3BackupHandle, ctx context.Context, filename string, filesize int64, firstAdd bool) (io.WriteCloser, error)
	checkCleanupError bool
	expectedResult    mysqlctl.BackupResult
	expectedStats     blackbox.StatSummary
}

func runBackupTest(t *testing.T, cfg backupTestConfig) {
	checkEnvForS3(t)
	s3backupstorage.InitFlag(s3backupstorage.FakeConfig{
		Region:    os.Getenv("AWS_REGION"),
		Endpoint:  os.Getenv("AWS_ENDPOINT"),
		Bucket:    os.Getenv("AWS_BUCKET"),
		ForcePath: true,
	})

	// Use context.Background() because ctx is captured by t.Cleanup callbacks
	// (e.g., bh.AbortBackup(ctx)). t.Context() is cancelled before t.Cleanup runs,
	// which would cause the cleanup S3 calls to fail with "context canceled".
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
		err := bh.AbortBackup(ctx)
		if cfg.checkCleanupError {
			require.NoError(t, err)
		}
	})
	bh.AddFileReturnF = cfg.addFileReturnFn

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
		Concurrency:          cfg.concurrency,
		HookExtraEnv:         map[string]string{},
		TopoServer:           ts,
		Keyspace:             keyspace,
		Shard:                shard,
		Stats:                fakeStats,
		MysqlShutdownTimeout: blackbox.MysqlShutdownTimeout,
	}, bh)

	require.Equal(t, cfg.expectedResult, backupResult)
	switch cfg.expectedResult {
	case mysqlctl.BackupUsable:
		require.NoError(t, err)
	case mysqlctl.BackupUnusable, mysqlctl.BackupEmpty:
		require.Error(t, err)
	}

	ss := blackbox.GetStats(fakeStats)
	require.Equal(t, cfg.expectedStats.DestinationCloseStats, ss.DestinationCloseStats)
	require.Equal(t, cfg.expectedStats.DestinationOpenStats, ss.DestinationOpenStats)
	require.Equal(t, cfg.expectedStats.DestinationWriteStats, ss.DestinationWriteStats)
	require.Equal(t, cfg.expectedStats.SourceCloseStats, ss.SourceCloseStats)
	require.Equal(t, cfg.expectedStats.SourceOpenStats, ss.SourceOpenStats)
	require.Equal(t, cfg.expectedStats.SourceReadStats, ss.SourceReadStats)
}

func TestExecuteBackupS3FailEachFileOnce(t *testing.T) {
	setup(t)

	runBackupTest(t, backupTestConfig{
		concurrency: 2,

		// Modify the fake S3 storage to always fail when trying to write a file for the first time
		addFileReturnFn:   s3backupstorage.FailFirstWrite,
		checkCleanupError: true,
		expectedResult:    mysqlctl.BackupUsable,

		// Even though we have 4 files, we expect '8' for all the values below as we re-do every file once.
		expectedStats: blackbox.StatSummary{
			DestinationCloseStats: 8,
			DestinationOpenStats:  8,
			DestinationWriteStats: 8,
			SourceCloseStats:      8,
			SourceOpenStats:       8,
			SourceReadStats:       8,
		},
	})
}

func TestExecuteBackupS3FailEachFileTwice(t *testing.T) {
	setup(t)

	runBackupTest(t, backupTestConfig{
		concurrency: 1,

		// Modify the fake S3 storage to always fail when trying to write a file for the first time
		addFileReturnFn: s3backupstorage.FailAllWrites,

		// If the code works as expected by this test, no files will be created on S3 and AbortBackup will
		// fail, for this reason, let's not check the error return.
		// We still call AbortBackup anyway in the event that the code is not behaving as expected and some
		// files were created by mistakes, we delete them.
		checkCleanupError: false,
		expectedResult:    mysqlctl.BackupUnusable,

		// All stats here must be equal to 5, we have four files, we go each of them, they all fail.
		// The logic decides to retry each file once, we retry the first failed file, it fails again
		// but since it has reached the limit of retries, the backup will fail anyway, thus we don't
		// retry the other 3 files.
		expectedStats: blackbox.StatSummary{
			DestinationCloseStats: 5,
			DestinationOpenStats:  5,
			DestinationWriteStats: 5,
			SourceCloseStats:      5,
			SourceOpenStats:       5,
			SourceReadStats:       5,
		},
	})
}

type restoreTestConfig struct {
	readFileReturnFn func(s3 *s3backupstorage.S3BackupHandle, ctx context.Context, filename string, firstRead bool) (io.ReadCloser, error)
	expectSuccess    bool
	expectedStats    blackbox.StatSummary
}

func runRestoreTest(t *testing.T, cfg restoreTestConfig) {
	checkEnvForS3(t)
	s3backupstorage.InitFlag(s3backupstorage.FakeConfig{
		Region:    os.Getenv("AWS_REGION"),
		Endpoint:  os.Getenv("AWS_ENDPOINT"),
		Bucket:    os.Getenv("AWS_BUCKET"),
		ForcePath: true,
	})

	// Use context.Background() because ctx is captured by t.Cleanup callbacks
	// (e.g., bh.AbortBackup(ctx)). t.Context() is cancelled before t.Cleanup runs,
	// which would cause the cleanup S3 calls to fail with "context canceled".
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

	// Backup is done, let's move on to the restore now

	restoreBh, err := s3backupstorage.NewFakeS3RestoreHandle(ctx, name, logger, fakeStats)
	require.NoError(t, err)
	restoreBh.ReadFileReturnF = cfg.readFileReturnFn

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

	if cfg.expectSuccess {
		assert.NoError(t, err)
		assert.NotNil(t, bm)
	} else {
		assert.Error(t, err)
	}

	ss := blackbox.GetStats(fakeStats)
	require.Equal(t, cfg.expectedStats.DestinationCloseStats, ss.DestinationCloseStats)
	require.Equal(t, cfg.expectedStats.DestinationOpenStats, ss.DestinationOpenStats)
	require.Equal(t, cfg.expectedStats.DestinationWriteStats, ss.DestinationWriteStats)
	require.Equal(t, cfg.expectedStats.SourceCloseStats, ss.SourceCloseStats)
	require.Equal(t, cfg.expectedStats.SourceOpenStats, ss.SourceOpenStats)
	require.Equal(t, cfg.expectedStats.SourceReadStats, ss.SourceReadStats)
}

func TestExecuteRestoreS3FailEachFileOnce(t *testing.T) {
	setup(t)

	runRestoreTest(t, restoreTestConfig{
		readFileReturnFn: s3backupstorage.FailFirstRead,
		expectSuccess:    true,
		expectedStats: blackbox.StatSummary{
			DestinationCloseStats: 8,
			DestinationOpenStats:  8,
			DestinationWriteStats: 4, // 4, because on the first attempt, we fail to read before writing to the filesystem
			SourceCloseStats:      8,
			SourceOpenStats:       8,
			SourceReadStats:       8,
		},
	})
}

func TestExecuteRestoreS3FailEachFileTwice(t *testing.T) {
	setup(t)

	runRestoreTest(t, restoreTestConfig{
		readFileReturnFn: s3backupstorage.FailAllReadExpectManifest,
		expectSuccess:    false,

		// Everything except destination writes must be equal to 5:
		// +1 for every file on the first attempt (= 4), and +1 for the first file we try for the second time.
		// Since we fail early as soon as a second-attempt-file fails, we won't see a value above 5.
		expectedStats: blackbox.StatSummary{
			DestinationCloseStats: 5,
			DestinationOpenStats:  5,
			DestinationWriteStats: 0, // 0, because on the both attempts, we fail to read before writing to the filesystem
			SourceCloseStats:      5,
			SourceOpenStats:       5,
			SourceReadStats:       5,
		},
	})
}
