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
	"io"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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
	These tests run against an S3-compatible object store described by the
	AWS_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION and
	AWS_BUCKET environment variables. In CI the setup-microceph action
	provisions a Ceph RGW on the runner and exports them; locally, point them
	at any S3-compatible store, or at a real S3 bucket, to run the tests.
	Without them the package is skipped outside of CI and fails inside it.

	This package and file are named 'endtoend', but it's more an integration test.
	However, we don't want our CI infra to mistake this for a regular unit-test,
	hence the rename to 'endtoend'.
*/

// s3Env holds the object store coordinates every test in this package uses.
type s3Env struct {
	endpoint  string
	accessKey string
	secretKey string
	region    string
	bucket    string
}

func s3EnvFromEnvironment() s3Env {
	return s3Env{
		endpoint:  os.Getenv("AWS_ENDPOINT"),
		accessKey: os.Getenv("AWS_ACCESS_KEY_ID"),
		secretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		region:    os.Getenv("AWS_REGION"),
		bucket:    os.Getenv("AWS_BUCKET"),
	}
}

// missing lists the environment variables that are unset. An empty AWS_BUCKET
// or AWS_REGION would otherwise be sent to the store as a request for bucket ""
// or signed with an empty region, and ensureBucket would retry the failure for
// a minute.
func (e s3Env) missing() []string {
	var missing []string
	for _, v := range []struct{ name, value string }{
		{"AWS_ENDPOINT", e.endpoint},
		{"AWS_ACCESS_KEY_ID", e.accessKey},
		{"AWS_SECRET_ACCESS_KEY", e.secretKey},
		{"AWS_REGION", e.region},
		{"AWS_BUCKET", e.bucket},
	} {
		if v.value == "" {
			missing = append(missing, v.name)
		}
	}
	return missing
}

func TestMain(m *testing.M) {
	env := s3EnvFromEnvironment()
	if missing := env.missing(); len(missing) > 0 {
		msg := "missing " + strings.Join(missing, ", ") + "; set AWS_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION and AWS_BUCKET to run the S3 backup tests"
		if os.Getenv("GITHUB_ACTIONS") != "" {
			// In CI the setup-microceph action must have provided these;
			// silently skipping the backup tests there is the one thing we
			// must not do.
			log.Fatal(msg)
		}
		log.Println("skipping:", msg)
		os.Exit(0)
	}
	if err := ensureBucket(context.Background(), env); err != nil {
		log.Fatalf("could not prepare bucket %q at %s: %v", env.bucket, env.endpoint, err)
	}
	os.Exit(m.Run())
}

// newS3Client builds a client of the same shape as s3backupstorage's
// (LoadDefaultConfig with the default credential chain, WithRegion,
// path-style), with BaseEndpoint standing in for its endpoint resolver. The
// default chain reads AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and, for
// temporary credentials, AWS_SESSION_TOKEN from the environment, so the setup
// client and the tests' client see the same identity.
func newS3Client(ctx context.Context, env s3Env) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(env.region))
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(env.endpoint)
		o.UsePathStyle = true
	}), nil
}

// ensureBucket waits for the object store to answer and makes sure the test
// bucket exists. It creates the bucket only if HeadBucket says it is missing,
// so it is safe against a pre-existing bucket on a real S3 account. The wait
// absorbs a gateway that is still starting.
func ensureBucket(ctx context.Context, env s3Env) error {
	client, err := newS3Client(ctx, env)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	var lastErr error
	for {
		_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(env.bucket)})
		if err == nil {
			return nil
		}
		if _, ok := errors.AsType[*types.NotFound](err); ok {
			input := &s3.CreateBucketInput{Bucket: aws.String(env.bucket)}
			// S3 requires a location constraint outside us-east-1 and rejects one in it.
			if env.region != "us-east-1" {
				input.CreateBucketConfiguration = &types.CreateBucketConfiguration{
					LocationConstraint: types.BucketLocationConstraint(env.region),
				}
			}
			_, err = client.CreateBucket(ctx, input)
			if err == nil {
				return nil
			}
		}
		// Once the deadline has passed the SDK reports the cancellation, not
		// what the store said; keep the store's error for the caller.
		if ctx.Err() == nil {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			if lastErr == nil {
				return ctx.Err()
			}
			return lastErr
		case <-time.After(time.Second):
		}
		log.Printf("object store at %s: %v; retrying", env.endpoint, lastErr)
	}
}

func checkEnvForS3(t *testing.T) {
	// We never want to skip the tests if we are running on CI.
	// We will always run these tests on CI with the TestMain and the object store setup-microceph provisions.
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
