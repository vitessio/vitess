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

package s3

import (
	"context"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/backupstats"
	"vitess.io/vitess/go/vt/mysqlctl/blackbox"
	"vitess.io/vitess/go/vt/mysqlctl/cephbackupstorage"
)

// TestCephBackupRestore runs the ceph backup storage plugin against the same
// gateway the S3 tests use: backup, list, restore, remove. The plugin names
// its bucket after the keyspace, so this also exercises bucket creation on
// the gateway, which the S3 tests never do because their bucket pre-exists.
func TestCephBackupRestore(t *testing.T) {
	checkEnvForS3(t)
	env := s3EnvFromEnvironment()

	// The plugin's endPoint is host:port with no scheme; useSSL picks the scheme.
	useSSL := strings.HasPrefix(env.endpoint, "https://")
	endpoint := strings.TrimPrefix(strings.TrimPrefix(env.endpoint, "http://"), "https://")
	bs := cephbackupstorage.NewFakeCephBackupStorage(cephbackupstorage.FakeConfig{
		AccessKey: env.accessKey,
		SecretKey: env.secretKey,
		EndPoint:  endpoint,
		UseSSL:    useSSL,
	})

	// context.Background() rather than t.Context(): the cleanup below runs
	// after t.Context() is cancelled.
	ctx := context.Background()
	const dirs, filesPerDir, fileSize = 2, 2, 13
	backupRoot, keyspace, shard, ts := blackbox.SetupCluster(ctx, t, dirs, filesPerDir, fileSize)
	dir := keyspace + "/" + shard
	name := time.Now().Format(mysqlctl.BackupTimestampFormat)
	// Mirrors the plugin's alterBucketName: first segment of dir, lowercased,
	// underscores replaced.
	bucket := strings.ReplaceAll(strings.ToLower(keyspace), "_", "-")

	// The plugin creates the bucket; remove it when done so repeated runs
	// against the same gateway start clean. A bucket must be empty before it
	// can be deleted, so remove the backup first (a no-op if the test already
	// did). Both errors are ignored: if the test failed before the bucket was
	// created there is nothing to remove, and if RemoveBackup fails with a
	// backup present, DeleteBucket fails too and the next run's StartBackup
	// simply finds the bucket, so at most one backup is left behind.
	t.Cleanup(func() {
		_ = bs.RemoveBackup(ctx, dir, name)
		client, err := newS3Client(ctx, env)
		if err == nil {
			_, _ = client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
		}
	})

	be := &mysqlctl.BuiltinBackupEngine{}
	oldDeadline := blackbox.SetBuiltinBackupMysqldDeadline(time.Second)
	defer blackbox.SetBuiltinBackupMysqldDeadline(oldDeadline)
	logger := logutil.NewMemoryLogger()

	// Backup.
	bh, err := bs.StartBackup(ctx, dir, name)
	require.NoError(t, err)

	fakedb := fakesqldb.New(t)
	defer fakedb.Close()
	mysqld := mysqlctl.NewFakeMysqlDaemon(fakedb)
	defer mysqld.Close()
	mysqld.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "START REPLICA"}

	backupStats := backupstats.NewFakeStats()
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
		Stats:                backupStats,
		MysqlShutdownTimeout: blackbox.MysqlShutdownTimeout,
	}, bh)
	require.NoError(t, err)
	require.Equal(t, mysqlctl.BackupUsable, backupResult)

	// List: the backup is there.
	backups, err := bs.ListBackups(ctx, dir)
	require.NoError(t, err)
	require.Len(t, backups, 1)
	assert.Equal(t, name, backups[0].Name())
	assert.Equal(t, dir, backups[0].Directory())

	// Restore from it.
	restoreStats := backupstats.NewFakeStats()
	fakedb2 := fakesqldb.New(t)
	defer fakedb2.Close()
	mysqld2 := mysqlctl.NewFakeMysqlDaemon(fakedb2)
	defer mysqld2.Close()
	mysqld2.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "START REPLICA"}

	bm, err := be.ExecuteRestore(ctx, mysqlctl.RestoreParams{
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
		Mysqld:               mysqld2,
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
		Stats:                restoreStats,
		MysqlShutdownTimeout: blackbox.MysqlShutdownTimeout,
	}, backups[0])
	require.NoError(t, err)
	require.NotNil(t, bm)

	ss := blackbox.GetStats(restoreStats)
	assert.Equal(t, dirs*filesPerDir*fileSize, ss.DestinationWriteBytes)

	// Remove, and confirm the listing is empty again.
	require.NoError(t, bs.RemoveBackup(ctx, dir, name))
	backups, err = bs.ListBackups(ctx, dir)
	require.NoError(t, err)
	assert.Empty(t, backups)
}
