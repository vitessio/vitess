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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/backupstats"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/mysqlctl/blackbox"
)

func TestBackupRestoreS3MicroCeph(t *testing.T) {
	cfg := SkipIfMicroCephUnavailable(t)
	if cfg == nil {
		return
	}
	setupMicroCephForTest(t, cfg, nil)

	ctx := context.Background()
	backupRoot, keyspace, shard, ts := blackbox.SetupCluster(ctx, t, 2, 2)
	backupDir := keyspace + "/" + shard

	storage, err := backupstorage.GetBackupStorage()
	require.NoError(t, err)
	defer storage.Close()

	fakeStats := backupstats.NewFakeStats()
	logger := logutil.NewMemoryLogger()
	bs := storage.WithParams(backupstorage.Params{Logger: logger, Stats: fakeStats})

	be := &mysqlctl.BuiltinBackupEngine{}
	backupName := t.Name() + "-" + strconv.Itoa(int(time.Now().Unix()))
	bh, err := bs.StartBackup(ctx, backupDir, backupName)
	require.NoError(t, err)
	backupEnded := false
	t.Cleanup(func() {
		if !backupEnded {
			_ = bh.AbortBackup(ctx)
		}
	})

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
	require.NoError(t, err)
	require.Equal(t, mysqlctl.BackupUsable, backupResult)
	backupEnded = true

	backups, err := bs.ListBackups(ctx, backupDir)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(backups), 1)
	var restoreBh backupstorage.BackupHandle
	for _, h := range backups {
		if h.Name() == backupName {
			restoreBh = h
			break
		}
	}
	require.NotNil(t, restoreBh, "backup %q not found in list", backupName)

	fakedb2 := fakesqldb.New(t)
	defer fakedb2.Close()
	mysqld2 := mysqlctl.NewFakeMysqlDaemon(fakedb2)
	defer mysqld2.Close()
	mysqld2.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "START REPLICA"}

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
		Mysqld:               mysqld2,
		Concurrency:          1,
		HookExtraEnv:         map[string]string{},
		DeleteBeforeRestore:  false,
		DbName:               "test",
		Keyspace:             keyspace,
		Shard:                shard,
		StartTime:            time.Now(),
		RestoreToPos:         replication.Position{},
		RestoreToTimestamp:   time.Time{},
		DryRun:               false,
		Stats:                fakeStats,
		MysqlShutdownTimeout: blackbox.MysqlShutdownTimeout,
	}

	bm, err := be.ExecuteRestore(ctx, restoreParams, restoreBh)
	require.NoError(t, err)
	require.NotNil(t, bm)

	ss := blackbox.GetStats(fakeStats)
	require.Greater(t, ss.DestinationCloseStats, 0)
	require.Greater(t, ss.SourceReadStats, 0)
}

// TestMicroCephInvalidAccessKey checks that we get an auth error when the access key is wrong (e.g. prod misconfig).
// Here we are: Vitess hits HeadBucket on first use and Ceph gives us 403 for bad keys.
func TestMicroCephInvalidAccessKey(t *testing.T) {
	cfg := SkipIfMicroCephUnavailable(t)
	if cfg == nil {
		return
	}
	setupMicroCephForTest(t, cfg, map[string]string{"AWS_ACCESS_KEY_ID": "wrong-access-key"})

	storage, err := backupstorage.GetBackupStorage()
	require.NoError(t, err)
	defer storage.Close()

	ctx := context.Background()
	_, err = storage.ListBackups(ctx, "ks/s0")
	require.Error(t, err)
	errStr := strings.ToLower(err.Error())
	require.True(t, strings.Contains(errStr, "forbidden") || strings.Contains(errStr, "403") || strings.Contains(errStr, "access denied"),
		"expected auth-related error, got: %v", err)
}

// TestMicroCephMissingBucket checks that we fail clearly when the bucket doesn't exist (typo in config, etc.).
// Same idea: first call does HeadBucket and we should get a bucket-not-found style error.
func TestMicroCephMissingBucket(t *testing.T) {
	cfg := SkipIfMicroCephUnavailable(t)
	if cfg == nil {
		return
	}
	setupMicroCephForTest(t, cfg, map[string]string{"AWS_BUCKET": "nonexistent-bucket-12345"})

	storage, err := backupstorage.GetBackupStorage()
	require.NoError(t, err)
	defer storage.Close()

	ctx := context.Background()
	_, err = storage.ListBackups(ctx, "ks/s0")
	require.Error(t, err)
	// This should look like "bucket not found", not a connection or auth error.
	errStr := strings.ToLower(err.Error())
	require.True(t, strings.Contains(errStr, "nosuchbucket") || strings.Contains(errStr, "404") || strings.Contains(errStr, "not found"),
		"expected bucket-not-found error, got: %v", err)
}
