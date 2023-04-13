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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	"vitess.io/vitess/go/vt/vttablet/faketmclient"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
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
		defer f.Close()
	}

	return nil
}

func TestExecuteBackup(t *testing.T) {
	// Set up local backup directory
	backupRoot := "testdata/builtinbackup_test"
	filebackupstorage.FileBackupStorageRoot = backupRoot
	require.NoError(t, createBackupDir(backupRoot, "innodb", "log", "datadir"))
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

	// Set up tm client
	// Note that using faketmclient.NewFakeTabletManagerClient will cause infinite recursion :shrug:
	tmclient.RegisterTabletManagerClientFactory("grpc",
		func() tmclient.TabletManagerClient { return &faketmclient.FakeTabletManagerClient{} },
	)

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

	ok, err := be.ExecuteBackup(ctx, mysqlctl.BackupParams{
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

	require.NoError(t, err)
	assert.True(t, ok)

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

// TestExecuteBackupWithCancelledContext test the ability of backup to gracefully
// handle cases where we encounter error due to any reasons for e.g context cancel etc.
// Process should not panic in these situations.
func TestExecuteBackupWithCancelledContext(t *testing.T) {
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

	// cancel the context deliberately
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

	// Set up tm client.
	// Note that using faketmclient.NewFakeTabletManagerClient will cause infinite recursion :shrug:
	tmclient.RegisterTabletManagerClientFactory("grpc2",
		func() tmclient.TabletManagerClient { return &faketmclient.FakeTabletManagerClient{} },
	)

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
