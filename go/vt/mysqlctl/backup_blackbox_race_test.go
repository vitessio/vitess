//go:build !race

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

// Package mysqlctl_test is the blackbox tests for package mysqlctl.
package mysqlctl_test

import (
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/backupstats"
	"vitess.io/vitess/go/vt/mysqlctl/filebackupstorage"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

// This test triggers a certain code path that only happens when a backup file fails to be backed up,
// only and only if, all the other backup files have either started or finished. When we reach
// this scenario, files no longer try to acquire the semaphore and thus the backup cannot fail
// because of context deadline when acquiring it. At this point, the only place where the backup
// can fail, is if the return of be.backupFiles fails, and we record the error correctly.
// This test specifically test this scenario and arose because of issue https://github.com/vitessio/vitess/issues/17063
// The test does:
//  1. Create the backup and data directory
//  2. Create a keyspace and shard
//  3. Already create the last backup file that would be created
//  4. Remove all permissions on this file
//  5. Execute the restore
//  6. The restore must fail due to an error on file number 3 ("cannot add file: 3")
//
// This test is extracted into its own file that won't be run if we do 'go test -race' as this test
// exposes an old race condition that will be fixed after https://github.com/vitessio/vitess/pull/17062
// Link to the race condition issue: https://github.com/vitessio/vitess/issues/17065
func TestExecuteBackupWithFailureOnLastFile(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

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

	needIt, err := needInnoDBRedoLogSubdir()
	require.NoError(t, err)
	if needIt {
		fpath := path.Join("log", mysql.DynamicRedoLogSubdir)
		if err := createBackupDir(backupRoot, fpath); err != nil {
			require.Failf(t, err.Error(), "failed to create directory: %s", fpath)
		}
	}

	// Set up topo
	keyspace, shard := "mykeyspace", "-"
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
	mysqld.ExpectedExecuteSuperQueryList = []string{"STOP SLAVE", "START SLAVE"}

	// With this setup, 4 backup files will be created (0, 1, 2, 3). For the last file (3), we create
	// it in advance and remove all permission on the file so that the backup be.ExecuteBackup will not
	// be able to override the file and thus will fail. Triggering the error mechanism after calling be.backupFile.
	lastBackupFile := path.Join(backupRoot, "3")
	f, err := os.Create(lastBackupFile)
	require.NoError(t, err)
	_, err = f.Write(make([]byte, 1024))
	require.NoError(t, err)
	require.NoError(t, f.Chmod(0444))
	require.NoError(t, f.Close())

	backupResult, err := be.ExecuteBackup(ctx, mysqlctl.BackupParams{
		Logger: logutil.NewConsoleLogger(),
		Mysqld: mysqld,
		Cnf: &mysqlctl.Mycnf{
			InnodbDataHomeDir:     path.Join(backupRoot, "innodb"),
			InnodbLogGroupHomeDir: path.Join(backupRoot, "log"),
			DataDir:               path.Join(backupRoot, "datadir"),
		},
		Stats:                backupstats.NewFakeStats(),
		Concurrency:          4,
		HookExtraEnv:         map[string]string{},
		TopoServer:           ts,
		Keyspace:             keyspace,
		Shard:                shard,
		MysqlShutdownTimeout: mysqlShutdownTimeout,
	}, bh)

	require.ErrorContains(t, err, "cannot add file: 3")
	require.Equal(t, mysqlctl.BackupUnusable, backupResult)
}
