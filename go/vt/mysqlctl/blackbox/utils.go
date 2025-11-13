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

package blackbox

import (
	"context"
	"fmt"
	"os"
	"path"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/os2"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/backupstats"
	"vitess.io/vitess/go/vt/mysqlctl/filebackupstorage"
	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

type StatSummary struct {
	DestinationCloseStats int
	DestinationOpenStats  int
	DestinationWriteStats int
	SourceCloseStats      int
	SourceOpenStats       int
	SourceReadStats       int
}

func GetStats(stats *backupstats.FakeStats) StatSummary {
	var ss StatSummary

	for _, sr := range stats.ScopeReturns {
		switch sr.ScopeV[backupstats.ScopeOperation] {
		case "Destination:Close":
			ss.DestinationCloseStats += len(sr.TimedIncrementCalls)
		case "Destination:Open":
			ss.DestinationOpenStats += len(sr.TimedIncrementCalls)
		case "Destination:Write":
			if len(sr.TimedIncrementBytesCalls) > 0 {
				ss.DestinationWriteStats++
			}
		case "Source:Close":
			ss.SourceCloseStats += len(sr.TimedIncrementCalls)
		case "Source:Open":
			ss.SourceOpenStats += len(sr.TimedIncrementCalls)
		case "Source:Read":
			if len(sr.TimedIncrementBytesCalls) > 0 {
				ss.SourceReadStats++
			}
		}
	}
	return ss
}

func AssertLogs(t *testing.T, expectedLogs []string, logger *logutil.MemoryLogger) {
	for _, log := range expectedLogs {
		require.Truef(t, slices.ContainsFunc(logger.LogEvents(), func(event *logutilpb.Event) bool {
			return event.GetValue() == log
		}), "%s is missing from the logs", log)
	}
}

func SetupCluster(ctx context.Context, t *testing.T, dirs, filesPerDir int) (backupRoot string, keyspace string, shard string, ts *topo.Server) {
	// Set up local backup directory
	id := strconv.FormatInt(time.Now().UnixNano(), 10)
	backupRoot = "testdata/builtinbackup_test_" + id
	filebackupstorage.FileBackupStorageRoot = backupRoot
	require.NoError(t, createBackupDir(backupRoot, "innodb", "log", "datadir"))
	dataDir := path.Join(backupRoot, "datadir")
	// Add some files under data directory to force backup to execute semaphore acquire inside
	// backupFiles() method (https://github.com/vitessio/vitess/blob/main/go/vt/mysqlctl/builtinbackupengine.go#L483).
	for dirI := range dirs {
		dirName := "test" + strconv.Itoa(dirI+1)
		require.NoError(t, createBackupDir(dataDir, dirName))
		require.NoError(t, createBackupFiles(path.Join(dataDir, dirName), filesPerDir, "ibd"))
	}
	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(backupRoot))
	})

	needIt, err := NeedInnoDBRedoLogSubdir()
	require.NoError(t, err)
	if needIt {
		fpath := path.Join("log", mysql.DynamicRedoLogSubdir)
		if err := createBackupDir(backupRoot, fpath); err != nil {
			require.Failf(t, err.Error(), "failed to create directory: %s", fpath)
		}
	}

	// Set up topo
	keyspace, shard = "mykeyspace", "-"
	ts = memorytopo.NewServer(ctx, "cell1")
	t.Cleanup(func() {
		ts.Close()
	})

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
	return backupRoot, keyspace, shard, ts
}

// NeedInnoDBRedoLogSubdir indicates whether we need to create a redo log subdirectory.
// Starting with MySQL 8.0.30, the InnoDB redo logs are stored in a subdirectory of the
// <innodb_log_group_home_dir> (<datadir>/. by default) called "#innodb_redo". See:
//
//	https://dev.mysql.com/doc/refman/8.0/en/innodb-redo-log.html#innodb-modifying-redo-log-capacity
func NeedInnoDBRedoLogSubdir() (needIt bool, err error) {
	mysqldVersionStr, err := mysqlctl.GetVersionString()
	if err != nil {
		return needIt, err
	}
	_, sv, err := mysqlctl.ParseVersionString(mysqldVersionStr)
	if err != nil {
		return needIt, err
	}
	versionStr := fmt.Sprintf("%d.%d.%d", sv.Major, sv.Minor, sv.Patch)
	capableOf := mysql.ServerVersionCapableOf(versionStr)
	if capableOf == nil {
		return needIt, fmt.Errorf("cannot determine database flavor details for version %s", versionStr)
	}
	return capableOf(capabilities.DynamicRedoLogCapacityFlavorCapability)
}

const MysqlShutdownTimeout = 1 * time.Minute

func SetBuiltinBackupMysqldDeadline(t time.Duration) time.Duration {
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
		f, err := os2.Create(path.Join(root, fmt.Sprintf("%d.%s", i, ext)))
		if err != nil {
			return err
		}
		if _, err := f.WriteString("hello, world!"); err != nil {
			return err
		}
		defer f.Close()
	}

	return nil
}
