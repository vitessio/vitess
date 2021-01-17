// Package mysqlctl_test is the blackbox tests for package mysqlctl.
// Tests that need to use fakemysqldaemon must be written as blackbox tests;
// since fakemysqldaemon imports mysqlctl, importing fakemysqldaemon in
// a `package mysqlctl` test would cause a circular import.
package mysqlctl_test

import (
	"context"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/fakemysqldaemon"
	"vitess.io/vitess/go/vt/mysqlctl/filebackupstorage"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/faketmclient"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

func setBuiltinBackupMysqldDeadline(t time.Duration) time.Duration {
	old := *mysqlctl.BuiltinBackupMysqldTimeout
	mysqlctl.BuiltinBackupMysqldTimeout = &t

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

func TestExecuteBackup(t *testing.T) {
	// Set up local backup directory
	backupRoot := "testdata/builtinbackup_test"
	*filebackupstorage.FileBackupStorageRoot = backupRoot
	require.NoError(t, createBackupDir(backupRoot, "innodb", "log", "datadir"))
	defer os.RemoveAll(backupRoot)

	ctx := context.Background()

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

	_, err := ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
		si.MasterAlias = &topodata.TabletAlias{Uid: 100, Cell: "cell1"}

		now := time.Now()
		si.MasterTermStartTime = &vttime.Time{Seconds: int64(now.Second()), Nanoseconds: int32(now.Nanosecond())}

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

	bh := filebackupstorage.FileBackupHandle{}

	// Spin up a fake daemon to be used in backups. It needs to be allowed to receive:
	//  "STOP SLAVE", "START SLAVE", in that order.
	mysqld := fakemysqldaemon.NewFakeMysqlDaemon(fakesqldb.New(t))
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
	}, &bh)

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
	}, &bh)

	assert.Error(t, err)
	assert.False(t, ok)
}
