/*
Copyright 2019 The Vitess Authors.

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

package testlib

import (
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/discovery"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/mysqlctl/filebackupstorage"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestBackupRestore(t *testing.T) {
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

	// Initialize our environment
	ctx := context.Background()
	db := fakesqldb.New(t)
	defer db.Close()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Set up mock query results.
	db.AddQuery("CREATE DATABASE IF NOT EXISTS _vt", &sqltypes.Result{})
	db.AddQuery("BEGIN", &sqltypes.Result{})
	db.AddQuery("COMMIT", &sqltypes.Result{})
	db.AddQueryPattern(`SET @@session\.sql_log_bin = .*`, &sqltypes.Result{})
	db.AddQueryPattern(`CREATE TABLE IF NOT EXISTS _vt\.shard_metadata .*`, &sqltypes.Result{})
	db.AddQueryPattern(`CREATE TABLE IF NOT EXISTS _vt\.local_metadata .*`, &sqltypes.Result{})
	db.AddQueryPattern(`ALTER TABLE _vt\.local_metadata .*`, &sqltypes.Result{})
	db.AddQueryPattern(`ALTER TABLE _vt\.shard_metadata .*`, &sqltypes.Result{})
	db.AddQueryPattern(`UPDATE _vt\.local_metadata SET db_name=.*`, &sqltypes.Result{})
	db.AddQueryPattern(`UPDATE _vt\.shard_metadata SET db_name=.*`, &sqltypes.Result{})
	db.AddQueryPattern(`INSERT INTO _vt\.local_metadata .*`, &sqltypes.Result{})

	// Initialize our temp dirs
	root, err := ioutil.TempDir("", "backuptest")
	require.NoError(t, err)
	defer os.RemoveAll(root)

	// Initialize BackupStorage
	fbsRoot := path.Join(root, "fbs")
	*filebackupstorage.FileBackupStorageRoot = fbsRoot
	*backupstorage.BackupStorageImplementation = "file"

	// Initialize the fake mysql root directories
	sourceInnodbDataDir := path.Join(root, "source_innodb_data")
	sourceInnodbLogDir := path.Join(root, "source_innodb_log")
	sourceDataDir := path.Join(root, "source_data")
	sourceDataDbDir := path.Join(sourceDataDir, "vt_db")
	for _, s := range []string{sourceInnodbDataDir, sourceInnodbLogDir, sourceDataDbDir} {
		require.NoError(t, os.MkdirAll(s, os.ModePerm))
	}
	require.NoError(t, ioutil.WriteFile(path.Join(sourceInnodbDataDir, "innodb_data_1"), []byte("innodb data 1 contents"), os.ModePerm))
	require.NoError(t, ioutil.WriteFile(path.Join(sourceInnodbLogDir, "innodb_log_1"), []byte("innodb log 1 contents"), os.ModePerm))
	require.NoError(t, ioutil.WriteFile(path.Join(sourceDataDbDir, "db.opt"), []byte("db opt file"), os.ModePerm))

	// create a master tablet, set its master position
	master := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, db)
	master.FakeMysqlDaemon.ReadOnly = false
	master.FakeMysqlDaemon.Replicating = false
	master.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 457,
			},
		},
	}

	// start master so that replica can fetch master position from it
	master.StartActionLoop(t, wr)
	defer master.StopActionLoop(t)

	// create a single tablet, set it up so we can do backups
	// set its position same as that of master so that backup doesn't wait for catchup
	sourceTablet := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, db)
	sourceTablet.FakeMysqlDaemon.ReadOnly = true
	sourceTablet.FakeMysqlDaemon.Replicating = true
	sourceTablet.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 457,
			},
		},
	}
	sourceTablet.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"START SLAVE",
	}
	sourceTablet.StartActionLoop(t, wr)
	defer sourceTablet.StopActionLoop(t)

	sourceTablet.TM.Cnf = &mysqlctl.Mycnf{
		DataDir:               sourceDataDir,
		InnodbDataHomeDir:     sourceInnodbDataDir,
		InnodbLogGroupHomeDir: sourceInnodbLogDir,
	}

	// run the backup
	require.NoError(t, vp.Run([]string{"Backup", topoproto.TabletAliasString(sourceTablet.Tablet.Alias)}))

	// verify the full status
	require.NoError(t, sourceTablet.FakeMysqlDaemon.CheckSuperQueryList())
	assert.True(t, sourceTablet.FakeMysqlDaemon.Replicating)
	assert.True(t, sourceTablet.FakeMysqlDaemon.Running)

	// create a destination tablet, set it up so we can do restores
	destTablet := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, db)
	destTablet.FakeMysqlDaemon.ReadOnly = true
	destTablet.FakeMysqlDaemon.Replicating = true
	destTablet.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 457,
			},
		},
	}
	destTablet.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET SLAVE POSITION",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	destTablet.FakeMysqlDaemon.FetchSuperQueryMap = map[string]*sqltypes.Result{
		"SHOW DATABASES": {},
	}
	destTablet.FakeMysqlDaemon.SetReplicationPositionPos = sourceTablet.FakeMysqlDaemon.CurrentMasterPosition
	destTablet.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(master.Tablet)

	destTablet.StartActionLoop(t, wr)
	defer destTablet.StopActionLoop(t)

	destTablet.TM.Cnf = &mysqlctl.Mycnf{
		DataDir:               sourceDataDir,
		InnodbDataHomeDir:     sourceInnodbDataDir,
		InnodbLogGroupHomeDir: sourceInnodbLogDir,
		BinLogPath:            path.Join(root, "bin-logs/filename_prefix"),
		RelayLogPath:          path.Join(root, "relay-logs/filename_prefix"),
		RelayLogIndexPath:     path.Join(root, "relay-log.index"),
		RelayLogInfoPath:      path.Join(root, "relay-log.info"),
	}

	require.NoError(t, destTablet.TM.RestoreData(ctx, logutil.NewConsoleLogger(), 0 /* waitForBackupInterval */, false /* deleteBeforeRestore */))
	// verify the full status
	require.NoError(t, destTablet.FakeMysqlDaemon.CheckSuperQueryList(), "destTablet.FakeMysqlDaemon.CheckSuperQueryList failed")
	assert.True(t, destTablet.FakeMysqlDaemon.Replicating)
	assert.True(t, destTablet.FakeMysqlDaemon.Running)

	// Initialize mycnf, required for restore
	masterInnodbDataDir := path.Join(root, "master_innodb_data")
	masterInnodbLogDir := path.Join(root, "master_innodb_log")
	masterDataDir := path.Join(root, "master_data")
	master.TM.Cnf = &mysqlctl.Mycnf{
		DataDir:               masterDataDir,
		InnodbDataHomeDir:     masterInnodbDataDir,
		InnodbLogGroupHomeDir: masterInnodbLogDir,
		BinLogPath:            path.Join(root, "bin-logs/filename_prefix"),
		RelayLogPath:          path.Join(root, "relay-logs/filename_prefix"),
		RelayLogIndexPath:     path.Join(root, "relay-log.index"),
		RelayLogInfoPath:      path.Join(root, "relay-log.info"),
	}

	master.FakeMysqlDaemon.FetchSuperQueryMap = map[string]*sqltypes.Result{
		"SHOW DATABASES": {},
	}
	master.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET SLAVE POSITION",
		"FAKE SET MASTER",
		"START SLAVE",
	}

	master.FakeMysqlDaemon.SetReplicationPositionPos = master.FakeMysqlDaemon.CurrentMasterPosition

	// restore master from backup
	require.NoError(t, master.TM.RestoreData(ctx, logutil.NewConsoleLogger(), 0 /* waitForBackupInterval */, false /* deleteBeforeRestore */), "RestoreData failed")
	// tablet was created as MASTER, so it's baseTabletType is MASTER
	assert.Equal(t, topodatapb.TabletType_MASTER, master.Tablet.Type)
	assert.False(t, master.FakeMysqlDaemon.Replicating)
	assert.True(t, master.FakeMysqlDaemon.Running)

	// restore master when database already exists
	// checkNoDb should return false
	// so fake the necessary queries
	master.FakeMysqlDaemon.FetchSuperQueryMap = map[string]*sqltypes.Result{
		"SHOW DATABASES":                      {Rows: [][]sqltypes.Value{{sqltypes.NewVarBinary("vt_test_keyspace")}}},
		"SHOW TABLES FROM `vt_test_keyspace`": {Rows: [][]sqltypes.Value{{sqltypes.NewVarBinary("a")}}},
	}

	require.NoError(t, master.TM.RestoreData(ctx, logutil.NewConsoleLogger(), 0 /* waitForBackupInterval */, false /* deleteBeforeRestore */), "RestoreData failed")
	// Tablet type should not change
	assert.Equal(t, topodatapb.TabletType_MASTER, master.Tablet.Type)
	assert.False(t, master.FakeMysqlDaemon.Replicating)
	assert.True(t, master.FakeMysqlDaemon.Running)
}

func TestRestoreUnreachableMaster(t *testing.T) {
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

	// Initialize our environment
	ctx := context.Background()
	db := fakesqldb.New(t)
	defer db.Close()
	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Set up mock query results.
	db.AddQuery("CREATE DATABASE IF NOT EXISTS _vt", &sqltypes.Result{})
	db.AddQuery("BEGIN", &sqltypes.Result{})
	db.AddQuery("COMMIT", &sqltypes.Result{})
	db.AddQueryPattern(`SET @@session\.sql_log_bin = .*`, &sqltypes.Result{})
	db.AddQueryPattern(`CREATE TABLE IF NOT EXISTS _vt\.shard_metadata .*`, &sqltypes.Result{})
	db.AddQueryPattern(`CREATE TABLE IF NOT EXISTS _vt\.local_metadata .*`, &sqltypes.Result{})
	db.AddQueryPattern(`ALTER TABLE _vt\.local_metadata .*`, &sqltypes.Result{})
	db.AddQueryPattern(`ALTER TABLE _vt\.shard_metadata .*`, &sqltypes.Result{})
	db.AddQueryPattern(`UPDATE _vt\.local_metadata SET db_name=.*`, &sqltypes.Result{})
	db.AddQueryPattern(`UPDATE _vt\.shard_metadata SET db_name=.*`, &sqltypes.Result{})
	db.AddQueryPattern(`INSERT INTO _vt\.local_metadata .*`, &sqltypes.Result{})

	// Initialize our temp dirs
	root, err := ioutil.TempDir("", "backuptest")
	require.NoError(t, err)
	defer os.RemoveAll(root)

	// Initialize BackupStorage
	fbsRoot := path.Join(root, "fbs")
	*filebackupstorage.FileBackupStorageRoot = fbsRoot
	*backupstorage.BackupStorageImplementation = "file"

	// Initialize the fake mysql root directories
	sourceInnodbDataDir := path.Join(root, "source_innodb_data")
	sourceInnodbLogDir := path.Join(root, "source_innodb_log")
	sourceDataDir := path.Join(root, "source_data")
	sourceDataDbDir := path.Join(sourceDataDir, "vt_db")
	for _, s := range []string{sourceInnodbDataDir, sourceInnodbLogDir, sourceDataDbDir} {
		require.NoError(t, os.MkdirAll(s, os.ModePerm))
	}
	require.NoError(t, ioutil.WriteFile(path.Join(sourceInnodbDataDir, "innodb_data_1"), []byte("innodb data 1 contents"), os.ModePerm))
	require.NoError(t, ioutil.WriteFile(path.Join(sourceInnodbLogDir, "innodb_log_1"), []byte("innodb log 1 contents"), os.ModePerm))
	require.NoError(t, ioutil.WriteFile(path.Join(sourceDataDbDir, "db.opt"), []byte("db opt file"), os.ModePerm))

	// create a master tablet, set its master position
	master := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, db)
	master.FakeMysqlDaemon.ReadOnly = false
	master.FakeMysqlDaemon.Replicating = false
	master.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 457,
			},
		},
	}

	// start master so that replica can fetch master position from it
	master.StartActionLoop(t, wr)

	// create a single tablet, set it up so we can do backups
	// set its position same as that of master so that backup doesn't wait for catchup
	sourceTablet := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, db)
	sourceTablet.FakeMysqlDaemon.ReadOnly = true
	sourceTablet.FakeMysqlDaemon.Replicating = true
	sourceTablet.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 457,
			},
		},
	}
	sourceTablet.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"START SLAVE",
	}
	sourceTablet.StartActionLoop(t, wr)
	defer sourceTablet.StopActionLoop(t)

	sourceTablet.TM.Cnf = &mysqlctl.Mycnf{
		DataDir:               sourceDataDir,
		InnodbDataHomeDir:     sourceInnodbDataDir,
		InnodbLogGroupHomeDir: sourceInnodbLogDir,
	}

	// run the backup
	require.NoError(t, vp.Run([]string{"Backup", topoproto.TabletAliasString(sourceTablet.Tablet.Alias)}))

	// create a destination tablet, set it up so we can do restores
	destTablet := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, db)
	destTablet.FakeMysqlDaemon.ReadOnly = true
	destTablet.FakeMysqlDaemon.Replicating = true
	destTablet.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 457,
			},
		},
	}
	destTablet.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET SLAVE POSITION",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	destTablet.FakeMysqlDaemon.FetchSuperQueryMap = map[string]*sqltypes.Result{
		"SHOW DATABASES": {},
	}
	destTablet.FakeMysqlDaemon.SetReplicationPositionPos = sourceTablet.FakeMysqlDaemon.CurrentMasterPosition
	destTablet.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(master.Tablet)

	destTablet.StartActionLoop(t, wr)
	defer destTablet.StopActionLoop(t)

	destTablet.TM.Cnf = &mysqlctl.Mycnf{
		DataDir:               sourceDataDir,
		InnodbDataHomeDir:     sourceInnodbDataDir,
		InnodbLogGroupHomeDir: sourceInnodbLogDir,
		BinLogPath:            path.Join(root, "bin-logs/filename_prefix"),
		RelayLogPath:          path.Join(root, "relay-logs/filename_prefix"),
		RelayLogIndexPath:     path.Join(root, "relay-log.index"),
		RelayLogInfoPath:      path.Join(root, "relay-log.info"),
	}

	// stop master so that it is unreachable
	master.StopActionLoop(t)

	// set a short timeout so that we don't have to wait 30 seconds
	*topo.RemoteOperationTimeout = 2 * time.Second
	// Restore should still succeed
	require.NoError(t, destTablet.TM.RestoreData(ctx, logutil.NewConsoleLogger(), 0 /* waitForBackupInterval */, false /* deleteBeforeRestore */))
	// verify the full status
	require.NoError(t, destTablet.FakeMysqlDaemon.CheckSuperQueryList(), "destTablet.FakeMysqlDaemon.CheckSuperQueryList failed")
	assert.True(t, destTablet.FakeMysqlDaemon.Replicating)
	assert.True(t, destTablet.FakeMysqlDaemon.Running)
}
