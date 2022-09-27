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
	"context"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/discovery"

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

type compressionDetails struct {
	CompressionEngineName   string
	ExternalCompressorCmd   string
	ExternalCompressorExt   string
	ExternalDecompressorCmd string
}

func TestBackupRestore(t *testing.T) {
	defer setDefaultCompressionFlag()
	err := testBackupRestore(t, nil)
	require.NoError(t, err)
}

func TestBackupRestoreWithPargzip(t *testing.T) {
	defer setDefaultCompressionFlag()
	cDetails := &compressionDetails{
		CompressionEngineName: "pargzip",
	}

	err := testBackupRestore(t, cDetails)
	require.NoError(t, err)
}

func setDefaultCompressionFlag() {
	mysqlctl.CompressionEngineName = "pgzip"
	mysqlctl.ExternalCompressorCmd = ""
	mysqlctl.ExternalCompressorExt = ""
	mysqlctl.ExternalDecompressorCmd = ""
}

func testBackupRestore(t *testing.T, cDetails *compressionDetails) error {
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
	root := t.TempDir()

	// Initialize BackupStorage
	fbsRoot := path.Join(root, "fbs")
	filebackupstorage.FileBackupStorageRoot = fbsRoot
	backupstorage.BackupStorageImplementation = "file"
	if cDetails != nil {
		if cDetails.CompressionEngineName != "" {
			mysqlctl.CompressionEngineName = cDetails.CompressionEngineName
		}
		if cDetails.ExternalCompressorCmd != "" {
			mysqlctl.ExternalCompressorCmd = cDetails.ExternalCompressorCmd
		}
		if cDetails.ExternalCompressorExt != "" {
			mysqlctl.ExternalCompressorExt = cDetails.ExternalCompressorExt
		}
		if cDetails.ExternalDecompressorCmd != "" {
			mysqlctl.ExternalDecompressorCmd = cDetails.ExternalDecompressorCmd
		}
	}

	// Initialize the fake mysql root directories
	sourceInnodbDataDir := path.Join(root, "source_innodb_data")
	sourceInnodbLogDir := path.Join(root, "source_innodb_log")
	sourceDataDir := path.Join(root, "source_data")
	sourceDataDbDir := path.Join(sourceDataDir, "vt_db")
	for _, s := range []string{sourceInnodbDataDir, sourceInnodbLogDir, sourceDataDbDir} {
		require.NoError(t, os.MkdirAll(s, os.ModePerm))
	}

	needIt, err := needInnoDBRedoLogSubdir()
	require.NoError(t, err)
	if needIt {
		newPath := path.Join(sourceInnodbLogDir, mysql.DynamicRedoLogSubdir)
		require.NoError(t, os.Mkdir(newPath, os.ModePerm))
		require.NoError(t, os.WriteFile(path.Join(newPath, "#ib_redo1"), []byte("innodb log 1 contents"), os.ModePerm))
	} else {
		require.NoError(t, os.WriteFile(path.Join(sourceInnodbLogDir, "innodb_log_1"), []byte("innodb log 1 contents"), os.ModePerm))
	}

	require.NoError(t, os.WriteFile(path.Join(sourceInnodbDataDir, "innodb_data_1"), []byte("innodb data 1 contents"), os.ModePerm))
	require.NoError(t, os.WriteFile(path.Join(sourceDataDbDir, "db.opt"), []byte("db opt file"), os.ModePerm))

	// create a primary tablet, set its primary position
	primary := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_PRIMARY, db)
	primary.FakeMysqlDaemon.ReadOnly = false
	primary.FakeMysqlDaemon.Replicating = false
	primary.FakeMysqlDaemon.CurrentPrimaryPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 457,
			},
		},
	}

	// start primary so that replica can fetch primary position from it
	primary.StartActionLoop(t, wr)
	defer primary.StopActionLoop(t)

	// create a single tablet, set it up so we can do backups
	// set its position same as that of primary so that backup doesn't wait for catchup
	sourceTablet := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, db)
	sourceTablet.FakeMysqlDaemon.ReadOnly = true
	sourceTablet.FakeMysqlDaemon.Replicating = true
	sourceTablet.FakeMysqlDaemon.SetReplicationSourceInputs = []string{fmt.Sprintf("%s:%d", primary.Tablet.MysqlHostname, primary.Tablet.MysqlPort)}
	sourceTablet.FakeMysqlDaemon.CurrentPrimaryPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 457,
			},
		},
	}
	sourceTablet.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		// This first set of STOP and START commands come from
		// the builtinBackupEngine implementation which stops the replication
		// while taking the backup
		"STOP SLAVE",
		"START SLAVE",
		// These commands come from SetReplicationSource RPC called
		// to set the correct primary and semi-sync after Backup has concluded
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
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

	backupTime := time.Now()

	// create a destination tablet, set it up so we can do restores
	destTablet := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, db)
	destTablet.FakeMysqlDaemon.ReadOnly = true
	destTablet.FakeMysqlDaemon.Replicating = true
	destTablet.FakeMysqlDaemon.CurrentPrimaryPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 457,
			},
		},
	}
	destTablet.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET SLAVE POSITION",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	destTablet.FakeMysqlDaemon.FetchSuperQueryMap = map[string]*sqltypes.Result{
		"SHOW DATABASES": {},
	}
	destTablet.FakeMysqlDaemon.SetReplicationPositionPos = sourceTablet.FakeMysqlDaemon.CurrentPrimaryPosition
	destTablet.FakeMysqlDaemon.SetReplicationSourceInputs = append(destTablet.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(primary.Tablet))

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

	err = destTablet.TM.RestoreData(ctx, logutil.NewConsoleLogger(), 0 /* waitForBackupInterval */, false /* deleteBeforeRestore */, time.Time{} /* backupTime */)
	if err != nil {
		return err
	}
	// verify the full status
	require.NoError(t, destTablet.FakeMysqlDaemon.CheckSuperQueryList(), "destTablet.FakeMysqlDaemon.CheckSuperQueryList failed")
	assert.True(t, destTablet.FakeMysqlDaemon.Replicating)
	assert.True(t, destTablet.FakeMysqlDaemon.Running)

	// Initialize mycnf, required for restore
	primaryInnodbDataDir := path.Join(root, "primary_innodb_data")
	primaryInnodbLogDir := path.Join(root, "primary_innodb_log")
	primaryDataDir := path.Join(root, "primary_data")
	primary.TM.Cnf = &mysqlctl.Mycnf{
		DataDir:               primaryDataDir,
		InnodbDataHomeDir:     primaryInnodbDataDir,
		InnodbLogGroupHomeDir: primaryInnodbLogDir,
		BinLogPath:            path.Join(root, "bin-logs/filename_prefix"),
		RelayLogPath:          path.Join(root, "relay-logs/filename_prefix"),
		RelayLogIndexPath:     path.Join(root, "relay-log.index"),
		RelayLogInfoPath:      path.Join(root, "relay-log.info"),
	}

	primary.FakeMysqlDaemon.FetchSuperQueryMap = map[string]*sqltypes.Result{
		"SHOW DATABASES": {},
	}
	primary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET SLAVE POSITION",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
	}

	primary.FakeMysqlDaemon.SetReplicationPositionPos = primary.FakeMysqlDaemon.CurrentPrimaryPosition

	// restore primary from latest backup
	require.NoError(t, primary.TM.RestoreData(ctx, logutil.NewConsoleLogger(), 0 /* waitForBackupInterval */, false /* deleteBeforeRestore */, time.Time{} /* restoreFromBackupTs */),
		"RestoreData failed")
	// tablet was created as PRIMARY, so it's baseTabletType is PRIMARY
	assert.Equal(t, topodatapb.TabletType_PRIMARY, primary.Tablet.Type)
	assert.False(t, primary.FakeMysqlDaemon.Replicating)
	assert.True(t, primary.FakeMysqlDaemon.Running)

	// restore primary when database already exists
	// checkNoDb should return false
	// so fake the necessary queries
	primary.FakeMysqlDaemon.FetchSuperQueryMap = map[string]*sqltypes.Result{
		"SHOW DATABASES":                      {Rows: [][]sqltypes.Value{{sqltypes.NewVarBinary("vt_test_keyspace")}}},
		"SHOW TABLES FROM `vt_test_keyspace`": {Rows: [][]sqltypes.Value{{sqltypes.NewVarBinary("a")}}},
	}

	// Test restore with the backup timestamp
	require.NoError(t, primary.TM.RestoreData(ctx, logutil.NewConsoleLogger(), 0 /* waitForBackupInterval */, false /* deleteBeforeRestore */, backupTime),
		"RestoreData with backup timestamp failed")
	assert.Equal(t, topodatapb.TabletType_PRIMARY, primary.Tablet.Type)
	assert.False(t, primary.FakeMysqlDaemon.Replicating)
	assert.True(t, primary.FakeMysqlDaemon.Running)
	return nil
}

// TestBackupRestoreLagged tests the changes made in https://github.com/vitessio/vitess/pull/5000
// While doing a backup or a restore, we wait for a change of the replica's position before completing the action
// This is because otherwise ReplicationLagSeconds is not accurate and the tablet may go into SERVING when it should not
func TestBackupRestoreLagged(t *testing.T) {
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
	root := t.TempDir()

	// Initialize BackupStorage
	fbsRoot := path.Join(root, "fbs")
	filebackupstorage.FileBackupStorageRoot = fbsRoot
	backupstorage.BackupStorageImplementation = "file"

	// Initialize the fake mysql root directories
	sourceInnodbDataDir := path.Join(root, "source_innodb_data")
	sourceInnodbLogDir := path.Join(root, "source_innodb_log")
	sourceDataDir := path.Join(root, "source_data")
	sourceDataDbDir := path.Join(sourceDataDir, "vt_db")
	for _, s := range []string{sourceInnodbDataDir, sourceInnodbLogDir, sourceDataDbDir} {
		require.NoError(t, os.MkdirAll(s, os.ModePerm))
	}
	require.NoError(t, os.WriteFile(path.Join(sourceInnodbDataDir, "innodb_data_1"), []byte("innodb data 1 contents"), os.ModePerm))

	needIt, err := needInnoDBRedoLogSubdir()
	require.NoError(t, err)
	if needIt {
		newPath := path.Join(sourceInnodbLogDir, mysql.DynamicRedoLogSubdir)
		require.NoError(t, os.Mkdir(newPath, os.ModePerm))
		require.NoError(t, os.WriteFile(path.Join(newPath, "#ib_redo1"), []byte("innodb log 1 contents"), os.ModePerm))
	} else {
		require.NoError(t, os.WriteFile(path.Join(sourceInnodbLogDir, "innodb_log_1"), []byte("innodb log 1 contents"), os.ModePerm))
	}

	require.NoError(t, os.WriteFile(path.Join(sourceDataDbDir, "db.opt"), []byte("db opt file"), os.ModePerm))

	// create a primary tablet, set its position
	primary := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_PRIMARY, db)
	primary.FakeMysqlDaemon.ReadOnly = false
	primary.FakeMysqlDaemon.Replicating = false
	primary.FakeMysqlDaemon.CurrentPrimaryPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 457,
			},
		},
	}

	// start primary so that replica can fetch primary position from it
	primary.StartActionLoop(t, wr)
	defer primary.StopActionLoop(t)

	// create a single tablet, set it up so we can do backups
	// set its position same as that of primary so that backup doesn't wait for catchup
	sourceTablet := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, db)
	sourceTablet.FakeMysqlDaemon.ReadOnly = true
	sourceTablet.FakeMysqlDaemon.Replicating = true
	sourceTablet.FakeMysqlDaemon.CurrentPrimaryPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 456,
			},
		},
	}
	sourceTablet.FakeMysqlDaemon.SetReplicationSourceInputs = []string{fmt.Sprintf("%s:%d", primary.Tablet.MysqlHostname, primary.Tablet.MysqlPort)}
	sourceTablet.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		// This first set of STOP and START commands come from
		// the builtinBackupEngine implementation which stops the replication
		// while taking the backup
		"STOP SLAVE",
		"START SLAVE",
		// These commands come from SetReplicationSource RPC called
		// to set the correct primary and semi-sync after Backup has concluded
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	sourceTablet.StartActionLoop(t, wr)
	defer sourceTablet.StopActionLoop(t)

	sourceTablet.TM.Cnf = &mysqlctl.Mycnf{
		DataDir:               sourceDataDir,
		InnodbDataHomeDir:     sourceInnodbDataDir,
		InnodbLogGroupHomeDir: sourceInnodbLogDir,
	}

	errCh := make(chan error, 1)
	go func(ctx context.Context, tablet *FakeTablet) {
		errCh <- vp.Run([]string{"Backup", topoproto.TabletAliasString(tablet.Tablet.Alias)})
	}(ctx, sourceTablet)

	timer := time.NewTicker(1 * time.Second)
	<-timer.C
	sourceTablet.FakeMysqlDaemon.CurrentPrimaryPositionLocked(mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 457,
			},
		},
	})

	timer2 := time.NewTicker(5 * time.Second)
	select {
	case err := <-errCh:
		require.Nil(t, err)
		// verify the full status
		// verify the full status
		require.NoError(t, sourceTablet.FakeMysqlDaemon.CheckSuperQueryList())
		assert.True(t, sourceTablet.FakeMysqlDaemon.Replicating)
		assert.True(t, sourceTablet.FakeMysqlDaemon.Running)
		assert.Equal(t, primary.FakeMysqlDaemon.CurrentPrimaryPosition, sourceTablet.FakeMysqlDaemon.CurrentPrimaryPosition)
	case <-timer2.C:
		require.FailNow(t, "Backup timed out")
	}

	// create a destination tablet, set it up so we can do restores
	destTablet := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, db)
	destTablet.FakeMysqlDaemon.ReadOnly = true
	destTablet.FakeMysqlDaemon.Replicating = true
	destTablet.FakeMysqlDaemon.CurrentPrimaryPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 456,
			},
		},
	}
	destTablet.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET SLAVE POSITION",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	destTablet.FakeMysqlDaemon.FetchSuperQueryMap = map[string]*sqltypes.Result{
		"SHOW DATABASES": {},
	}
	destTablet.FakeMysqlDaemon.SetReplicationPositionPos = destTablet.FakeMysqlDaemon.CurrentPrimaryPosition
	destTablet.FakeMysqlDaemon.SetReplicationSourceInputs = append(destTablet.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(primary.Tablet))

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

	errCh = make(chan error, 1)
	go func(ctx context.Context, tablet *FakeTablet) {
		errCh <- tablet.TM.RestoreData(ctx, logutil.NewConsoleLogger(), 0 /* waitForBackupInterval */, false /* deleteBeforeRestore */, time.Time{} /* restoreFromBackupTs */)
	}(ctx, destTablet)

	timer = time.NewTicker(1 * time.Second)
	<-timer.C
	destTablet.FakeMysqlDaemon.CurrentPrimaryPositionLocked(mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 457,
			},
		},
	})

	timer2 = time.NewTicker(5 * time.Second)
	select {
	case err := <-errCh:
		require.Nil(t, err)
		// verify the full status
		require.NoError(t, destTablet.FakeMysqlDaemon.CheckSuperQueryList(), "destTablet.FakeMysqlDaemon.CheckSuperQueryList failed")
		assert.True(t, destTablet.FakeMysqlDaemon.Replicating)
		assert.True(t, destTablet.FakeMysqlDaemon.Running)
		assert.Equal(t, primary.FakeMysqlDaemon.CurrentPrimaryPosition, destTablet.FakeMysqlDaemon.CurrentPrimaryPosition)
	case <-timer2.C:
		require.FailNow(t, "Restore timed out")
	}
}

func TestRestoreUnreachablePrimary(t *testing.T) {
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
	root := t.TempDir()

	// Initialize BackupStorage
	fbsRoot := path.Join(root, "fbs")
	filebackupstorage.FileBackupStorageRoot = fbsRoot
	backupstorage.BackupStorageImplementation = "file"

	// Initialize the fake mysql root directories
	sourceInnodbDataDir := path.Join(root, "source_innodb_data")
	sourceInnodbLogDir := path.Join(root, "source_innodb_log")
	sourceDataDir := path.Join(root, "source_data")
	sourceDataDbDir := path.Join(sourceDataDir, "vt_db")
	for _, s := range []string{sourceInnodbDataDir, sourceInnodbLogDir, sourceDataDbDir} {
		require.NoError(t, os.MkdirAll(s, os.ModePerm))
	}
	require.NoError(t, os.WriteFile(path.Join(sourceInnodbDataDir, "innodb_data_1"), []byte("innodb data 1 contents"), os.ModePerm))

	needIt, err := needInnoDBRedoLogSubdir()
	require.NoError(t, err)
	if needIt {
		newPath := path.Join(sourceInnodbLogDir, mysql.DynamicRedoLogSubdir)
		require.NoError(t, os.Mkdir(newPath, os.ModePerm))
		require.NoError(t, os.WriteFile(path.Join(newPath, "#ib_redo1"), []byte("innodb log 1 contents"), os.ModePerm))
	} else {
		require.NoError(t, os.WriteFile(path.Join(sourceInnodbLogDir, "innodb_log_1"), []byte("innodb log 1 contents"), os.ModePerm))
	}

	require.NoError(t, os.WriteFile(path.Join(sourceDataDbDir, "db.opt"), []byte("db opt file"), os.ModePerm))

	// create a primary tablet, set its primary position
	primary := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_PRIMARY, db)
	primary.FakeMysqlDaemon.ReadOnly = false
	primary.FakeMysqlDaemon.Replicating = false
	primary.FakeMysqlDaemon.CurrentPrimaryPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 457,
			},
		},
	}

	// start primary so that replica can fetch primary position from it
	primary.StartActionLoop(t, wr)

	// create a single tablet, set it up so we can do backups
	// set its position same as that of primary so that backup doesn't wait for catchup
	sourceTablet := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, db)
	sourceTablet.FakeMysqlDaemon.ReadOnly = true
	sourceTablet.FakeMysqlDaemon.Replicating = true
	sourceTablet.FakeMysqlDaemon.CurrentPrimaryPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 457,
			},
		},
	}
	sourceTablet.FakeMysqlDaemon.SetReplicationSourceInputs = []string{fmt.Sprintf("%s:%d", primary.Tablet.MysqlHostname, primary.Tablet.MysqlPort)}
	sourceTablet.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		// This first set of STOP and START commands come from
		// the builtinBackupEngine implementation which stops the replication
		// while taking the backup
		"STOP SLAVE",
		"START SLAVE",
		// These commands come from SetReplicationSource RPC called
		// to set the correct primary and semi-sync after Backup has concluded
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
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
	destTablet.FakeMysqlDaemon.CurrentPrimaryPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 457,
			},
		},
	}
	destTablet.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET SLAVE POSITION",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	destTablet.FakeMysqlDaemon.FetchSuperQueryMap = map[string]*sqltypes.Result{
		"SHOW DATABASES": {},
	}
	destTablet.FakeMysqlDaemon.SetReplicationPositionPos = sourceTablet.FakeMysqlDaemon.CurrentPrimaryPosition
	destTablet.FakeMysqlDaemon.SetReplicationSourceInputs = append(destTablet.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(primary.Tablet))

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

	// stop primary so that it is unreachable
	primary.StopActionLoop(t)

	// set a short timeout so that we don't have to wait 30 seconds
	*topo.RemoteOperationTimeout = 2 * time.Second
	// Restore should still succeed
	require.NoError(t, destTablet.TM.RestoreData(ctx, logutil.NewConsoleLogger(), 0 /* waitForBackupInterval */, false /* deleteBeforeRestore */, time.Time{} /* restoreFromBackupTs */))
	// verify the full status
	require.NoError(t, destTablet.FakeMysqlDaemon.CheckSuperQueryList(), "destTablet.FakeMysqlDaemon.CheckSuperQueryList failed")
	assert.True(t, destTablet.FakeMysqlDaemon.Replicating)
	assert.True(t, destTablet.FakeMysqlDaemon.Running)
}

func TestDisableActiveReparents(t *testing.T) {
	mysqlctl.DisableActiveReparents = true
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		// When you mess with globals you must remember to reset them
		mysqlctl.DisableActiveReparents = false
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
	root := t.TempDir()

	// Initialize BackupStorage
	fbsRoot := path.Join(root, "fbs")
	filebackupstorage.FileBackupStorageRoot = fbsRoot
	backupstorage.BackupStorageImplementation = "file"

	// Initialize the fake mysql root directories
	sourceInnodbDataDir := path.Join(root, "source_innodb_data")
	sourceInnodbLogDir := path.Join(root, "source_innodb_log")
	sourceDataDir := path.Join(root, "source_data")
	sourceDataDbDir := path.Join(sourceDataDir, "vt_db")
	for _, s := range []string{sourceInnodbDataDir, sourceInnodbLogDir, sourceDataDbDir} {
		require.NoError(t, os.MkdirAll(s, os.ModePerm))
	}
	require.NoError(t, os.WriteFile(path.Join(sourceInnodbDataDir, "innodb_data_1"), []byte("innodb data 1 contents"), os.ModePerm))

	needIt, err := needInnoDBRedoLogSubdir()
	require.NoError(t, err)
	if needIt {
		newPath := path.Join(sourceInnodbLogDir, mysql.DynamicRedoLogSubdir)
		require.NoError(t, os.Mkdir(newPath, os.ModePerm))
		require.NoError(t, os.WriteFile(path.Join(newPath, "#ib_redo1"), []byte("innodb log 1 contents"), os.ModePerm))
	} else {
		require.NoError(t, os.WriteFile(path.Join(sourceInnodbLogDir, "innodb_log_1"), []byte("innodb log 1 contents"), os.ModePerm))
	}

	require.NoError(t, os.WriteFile(path.Join(sourceDataDbDir, "db.opt"), []byte("db opt file"), os.ModePerm))

	// create a primary tablet, set its primary position
	primary := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_PRIMARY, db)
	primary.FakeMysqlDaemon.ReadOnly = false
	primary.FakeMysqlDaemon.Replicating = false
	primary.FakeMysqlDaemon.CurrentPrimaryPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			2: mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 457,
			},
		},
	}

	// start primary so that replica can fetch primary position from it
	primary.StartActionLoop(t, wr)
	defer primary.StopActionLoop(t)

	// create a single tablet, set it up so we can do backups
	// set its position same as that of primary so that backup doesn't wait for catchup
	sourceTablet := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, db)
	sourceTablet.FakeMysqlDaemon.ReadOnly = true
	sourceTablet.FakeMysqlDaemon.Replicating = true
	sourceTablet.FakeMysqlDaemon.CurrentPrimaryPosition = mysql.Position{
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
	assert.False(t, sourceTablet.FakeMysqlDaemon.Replicating)
	assert.True(t, sourceTablet.FakeMysqlDaemon.Running)

	// create a destination tablet, set it up so we can do restores
	destTablet := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, db)
	destTablet.FakeMysqlDaemon.ReadOnly = true
	destTablet.FakeMysqlDaemon.Replicating = true
	destTablet.FakeMysqlDaemon.CurrentPrimaryPosition = mysql.Position{
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
	}
	destTablet.FakeMysqlDaemon.FetchSuperQueryMap = map[string]*sqltypes.Result{
		"SHOW DATABASES": {},
	}
	destTablet.FakeMysqlDaemon.SetReplicationPositionPos = sourceTablet.FakeMysqlDaemon.CurrentPrimaryPosition
	destTablet.FakeMysqlDaemon.SetReplicationSourceInputs = append(destTablet.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(primary.Tablet))

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

	require.NoError(t, destTablet.TM.RestoreData(ctx, logutil.NewConsoleLogger(), 0 /* waitForBackupInterval */, false /* deleteBeforeRestore */, time.Time{} /* restoreFromBackupTs */))
	// verify the full status
	require.NoError(t, destTablet.FakeMysqlDaemon.CheckSuperQueryList(), "destTablet.FakeMysqlDaemon.CheckSuperQueryList failed")
	assert.False(t, destTablet.FakeMysqlDaemon.Replicating)
	assert.True(t, destTablet.FakeMysqlDaemon.Running)
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
