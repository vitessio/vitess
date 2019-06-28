/*
Copyright 2017 Google Inc.

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

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/mysqlctl/filebackupstorage"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestBackupRestore(t *testing.T) {
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
	if err != nil {
		t.Fatalf("os.TempDir failed: %v", err)
	}
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
		if err := os.MkdirAll(s, os.ModePerm); err != nil {
			t.Fatalf("failed to create directory %v: %v", s, err)
		}
	}
	if err := ioutil.WriteFile(path.Join(sourceInnodbDataDir, "innodb_data_1"), []byte("innodb data 1 contents"), os.ModePerm); err != nil {
		t.Fatalf("failed to write file innodb_data_1: %v", err)
	}
	if err := ioutil.WriteFile(path.Join(sourceInnodbLogDir, "innodb_log_1"), []byte("innodb log 1 contents"), os.ModePerm); err != nil {
		t.Fatalf("failed to write file innodb_log_1: %v", err)
	}
	if err := ioutil.WriteFile(path.Join(sourceDataDbDir, "db.opt"), []byte("db opt file"), os.ModePerm); err != nil {
		t.Fatalf("failed to write file db.opt: %v", err)
	}

	// create a master tablet, not started, just for shard health
	master := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, db)

	// create a single tablet, set it up so we can do backups
	sourceTablet := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, db)
	sourceTablet.FakeMysqlDaemon.ReadOnly = true
	sourceTablet.FakeMysqlDaemon.Replicating = true
	sourceTablet.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
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

	sourceTablet.Agent.Cnf = &mysqlctl.Mycnf{
		DataDir:               sourceDataDir,
		InnodbDataHomeDir:     sourceInnodbDataDir,
		InnodbLogGroupHomeDir: sourceInnodbLogDir,
	}

	// run the backup
	if err := vp.Run([]string{"Backup", topoproto.TabletAliasString(sourceTablet.Tablet.Alias)}); err != nil {
		t.Fatalf("Backup failed: %v", err)
	}

	// verify the full status
	if err := sourceTablet.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Errorf("sourceTablet.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if !sourceTablet.FakeMysqlDaemon.Replicating {
		t.Errorf("sourceTablet.FakeMysqlDaemon.Replicating not set")
	}
	if !sourceTablet.FakeMysqlDaemon.Running {
		t.Errorf("sourceTablet.FakeMysqlDaemon.Running not set")
	}

	// create a destination tablet, set it up so we can do restores
	destTablet := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, db)
	destTablet.FakeMysqlDaemon.ReadOnly = true
	destTablet.FakeMysqlDaemon.Replicating = true
	destTablet.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
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
	destTablet.FakeMysqlDaemon.SetSlavePositionPos = sourceTablet.FakeMysqlDaemon.CurrentMasterPosition
	destTablet.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(master.Tablet)

	destTablet.StartActionLoop(t, wr)
	defer destTablet.StopActionLoop(t)

	destTablet.Agent.Cnf = &mysqlctl.Mycnf{
		DataDir:               sourceDataDir,
		InnodbDataHomeDir:     sourceInnodbDataDir,
		InnodbLogGroupHomeDir: sourceInnodbLogDir,
		BinLogPath:            path.Join(root, "bin-logs/filename_prefix"),
		RelayLogPath:          path.Join(root, "relay-logs/filename_prefix"),
		RelayLogIndexPath:     path.Join(root, "relay-log.index"),
		RelayLogInfoPath:      path.Join(root, "relay-log.info"),
	}

	if err := destTablet.Agent.RestoreData(ctx, logutil.NewConsoleLogger(), 0 /* waitForBackupInterval */, false /* deleteBeforeRestore */); err != nil {
		t.Fatalf("RestoreData failed: %v", err)
	}

	// verify the full status
	if err := destTablet.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Errorf("destTablet.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if !destTablet.FakeMysqlDaemon.Replicating {
		t.Errorf("destTablet.FakeMysqlDaemon.Replicating not set")
	}
	if !destTablet.FakeMysqlDaemon.Running {
		t.Errorf("destTablet.FakeMysqlDaemon.Running not set")
	}

}
