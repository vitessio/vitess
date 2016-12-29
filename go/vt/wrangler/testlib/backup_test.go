// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testlib

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/mysqlctl/backupstorage"
	"github.com/youtube/vitess/go/vt/mysqlctl/filebackupstorage"
	"github.com/youtube/vitess/go/vt/mysqlctl/replication"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vttest/fakesqldb"
	"github.com/youtube/vitess/go/vt/wrangler"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestBackupRestore(t *testing.T) {
	// Initialize our environment
	ctx := context.Background()
	db := fakesqldb.Register()
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
	sourceTablet.FakeMysqlDaemon.CurrentMasterPosition = replication.Position{
		GTIDSet: replication.MariadbGTID{
			Domain:   2,
			Server:   123,
			Sequence: 457,
		},
	}
	sourceTablet.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"START SLAVE",
	}
	sourceTablet.FakeMysqlDaemon.Mycnf = &mysqlctl.Mycnf{
		DataDir:               sourceDataDir,
		InnodbDataHomeDir:     sourceInnodbDataDir,
		InnodbLogGroupHomeDir: sourceInnodbLogDir,
	}
	sourceTablet.StartActionLoop(t, wr)
	defer sourceTablet.StopActionLoop(t)

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
	destTablet.FakeMysqlDaemon.CurrentMasterPosition = replication.Position{
		GTIDSet: replication.MariadbGTID{
			Domain:   2,
			Server:   123,
			Sequence: 457,
		},
	}
	destTablet.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"cmd1",
		"set master cmd 1",
		"START SLAVE",
	}
	destTablet.FakeMysqlDaemon.Mycnf = &mysqlctl.Mycnf{
		DataDir:               sourceDataDir,
		InnodbDataHomeDir:     sourceInnodbDataDir,
		InnodbLogGroupHomeDir: sourceInnodbLogDir,
		BinLogPath:            path.Join(root, "bin-logs/filename_prefix"),
		RelayLogPath:          path.Join(root, "relay-logs/filename_prefix"),
		RelayLogIndexPath:     path.Join(root, "relay-log.index"),
		RelayLogInfoPath:      path.Join(root, "relay-log.info"),
	}
	destTablet.FakeMysqlDaemon.FetchSuperQueryMap = map[string]*sqltypes.Result{
		"SHOW DATABASES": {},
	}
	destTablet.FakeMysqlDaemon.SetSlavePositionCommandsPos = sourceTablet.FakeMysqlDaemon.CurrentMasterPosition
	destTablet.FakeMysqlDaemon.SetSlavePositionCommandsResult = []string{"cmd1"}
	destTablet.FakeMysqlDaemon.SetMasterCommandsInput = fmt.Sprintf("%v:%v", master.Tablet.Hostname, master.Tablet.PortMap["mysql"])
	destTablet.FakeMysqlDaemon.SetMasterCommandsResult = []string{"set master cmd 1"}

	destTablet.StartActionLoop(t, wr)
	defer destTablet.StopActionLoop(t)

	if err := destTablet.Agent.RestoreData(ctx, logutil.NewConsoleLogger(), false /* deleteBeforeRestore */); err != nil {
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
