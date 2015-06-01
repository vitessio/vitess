// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testlib

import (
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/mysqlctl/backupstorage"
	"github.com/youtube/vitess/go/vt/mysqlctl/filebackupstorage"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/zktopo"
	"golang.org/x/net/context"
)

func TestBackupRestore(t *testing.T) {
	// Initialize our environment
	ctx := context.Background()
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient(), time.Second)
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

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
	master := NewFakeTablet(t, wr, "cell1", 0, topo.TYPE_MASTER)

	// create a single tablet, set it up so we can do backups
	sourceTablet := NewFakeTablet(t, wr, "cell1", 1, topo.TYPE_REPLICA)
	sourceTablet.FakeMysqlDaemon.ReadOnly = true
	sourceTablet.FakeMysqlDaemon.Replicating = true
	sourceTablet.FakeMysqlDaemon.CurrentMasterPosition = myproto.ReplicationPosition{
		GTIDSet: myproto.MariadbGTID{
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
	if err := vp.Run([]string{"Backup", sourceTablet.Tablet.Alias.String()}); err != nil {
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
	destTablet := NewFakeTablet(t, wr, "cell1", 2, topo.TYPE_REPLICA)
	destTablet.FakeMysqlDaemon.ReadOnly = true
	destTablet.FakeMysqlDaemon.Replicating = true
	destTablet.FakeMysqlDaemon.CurrentMasterPosition = myproto.ReplicationPosition{
		GTIDSet: myproto.MariadbGTID{
			Domain:   2,
			Server:   123,
			Sequence: 457,
		},
	}
	destTablet.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"cmd1",
	}
	destTablet.FakeMysqlDaemon.Mycnf = &mysqlctl.Mycnf{
		DataDir:               sourceDataDir,
		InnodbDataHomeDir:     sourceInnodbDataDir,
		InnodbLogGroupHomeDir: sourceInnodbLogDir,
	}
	destTablet.FakeMysqlDaemon.FetchSuperQueryMap = map[string]*mproto.QueryResult{
		"SHOW DATABASES": &mproto.QueryResult{},
	}
	destTablet.FakeMysqlDaemon.StartReplicationCommandsStatus = &myproto.ReplicationStatus{
		Position:           sourceTablet.FakeMysqlDaemon.CurrentMasterPosition,
		MasterHost:         master.Tablet.Hostname,
		MasterPort:         master.Tablet.Portmap["mysql"],
		MasterConnectRetry: 10,
	}
	destTablet.FakeMysqlDaemon.StartReplicationCommandsResult = []string{"cmd1"}

	destTablet.StartActionLoop(t, wr)
	defer destTablet.StopActionLoop(t)

	if err := destTablet.Agent.RestoreFromBackup(ctx); err != nil {
		t.Fatalf("RestoreFromBackup failed: %v", err)
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
