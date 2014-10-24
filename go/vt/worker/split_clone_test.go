// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/logutil"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	_ "github.com/youtube/vitess/go/vt/tabletmanager/gorpctmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/wrangler/testlib"
	"github.com/youtube/vitess/go/vt/zktopo"
)

func TestSplitClone(t *testing.T) {
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, time.Minute, time.Second)

	sourceMaster := testlib.NewFakeTablet(t, wr, "cell1", 0,
		topo.TYPE_MASTER, testlib.TabletKeyspaceShard(t, "ks", "-80"))
	sourceRdonly := testlib.NewFakeTablet(t, wr, "cell1", 1,
		topo.TYPE_RDONLY, testlib.TabletKeyspaceShard(t, "ks", "-80"),
		testlib.TabletParent(sourceMaster.Tablet.Alias))

	leftMaster := testlib.NewFakeTablet(t, wr, "cell1", 10,
		topo.TYPE_MASTER, testlib.TabletKeyspaceShard(t, "ks", "-40"))
	leftRdonly := testlib.NewFakeTablet(t, wr, "cell1", 11,
		topo.TYPE_RDONLY, testlib.TabletKeyspaceShard(t, "ks", "-40"),
		testlib.TabletParent(leftMaster.Tablet.Alias))

	rightMaster := testlib.NewFakeTablet(t, wr, "cell1", 20,
		topo.TYPE_MASTER, testlib.TabletKeyspaceShard(t, "ks", "40-80"))
	rightRdonly := testlib.NewFakeTablet(t, wr, "cell1", 21,
		topo.TYPE_RDONLY, testlib.TabletKeyspaceShard(t, "ks", "40-80"),
		testlib.TabletParent(rightMaster.Tablet.Alias))

	for _, ft := range []*testlib.FakeTablet{sourceMaster, sourceRdonly, leftMaster, leftRdonly, rightMaster, rightRdonly} {
		ft.StartActionLoop(t, wr)
		defer ft.StopActionLoop(t)
	}

	// add the topo and schema data we'll need
	if err := topo.CreateShard(ts, "ks", "80-"); err != nil {
		t.Fatalf("CreateShard(\"-80\") failed: %v", err)
	}
	if err := wr.SetKeyspaceShardingInfo("ks", "keyspace_id", key.KIT_UINT64, 4, false); err != nil {
		t.Fatalf("SetKeyspaceShardingInfo failed: %v", err)
	}
	if err := wr.RebuildKeyspaceGraph("ks", nil, nil); err != nil {
		t.Fatalf("RebuildKeyspaceGraph failed: %v", err)
	}
	sourceRdonly.FakeMysqlDaemon.Schema = &myproto.SchemaDefinition{
		DatabaseSchema: "CREATE DATABASE `{{.DatabaseName}}` /*!40100 DEFAULT CHARACTER SET utf8 */",
		TableDefinitions: []*myproto.TableDefinition{
			&myproto.TableDefinition{
				Name:              "table1",
				Schema:            "CREATE TABLE `resharding1` (\n  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n  `msg` varchar(64) DEFAULT NULL,\n  `keyspace_id` bigint(20) unsigned NOT NULL,\n  PRIMARY KEY (`id`),\n  KEY `by_msg` (`msg`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8",
				Columns:           []string{"id", "msg", "keyspace_id"},
				PrimaryKeyColumns: []string{"id"},
				Type:              myproto.TABLE_BASE_TABLE,
				DataLength:        2048,
				RowCount:          10,
			},
		},
		Version: "unused",
	}

	wrk := NewSplitCloneWorker(wr, "cell1", "ks", "-80", nil, "populateBlpCheckpoint", 10, 1, 10)
	wrk.Run()
	status := wrk.StatusAsText()
	t.Logf("Got status: %v", status)
}
