// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testlib

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/topo/zk2topo"
	"github.com/youtube/vitess/go/vt/vttest/fakesqldb"
	"github.com/youtube/vitess/go/vt/wrangler"

	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestCopySchemaShard_UseTabletAsSource(t *testing.T) {
	copySchema(t, false /* useShardAsSource */)
}

func TestCopySchemaShard_UseShardAsSource(t *testing.T) {
	copySchema(t, true /* useShardAsSource */)
}

func copySchema(t *testing.T, useShardAsSource bool) {
	db := fakesqldb.Register()
	ts := zk2topo.NewFakeServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	if err := ts.CreateKeyspace(context.Background(), "ks", &topodatapb.Keyspace{
		ShardingColumnName: "keyspace_id",
		ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
	}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}

	sourceMaster := NewFakeTablet(t, wr, "cell1", 0,
		topodatapb.TabletType_MASTER, db, TabletKeyspaceShard(t, "ks", "-80"))
	sourceRdonly := NewFakeTablet(t, wr, "cell1", 1,
		topodatapb.TabletType_RDONLY, db, TabletKeyspaceShard(t, "ks", "-80"))

	destinationMaster := NewFakeTablet(t, wr, "cell1", 10,
		topodatapb.TabletType_MASTER, db, TabletKeyspaceShard(t, "ks", "-40"))

	for _, ft := range []*FakeTablet{sourceMaster, sourceRdonly, destinationMaster} {
		ft.StartActionLoop(t, wr)
		defer ft.StopActionLoop(t)
	}

	schema := &tabletmanagerdatapb.SchemaDefinition{
		DatabaseSchema: "CREATE DATABASE `{{.DatabaseName}}` /*!40100 DEFAULT CHARACTER SET utf8 */",
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
			{
				Name:   "table1",
				Schema: "CREATE TABLE `table1` (\n  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n  `msg` varchar(64) DEFAULT NULL,\n  `keyspace_id` bigint(20) unsigned NOT NULL,\n  PRIMARY KEY (`id`),\n  KEY `by_msg` (`msg`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8",
				Type:   tmutils.TableBaseTable,
			},
			{
				Name:   "view1",
				Schema: "CREATE TABLE `view1` (\n  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n  `msg` varchar(64) DEFAULT NULL,\n  `keyspace_id` bigint(20) unsigned NOT NULL,\n  PRIMARY KEY (`id`),\n  KEY `by_msg` (`msg`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8",
				Type:   tmutils.TableView,
			},
		},
	}
	schemaEmptyDb := &tabletmanagerdatapb.SchemaDefinition{
		DatabaseSchema:   "CREATE DATABASE `{{.DatabaseName}}` /*!40100 DEFAULT CHARACTER SET utf8 */",
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{},
	}
	sourceMaster.FakeMysqlDaemon.Schema = schema
	sourceRdonly.FakeMysqlDaemon.Schema = schema

	changeToDb := "USE vt_ks"
	createDb := "CREATE DATABASE `vt_ks` /*!40100 DEFAULT CHARACTER SET utf8 */"
	createTable := "CREATE TABLE `vt_ks`.`table1` (\n" +
		"  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
		"  `msg` varchar(64) DEFAULT NULL,\n" +
		"  `keyspace_id` bigint(20) unsigned NOT NULL,\n" +
		"  PRIMARY KEY (`id`),\n" +
		"  KEY `by_msg` (`msg`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8"
	createTableView := "CREATE TABLE `view1` (\n" +
		"  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
		"  `msg` varchar(64) DEFAULT NULL,\n" +
		"  `keyspace_id` bigint(20) unsigned NOT NULL,\n" +
		"  PRIMARY KEY (`id`),\n" +
		"  KEY `by_msg` (`msg`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8"
	selectInformationSchema := "SELECT 1 FROM information_schema.tables WHERE table_schema = '_vt' AND table_name = 'shard_metadata'"
	selectShardMetadata := "SELECT name, value FROM _vt.shard_metadata"

	db.AddQuery(changeToDb, &sqltypes.Result{})
	db.AddQuery(createDb, &sqltypes.Result{})
	db.AddQuery(createTable, &sqltypes.Result{})
	db.AddQuery(createTableView, &sqltypes.Result{})
	db.AddQuery(selectInformationSchema, &sqltypes.Result{Rows: make([][]sqltypes.Value, 1)})
	db.AddQuery(selectShardMetadata, &sqltypes.Result{})

	destinationMaster.FakeMysqlDaemon.SchemaFunc = func() (*tabletmanagerdatapb.SchemaDefinition, error) {
		if db.GetQueryCalledNum(createTableView) == 1 {
			return schema, nil
		}
		return schemaEmptyDb, nil
	}

	source := topoproto.TabletAliasString(sourceRdonly.Tablet.Alias)
	if useShardAsSource {
		source = "ks/-80"
	}
	if err := vp.Run([]string{"CopySchemaShard", "-include-views", source, "ks/-40"}); err != nil {
		t.Fatalf("CopySchemaShard failed: %v", err)
	}
	if count := db.GetQueryCalledNum(changeToDb); count != 5 {
		t.Fatalf("CopySchemaShard did not change to the db 5 times. Query count: %v", count)
	}
	if count := db.GetQueryCalledNum(createDb); count != 1 {
		t.Fatalf("CopySchemaShard did not create the db exactly once. Query count: %v", count)
	}
	if count := db.GetQueryCalledNum(createTable); count != 1 {
		t.Fatalf("CopySchemaShard did not create the table exactly once. Query count: %v", count)
	}
	if count := db.GetQueryCalledNum(createTableView); count != 1 {
		t.Fatalf("CopySchemaShard did not create the table view exactly once. Query count: %v", count)
	}
	if count := db.GetQueryCalledNum(selectInformationSchema); count != 1 {
		t.Fatalf("CopySchemaShard did not select data from information_schema.tables exactly once. Query count: %v", count)
	}
	if count := db.GetQueryCalledNum(selectShardMetadata); count != 1 {
		t.Fatalf("CopySchemaShard did not select data from _vt.shard_metadata exactly once. Query count: %v", count)
	}
}
