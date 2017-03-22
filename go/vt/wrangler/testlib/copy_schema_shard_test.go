// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testlib

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysqlconn/fakesqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vttablet/tmclient"
	"github.com/youtube/vitess/go/vt/wrangler"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
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
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	if err := ts.CreateKeyspace(context.Background(), "ks", &topodatapb.Keyspace{
		ShardingColumnName: "keyspace_id",
		ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
	}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}

	sourceMasterDb := fakesqldb.New(t).SetName("sourceMasterDb")
	defer sourceMasterDb.Close()
	sourceMaster := NewFakeTablet(t, wr, "cell1", 0,
		topodatapb.TabletType_MASTER, sourceMasterDb, TabletKeyspaceShard(t, "ks", "-80"))

	sourceRdonlyDb := fakesqldb.New(t).SetName("sourceRdonlyDb")
	defer sourceRdonlyDb.Close()
	sourceRdonly := NewFakeTablet(t, wr, "cell1", 1,
		topodatapb.TabletType_RDONLY, sourceRdonlyDb, TabletKeyspaceShard(t, "ks", "-80"))

	destinationMasterDb := fakesqldb.New(t).SetName("destinationMasterDb")
	defer destinationMasterDb.Close()
	destinationMaster := NewFakeTablet(t, wr, "cell1", 10,
		topodatapb.TabletType_MASTER, destinationMasterDb, TabletKeyspaceShard(t, "ks", "-40"))

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

	// The source table is asked about its schema.
	// It may be the master or the rdonly.
	sourceDb := sourceRdonlyDb
	if useShardAsSource {
		sourceDb = sourceMasterDb
	}
	sourceDb.AddQuery(changeToDb, &sqltypes.Result{})
	sourceDb.AddQuery(selectInformationSchema, &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Type: querypb.Type_INT64,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.Value{},
			},
		},
	})
	sourceDb.AddQuery(selectShardMetadata, &sqltypes.Result{})

	// The destination table is asked to create the new schema.
	destinationMasterDb.AddQuery(changeToDb, &sqltypes.Result{})
	destinationMasterDb.AddQuery(createDb, &sqltypes.Result{})
	destinationMasterDb.AddQuery(createTable, &sqltypes.Result{})
	destinationMasterDb.AddQuery(createTableView, &sqltypes.Result{})

	destinationMaster.FakeMysqlDaemon.SchemaFunc = func() (*tabletmanagerdatapb.SchemaDefinition, error) {
		if destinationMasterDb.GetQueryCalledNum(createTableView) == 1 {
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

	// Check call count on the source.
	if count := sourceDb.GetQueryCalledNum(changeToDb); count != 2 {
		t.Errorf("CopySchemaShard did not change to the db 2 times. Query count: %v", count)
	}
	if count := sourceDb.GetQueryCalledNum(selectInformationSchema); count != 1 {
		t.Errorf("CopySchemaShard did not select data from information_schema.tables exactly once. Query count: %v", count)
	}
	if count := sourceDb.GetQueryCalledNum(selectShardMetadata); count != 1 {
		t.Errorf("CopySchemaShard did not select data from _vt.shard_metadata exactly once. Query count: %v", count)
	}

	// Check call count on destinationMasterDb
	if count := destinationMasterDb.GetQueryCalledNum(createDb); count != 1 {
		t.Errorf("CopySchemaShard did not create the db exactly once. Query count: %v", count)
	}
	if count := destinationMasterDb.GetQueryCalledNum(createTable); count != 1 {
		t.Errorf("CopySchemaShard did not create the table exactly once. Query count: %v", count)
	}
	if count := destinationMasterDb.GetQueryCalledNum(createTableView); count != 1 {
		t.Errorf("CopySchemaShard did not create the table view exactly once. Query count: %v", count)
	}
}
