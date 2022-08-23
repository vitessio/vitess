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
	"fmt"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/discovery"

	"context"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestCopySchemaShard_UseTabletAsSource(t *testing.T) {
	copySchema(t, false /* useShardAsSource */)
}

func TestCopySchemaShard_UseShardAsSource(t *testing.T) {
	copySchema(t, true /* useShardAsSource */)
}

func copySchema(t *testing.T, useShardAsSource bool) {
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	if err := ts.CreateKeyspace(context.Background(), "ks", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}

	sourcePrimaryDb := fakesqldb.New(t).SetName("sourcePrimaryDb")
	defer sourcePrimaryDb.Close()
	sourcePrimary := NewFakeTablet(t, wr, "cell1", 0,
		topodatapb.TabletType_PRIMARY, sourcePrimaryDb, TabletKeyspaceShard(t, "ks", "-80"))

	sourceRdonlyDb := fakesqldb.New(t).SetName("sourceRdonlyDb")
	defer sourceRdonlyDb.Close()
	sourceRdonly := NewFakeTablet(t, wr, "cell1", 1,
		topodatapb.TabletType_RDONLY, sourceRdonlyDb, TabletKeyspaceShard(t, "ks", "-80"))
	sourceRdonly.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	sourceRdonly.FakeMysqlDaemon.SetReplicationSourceInputs = append(sourceRdonly.FakeMysqlDaemon.SetReplicationSourceInputs, fmt.Sprintf("%v:%v", sourcePrimary.Tablet.MysqlHostname, sourcePrimary.Tablet.MysqlPort))

	destinationPrimaryDb := fakesqldb.New(t).SetName("destinationPrimaryDb")
	defer destinationPrimaryDb.Close()
	destinationPrimary := NewFakeTablet(t, wr, "cell1", 10,
		topodatapb.TabletType_PRIMARY, destinationPrimaryDb, TabletKeyspaceShard(t, "ks", "-40"))

	for _, ft := range []*FakeTablet{sourcePrimary, sourceRdonly, destinationPrimary} {
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
	sourcePrimary.FakeMysqlDaemon.Schema = schema
	sourceRdonly.FakeMysqlDaemon.Schema = schema

	setSQLMode := fmt.Sprintf("SET @@session.sql_mode='%v'", vreplication.SQLMode)
	changeToDb := "USE `vt_ks`"
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
	selectShardMetadata := "SELECT db_name, name, value FROM _vt.shard_metadata"

	// The source table is asked about its schema.
	// It may be the primary or the rdonly.
	sourceDb := sourceRdonlyDb
	if useShardAsSource {
		sourceDb = sourcePrimaryDb
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
	destinationPrimaryDb.AddQuery(setSQLMode, &sqltypes.Result{})
	destinationPrimaryDb.AddQuery(createDb, &sqltypes.Result{})
	destinationPrimaryDb.AddQuery(changeToDb, &sqltypes.Result{})
	destinationPrimaryDb.AddQuery(createTable, &sqltypes.Result{})
	destinationPrimaryDb.AddQuery(createTableView, &sqltypes.Result{})

	destinationPrimary.FakeMysqlDaemon.SchemaFunc = func() (*tabletmanagerdatapb.SchemaDefinition, error) {
		if destinationPrimaryDb.GetQueryCalledNum(createTableView) == 1 {
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

	// Check call count on destinationPrimaryDb
	if count := destinationPrimaryDb.GetQueryCalledNum(createDb); count != 1 {
		t.Errorf("CopySchemaShard did not create the db exactly once. Query count: %v", count)
	}
	if count := destinationPrimaryDb.GetQueryCalledNum(createTable); count != 1 {
		t.Errorf("CopySchemaShard did not create the table exactly once. Query count: %v", count)
	}
	if count := destinationPrimaryDb.GetQueryCalledNum(createTableView); count != 1 {
		t.Errorf("CopySchemaShard did not create the table view exactly once. Query count: %v", count)
	}
}
