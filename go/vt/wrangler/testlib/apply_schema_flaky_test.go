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
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/discovery"

	"context"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestApplySchema_AllowLongUnavailability is an integration test for the
// -allow_long_unavailability flag of vtctl ApplySchema.
// Only if the flag is specified, potentially long running schema changes are
// allowed.
func TestApplySchema_AllowLongUnavailability(t *testing.T) {
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

	cell := "cell1"
	db := fakesqldb.New(t)
	defer db.Close()
	ts := memorytopo.NewServer(cell)
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	if err := ts.CreateKeyspace(context.Background(), "ks", &topodatapb.Keyspace{
		ShardingColumnName: "keyspace_id",
		ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
	}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}

	beforeSchema := &tabletmanagerdatapb.SchemaDefinition{
		DatabaseSchema: "CREATE DATABASE `{{.DatabaseName}}` /*!40100 DEFAULT CHARACTER SET utf8 */",
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
			{
				Name:     "table1",
				Schema:   "CREATE TABLE `table1` (\n  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n  `msg` varchar(64) DEFAULT NULL,\n  `keyspace_id` bigint(20) unsigned NOT NULL,\n  PRIMARY KEY (`id`),\n  KEY `by_msg` (`msg`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8",
				Type:     tmutils.TableBaseTable,
				RowCount: 3000000,
			},
		},
	}
	afterSchema := &tabletmanagerdatapb.SchemaDefinition{
		DatabaseSchema: "CREATE DATABASE `{{.DatabaseName}}` /*!40100 DEFAULT CHARACTER SET utf8 */",
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
			{
				Name:     "table1",
				Schema:   "CREATE TABLE `table1` (\n  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n  `msg` varchar(64) DEFAULT NULL,\n  `keyspace_id` bigint(20) unsigned NOT NULL,\n  `id` bigint(20),\n  PRIMARY KEY (`id`),\n  KEY `by_msg` (`msg`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8",
				Type:     tmutils.TableBaseTable,
				RowCount: 3000000,
			},
		},
	}
	preflightSchemaChanges := []*tabletmanagerdatapb.SchemaChangeResult{
		{
			BeforeSchema: beforeSchema,
			AfterSchema:  afterSchema,
		},
	}

	tShard1 := NewFakeTablet(t, wr, cell, 0,
		topodatapb.TabletType_MASTER, db, TabletKeyspaceShard(t, "ks", "-80"))
	tShard2 := NewFakeTablet(t, wr, cell, 1,
		topodatapb.TabletType_MASTER, db, TabletKeyspaceShard(t, "ks", "80-"))
	for _, ft := range []*FakeTablet{tShard1, tShard2} {
		ft.StartActionLoop(t, wr)
		defer ft.StopActionLoop(t)

		ft.FakeMysqlDaemon.Schema = beforeSchema
		ft.FakeMysqlDaemon.PreflightSchemaChangeResult = preflightSchemaChanges
	}

	changeToDb := "USE `vt_ks`"
	addColumn := "ALTER TABLE table1 ADD COLUMN new_id bigint(20)"
	db.AddQuery(changeToDb, &sqltypes.Result{})
	db.AddQuery(addColumn, &sqltypes.Result{})

	// First ApplySchema fails because the table is very big and -allow_long_unavailability is missing.
	if err := vp.Run([]string{"ApplySchema", "-sql", addColumn, "ks"}); err == nil {
		t.Fatal("ApplySchema should have failed but did not.")
	} else if !strings.Contains(err.Error(), "big schema change detected") ||
		!strings.Contains(strings.ToLower(err.Error()), "alter table table1") {
		t.Fatalf("ApplySchema failed with wrong error. got: %v", err)
	}

	// Second ApplySchema succeeds because -allow_long_unavailability is set.
	if err := vp.Run([]string{"ApplySchema", "-allow_long_unavailability", "-sql", addColumn, "ks"}); err != nil {
		t.Fatalf("ApplySchema failed: %v", err)
	}
	if count := db.GetQueryCalledNum(changeToDb); count != 2 {
		t.Fatalf("ApplySchema: unexpected call count. Query: %v got: %v want: %v", changeToDb, count, 2)
	}
	if count := db.GetQueryCalledNum(addColumn); count != 2 {
		t.Fatalf("ApplySchema: unexpected call count. Query: %v got: %v want: %v", addColumn, count, 2)
	}
}
