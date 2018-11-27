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

package schemamanager

import (
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/wrangler"
)

var (
	testWaitSlaveTimeout = 10 * time.Second
)

func TestTabletExecutorOpen(t *testing.T) {
	executor := newFakeExecutor(t)
	ctx := context.Background()

	if err := executor.Open(ctx, "test_keyspace"); err != nil {
		t.Fatalf("executor.Open should succeed")
	}

	defer executor.Close()

	if err := executor.Open(ctx, "test_keyspace"); err != nil {
		t.Fatalf("open an opened executor should also succeed")
	}
}

func TestTabletExecutorOpenWithEmptyMasterAlias(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("test_cell")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, newFakeTabletManagerClient())
	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "test_cell",
			Uid:  1,
		},
		Keyspace: "test_keyspace",
		Shard:    "0",
		Type:     topodatapb.TabletType_REPLICA,
	}
	// This will create the Keyspace, Shard and Tablet record.
	// Since this is a replica tablet, the Shard will have no master.
	if err := wr.InitTablet(ctx, tablet, false /*allowMasterOverride*/, true /*createShardAndKeyspace*/, false /*allowUpdate*/); err != nil {
		t.Fatalf("InitTablet failed: %v", err)
	}
	executor := NewTabletExecutor(wr, testWaitSlaveTimeout)
	if err := executor.Open(ctx, "test_keyspace"); err == nil || !strings.Contains(err.Error(), "does not have a master") {
		t.Fatalf("executor.Open() = '%v', want error", err)
	}
	executor.Close()
}

func TestTabletExecutorValidate(t *testing.T) {
	fakeTmc := newFakeTabletManagerClient()

	fakeTmc.AddSchemaDefinition("vt_test_keyspace", &tabletmanagerdatapb.SchemaDefinition{
		DatabaseSchema: "CREATE DATABASE `{{.DatabaseName}}` /*!40100 DEFAULT CHARACTER SET utf8 */",
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
			{
				Name:   "test_table",
				Schema: "table schema",
				Type:   tmutils.TableBaseTable,
			},
			{
				Name:     "test_table_03",
				Schema:   "table schema",
				Type:     tmutils.TableBaseTable,
				RowCount: 200000,
			},
			{
				Name:     "test_table_04",
				Schema:   "table schema",
				Type:     tmutils.TableBaseTable,
				RowCount: 3000000,
			},
		},
	})

	wr := wrangler.New(logutil.NewConsoleLogger(), newFakeTopo(t), fakeTmc)
	executor := NewTabletExecutor(wr, testWaitSlaveTimeout)
	ctx := context.Background()

	sqls := []string{
		"ALTER TABLE test_table ADD COLUMN new_id bigint(20)",
		"CREATE TABLE test_table_02 (pk int)",
	}

	if err := executor.Validate(ctx, sqls); err == nil {
		t.Fatalf("validate should fail because executor is closed")
	}

	executor.Open(ctx, "test_keyspace")
	defer executor.Close()

	// schema changes with DMLs should fail
	if err := executor.Validate(ctx, []string{
		"INSERT INTO test_table VALUES(1)"}); err == nil {
		t.Fatalf("schema changes are for DDLs")
	}

	// validates valid ddls
	if err := executor.Validate(ctx, sqls); err != nil {
		t.Fatalf("executor.Validate should succeed, but got error: %v", err)
	}

	// alter a table with more than 100,000 rows
	if err := executor.Validate(ctx, []string{
		"ALTER TABLE test_table_03 ADD COLUMN new_id bigint(20)",
	}); err == nil {
		t.Fatalf("executor.Validate should fail, alter a table more than 100,000 rows")
	}

	if err := executor.Validate(ctx, []string{
		"TRUNCATE TABLE test_table_04",
	}); err != nil {
		t.Fatalf("executor.Validate should succeed, drop a table with more than 2,000,000 rows is allowed")
	}

	if err := executor.Validate(ctx, []string{
		"DROP TABLE test_table_04",
	}); err != nil {
		t.Fatalf("executor.Validate should succeed, drop a table with more than 2,000,000 rows is allowed")
	}

	executor.AllowBigSchemaChange()
	// alter a table with more than 100,000 rows
	if err := executor.Validate(ctx, []string{
		"ALTER TABLE test_table_03 ADD COLUMN new_id bigint(20)",
	}); err != nil {
		t.Fatalf("executor.Validate should succeed, big schema change is disabled")
	}

	executor.DisallowBigSchemaChange()
	if err := executor.Validate(ctx, []string{
		"ALTER TABLE test_table_03 ADD COLUMN new_id bigint(20)",
	}); err == nil {
		t.Fatalf("executor.Validate should fail, alter a table more than 100,000 rows")
	}
}

func TestTabletExecutorDML(t *testing.T) {
	fakeTmc := newFakeTabletManagerClient()

	fakeTmc.AddSchemaDefinition("vt_test_keyspace", &tabletmanagerdatapb.SchemaDefinition{
		DatabaseSchema: "CREATE DATABASE `{{.DatabaseName}}` /*!40100 DEFAULT CHARACTER SET utf8 */",
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
			{
				Name:   "test_table",
				Schema: "table schema",
				Type:   tmutils.TableBaseTable,
			},
			{
				Name:     "test_table_03",
				Schema:   "table schema",
				Type:     tmutils.TableBaseTable,
				RowCount: 200000,
			},
			{
				Name:     "test_table_04",
				Schema:   "table schema",
				Type:     tmutils.TableBaseTable,
				RowCount: 3000000,
			},
		},
	})

	wr := wrangler.New(logutil.NewConsoleLogger(), newFakeTopo(t), fakeTmc)
	executor := NewTabletExecutor(wr, testWaitSlaveTimeout)
	ctx := context.Background()

	executor.Open(ctx, "unsharded_keyspace")
	defer executor.Close()

	// schema changes with DMLs should fail
	if err := executor.Validate(ctx, []string{
		"INSERT INTO test_table VALUES(1)"}); err != nil {
		t.Fatalf("executor.Validate should succeed, for DML to unsharded keyspace")
	}
}

func TestTabletExecutorExecute(t *testing.T) {
	executor := newFakeExecutor(t)
	ctx := context.Background()

	sqls := []string{"DROP TABLE unknown_table"}

	result := executor.Execute(ctx, sqls)
	if result.ExecutorErr == "" {
		t.Fatalf("execute should fail, call execute.Open first")
	}
}
