// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package schemamanager

import (
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	"github.com/youtube/vitess/go/vt/wrangler"
)

var (
	testWaitSlaveTimeout = 10 * time.Second
)

func TestTabletExecutorOpen(t *testing.T) {
	executor := newFakeExecutor()
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
	ft := newFakeTopo()
	ft.Impl.(*fakeTopo).WithEmptyMasterAlias = true
	wr := wrangler.New(logutil.NewConsoleLogger(), ft, newFakeTabletManagerClient())
	executor := NewTabletExecutor(wr, testWaitSlaveTimeout)
	ctx := context.Background()

	if err := executor.Open(ctx, "test_keyspace"); err == nil {
		t.Fatalf("executor.Open() = nil, want error")
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

	wr := wrangler.New(logutil.NewConsoleLogger(), newFakeTopo(), fakeTmc)
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

	// change a table with more than 2,000,000 rows
	if err := executor.Validate(ctx, []string{
		"RENAME TABLE test_table_04 TO test_table_05",
	}); err == nil {
		t.Fatalf("executor.Validate should fail, change a table more than 2,000,000 rows")
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

func TestTabletExecutorExecute(t *testing.T) {
	executor := newFakeExecutor()
	ctx := context.Background()

	sqls := []string{"DROP TABLE unknown_table"}

	result := executor.Execute(ctx, sqls)
	if result.ExecutorErr == "" {
		t.Fatalf("execute should fail, call execute.Open first")
	}
}

func TestTabletExecutorExecute_PreflightWithoutChangesIsAnError(t *testing.T) {
	executor := newFakeExecutor()
	ctx := context.Background()
	executor.Open(ctx, "test_keyspace")
	defer executor.Close()

	sqls := []string{"DROP TABLE unknown_table"}
	result := executor.Execute(ctx, sqls)
	if result.ExecutorErr == "" {
		t.Fatalf("execute should fail, ddl does not introduce any table schema change")
	}
}
