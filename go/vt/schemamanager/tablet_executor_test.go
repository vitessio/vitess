// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package schemamanager

import (
	"testing"

	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"golang.org/x/net/context"
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

func TestTabletExecutorValidate(t *testing.T) {
	fakeTmc := newFakeTabletManagerClient()

	fakeTmc.AddSchemaDefinition("vt_test_keyspace", &proto.SchemaDefinition{
		DatabaseSchema: "CREATE DATABASE `{{.DatabaseName}}` /*!40100 DEFAULT CHARACTER SET utf8 */",
		TableDefinitions: []*proto.TableDefinition{
			&proto.TableDefinition{
				Name:   "test_table",
				Schema: "table schema",
				Type:   proto.TableBaseTable,
			},
			&proto.TableDefinition{
				Name:     "test_table_03",
				Schema:   "table schema",
				Type:     proto.TableBaseTable,
				RowCount: 200000,
			},
			&proto.TableDefinition{
				Name:     "test_table_04",
				Schema:   "table schema",
				Type:     proto.TableBaseTable,
				RowCount: 3000000,
			},
		},
	})

	executor := NewTabletExecutor(
		fakeTmc,
		newFakeTopo())
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
}

func TestTabletExecutorExecute(t *testing.T) {
	executor := newFakeExecutor()
	ctx := context.Background()

	sqls := []string{"DROP TABLE unknown_table"}

	result := executor.Execute(ctx, sqls)
	if result.ExecutorErr == "" {
		t.Fatalf("execute should fail, call execute.Open first")
	}

	executor.Open(ctx, "test_keyspace")
	defer executor.Close()

	result = executor.Execute(ctx, sqls)
	if result.ExecutorErr == "" {
		t.Fatalf("execute should fail, ddl does not introduce any table schema change")
	}
}
