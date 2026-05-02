/*
Copyright 2026 The Vitess Authors.

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

package schema

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema/schematest"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// TestCheckCreateTableLimit exercises the shared gate helper directly so the
// behavior is verified independently of any RPC wiring. The QueryExecutor and
// TabletManager paths both delegate to this helper.
func TestCheckCreateTableLimit(t *testing.T) {
	parser := sqlparser.NewTestParser()

	parse := func(t *testing.T, sql string) sqlparser.Statement {
		t.Helper()
		stmt, err := parser.Parse(sql)
		require.NoError(t, err)
		return stmt
	}

	stmts := func(t *testing.T, sqls ...string) []sqlparser.Statement {
		t.Helper()
		out := make([]sqlparser.Statement, 0, len(sqls))
		for _, s := range sqls {
			out = append(out, parse(t, s))
		}
		return out
	}

	openEngine := func(t *testing.T) *Engine {
		t.Helper()
		db := fakesqldb.New(t)
		t.Cleanup(db.Close)
		schematest.AddDefaultQueries(db)
		db.AddQuery(mysql.BaseShowTables, &sqltypes.Result{
			Fields: mysql.BaseShowTablesFields,
		})
		AddFakeInnoDBReadRowsResult(db, 0)
		se := newEngine(1*time.Second, 1*time.Second, 0, db, nil)
		require.NoError(t, se.Open())
		t.Cleanup(se.Close)
		return se
	}

	withLimit := func(t *testing.T, n int) {
		t.Helper()
		original := MaxTableCount()
		SetMaxTableCount(n)
		t.Cleanup(func() { SetMaxTableCount(original) })
	}

	t.Run("nil engine is a no-op", func(t *testing.T) {
		err := CheckCreateTableLimit(nil, stmts(t, "create table foo (id int primary key)"), 0)
		assert.NoError(t, err)
	})

	t.Run("non-CreateTable statement is a no-op", func(t *testing.T) {
		se := openEngine(t)
		err := CheckCreateTableLimit(se, stmts(t, "drop table foo"), 0)
		assert.NoError(t, err)
	})

	t.Run("temporary table is a no-op even at zero limit", func(t *testing.T) {
		se := openEngine(t)
		withLimit(t, 0)

		err := CheckCreateTableLimit(se, stmts(t, "create temporary table tmp (id int primary key)"), 0)
		assert.NoError(t, err)
	})

	t.Run("recreating existing table is a no-op even at limit", func(t *testing.T) {
		se := openEngine(t)
		se.SetTableForTests(NewTable("existing", NoType))
		withLimit(t, 1)

		err := CheckCreateTableLimit(se, stmts(t, "create table existing (id int primary key)"), 0)
		assert.NoError(t, err)
	})

	t.Run("under limit is a no-op", func(t *testing.T) {
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		withLimit(t, 10)

		err := CheckCreateTableLimit(se, stmts(t, "create table b (id int primary key)"), 0)
		assert.NoError(t, err)
	})

	t.Run("at limit returns RESOURCE_EXHAUSTED", func(t *testing.T) {
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimit(se, stmts(t, "create table c (id int primary key)"), 0)
		require.Error(t, err)
		assert.ErrorContains(t, err, "schema engine table limit of 2 reached")
		assert.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
	})

	// Batch-aware behavior: multiple statements processed in order, with
	// CREATE TABLE / CREATE VIEW / DROP TABLE / DROP VIEW effects simulated
	// step-by-step so a batch is rejected only if the running count actually
	// exceeds the limit at some point during execution.

	t.Run("batch under limit passes", func(t *testing.T) {
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		withLimit(t, 10)

		err := CheckCreateTableLimit(se, stmts(t,
			"create table b (id int primary key)",
			"create table c (id int primary key)",
		), 0)
		assert.NoError(t, err)
	})

	t.Run("batch crossing limit rejects on the offending CREATE", func(t *testing.T) {
		// With 1 existing and limit 2, a second new CREATE in the batch
		// pushes the running count to 3 and is rejected. The error names
		// the statement that breaks the limit (not the first one).
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimit(se, stmts(t,
			"create table b (id int primary key)",
			"create table c (id int primary key)",
		), 0)
		require.Error(t, err)
		assert.ErrorContains(t, err, "cannot create table")
		assert.ErrorContains(t, err, "c")
		assert.ErrorContains(t, err, "schema engine table limit of 2 reached")
		assert.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
	})

	t.Run("batch with only existing tables passes", func(t *testing.T) {
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimit(se, stmts(t,
			"create table if not exists a (id int primary key)",
			"create table b (id int primary key)",
		), 0)
		assert.NoError(t, err)
	})

	t.Run("batch dedupes duplicate names", func(t *testing.T) {
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimit(se, stmts(t,
			"create table b (id int primary key)",
			"create table if not exists b (id int primary key)",
		), 0)
		assert.NoError(t, err)
	})

	t.Run("batch with mixed temp and real only counts real", func(t *testing.T) {
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimit(se, stmts(t,
			"create temporary table tmp1 (id int primary key)",
			"create table c (id int primary key)",
		), 0)
		require.Error(t, err)
		assert.ErrorContains(t, err, "schema engine table limit of 2 reached")
	})

	t.Run("batch dedupes qualified and unqualified names", func(t *testing.T) {
		// Same table as `b` and `db.b` in a single batch — engine stores
		// by unqualified name, so both refer to the same target.
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimit(se, stmts(t,
			"create table b (id int primary key)",
			"create table if not exists db.b (id int primary key)",
		), 0)
		assert.NoError(t, err)
	})

	// Ordered simulation: DROP TABLE frees a slot for a subsequent CREATE
	// TABLE within the same batch. This is the case the user flagged.

	t.Run("drop existing then create new at limit passes", func(t *testing.T) {
		// 2 existing, limit 2. DROP TABLE old; CREATE TABLE new is fine
		// because the running count never goes above 2.
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimit(se, stmts(t,
			"drop table a",
			"create table c (id int primary key)",
		), 0)
		assert.NoError(t, err)
	})

	t.Run("drop temporary table does not free persistent table slot", func(t *testing.T) {
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimit(se, stmts(t,
			"drop temporary table if exists a",
			"create table c (id int primary key)",
		), 0)
		require.Error(t, err)
		assert.ErrorContains(t, err, "schema engine table limit of 2 reached")
	})

	t.Run("rename then recreate old name at limit rejects", func(t *testing.T) {
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		withLimit(t, 1)

		err := CheckCreateTableLimit(se, stmts(t,
			"rename table a to b",
			"create table a (id int primary key)",
		), 0)
		require.Error(t, err)
		assert.ErrorContains(t, err, "schema engine table limit of 1 reached")
	})

	t.Run("alter rename then recreate old name at limit rejects", func(t *testing.T) {
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		withLimit(t, 1)

		err := CheckCreateTableLimit(se, stmts(t,
			"alter table a rename to b",
			"create table a (id int primary key)",
		), 0)
		require.Error(t, err)
		assert.ErrorContains(t, err, "schema engine table limit of 1 reached")
	})

	t.Run("multi-target rename frees source names within batch", func(t *testing.T) {
		// Rename a→c and b→d in one statement, then create new tables
		// using the freed names a and b. Two new tables push the count to
		// 4, which fits within the limit.
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 4)

		err := CheckCreateTableLimit(se, stmts(t,
			"rename table a to c, b to d",
			"create table a (id int primary key)",
			"create table b (id int primary key)",
		), 0)
		assert.NoError(t, err)
	})

	t.Run("multi-target rename at limit rejects subsequent create on conflicting name", func(t *testing.T) {
		// After a→c, b→d, the table count is unchanged (still 2). At
		// limit 2, creating a third table must reject.
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimit(se, stmts(t,
			"rename table a to c, b to d",
			"create table e (id int primary key)",
		), 0)
		require.Error(t, err)
		assert.ErrorContains(t, err, "schema engine table limit of 2 reached")
	})

	t.Run("chain rename through intermediate name within single statement", func(t *testing.T) {
		// `RENAME TABLE a TO b, b TO c` is a chain rename: a → b, then
		// b → c. The count is unchanged at 1; a and b are both freed,
		// only c is occupied. A subsequent CREATE TABLE a must succeed
		// (running count goes to 2 within limit), but CREATE TABLE c
		// would be a no-op because c is now present.
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimit(se, stmts(t,
			"rename table a to b, b to c",
			"create table a (id int primary key)",
			"create table if not exists c (id int primary key)",
		), 0)
		assert.NoError(t, err)
	})

	t.Run("chain rename does not allow exceeding the limit", func(t *testing.T) {
		// After `RENAME TABLE a TO b, b TO c`, c is present (running
		// count 1). Creating a new table a (count 2) is fine, but a
		// subsequent CREATE TABLE b pushes the count to 3, exceeding
		// the limit of 2.
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimit(se, stmts(t,
			"rename table a to b, b to c",
			"create table a (id int primary key)",
			"create table b (id int primary key)",
		), 0)
		require.Error(t, err)
		assert.ErrorContains(t, err, "schema engine table limit of 2 reached")
	})

	t.Run("create then drop same table within batch passes when under limit", func(t *testing.T) {
		// 1 existing, limit 2. Create c (running=2), drop c (running=1),
		// create d (running=2), drop d (running=1). Running count never
		// exceeds the limit, so the batch passes.
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimit(se, stmts(t,
			"create table c (id int primary key)",
			"drop table c",
			"create table d (id int primary key)",
			"drop table d",
		), 0)
		assert.NoError(t, err)
	})

	t.Run("create that briefly exceeds limit before a drop still rejects", func(t *testing.T) {
		// 2 existing, limit 2. CREATE c would push running to 3 even
		// though a subsequent DROP would bring it back to 2. Real
		// execution would observe the over-limit state, so reject.
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimit(se, stmts(t,
			"create table c (id int primary key)",
			"drop table c",
		), 0)
		require.Error(t, err)
		assert.ErrorContains(t, err, "schema engine table limit of 2 reached")
	})

	t.Run("ordered simulation rejects when running count exceeds mid-batch", func(t *testing.T) {
		// Limit 2, 1 existing. Two CREATEs followed by a DROP would
		// briefly hit 3 tables — must reject at the second CREATE even
		// though the final cardinality (after the DROP) would be 2.
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimit(se, stmts(t,
			"create table b (id int primary key)",
			"create table c (id int primary key)",
			"drop table b",
		), 0)
		require.Error(t, err)
		assert.ErrorContains(t, err, "schema engine table limit of 2 reached")
	})

	t.Run("multi-target drop frees multiple slots", func(t *testing.T) {
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		se.SetTableForTests(NewTable("c", NoType))
		withLimit(t, 3)

		err := CheckCreateTableLimit(se, stmts(t,
			"drop table a, b",
			"create table d (id int primary key)",
			"create table e (id int primary key)",
		), 0)
		assert.NoError(t, err)
	})

	t.Run("drop of non-existent table is a no-op", func(t *testing.T) {
		// Limit 2, 2 existing. A drop of a non-existent table doesn't
		// free a slot, so a subsequent create still rejects.
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimit(se, stmts(t,
			"drop table if exists nonexistent",
			"create table c (id int primary key)",
		), 0)
		require.Error(t, err)
		assert.ErrorContains(t, err, "schema engine table limit of 2 reached")
	})

	// View handling: Engine.TableCount() includes views (everything in
	// se.tables), so the simulator must model CREATE VIEW / DROP VIEW the
	// same way it models CREATE TABLE / DROP TABLE — otherwise mixed
	// view/table batches give inconsistent answers.

	t.Run("create view at limit returns RESOURCE_EXHAUSTED", func(t *testing.T) {
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", View))
		withLimit(t, 2)

		err := CheckCreateTableLimit(se, stmts(t, "create view v as select 1 from dual"), 0)
		require.Error(t, err)
		assert.ErrorContains(t, err, "cannot create view")
		assert.ErrorContains(t, err, "v")
		assert.ErrorContains(t, err, "schema engine table limit of 2 reached")
		assert.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
	})

	t.Run("drop view then create table at limit passes", func(t *testing.T) {
		// 1 table + 1 view at limit 2. DROP VIEW frees a slot, so a
		// subsequent CREATE TABLE fits without exceeding the limit.
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("v", View))
		withLimit(t, 2)

		err := CheckCreateTableLimit(se, stmts(t,
			"drop view v",
			"create table b (id int primary key)",
		), 0)
		assert.NoError(t, err)
	})

	t.Run("create view then create table at limit-1 rejects", func(t *testing.T) {
		// 1 existing, limit 2. CREATE VIEW pushes running to 2, then
		// CREATE TABLE pushes running to 3 — must reject because the
		// view counts toward the engine's table-count limit.
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimit(se, stmts(t,
			"create view v as select 1 from dual",
			"create table b (id int primary key)",
		), 0)
		require.Error(t, err)
		assert.ErrorContains(t, err, "cannot create table")
		assert.ErrorContains(t, err, "b")
		assert.ErrorContains(t, err, "schema engine table limit of 2 reached")
	})

	t.Run("create or replace view on existing is a no-op", func(t *testing.T) {
		// CREATE OR REPLACE VIEW on a name already present must not
		// increment the count.
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("v", View))
		withLimit(t, 2)

		err := CheckCreateTableLimit(se, stmts(t, "create or replace view v as select 1 from dual"), 0)
		assert.NoError(t, err)
	})

	t.Run("multi-target drop view frees multiple slots", func(t *testing.T) {
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("v1", View))
		se.SetTableForTests(NewTable("v2", View))
		withLimit(t, 3)

		err := CheckCreateTableLimit(se, stmts(t,
			"drop view v1, v2",
			"create table b (id int primary key)",
			"create table c (id int primary key)",
		), 0)
		assert.NoError(t, err)
	})

	t.Run("drop of non-existent view is a no-op", func(t *testing.T) {
		// 2 existing, limit 2. DROP VIEW IF EXISTS on a missing name
		// frees nothing, so a subsequent CREATE TABLE still rejects.
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("v", View))
		withLimit(t, 2)

		err := CheckCreateTableLimit(se, stmts(t,
			"drop view if exists nonexistent",
			"create table b (id int primary key)",
		), 0)
		require.Error(t, err)
		assert.ErrorContains(t, err, "schema engine table limit of 2 reached")
	})

	// Synthetic "dual" handling: Engine.Open seeds se.tables with a "dual"
	// entry that does NOT correspond to a real MySQL table. TableCount()
	// excludes it, so the simulator must treat DROP / RENAME / CREATE on
	// dual as no-ops to keep the running count consistent with what mysqld
	// will actually see post-batch.

	t.Run("drop dual then create at limit rejects", func(t *testing.T) {
		// 2 existing, limit 2. DROP TABLE dual is a no-op against mysqld
		// (dual is synthetic), so it must NOT free a slot for the
		// subsequent CREATE TABLE — otherwise the simulator under-counts
		// and we exceed the limit at runtime.
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimit(se, stmts(t,
			"drop table dual",
			"create table c (id int primary key)",
		), 0)
		require.Error(t, err)
		assert.ErrorContains(t, err, "schema engine table limit of 2 reached")
	})

	t.Run("drop dual case-insensitive", func(t *testing.T) {
		// Same protection regardless of identifier case.
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimit(se, stmts(t,
			"drop table DUAL",
			"create table c (id int primary key)",
		), 0)
		require.Error(t, err)
		assert.ErrorContains(t, err, "schema engine table limit of 2 reached")
	})

	t.Run("rename dual then drop renamed then create rejects", func(t *testing.T) {
		// Renaming dual must be a no-op too. Otherwise pendingState would
		// mark the renamed name as present (and dual as dropped), letting
		// a subsequent DROP free a slot for a CREATE that should have been
		// rejected.
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimit(se, stmts(t,
			"rename table dual to renamed",
			"drop table renamed",
			"create table c (id int primary key)",
		), 0)
		require.Error(t, err)
		assert.ErrorContains(t, err, "schema engine table limit of 2 reached")
	})

	t.Run("create dual at limit is a no-op", func(t *testing.T) {
		// CREATE TABLE dual would be rejected by mysqld anyway (reserved),
		// so the simulator should not charge it against the limit.
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimit(se, stmts(t, "create table dual (id int primary key)"), 0)
		assert.NoError(t, err)
	})

	// Worst-case parseFailures handling: vttablet cannot tell what an
	// unparseable statement actually is, so each one is treated as a
	// potential CREATE TABLE / CREATE VIEW.

	t.Run("parseFailures within budget passes", func(t *testing.T) {
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		withLimit(t, 10)

		// Engine has 1 + 1 created + 2 worst-case = 4. Limit 10.
		err := CheckCreateTableLimit(se,
			stmts(t, "create table b (id int primary key)"),
			2)
		assert.NoError(t, err)
	})

	t.Run("parseFailures pushing past limit rejects", func(t *testing.T) {
		// count=1, parsed CREATE=1, parseFailures=2 => worst-case 4 tables
		// against limit 3. Reject.
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		withLimit(t, 3)

		err := CheckCreateTableLimit(se,
			stmts(t, "create table b (id int primary key)"),
			2)
		require.Error(t, err)
		assert.ErrorContains(t, err, "schema engine table limit of 3 would be exceeded")
		assert.ErrorContains(t, err, "unparseable statement")
		assert.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
	})

	t.Run("parseFailures alone can push past limit", func(t *testing.T) {
		// No parsed stmts at all, just unparseable input. count=2, limit=2,
		// parseFailures=1 => worst-case 3 > 2. Reject.
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimit(se, nil, 1)
		require.Error(t, err)
		assert.ErrorContains(t, err, "schema engine table limit of 2 would be exceeded")
		assert.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
	})

	t.Run("parseFailures with headroom passes", func(t *testing.T) {
		// count=2, limit=10, parseFailures=3 => worst-case 5 ≤ 10. OK.
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 10)

		assert.NoError(t, CheckCreateTableLimit(se, nil, 3))
	})

	t.Run("parseFailures benefits from in-batch DROP", func(t *testing.T) {
		// count=2, limit=2, drop existing, then 1 parseFailure => running=1,
		// 1 + 1 = 2 ≤ 2. OK.
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimit(se, stmts(t, "drop table a"), 1)
		assert.NoError(t, err)
	})

	t.Run("ordered create-like parse failure before drop rejects at limit", func(t *testing.T) {
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimitForQueries(se, parser, []string{
			"create table c (",
			"drop table a",
		})
		require.Error(t, err)
		assert.ErrorContains(t, err, "schema engine table limit of 2 would be exceeded")
		assert.ErrorContains(t, err, "running count: 3")
	})

	t.Run("ordered create-like parse failure after drop uses freed slot", func(t *testing.T) {
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimitForQueries(se, parser, []string{
			"drop table a",
			"create table c (",
		})
		assert.NoError(t, err)
	})

	t.Run("ordered non-create parse failures do not count against table limit", func(t *testing.T) {
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimitForQueries(se, parser, []string{
			"stop replica",
			"start replica",
		})
		assert.NoError(t, err)
	})

	t.Run("ordered temporary create parse failure does not count against table limit", func(t *testing.T) {
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimitForQueries(se, parser, []string{
			"create temporary table c (",
		})
		assert.NoError(t, err)
	})

	t.Run("ordered create-view parse failure counts against table limit", func(t *testing.T) {
		// Unparseable CREATE VIEW must be treated as a worst-case CREATE
		// the same way unparseable CREATE TABLE is — Engine.TableCount()
		// includes views.
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimitForQueries(se, parser, []string{
			"create view v as select",
		})
		require.Error(t, err)
		assert.ErrorContains(t, err, "schema engine table limit of 2 would be exceeded")
	})

	t.Run("ordered create-or-replace-view parse failure counts against table limit", func(t *testing.T) {
		// CREATE VIEW allows OR REPLACE / ALGORITHM= / DEFINER= prefixes
		// before the VIEW keyword. The token walker must see through
		// those and still treat it as a worst-case CREATE.
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimitForQueries(se, parser, []string{
			"create or replace algorithm=merge view v as select",
		})
		require.Error(t, err)
		assert.ErrorContains(t, err, "schema engine table limit of 2 would be exceeded")
	})

	t.Run("ordered create-view parse failure with definer counts against table limit", func(t *testing.T) {
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimitForQueries(se, parser, []string{
			"create definer = user@host view v as select",
		})
		require.Error(t, err)
		assert.ErrorContains(t, err, "schema engine table limit of 2 would be exceeded")
	})

	t.Run("ordered create-index parse failure does not count against table limit", func(t *testing.T) {
		// CREATE INDEX is not a table or view, so an unparseable form
		// must not be charged against the limit.
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimitForQueries(se, parser, []string{
			"create unique index idx on (",
		})
		assert.NoError(t, err)
	})

	// ParsedStatements variant: callers that have already parsed each SQL
	// piece can pass them through with nil entries marking parse failures.
	// The behavior must match CheckCreateTableLimitForQueries exactly.

	t.Run("parsed statements: create-like nil entry rejects when over limit", func(t *testing.T) {
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimitForParsedStatements(se, parser, []string{
			"create table c (",
			"drop table a",
		}, []sqlparser.Statement{
			nil,
			parse(t, "drop table a"),
		})
		require.Error(t, err)
		assert.ErrorContains(t, err, "schema engine table limit of 2 would be exceeded")
	})

	t.Run("parsed statements: create-like nil entry after drop uses freed slot", func(t *testing.T) {
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimitForParsedStatements(se, parser, []string{
			"drop table a",
			"create table c (",
		}, []sqlparser.Statement{
			parse(t, "drop table a"),
			nil,
		})
		assert.NoError(t, err)
	})

	t.Run("parsed statements: non-create nil entries do not count against table limit", func(t *testing.T) {
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimitForParsedStatements(se, parser, []string{
			"stop replica",
			"start replica",
		}, []sqlparser.Statement{
			nil,
			nil,
		})
		assert.NoError(t, err)
	})
}
