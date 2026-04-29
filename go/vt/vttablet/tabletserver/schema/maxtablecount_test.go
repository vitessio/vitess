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
	// DROP TABLE and CREATE TABLE effects simulated step-by-step so a batch
	// is rejected only if the running count actually exceeds the limit at
	// some point during execution.

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

	// Worst-case parseFailures handling: vttablet cannot tell what an
	// unparseable statement actually is, so each one is treated as a
	// potential CREATE TABLE.

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

	t.Run("ordered parse failure before drop rejects at limit", func(t *testing.T) {
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimitForQueries(se, parser, []string{
			"vitess parser cannot parse this",
			"drop table a",
		})
		require.Error(t, err)
		assert.ErrorContains(t, err, "schema engine table limit of 2 would be exceeded")
	})

	t.Run("ordered parse failure after drop uses freed slot", func(t *testing.T) {
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		withLimit(t, 2)

		err := CheckCreateTableLimitForQueries(se, parser, []string{
			"drop table a",
			"vitess parser cannot parse this",
		})
		assert.NoError(t, err)
	})
}
