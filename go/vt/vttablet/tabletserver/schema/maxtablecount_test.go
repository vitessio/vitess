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

	t.Run("nil engine is a no-op", func(t *testing.T) {
		err := CheckCreateTableLimit(nil, parse(t, "create table foo (id int primary key)"))
		assert.NoError(t, err)
	})

	t.Run("non-CreateTable statement is a no-op", func(t *testing.T) {
		se := openEngine(t)
		err := CheckCreateTableLimit(se, parse(t, "drop table foo"))
		assert.NoError(t, err)
	})

	t.Run("temporary table is a no-op even at zero limit", func(t *testing.T) {
		se := openEngine(t)
		originalLimit := MaxTableCount()
		SetMaxTableCount(0)
		t.Cleanup(func() { SetMaxTableCount(originalLimit) })

		err := CheckCreateTableLimit(se, parse(t, "create temporary table tmp (id int primary key)"))
		assert.NoError(t, err)
	})

	t.Run("recreating existing table is a no-op even at limit", func(t *testing.T) {
		se := openEngine(t)
		se.SetTableForTests(NewTable("existing", NoType))
		originalLimit := MaxTableCount()
		SetMaxTableCount(1)
		t.Cleanup(func() { SetMaxTableCount(originalLimit) })

		err := CheckCreateTableLimit(se, parse(t, "create table existing (id int primary key)"))
		assert.NoError(t, err)
	})

	t.Run("under limit is a no-op", func(t *testing.T) {
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		originalLimit := MaxTableCount()
		SetMaxTableCount(10)
		t.Cleanup(func() { SetMaxTableCount(originalLimit) })

		err := CheckCreateTableLimit(se, parse(t, "create table b (id int primary key)"))
		assert.NoError(t, err)
	})

	t.Run("at limit returns RESOURCE_EXHAUSTED", func(t *testing.T) {
		se := openEngine(t)
		se.ResetTablesForTests()
		se.SetTableForTests(NewTable("a", NoType))
		se.SetTableForTests(NewTable("b", NoType))
		originalLimit := MaxTableCount()
		SetMaxTableCount(2)
		t.Cleanup(func() { SetMaxTableCount(originalLimit) })

		err := CheckCreateTableLimit(se, parse(t, "create table c (id int primary key)"))
		require.Error(t, err)
		assert.ErrorContains(t, err, "schema engine table limit of 2 reached")
		assert.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
	})
}
