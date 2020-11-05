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

package vexec

import (
	"context"
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/stretchr/testify/assert"
)

const (
	tWorkflow = "myworkflow"
	tKeyspace = "mykeyspace"
)

func TestNewTabletVExec(t *testing.T) {
	vx := NewTabletVExec(tWorkflow, tKeyspace)
	assert.NotNil(t, vx)
	assert.Equal(t, vx.Workflow, tWorkflow)
	assert.Equal(t, vx.Keyspace, tKeyspace)
}

func TestAnalyzeQuerySelect1(t *testing.T) {
	query := `select migration_status, strategy from _vt.schema_migrations where migration_uuid='123'`
	vx := NewTabletVExec(tWorkflow, tKeyspace)
	err := vx.AnalyzeQuery(context.Background(), query)
	assert.NoError(t, err)

	assert.Equal(t, vx.Query, query)
	assert.Equal(t, vx.TableName, "_vt.schema_migrations")

	_, ok := vx.WhereCols["migration_uuid"]
	assert.True(t, ok)
	_, ok = vx.WhereCols["strategy"]
	assert.False(t, ok)

	_, ok = vx.UpdateCols["strategy"]
	assert.False(t, ok)
}
func TestAnalyzeQuerySelect2(t *testing.T) {
	query := `select migration_status, strategy from _vt.schema_migrations where migration_uuid='123' or requested_timestamp<now()`
	vx := NewTabletVExec(tWorkflow, tKeyspace)
	err := vx.AnalyzeQuery(context.Background(), query)
	assert.NoError(t, err)

	assert.Equal(t, vx.Query, query)
	assert.Equal(t, vx.TableName, "_vt.schema_migrations")

	_, ok := vx.WhereCols["migration_uuid"]
	assert.False(t, ok)
	_, ok = vx.WhereCols["requested_timestamp"]
	assert.False(t, ok)
	_, ok = vx.WhereCols["strategy"]
	assert.False(t, ok)

	_, ok = vx.UpdateCols["strategy"]
	assert.False(t, ok)
}

func TestAnalyzeQueryUpdate1(t *testing.T) {
	query := `update _vt.schema_migrations set migration_status='running', liveness_timestamp=now() where migration_uuid='123' and requested_timestamp<now() and strategy='pt-osc'`
	vx := NewTabletVExec(tWorkflow, tKeyspace)
	err := vx.AnalyzeQuery(context.Background(), query)
	assert.NoError(t, err)

	assert.Equal(t, vx.Query, query)
	assert.Equal(t, vx.TableName, "_vt.schema_migrations")

	_, ok := vx.WhereCols["migration_uuid"]
	assert.True(t, ok)
	_, ok = vx.WhereCols["requested_timestamp"]
	assert.False(t, ok)
	_, ok = vx.WhereCols["strategy"]
	assert.True(t, ok)

	_, ok = vx.UpdateCols["migration_status"]
	assert.True(t, ok)
	_, ok = vx.UpdateCols["strategy"]
	assert.False(t, ok)
	_, ok = vx.UpdateCols["liveness_timestamp"]
	assert.False(t, ok)
}

func TestAnalyzeQueryInsert1(t *testing.T) {
	query := `insert into _vt.schema_migrations
		(migration_uuid, migration_status, count, liveness_timestamp) values
		('abc123', 'running', 5, now())
		`
	vx := NewTabletVExec(tWorkflow, tKeyspace)
	err := vx.AnalyzeQuery(context.Background(), query)
	assert.NoError(t, err)

	assert.Equal(t, vx.Query, query)
	assert.Equal(t, vx.TableName, "_vt.schema_migrations")

	_, ok := vx.InsertCols["migration_uuid"]
	assert.True(t, ok)
	_, ok = vx.InsertCols["count"]
	assert.True(t, ok)
	_, ok = vx.InsertCols["requested_timestamp"]
	assert.False(t, ok) // column does not exist
	_, ok = vx.InsertCols["liveness_timestamp"]
	assert.False(t, ok) // because it's not a literal

	var val string
	val, err = vx.ColumnStringVal(vx.InsertCols, "migration_uuid")
	assert.NoError(t, err)
	assert.Equal(t, "abc123", val)

	val, err = vx.ColumnStringVal(vx.InsertCols, "count")
	assert.NoError(t, err)
	assert.Equal(t, "5", val)

	_, err = vx.ColumnStringVal(vx.InsertCols, "liveness_timestamp")
	assert.Error(t, err)

	vx.SetColumnStringVal(vx.InsertCols, "migration_uuid", "other")
	val, err = vx.ColumnStringVal(vx.InsertCols, "migration_uuid")
	assert.NoError(t, err)
	assert.Equal(t, "other", val)

	vx.SetColumnStringVal(vx.InsertCols, "liveness_timestamp", "another")
	val, err = vx.ColumnStringVal(vx.InsertCols, "liveness_timestamp")
	assert.NoError(t, err)
	assert.Equal(t, "another", val)
}

func TestAnalyzeQueryInsert2(t *testing.T) {
	query := `insert into _vt.schema_migrations
		(migration_uuid, migration_status, count, liveness_timestamp) values
		('abc123', 'running', 5, now())
		`
	vx := NewTabletVExec(tWorkflow, tKeyspace)
	err := vx.AnalyzeQuery(context.Background(), query)
	assert.NoError(t, err)

	assert.Equal(t, vx.Query, query)

	newVal := vx.ToStringVal("newval")
	err = vx.ReplaceInsertColumnVal("no_such_column", newVal)
	assert.Error(t, err)
	assert.Equal(t, ErrColumNotFound, err)
	assert.Equal(t, vx.Query, query)

	err = vx.ReplaceInsertColumnVal("migration_uuid", newVal)
	assert.NoError(t, err)
	assert.NotEqual(t, vx.Query, query)

	err = vx.ReplaceInsertColumnVal("liveness_timestamp", newVal)
	assert.NoError(t, err)
	assert.NotEqual(t, vx.Query, query)

	assert.Equal(t, vx.TableName, "_vt.schema_migrations")

	testVals := func() {
		_, ok := vx.InsertCols["migration_uuid"]
		assert.True(t, ok)
		_, ok = vx.InsertCols["count"]
		assert.True(t, ok)
		_, ok = vx.InsertCols["requested_timestamp"]
		assert.False(t, ok) // column does not exist
		_, ok = vx.InsertCols["liveness_timestamp"]
		assert.True(t, ok) // because it's not a literal

		var val string
		val, err = vx.ColumnStringVal(vx.InsertCols, "migration_uuid")
		assert.NoError(t, err)
		assert.Equal(t, "newval", val)

		val, err = vx.ColumnStringVal(vx.InsertCols, "count")
		assert.NoError(t, err)
		assert.Equal(t, "5", val)

		val, err = vx.ColumnStringVal(vx.InsertCols, "liveness_timestamp")
		assert.NoError(t, err)
		assert.Equal(t, "newval", val)
	}
	testVals()
	rewrittenQuery := sqlparser.String(vx.Stmt)

	vx = NewTabletVExec(tWorkflow, tKeyspace)
	err = vx.AnalyzeQuery(context.Background(), rewrittenQuery)
	assert.NoError(t, err)
	assert.Equal(t, vx.Query, rewrittenQuery)
	testVals()
}

func TestAnalyzeQueryInsert3(t *testing.T) {
	query := `insert into _vt.schema_migrations
		(migration_uuid, migration_status, count, liveness_timestamp) values
		('abc123', 'running', 5, now())
		`
	vx := NewTabletVExec(tWorkflow, tKeyspace)
	err := vx.AnalyzeQuery(context.Background(), query)
	assert.NoError(t, err)

	assert.Equal(t, vx.Query, query)

	newVal := vx.ToStringVal("newval")
	err = vx.AddOrReplaceInsertColumnVal("a_new_column", newVal)
	assert.NoError(t, err)
	assert.NotEqual(t, vx.Query, query)
	assert.Contains(t, vx.Query, "a_new_column")
	assert.Contains(t, vx.Query, "newval")

	err = vx.AddOrReplaceInsertColumnVal("migration_uuid", newVal)
	assert.NoError(t, err)
	assert.NotEqual(t, vx.Query, query)

	err = vx.AddOrReplaceInsertColumnVal("liveness_timestamp", newVal)
	assert.NoError(t, err)
	assert.NotEqual(t, vx.Query, query)

	assert.Equal(t, vx.TableName, "_vt.schema_migrations")

	testVals := func() {
		_, ok := vx.InsertCols["migration_uuid"]
		assert.True(t, ok)
		_, ok = vx.InsertCols["count"]
		assert.True(t, ok)
		_, ok = vx.InsertCols["requested_timestamp"]
		assert.False(t, ok) // column does not exist
		_, ok = vx.InsertCols["liveness_timestamp"]
		assert.True(t, ok) // because it's not a literal
		_, ok = vx.InsertCols["a_new_column"]
		assert.True(t, ok)

		var val string
		val, err = vx.ColumnStringVal(vx.InsertCols, "migration_uuid")
		assert.NoError(t, err)
		assert.Equal(t, "newval", val)

		val, err = vx.ColumnStringVal(vx.InsertCols, "count")
		assert.NoError(t, err)
		assert.Equal(t, "5", val)

		val, err = vx.ColumnStringVal(vx.InsertCols, "liveness_timestamp")
		assert.NoError(t, err)
		assert.Equal(t, "newval", val)

		val, err = vx.ColumnStringVal(vx.InsertCols, "a_new_column")
		assert.NoError(t, err)
		assert.Equal(t, "newval", val)
	}
	testVals()
	rewrittenQuery := sqlparser.String(vx.Stmt)

	vx = NewTabletVExec(tWorkflow, tKeyspace)
	err = vx.AnalyzeQuery(context.Background(), rewrittenQuery)
	assert.NoError(t, err)
	assert.Equal(t, vx.Query, rewrittenQuery)
	testVals()
}
