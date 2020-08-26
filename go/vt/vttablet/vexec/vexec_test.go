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

func TestAnalyzeQuery1(t *testing.T) {
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
func TestAnalyzeQuery2(t *testing.T) {
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

func TestAnalyzeQuery3(t *testing.T) {
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
