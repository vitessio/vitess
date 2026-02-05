/*
Copyright 2024 The Vitess Authors.

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

package mysqlctl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
)

func TestProcessCanDisableRedoLog(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SELECT variable_value FROM performance_schema.global_status WHERE variable_name = 'innodb_redo_log_enabled'", sqltypes.MakeTestResult(sqltypes.MakeTestFields("field1", "varchar"), "val1"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	res, err := testMysqld.ProcessCanDisableRedoLog(context.Background())
	assert.NoError(t, err)
	assert.True(t, res)

	db.AddQuery("SELECT variable_value FROM performance_schema.global_status WHERE variable_name = 'innodb_redo_log_enabled'", &sqltypes.Result{})
	res, err = testMysqld.ProcessCanDisableRedoLog(context.Background())
	assert.Error(t, err)
	assert.False(t, res)
}
