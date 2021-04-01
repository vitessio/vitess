/*
Copyright 2020 The Vitess Authors.

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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
)

func getTestSchemaEngine(t *testing.T) (*Engine, *fakesqldb.DB, func()) {
	db := fakesqldb.New(t)
	db.AddQuery("select unix_timestamp()", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"t",
		"int64"),
		"1427325876",
	))
	db.AddQueryPattern(baseShowTablesPattern, &sqltypes.Result{})
	db.AddQuery(mysql.BaseShowPrimary, &sqltypes.Result{})
	AddFakeInnoDBReadRowsResult(db, 1)
	se := newEngine(10, 10*time.Second, 10*time.Second, db)
	require.NoError(t, se.Open())
	cancel := func() {
		defer db.Close()
		defer se.Close()
	}
	return se, db, cancel
}
