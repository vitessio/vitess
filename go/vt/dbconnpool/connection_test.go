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

package dbconnpool

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/vt/dbconfigs"
)

// A connection error during ExecuteFetchMulti must mark the connection closed,
// like ExecuteFetch does, so a pool never recycles a dead connection whose
// IsClosed still reports false.
func TestDBConnectionExecuteFetchMultiConnError(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := dbconfigs.New(db.ConnParams())
	conn, err := NewDBConnection(t.Context(), params)
	require.NoError(t, err)
	defer conn.Close()

	sql := "select intval from test_table"
	db.AddRejectedQuery(sql, sqlerror.NewSQLError(sqlerror.CRServerLost, sqlerror.SSUnknownSQLState, "Lost connection to MySQL server during query"))

	_, _, err = conn.ExecuteFetchMulti(sql, 10, false)
	require.Error(t, err)
	require.ErrorContains(t, err, "Lost connection to MySQL server during query")
	require.True(t, conn.IsClosed(), "a connection error must mark the connection closed so it is not reused")
}
