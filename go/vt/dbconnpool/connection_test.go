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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
)

// TestExecuteStreamFetchCarriesOKPacket verifies that ExecuteStreamFetch forwards
// the OK-packet RowsAffected and InsertID to the callback for a query that
// produces no result set (e.g. a CALL of a procedure that performs DML), matching
// the buffered ExecuteFetch path.
func TestExecuteStreamFetchCarriesOKPacket(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	const query = "CALL sp_insert()"
	db.AddQuery(query, &sqltypes.Result{
		RowsAffected:    7,
		InsertID:        99,
		InsertIDChanged: true,
	})

	conn, err := NewDBConnection(t.Context(), dbconfigs.New(db.ConnParams()))
	require.NoError(t, err)
	defer conn.Close()

	got := &sqltypes.Result{}
	err = conn.ExecuteStreamFetch(query, func(r *sqltypes.Result) error {
		got.RowsAffected += r.RowsAffected
		if r.InsertIDChanged {
			got.InsertID = r.InsertID
			got.InsertIDChanged = true
		}
		return nil
	}, func() *sqltypes.Result { return &sqltypes.Result{} }, 4096)
	require.NoError(t, err)

	assert.EqualValues(t, 7, got.RowsAffected, "streamed OK packet must carry RowsAffected to the callback")
	assert.EqualValues(t, 99, got.InsertID, "streamed OK packet must carry InsertID to the callback")
	assert.True(t, got.InsertIDChanged)
}
