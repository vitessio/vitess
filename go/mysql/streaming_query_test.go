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

package mysql

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// TestExecuteStreamFetchOKPacket verifies that a streaming query which returns an
// OK packet instead of a result set (e.g. a CALL of a procedure that performs DML)
// exposes the OK-packet RowsAffected and InsertID via StreamOKResult. This mirrors
// the buffered ExecuteFetch path, which builds the same Result from the OK packet.
func TestExecuteStreamFetchOKPacket(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	wg := sync.WaitGroup{}
	var streamErr error
	var okRes *sqltypes.Result
	wg.Go(func() {
		streamErr = cConn.ExecuteStreamFetch("CALL sp_insert()")
		if streamErr != nil {
			return
		}
		okRes = cConn.StreamOKResult()
	})

	// The server reads the COM_QUERY and responds with an OK packet carrying
	// RowsAffected, InsertID and Info but no result set.
	data, err := sConn.readEphemeralPacket()
	require.NoError(t, err)
	require.EqualValues(t, ComQuery, data[0])
	sConn.recycleReadPacket()
	require.NoError(t, sConn.writeOKPacket(&PacketOK{
		affectedRows: 7,
		lastInsertID: 99,
	}))

	wg.Wait()
	require.NoError(t, streamErr)
	require.NotNil(t, okRes, "streaming OK packet must be exposed via StreamOKResult")
	assert.EqualValues(t, 7, okRes.RowsAffected)
	assert.EqualValues(t, 99, okRes.InsertID)
	assert.True(t, okRes.InsertIDChanged)
}

// TestExecuteStreamFetchNoOKResultForRows verifies that a streaming query which
// returns a result set leaves StreamOKResult nil, so a later OK-packet query on a
// recycled connection cannot observe a stale result.
func TestExecuteStreamFetchNoOKResultForRows(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	result := &sqltypes.Result{
		Fields: []*querypb.Field{{Type: querypb.Type_INT64, Name: "id"}},
		Rows:   [][]sqltypes.Value{{sqltypes.MakeTrusted(querypb.Type_INT64, []byte("1"))}},
	}

	wg := sync.WaitGroup{}
	var streamErr error
	var okRes *sqltypes.Result
	wg.Go(func() {
		streamErr = cConn.ExecuteStreamFetch("select id from t")
		if streamErr != nil {
			return
		}
		okRes = cConn.StreamOKResult()
		cConn.CloseResult()
	})

	handler := testHandler{result: result}
	require.True(t, sConn.handleNextCommand(&handler))

	wg.Wait()
	require.NoError(t, streamErr)
	assert.Nil(t, okRes, "a row-returning streaming query must not expose an OK result")
}
