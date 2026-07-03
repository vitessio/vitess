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

package vtgate

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var chunkerFields = []*querypb.Field{
	{Name: "col", Type: sqltypes.VarChar},
}

func row(val string) []sqltypes.Value {
	return []sqltypes.Value{sqltypes.NewVarChar(val)}
}

func collectChunker(size int) (chunked func(*sqltypes.Result) error, flush func() error, results *[]*sqltypes.Result) {
	results = &[]*sqltypes.Result{}
	chunked, flush = newStreamResponseChunker(size, func(qr *sqltypes.Result) error {
		*results = append(*results, qr)
		return nil
	})
	return chunked, flush, results
}

func TestStreamResponseChunkerSplitsFieldsFromRows(t *testing.T) {
	chunked, flush, results := collectChunker(1000)

	require.NoError(t, chunked(&sqltypes.Result{
		Fields: chunkerFields,
		Rows:   [][]sqltypes.Value{row("a"), row("b")},
	}))
	require.NoError(t, flush())

	want := []*sqltypes.Result{
		{Fields: chunkerFields},
		{Fields: chunkerFields, Rows: [][]sqltypes.Value{row("a"), row("b")}},
	}
	utils.MustMatch(t, want, *results)
}

func TestStreamResponseChunkerCoalescesSmallPackets(t *testing.T) {
	// Single-row packets, as emitted by a streaming merge sort, are coalesced
	// up to the chunk size. Each row is 4 bytes; with a chunk size of 8 every
	// second row completes a chunk.
	chunked, flush, results := collectChunker(8)

	require.NoError(t, chunked(&sqltypes.Result{Fields: chunkerFields}))
	for _, val := range []string{"aaaa", "bbbb", "cccc", "dddd", "eeee"} {
		require.NoError(t, chunked(&sqltypes.Result{Rows: [][]sqltypes.Value{row(val)}}))
	}
	require.NoError(t, flush())

	want := []*sqltypes.Result{
		{Fields: chunkerFields},
		{Fields: chunkerFields, Rows: [][]sqltypes.Value{row("aaaa"), row("bbbb")}},
		{Rows: [][]sqltypes.Value{row("cccc"), row("dddd")}},
		{Rows: [][]sqltypes.Value{row("eeee")}},
	}
	utils.MustMatch(t, want, *results)
}

func TestStreamResponseChunkerSplitsLargePackets(t *testing.T) {
	// A single oversized packet, as emitted by an in-memory sort, is split at
	// the chunk size instead of being forwarded as one giant gRPC message.
	chunked, flush, results := collectChunker(8)

	require.NoError(t, chunked(&sqltypes.Result{
		Fields: chunkerFields,
		Rows:   [][]sqltypes.Value{row("aaaa"), row("bbbb"), row("cccc"), row("dddd"), row("eeee")},
	}))
	require.NoError(t, flush())

	want := []*sqltypes.Result{
		{Fields: chunkerFields},
		{Fields: chunkerFields, Rows: [][]sqltypes.Value{row("aaaa"), row("bbbb")}},
		{Rows: [][]sqltypes.Value{row("cccc"), row("dddd")}},
		{Rows: [][]sqltypes.Value{row("eeee")}},
	}
	utils.MustMatch(t, want, *results)
}

func TestStreamResponseChunkerFieldsOnlyResult(t *testing.T) {
	// A SELECT returning no rows still delivers the fields; flush must not
	// append an empty packet after it.
	chunked, flush, results := collectChunker(1000)

	require.NoError(t, chunked(&sqltypes.Result{Fields: chunkerFields}))
	require.NoError(t, flush())

	want := []*sqltypes.Result{{Fields: chunkerFields}}
	utils.MustMatch(t, want, *results)
}

func TestStreamResponseChunkerDedupsScatterFields(t *testing.T) {
	// In a scatter query every shard stream leads with its own fields; only
	// the first one is forwarded.
	chunked, flush, results := collectChunker(1000)

	require.NoError(t, chunked(&sqltypes.Result{Fields: chunkerFields, Rows: [][]sqltypes.Value{row("a")}}))
	require.NoError(t, chunked(&sqltypes.Result{Fields: chunkerFields, Rows: [][]sqltypes.Value{row("b")}}))
	require.NoError(t, flush())

	want := []*sqltypes.Result{
		{Fields: chunkerFields},
		{Fields: chunkerFields, Rows: [][]sqltypes.Value{row("a"), row("b")}},
	}
	utils.MustMatch(t, want, *results)
}

func TestStreamResponseChunkerOKPacketPassthrough(t *testing.T) {
	// A stream whose first packet has no fields carries OK packets (e.g.
	// DML); those pass through unchanged, keeping fields like
	// SessionStateChanges intact, and flush adds nothing.
	chunked, flush, results := collectChunker(1000)

	ok := &sqltypes.Result{
		RowsAffected:        3,
		InsertID:            42,
		SessionStateChanges: "state",
	}
	require.NoError(t, chunked(ok))
	require.NoError(t, flush())

	utils.MustMatch(t, []*sqltypes.Result{ok}, *results)
}

func TestStreamResponseChunkerCarriesOKDataToRows(t *testing.T) {
	// A row-returning stream whose packets carry OK data (e.g. a CALL of a
	// procedure that performs DML) reports it on the leading fields packet
	// and on the next flushed row packet.
	chunked, flush, results := collectChunker(1000)

	require.NoError(t, chunked(&sqltypes.Result{
		Fields:          chunkerFields,
		Rows:            [][]sqltypes.Value{row("a")},
		RowsAffected:    7,
		InsertID:        99,
		InsertIDChanged: true,
		Info:            "info",
	}))
	require.NoError(t, flush())

	want := []*sqltypes.Result{
		{Fields: chunkerFields, RowsAffected: 7, InsertID: 99, InsertIDChanged: true, Info: "info"},
		{Fields: chunkerFields, Rows: [][]sqltypes.Value{row("a")}, RowsAffected: 7, InsertID: 99, InsertIDChanged: true, Info: "info"},
	}
	utils.MustMatch(t, want, *results)
}

func TestStreamResponseChunkerEmptyStream(t *testing.T) {
	// If the stream produced no packets at all, flush still delivers one
	// empty result so the client sees a response.
	chunked, flush, results := collectChunker(1000)
	_ = chunked

	require.NoError(t, flush())
	utils.MustMatch(t, []*sqltypes.Result{{}}, *results)
}

type multiPacket struct {
	qr          sqltypes.QueryResponse
	more        bool
	firstPacket bool
}

func TestStreamExecuteMultiChunker(t *testing.T) {
	var got []multiPacket
	callback, flush := NewStreamExecuteMultiChunker(func(qr sqltypes.QueryResponse, more bool, firstPacket bool) error {
		got = append(got, multiPacket{qr: qr, more: more, firstPacket: firstPacket})
		return nil
	})

	// Statement 1: fields, then a row, buffered until statement 2 starts.
	require.NoError(t, callback(sqltypes.QueryResponse{QueryResult: &sqltypes.Result{Fields: chunkerFields}}, true, true))
	require.NoError(t, callback(sqltypes.QueryResponse{QueryResult: &sqltypes.Result{Rows: [][]sqltypes.Value{row("a")}}}, true, false))
	// Statement 2: a single combined packet, buffered until flush.
	require.NoError(t, callback(sqltypes.QueryResponse{QueryResult: &sqltypes.Result{Fields: chunkerFields, Rows: [][]sqltypes.Value{row("b")}}}, false, true))
	require.NoError(t, flush())

	want := []multiPacket{
		{qr: sqltypes.QueryResponse{QueryResult: &sqltypes.Result{Fields: chunkerFields}}, more: true, firstPacket: true},
		{qr: sqltypes.QueryResponse{QueryResult: &sqltypes.Result{Fields: chunkerFields, Rows: [][]sqltypes.Value{row("a")}}}, more: true, firstPacket: false},
		{qr: sqltypes.QueryResponse{QueryResult: &sqltypes.Result{Fields: chunkerFields}}, more: false, firstPacket: true},
		{qr: sqltypes.QueryResponse{QueryResult: &sqltypes.Result{Fields: chunkerFields, Rows: [][]sqltypes.Value{row("b")}}}, more: false, firstPacket: false},
	}
	utils.MustMatch(t, want, got)
}

func TestStreamExecuteMultiChunkerError(t *testing.T) {
	var got []multiPacket
	callback, flush := NewStreamExecuteMultiChunker(func(qr sqltypes.QueryResponse, more bool, firstPacket bool) error {
		got = append(got, multiPacket{qr: qr, more: more, firstPacket: firstPacket})
		return nil
	})

	// Statement 1 succeeds, statement 2 fails before producing results.
	require.NoError(t, callback(sqltypes.QueryResponse{QueryResult: &sqltypes.Result{Fields: chunkerFields, Rows: [][]sqltypes.Value{row("a")}}}, true, true))
	queryErr := sqltypes.QueryResponse{QueryError: sqltypes.ErrIncompatibleTypeCast}
	require.NoError(t, callback(queryErr, false, true))
	require.NoError(t, flush())

	want := []multiPacket{
		{qr: sqltypes.QueryResponse{QueryResult: &sqltypes.Result{Fields: chunkerFields}}, more: true, firstPacket: true},
		{qr: sqltypes.QueryResponse{QueryResult: &sqltypes.Result{Fields: chunkerFields, Rows: [][]sqltypes.Value{row("a")}}}, more: true, firstPacket: false},
		{qr: queryErr, more: false, firstPacket: true},
	}
	utils.MustMatch(t, want, got)
}
