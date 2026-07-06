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

func collectChunker(size int) (chunked func(*sqltypes.Result) error, results *[]*sqltypes.Result) {
	results = &[]*sqltypes.Result{}
	chunked = newStreamResponseChunker(size, func(qr *sqltypes.Result) error {
		*results = append(*results, qr)
		return nil
	})
	return chunked, results
}

func TestStreamResponseChunkerForwardsPacketsImmediately(t *testing.T) {
	// Packets that fit the chunk size are forwarded as-is from within the
	// callback. Nothing may be buffered across callbacks: a message stream
	// (stream * from msg) trickles rows for the lifetime of the stream, and a
	// buffered row would not be delivered until the buffer fills, which for a
	// message stream is never.
	chunked, results := collectChunker(1000)

	require.NoError(t, chunked(&sqltypes.Result{Fields: chunkerFields}))
	utils.MustMatch(t, []*sqltypes.Result{{Fields: chunkerFields}}, *results)

	require.NoError(t, chunked(&sqltypes.Result{Rows: [][]sqltypes.Value{row("a")}}))
	utils.MustMatch(t, []*sqltypes.Result{
		{Fields: chunkerFields},
		{Rows: [][]sqltypes.Value{row("a")}},
	}, *results)

	require.NoError(t, chunked(&sqltypes.Result{Rows: [][]sqltypes.Value{row("b")}}))
	utils.MustMatch(t, []*sqltypes.Result{
		{Fields: chunkerFields},
		{Rows: [][]sqltypes.Value{row("a")}},
		{Rows: [][]sqltypes.Value{row("b")}},
	}, *results)
}

func TestStreamResponseChunkerSplitsLargePackets(t *testing.T) {
	// A single oversized packet, as emitted by an in-memory sort, is split at
	// the chunk size instead of being forwarded as one giant gRPC message. The
	// packet's fields and OK data ride on the first chunk only.
	chunked, results := collectChunker(8)

	require.NoError(t, chunked(&sqltypes.Result{
		Fields:       chunkerFields,
		Rows:         [][]sqltypes.Value{row("aaaa"), row("bbbb"), row("cccc"), row("dddd"), row("eeee")},
		RowsAffected: 5,
	}))

	want := []*sqltypes.Result{
		{Fields: chunkerFields, Rows: [][]sqltypes.Value{row("aaaa"), row("bbbb")}, RowsAffected: 5},
		{Rows: [][]sqltypes.Value{row("cccc"), row("dddd")}},
		{Rows: [][]sqltypes.Value{row("eeee")}},
	}
	utils.MustMatch(t, want, *results)
}

func TestStreamResponseChunkerDedupsScatterFields(t *testing.T) {
	// In a scatter query every shard stream leads with its own fields packet,
	// and with the coalesced tablet stream format the rows ride along in it;
	// only the first stream's fields are forwarded.
	chunked, results := collectChunker(1000)

	require.NoError(t, chunked(&sqltypes.Result{Fields: chunkerFields, Rows: [][]sqltypes.Value{row("a")}}))
	require.NoError(t, chunked(&sqltypes.Result{Fields: chunkerFields, Rows: [][]sqltypes.Value{row("b")}}))

	want := []*sqltypes.Result{
		{Fields: chunkerFields, Rows: [][]sqltypes.Value{row("a")}},
		{Rows: [][]sqltypes.Value{row("b")}},
	}
	utils.MustMatch(t, want, *results)
}

func TestStreamResponseChunkerOKPacketPassthrough(t *testing.T) {
	// A stream whose first packet has no fields carries OK packets (e.g.
	// DML); those pass through unchanged, keeping fields like
	// SessionStateChanges intact.
	chunked, results := collectChunker(1000)

	ok := &sqltypes.Result{
		RowsAffected:        3,
		InsertID:            42,
		SessionStateChanges: "state",
	}
	require.NoError(t, chunked(ok))

	utils.MustMatch(t, []*sqltypes.Result{ok}, *results)
}

func TestStreamResponseChunkerDoesNotMutateSource(t *testing.T) {
	// Stripping duplicate fields and splitting must not modify the caller's
	// result: the result belongs to the producing stream, which may reuse it.
	chunked, _ := collectChunker(8)

	first := &sqltypes.Result{Fields: chunkerFields, Rows: [][]sqltypes.Value{row("a")}}
	require.NoError(t, chunked(first))

	second := &sqltypes.Result{
		Fields: chunkerFields,
		Rows:   [][]sqltypes.Value{row("aaaa"), row("bbbb"), row("cccc")},
	}
	require.NoError(t, chunked(second))

	utils.MustMatch(t, &sqltypes.Result{Fields: chunkerFields, Rows: [][]sqltypes.Value{row("a")}}, first)
	utils.MustMatch(t, &sqltypes.Result{
		Fields: chunkerFields,
		Rows:   [][]sqltypes.Value{row("aaaa"), row("bbbb"), row("cccc")},
	}, second)
}

type multiPacket struct {
	qr          sqltypes.QueryResponse
	more        bool
	firstPacket bool
}

func TestStreamExecuteMultiChunker(t *testing.T) {
	var got []multiPacket
	callback := NewStreamExecuteMultiChunker(func(qr sqltypes.QueryResponse, more bool, firstPacket bool) error {
		got = append(got, multiPacket{qr: qr, more: more, firstPacket: firstPacket})
		return nil
	})

	// Statement 1: fields, then a row.
	require.NoError(t, callback(sqltypes.QueryResponse{QueryResult: &sqltypes.Result{Fields: chunkerFields}}, true, true))
	require.NoError(t, callback(sqltypes.QueryResponse{QueryResult: &sqltypes.Result{Rows: [][]sqltypes.Value{row("a")}}}, true, false))
	// Statement 2: a single combined packet; its fields are forwarded again
	// because it is a new statement.
	require.NoError(t, callback(sqltypes.QueryResponse{QueryResult: &sqltypes.Result{Fields: chunkerFields, Rows: [][]sqltypes.Value{row("b")}}}, false, true))

	want := []multiPacket{
		{qr: sqltypes.QueryResponse{QueryResult: &sqltypes.Result{Fields: chunkerFields}}, more: true, firstPacket: true},
		{qr: sqltypes.QueryResponse{QueryResult: &sqltypes.Result{Rows: [][]sqltypes.Value{row("a")}}}, more: true, firstPacket: false},
		{qr: sqltypes.QueryResponse{QueryResult: &sqltypes.Result{Fields: chunkerFields, Rows: [][]sqltypes.Value{row("b")}}}, more: false, firstPacket: true},
	}
	utils.MustMatch(t, want, got)
}

func TestStreamExecuteMultiChunkerSplitKeepsFlags(t *testing.T) {
	// When one source packet splits into several chunks, only the first chunk
	// of the statement's first packet keeps firstPacket=true, and all chunks
	// carry the statement's more flag.
	var got []multiPacket
	callback := newStreamExecuteMultiChunker(8, func(qr sqltypes.QueryResponse, more bool, firstPacket bool) error {
		got = append(got, multiPacket{qr: qr, more: more, firstPacket: firstPacket})
		return nil
	})

	require.NoError(t, callback(sqltypes.QueryResponse{QueryResult: &sqltypes.Result{
		Fields: chunkerFields,
		Rows:   [][]sqltypes.Value{row("aaaa"), row("bbbb"), row("cccc")},
	}}, true, true))

	want := []multiPacket{
		{qr: sqltypes.QueryResponse{QueryResult: &sqltypes.Result{Fields: chunkerFields, Rows: [][]sqltypes.Value{row("aaaa"), row("bbbb")}}}, more: true, firstPacket: true},
		{qr: sqltypes.QueryResponse{QueryResult: &sqltypes.Result{Rows: [][]sqltypes.Value{row("cccc")}}}, more: true, firstPacket: false},
	}
	utils.MustMatch(t, want, got)
}

func TestStreamExecuteMultiChunkerError(t *testing.T) {
	var got []multiPacket
	callback := NewStreamExecuteMultiChunker(func(qr sqltypes.QueryResponse, more bool, firstPacket bool) error {
		got = append(got, multiPacket{qr: qr, more: more, firstPacket: firstPacket})
		return nil
	})

	// Statement 1 succeeds, statement 2 fails before producing results. The
	// error passes through unchanged and statement 1's rows were already
	// delivered from within their callback.
	require.NoError(t, callback(sqltypes.QueryResponse{QueryResult: &sqltypes.Result{Fields: chunkerFields, Rows: [][]sqltypes.Value{row("a")}}}, true, true))
	queryErr := sqltypes.QueryResponse{QueryError: sqltypes.ErrIncompatibleTypeCast}
	require.NoError(t, callback(queryErr, false, true))

	want := []multiPacket{
		{qr: sqltypes.QueryResponse{QueryResult: &sqltypes.Result{Fields: chunkerFields, Rows: [][]sqltypes.Value{row("a")}}}, more: true, firstPacket: true},
		{qr: queryErr, more: false, firstPacket: true},
	}
	utils.MustMatch(t, want, got)
}
