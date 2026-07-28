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
	"vitess.io/vitess/go/sqltypes"
)

// NewStreamResponseChunker wraps a streaming callback so that row-returning
// result packets larger than --stream-buffer-size are split into
// --stream-buffer-size'd packets, and repeated field metadata (each shard of
// a scatter query leads with its own fields) is forwarded only once. This
// keeps the packet sizes of the gRPC streaming API independent of primitives
// that emit their complete result as one packet (e.g. an in-memory sort).
//
// Packets are never buffered across callbacks: everything a packet carries is
// delivered to the callback before the wrapper returns. Tailing streams (e.g.
// a message stream, which trickles rows for the lifetime of the stream) and
// streams that end in an error therefore deliver every row they produced.
// Streams whose first packet has no fields carry OK packets (e.g. DML) and
// pass through unchanged.
//
// The returned callback is not safe for concurrent use; the executor already
// serializes streaming callbacks.
func NewStreamResponseChunker(callback func(*sqltypes.Result) error) func(*sqltypes.Result) error {
	return newStreamResponseChunker(streamBufferSize, callback)
}

func newStreamResponseChunker(size int, callback func(*sqltypes.Result) error) func(*sqltypes.Result) error {
	firstPacket := true
	okOnly := false
	fieldsSent := false

	return func(qr *sqltypes.Result) error {
		if firstPacket {
			firstPacket = false
			okOnly = len(qr.Fields) == 0
		}
		if okOnly {
			return callback(qr)
		}

		// Forward the field metadata once per stream. The packet is copied
		// before stripping repeated fields: it belongs to the producing
		// stream, which may reuse it.
		if len(qr.Fields) > 0 {
			if fieldsSent {
				stripped := *qr
				stripped.Fields = nil
				qr = &stripped
			}
			fieldsSent = true
		}

		total := 0
		for _, row := range qr.Rows {
			for _, col := range row {
				total += col.Len()
			}
		}
		if total <= size {
			return callback(qr)
		}

		// Split the oversized packet. The first chunk carries the packet's
		// fields and OK data (RowsAffected, InsertID, Info, ...), the rest
		// carry rows only.
		head := *qr
		emitted := false
		emit := func(rows []sqltypes.Row) error {
			if !emitted {
				emitted = true
				head.Rows = rows
				return callback(&head)
			}
			return callback(&sqltypes.Result{Rows: rows})
		}
		var rows []sqltypes.Row
		byteCount := 0
		for _, row := range qr.Rows {
			rows = append(rows, row)
			for _, col := range row {
				byteCount += col.Len()
			}
			if byteCount >= size {
				if err := emit(rows); err != nil {
					return err
				}
				rows = nil
				byteCount = 0
			}
		}
		if len(rows) > 0 {
			return emit(rows)
		}
		return nil
	}
}

// NewStreamExecuteMultiChunker applies NewStreamResponseChunker to each
// statement's result stream of a StreamExecuteMulti call. When a source
// packet is split, only the first chunk of a statement's first packet keeps
// firstPacket set, and every chunk carries the statement's more flag. Error
// responses pass through unchanged.
func NewStreamExecuteMultiChunker(send func(qr sqltypes.QueryResponse, more bool, firstPacket bool) error) func(qr sqltypes.QueryResponse, more bool, firstPacket bool) error {
	return newStreamExecuteMultiChunker(streamBufferSize, send)
}

func newStreamExecuteMultiChunker(size int, send func(qr sqltypes.QueryResponse, more bool, firstPacket bool) error) func(qr sqltypes.QueryResponse, more bool, firstPacket bool) error {
	var chunked func(*sqltypes.Result) error
	var more bool
	first := true

	forward := func(result *sqltypes.Result) error {
		isFirst := first
		first = false
		return send(sqltypes.QueryResponse{QueryResult: result}, more, isFirst)
	}

	return func(qr sqltypes.QueryResponse, qrMore bool, firstPacket bool) error {
		if firstPacket {
			first = true
			chunked = newStreamResponseChunker(size, forward)
		}
		more = qrMore
		if qr.QueryError != nil {
			isFirst := first
			first = false
			return send(qr, qrMore, isFirst)
		}
		return chunked(qr.QueryResult)
	}
}
