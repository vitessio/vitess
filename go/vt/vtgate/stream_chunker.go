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
// results are reshaped into --stream-buffer-size'd packets: the field
// metadata is delivered as its own leading packet, small row packets are
// coalesced and large ones are split. This keeps the packet sizes of the gRPC
// streaming API independent of how the underlying primitives chunk their
// output: a merge sort emits single-row packets and an in-memory sort emits
// its complete result as one packet, neither of which is a reasonable gRPC
// message. Streams whose first packet has no fields carry OK packets (e.g.
// DML) and pass through unchanged.
//
// The returned flush function must be called after the streaming call
// completes without error to deliver the buffered rows.
//
// The returned callbacks are not safe for concurrent use; the executor
// already serializes streaming callbacks.
func NewStreamResponseChunker(callback func(*sqltypes.Result) error) (chunked func(*sqltypes.Result) error, flush func() error) {
	return newStreamResponseChunker(streamBufferSize, callback)
}

func newStreamResponseChunker(size int, callback func(*sqltypes.Result) error) (chunked func(*sqltypes.Result) error, flush func() error) {
	firstPacket := true
	okOnly := false
	fieldsSent := false
	seenResults := false
	byteCount := 0
	result := &sqltypes.Result{}

	chunked = func(qr *sqltypes.Result) error {
		if firstPacket {
			firstPacket = false
			okOnly = len(qr.Fields) == 0
		}
		if okOnly {
			seenResults = true
			return callback(qr)
		}

		// Carry the OK-packet data over to the next flushed packet so
		// statements that return one (e.g. a CALL of a procedure that
		// performs DML) report them to the client. The InsertID handling
		// mirrors Result.AppendResult.
		result.RowsAffected += qr.RowsAffected
		if qr.InsertIDUpdated() {
			result.InsertID = qr.InsertID
			result.InsertIDChanged = true
		}
		if qr.Info != "" {
			result.Info = qr.Info
		}
		if len(qr.Fields) > 0 {
			result.Fields = qr.Fields
			if !fieldsSent {
				fieldsSent = true
				if err := callback(qr.Metadata()); err != nil {
					return err
				}
				seenResults = true
			}
		}
		for _, row := range qr.Rows {
			result.Rows = append(result.Rows, row)
			for _, col := range row {
				byteCount += col.Len()
			}
			if byteCount >= size {
				err := callback(result)
				seenResults = true
				result = &sqltypes.Result{}
				byteCount = 0
				if err != nil {
					return err
				}
			}
		}
		return nil
	}

	flush = func() error {
		if len(result.Rows) > 0 || !seenResults {
			return callback(result)
		}
		return nil
	}
	return chunked, flush
}

// NewStreamExecuteMultiChunker applies NewStreamResponseChunker to each
// statement's result stream of a StreamExecuteMulti call, preserving the
// per-packet more and firstPacket flags. Error responses pass through
// unchanged. The returned flush function must be called after the streaming
// call completes without error to deliver the last statement's buffered rows.
func NewStreamExecuteMultiChunker(send func(qr sqltypes.QueryResponse, more bool, firstPacket bool) error) (callback func(qr sqltypes.QueryResponse, more bool, firstPacket bool) error, flush func() error) {
	var chunked func(*sqltypes.Result) error
	var flushCurrent func() error
	var more bool
	first := true

	forward := func(result *sqltypes.Result) error {
		isFirst := first
		first = false
		return send(sqltypes.QueryResponse{QueryResult: result}, more, isFirst)
	}

	callback = func(qr sqltypes.QueryResponse, qrMore bool, firstPacket bool) error {
		if firstPacket {
			// Deliver the previous statement's buffered rows before starting
			// on this statement; they still carry the previous more flag.
			if flushCurrent != nil {
				if err := flushCurrent(); err != nil {
					return err
				}
			}
			first = true
			chunked, flushCurrent = NewStreamResponseChunker(forward)
		}
		more = qrMore
		if qr.QueryError != nil {
			// The statement ended in an error; there is nothing to flush
			// for it.
			flushCurrent = nil
			isFirst := first
			first = false
			return send(qr, qrMore, isFirst)
		}
		return chunked(qr.QueryResult)
	}

	flush = func() error {
		if flushCurrent == nil {
			return nil
		}
		return flushCurrent()
	}
	return callback, flush
}
