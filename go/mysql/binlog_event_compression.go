/*
Copyright 2023 The Vitess Authors.

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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// This file contains code related to handling compression related
// events. More specifically today, compressed transaction payloads:
// See: https://dev.mysql.com/doc/refman/en/binary-log-transaction-compression.html

// Transaction Payload wire protocol fields:
// https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1codecs_1_1binary_1_1Transaction__payload.html
const (
	payloadHeaderEndMark = iota
	payloadSizeField
	payloadCompressionTypeField
	payloadUncompressedSizeField
)

// Compression algorithms that are supported (only zstd today
// in MySQL 8.0):
// https://dev.mysql.com/doc/refman/8.0/en/binary-log-transaction-compression.html
const (
	TransactionPayloadCompressionZstd = 0
	TransactionPayloadCompressionNone = 255
)

var TransactionPayloadCompressionTypes = map[uint64]string{
	TransactionPayloadCompressionZstd: "ZSTD",
	TransactionPayloadCompressionNone: "NONE",
}

// Create a reader that caches decompressors. This is used for
// smaller events that we want to handle entirely using in-memory
// buffers.
var zstdDecoder, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))

// At what size should we switch from the in-memory buffer
// decoding to streaming mode -- which is slower, but does not
// require everything be done in memory.
const zstdInMemoryDecompressorMaxSize = 128 << (10 * 2) // 128MiB

type TransactionPayload struct {
	Size             uint64
	CompressionType  uint64
	UncompressedSize uint64
	Payload          []byte
	Events           []BinlogEvent
}

// IsTransactionPayload returns true if a compressed transaction
// payload event is found (binlog_transaction_compression=ON).
func (ev binlogEvent) IsTransactionPayload() bool {
	return ev.Type() == eTransactionPayloadEvent
}

// TransactionPayload returns the BinlogEvents contained within
// the compressed transaction.
// The following event types are compressed as part of the
// transaction payload:
//
//	QUERY_EVENT = 2
//	INTVAR_EVENT = 5
//	APPEND_BLOCK_EVENT = 9
//	DELETE_FILE_EVENT = 11
//	RAND_EVENT = 13
//	USER_VAR_EVENT = 14
//	XID_EVENT = 16
//	BEGIN_LOAD_QUERY_EVENT = 17
//	EXECUTE_LOAD_QUERY_EVENT = 18
//	TABLE_MAP_EVENT = 19
//	WRITE_ROWS_EVENT_V1 = 23
//	UPDATE_ROWS_EVENT_V1 = 24
//	DELETE_ROWS_EVENT_V1 = 25
//	IGNORABLE_LOG_EVENT = 28
//	ROWS_QUERY_LOG_EVENT = 29
//	WRITE_ROWS_EVENT = 30
//	UPDATE_ROWS_EVENT = 31
//	DELETE_ROWS_EVENT = 32
//	XA_PREPARE_LOG_EVENT = 38
//	PARTIAL_UPDATE_ROWS_EVENT = 39
//
// When transaction compression is enabled, the GTID log event has
// the following fields:
// +-----------------------------------------+
// | field_type (1-9 bytes)                  |
// +-----------------------------------------+
// | field_size (1-9 bytes)                  |
// +-----------------------------------------+
// | m_payload (1 to N bytes)                |
// +-----------------------------------------+
// | field_type (1-9 bytes)                  |
// +-----------------------------------------+
// | field_size (1-9 bytes)                  |
// +-----------------------------------------+
// | m_compression_type (1 to 9 bytes)       |
// +-----------------------------------------+
// | field_type (1-9 bytes)                  |
// +-----------------------------------------+
// | field_size (1-9 bytes)                  |
// +-----------------------------------------+
// | m_uncompressed_size size (0 to 9 bytes) |
// +-----------------------------------------+
//
// We need to extract the compressed transaction payload from the GTID
// event, decompress it with zstd, and then process the internal events
// (e.g. Query and Row events) that make up the transaction.
func (ev binlogEvent) TransactionPayload(format BinlogFormat) ([]BinlogEvent, error) {
	tp := &TransactionPayload{}
	if err := tp.Decode(ev.Bytes()[format.HeaderLength:]); err != nil {
		return nil, vterrors.Wrapf(err, "error decoding transaction payload event")
	}
	return tp.Events, nil
}

// Decode decodes and decompresses the payload.
func (tp *TransactionPayload) Decode(data []byte) error {
	if err := tp.read(data); err != nil {
		return err
	}
	return tp.decode()
}

// read unmarshalls the transaction payload event into the
// TransactionPayload struct. The compressed payload itself will still
// need to be decoded -- meaning decompressing it and extracting the
// internal events.
func (tp *TransactionPayload) read(data []byte) error {
	pos := uint64(0)

	for {
		fieldType, ok := readFixedLenUint64(data[pos : pos+1])
		if !ok {
			return vterrors.New(vtrpcpb.Code_INTERNAL, "error reading field type")
		}
		pos++

		if fieldType == payloadHeaderEndMark {
			tp.Payload = data[pos:]
			return nil // we're done
		}

		fieldLen, ok := readFixedLenUint64(data[pos : pos+1])
		if !ok {
			return vterrors.New(vtrpcpb.Code_INTERNAL, "error reading field length")
		}
		pos++

		switch fieldType {
		case payloadSizeField:
			tp.Size, ok = readFixedLenUint64(data[pos : pos+fieldLen])
			if !ok {
				return vterrors.New(vtrpcpb.Code_INTERNAL, "error reading payload size")
			}
		case payloadCompressionTypeField:
			tp.CompressionType, ok = readFixedLenUint64(data[pos : pos+fieldLen])
			if !ok {
				return vterrors.New(vtrpcpb.Code_INTERNAL, "error reading compression type")
			}
		case payloadUncompressedSizeField:
			tp.UncompressedSize, ok = readFixedLenUint64(data[pos : pos+fieldLen])
			if !ok {
				return vterrors.New(vtrpcpb.Code_INTERNAL, "error reading uncompressed payload size")
			}
		}

		pos += fieldLen
	}
}

// decode decompresses the payload and extracts the internal binlog
// events.
func (tp *TransactionPayload) decode() error {
	if tp.CompressionType != TransactionPayloadCompressionZstd {
		return vterrors.New(vtrpcpb.Code_INTERNAL,
			fmt.Sprintf("TransactionPayload has unsupported compression type of %d", tp.CompressionType))
	}

	decompressedPayload, err := tp.decompress()
	decompressedPayloadLen := uint64(len(decompressedPayload))
	if err != nil {
		return vterrors.Wrapf(err, "error decompressing transaction payload")
	}

	pos := uint64(0)

	for {
		eventLenPosEnd := pos + BinlogEventLenOffset + 4
		if eventLenPosEnd > decompressedPayloadLen { // No more events in the payload
			break
		}
		eventLen := uint64(binary.LittleEndian.Uint32(decompressedPayload[pos+BinlogEventLenOffset : eventLenPosEnd]))
		if pos+eventLen > decompressedPayloadLen {
			return vterrors.New(vtrpcpb.Code_INTERNAL,
				fmt.Sprintf("[BUG] event length of %d at pos %d in decompressed transaction payload is beyond the expected payload length of %d",
					eventLen, pos, decompressedPayloadLen))
		}
		eventData := decompressedPayload[pos : pos+eventLen]
		ble := NewMysql56BinlogEvent(eventData)
		tp.Events = append(tp.Events, ble)

		pos += eventLen
	}

	return nil
}

// Decompress the payload.
func (tp *TransactionPayload) decompress() ([]byte, error) {
	if len(tp.Payload) == 0 {
		return []byte{}, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "cannot decompress empty payload")
	}
	var (
		decompressedBytes []byte
		err               error
	)

	// Switch to slower but less memory intensive stream mode for larger payloads.
	if tp.UncompressedSize > zstdInMemoryDecompressorMaxSize {
		in := bytes.NewReader(tp.Payload)
		streamDecoder, err := zstd.NewReader(in)
		if err != nil {
			return nil, err
		}
		defer streamDecoder.Close()
		out := io.Writer(&bytes.Buffer{})
		_, err = io.Copy(out, streamDecoder)
		if err != nil {
			return nil, err
		}
		decompressedBytes = out.(*bytes.Buffer).Bytes()
	} else { // Process smaller payloads using in-memory buffers.
		decompressedBytes, err = zstdDecoder.DecodeAll(tp.Payload, nil)
		if err != nil {
			return nil, err
		}
	}

	if uint64(len(decompressedBytes)) != tp.UncompressedSize {
		return []byte{}, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("decompressed size %d does not match expected size %d", len(decompressedBytes), tp.UncompressedSize))
	}

	return decompressedBytes, nil
}
