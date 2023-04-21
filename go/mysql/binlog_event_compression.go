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
	"encoding/hex"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"

	"vitess.io/vitess/go/vt/log"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// This file contains code related to handling compression related events.
// More specifically today, compressed transaction payloads:
// See: https://dev.mysql.com/doc/refman/en/binary-log-transaction-compression.html

// Transaction Payload wire protocol fields:
// https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1codecs_1_1binary_1_1Transaction__payload.html
const (
	payloadHeaderEndMark = iota
	payloadSizeField
	payloadCompressionTypeField
	payloadUncompressedSizeField
)

const (
	binlogEventLenOffset = 9
)

// Compression algorithms (only zstd is supported today in 8.0):
// https://dev.mysql.com/doc/refman/8.0/en/binary-log-transaction-compression.html
const (
	transactionPayloadCompressionZstd = 0
	transactionPayloadCompressionNone = 255
)

var transactionPayloadCompressionTypes = map[uint64]string{
	transactionPayloadCompressionZstd: "ZSTD",
	transactionPayloadCompressionNone: "NONE",
}

type TransactionPayload struct {
	Size             uint64
	UncompressedSize uint64
	CompressionType  uint64
	Payload          []byte
	Events           []BinlogEvent
}

// IsTransactionPayload returns true if a compressed transaction payload
// event is found (binlog_transaction_compression=ON).
func (ev binlogEvent) IsTransactionPayload() bool {
	return ev.Type() == eTransactionPayloadEvent
}

// TransactionPayload returns the BinlogEvents contained within the
// compressed transaction.
// The following event types are compressed as part of the transaction payload:
//
//	QUERY_EVENT = 2,
//	INTVAR_EVENT = 5,
//	APPEND_BLOCK_EVENT = 9,
//	DELETE_FILE_EVENT = 11,
//	RAND_EVENT = 13,
//	USER_VAR_EVENT = 14,
//	XID_EVENT = 16,
//	BEGIN_LOAD_QUERY_EVENT = 17,
//	EXECUTE_LOAD_QUERY_EVENT = 18,
//	TABLE_MAP_EVENT = 19,
//	WRITE_ROWS_EVENT_V1 = 23,
//	UPDATE_ROWS_EVENT_V1 = 24,
//	DELETE_ROWS_EVENT_V1 = 25,
//	IGNORABLE_LOG_EVENT = 28,
//	ROWS_QUERY_LOG_EVENT = 29,
//	WRITE_ROWS_EVENT = 30,
//	UPDATE_ROWS_EVENT = 31,
//	DELETE_ROWS_EVENT = 32,
//	XA_PREPARE_LOG_EVENT = 38,
//	PARTIAL_UPDATE_ROWS_EVENT = 39,
//
// When the transaction compression is enabled, the GTID log event has the following
// fields:
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
// We need to extract the compressed transaction payload from the GTID event, uncompress it
// with zstd, and then process the events (e.g. Query and Row events) that make up the
// transaction.
func (ev binlogEvent) TransactionPayload(format BinlogFormat) ([]BinlogEvent, error) {
	tp := &TransactionPayload{}
	if err := tp.Decode(ev.Bytes()[format.HeaderLength:]); err != nil {
		return nil, vterrors.Wrapf(err, "error decoding transaction payload event")
	}
	return tp.Events, nil
}

func (tp *TransactionPayload) Decode(data []byte) error {
	log.Infof("Compressed payload event; Len: %d :: Bytes: %v", len(data), data)
	if err := tp.read(data); err != nil {
		return err
	}
	return tp.decode()
}

func (tp *TransactionPayload) read(data []byte) error {
	pos := uint64(0)

	for {
		fieldType, ok := readFixedLenUint64(data[pos : pos+1])
		if !ok {
			return vterrors.New(vtrpcpb.Code_INTERNAL, "error reading field type")
		}
		log.Infof("Compressed payload event; field type: %d", fieldType)
		pos++

		if fieldType == payloadHeaderEndMark {
			tp.Payload = data[pos:]
			log.Infof("Compressed payload event; found header end mark; payload: %s", hex.EncodeToString(tp.Payload))
			break
		} else {
			fieldLen, ok := readFixedLenUint64(data[pos : pos+1])
			if !ok {
				return vterrors.New(vtrpcpb.Code_INTERNAL, "error reading field length")
			}
			log.Infof("Compressed payload event; for field type %d, field length: %d", fieldType, fieldLen)
			pos++

			switch fieldType {
			case payloadSizeField:
				tp.Size, ok = readFixedLenUint64(data[pos : pos+fieldLen])
				if !ok {
					return vterrors.New(vtrpcpb.Code_INTERNAL, "error reading payload size")
				}
				log.Infof("Compressed payload event; compressed size: %d", tp.Size)
			case payloadCompressionTypeField:
				tp.CompressionType, ok = readFixedLenUint64(data[pos : pos+fieldLen])
				if !ok {
					return vterrors.New(vtrpcpb.Code_INTERNAL, "error reading compression type")
				}
				log.Infof("Compressed payload event; compression type: %s", transactionPayloadCompressionTypes[tp.CompressionType])
			case payloadUncompressedSizeField:
				tp.UncompressedSize, ok = readFixedLenUint64(data[pos : pos+fieldLen])
				if !ok {
					return vterrors.New(vtrpcpb.Code_INTERNAL, "error reading uncompressed payload size")
				}
				log.Infof("Compressed payload event; uncompressed size: %d", tp.UncompressedSize)
			}

			pos += fieldLen
		}
		log.Flush()
	}

	return nil
}

func (tp *TransactionPayload) decode() error {
	if tp.CompressionType != transactionPayloadCompressionZstd {
		return vterrors.New(vtrpcpb.Code_INTERNAL,
			fmt.Sprintf("TransactionPayload has unsupported compression type of %d", tp.CompressionType))
	}

	decompressedPayload, err := tp.decompress()
	decompressedPayloadLen := uint32(len(decompressedPayload))
	if err != nil {
		return vterrors.Wrapf(err, "error decompressing transaction payload")
	}
	log.Infof("Decompressed payload (length: %d): %s", decompressedPayloadLen, hex.EncodeToString(decompressedPayload))

	pos := uint32(0)

	for {
		eventLenPosEnd := pos + binlogEventLenOffset + 4
		if eventLenPosEnd > decompressedPayloadLen { // No more events in the payload
			break
		}
		eventLen := binary.LittleEndian.Uint32(decompressedPayload[pos+binlogEventLenOffset : eventLenPosEnd])
		if pos+eventLen > decompressedPayloadLen {
			return vterrors.New(vtrpcpb.Code_INTERNAL,
				fmt.Sprintf("[BUG] event length of %d at pos %d in uncompressed transaction payload is beyond the expected payload length of %d",
					eventLen, pos, decompressedPayloadLen))
		}
		eventData := decompressedPayload[pos : pos+eventLen]
		ble := NewMysql56BinlogEvent(eventData)
		log.Infof("Decoded binlog event from uncompressed payload (length: %d, type: %d): Bytes: %v",
			eventLen, ble.(mysql56BinlogEvent).Type(), ble.Bytes())
		tp.Events = append(tp.Events, ble)

		pos += eventLen
	}

	return nil
}

// Decompress the payload's bytes.
func (tp *TransactionPayload) decompress() ([]byte, error) {
	if len(tp.Payload) == 0 {
		return []byte{}, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "cannot decompress missing bytes")
	}

	in := bytes.NewReader(tp.Payload)
	d, err := zstd.NewReader(in)
	if err != nil {
		return nil, err
	}
	defer d.Close()
	out := io.Writer(&bytes.Buffer{})
	written, err := io.Copy(out, d)
	if err != nil {
		return []byte{}, err
	}
	if written != int64(tp.UncompressedSize) {
		return []byte{}, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("decompressed size %d does not match expected size %d", written, tp.UncompressedSize))
	}
	log.Infof("Decompressed %d bytes to %d bytes; Value: %s", len(tp.Payload), written, out.(*bytes.Buffer).String())

	return out.(*bytes.Buffer).Bytes(), nil
}
