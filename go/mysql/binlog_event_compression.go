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
	"os"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/exp/mmap"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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

const (
	// Compression algorithms that are supported (only zstd today
	// in MySQL 8.0):
	// https://dev.mysql.com/doc/refman/8.0/en/binary-log-transaction-compression.html
	TransactionPayloadCompressionZstd = 0
	TransactionPayloadCompressionNone = 255

	// Length of the binlog event header in the transaction payload.
	headerLen = int64(BinlogEventLenOffset + 4)

	// At what size should we switch from the in-memory buffer
	// decoding to streaming mode via a temporary file -- which
	// is much slower, but does not require everything be done
	// in memory.
	zstdInMemoryDecompressorMaxSize = 128 << (10 * 2) // 128MiB
	// When using a temporary file to process the payload use
	// this file name pattern.
	tmpFilePattern = "binlog-transaction-payload-*"
)

var (
	TransactionPayloadCompressionTypes = map[uint64]string{
		TransactionPayloadCompressionZstd: "ZSTD",
		TransactionPayloadCompressionNone: "NONE",
	}

	// Metrics.
	compressedTrxPayloadsInMem     = stats.NewCounter("CompressedTransactionPayloadsInMemory", "The number of compressed binlog transaction payloads that were processed in memory")
	compressedTrxPayloadsUsingFile = stats.NewCounter("CompressedTransactionPayloadsViaFile", "The number of compressed binlog transaction payloads that were processed using a temporary file")

	// Create a reader that caches decompressors. This is used for
	// smaller events that we want to handle entirely using in-memory
	// buffers.
	zstdDecoder, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
)

type TransactionPayload struct {
	size             uint64
	compressionType  uint64
	uncompressedSize uint64
	payload          []byte
	iterator         func() (BinlogEvent, error)
}

// IsTransactionPayload returns true if a compressed transaction
// payload event is found (binlog_transaction_compression=ON).
func (ev binlogEvent) IsTransactionPayload() bool {
	return ev.Type() == eTransactionPayloadEvent
}

// TransactionPayload processes the payload and returns an iterator that
// should be used in a loop to read BinlogEvents one by one that were in
// the compressed transaction. The iterator function will return io.EOF
// when there are no more events left in the payload.
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
func (ev binlogEvent) TransactionPayload(format BinlogFormat) (func() (BinlogEvent, error), error) {
	tp := &TransactionPayload{}
	if err := tp.Decode(ev.Bytes()[format.HeaderLength:]); err != nil {
		return nil, vterrors.Wrapf(err, "error decoding transaction payload event")
	}
	return tp.iterator, nil
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
			tp.payload = data[pos:]
			return nil // we're done
		}

		fieldLen, ok := readFixedLenUint64(data[pos : pos+1])
		if !ok {
			return vterrors.New(vtrpcpb.Code_INTERNAL, "error reading field length")
		}
		pos++

		switch fieldType {
		case payloadSizeField:
			tp.size, ok = readFixedLenUint64(data[pos : pos+fieldLen])
			if !ok {
				return vterrors.New(vtrpcpb.Code_INTERNAL, "error reading payload size")
			}
		case payloadCompressionTypeField:
			tp.compressionType, ok = readFixedLenUint64(data[pos : pos+fieldLen])
			if !ok {
				return vterrors.New(vtrpcpb.Code_INTERNAL, "error reading compression type")
			}
		case payloadUncompressedSizeField:
			tp.uncompressedSize, ok = readFixedLenUint64(data[pos : pos+fieldLen])
			if !ok {
				return vterrors.New(vtrpcpb.Code_INTERNAL, "error reading uncompressed payload size")
			}
		}

		pos += fieldLen
	}
}

// decode decompresses the payload and extracts the internal binlog
// events, assigning the iterator to a function that can then be used
// to retrieve the events from the uncompressed transaction one by one.
func (tp *TransactionPayload) decode() error {
	if tp.compressionType != TransactionPayloadCompressionZstd {
		return vterrors.New(vtrpcpb.Code_INTERNAL,
			fmt.Sprintf("TransactionPayload has unsupported compression type of %d", tp.compressionType))
	}

	decompressedReader, err := tp.decompress()
	if err != nil {
		return vterrors.Wrapf(err, "error decompressing transaction payload")
	}

	header := make([]byte, headerLen)
	pos := int64(0)
	tp.iterator = func() (ble BinlogEvent, err error) {
		bytesRead, err := decompressedReader.ReadAt(header, pos)
		if err != nil {
			if err == io.EOF {
				return nil, io.EOF
			}
			return nil, vterrors.Wrapf(err, "error reading event header from decompressed transaction payload")
		}
		if int64(bytesRead) != headerLen {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] expected header length of %d but only read %d bytes",
				headerLen, bytesRead)
		}
		// The event includes the header so we don't move the pos.
		eventLen := uint64(binary.LittleEndian.Uint32(header[BinlogEventLenOffset:headerLen]))
		eventData := make([]byte, eventLen)
		bytesRead, err = decompressedReader.ReadAt(eventData, pos)
		if err != nil && err != io.EOF {
			return nil, vterrors.Wrapf(err, "error reading binlog event data from decompressed transaction payload")
		}
		if uint64(bytesRead) != eventLen {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] expected binlog event length of %d but only read %d bytes",
				eventLen, bytesRead)
		}
		pos += int64(eventLen)
		return NewMysql56BinlogEvent(eventData), nil
	}
	return nil
}

// decompress decompresses the payload. If the payload is larger than
// zstdInMemoryDecompressorMaxSize then we stream the decompression
// to a temporary file and mmap it, otherwise we use in-memory buffers.
// In either case, we return an io.ReaderAt that can be used to read the
// events in the decompressed payload.
func (tp *TransactionPayload) decompress() (io.ReaderAt, error) {
	if len(tp.payload) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "cannot decompress empty compressed transaction payload")
	}

	// Switch to slower but less memory intensive stream mode for larger
	// payloads. We perform the decompression as a stream and write the
	// output to a temporary file which we then mmap.
	if tp.uncompressedSize > zstdInMemoryDecompressorMaxSize {
		// Create a temporary file to stream the uncompressed payload to.
		tmpFile, err := os.CreateTemp("", tmpFilePattern)
		if err != nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL,
				"error creating temporary file to store uncompressed transaction payload: %v", err)
		}
		// Close the initial FD and delete the file path on the FS. It
		// will then be fully removed when the finalizer runs on our
		// returned mmap.ReaderAt.
		defer func() {
			tmpFile.Close()
			os.Remove(tmpFile.Name())
		}()
		in := bytes.NewReader(tp.payload)
		streamDecoder, err := zstd.NewReader(in, zstd.WithDecoderMaxMemory(zstdInMemoryDecompressorMaxSize))
		if err != nil {
			return nil, err
		}
		defer streamDecoder.Close()
		out := io.Writer(tmpFile)
		_, err = io.Copy(out, streamDecoder)
		if err != nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL,
				"error copying %s uncompressed bytes to temporary file %s: %v",
				TransactionPayloadCompressionTypes[tp.compressionType], tmpFile.Name(), err)
		}
		// Open adds a finalizer for the garbage collector to munmap
		// the data in the reader when it frees the object.
		reader, err := mmap.Open(tmpFile.Name())
		if err != nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL,
				"error mmapping temporary file %s storing uncompressed transaction payload: %v",
				tmpFile.Name(), err)
		}
		compressedTrxPayloadsUsingFile.Add(1)
		return reader, nil
	}

	// Process smaller payloads using only in-memory buffers.
	decompressedBytes := make([]byte, 0, tp.uncompressedSize) // Perform a single pre-allocation
	decompressedBytes, err := zstdDecoder.DecodeAll(tp.payload, decompressedBytes[:0])
	if err != nil {
		return nil, err
	}
	if uint64(len(decompressedBytes)) != tp.uncompressedSize {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
			"uncompressed transaction payload size %d does not match expected size %d", len(decompressedBytes), tp.uncompressedSize)
	}
	compressedTrxPayloadsInMem.Add(1)
	return bytes.NewReader(decompressedBytes), nil
}

// Events returns an iterator over the internal binlog events that
// were contained within the compressed transaction payload/event.
// It returns a single-use iterator.
// TODO: come back to this when main is on go 1.23. See:
// - https://tip.golang.org/wiki/RangefuncExperiment
// - https://github.com/golang/go/blob/release-branch.go1.23/src/iter/iter.go
//func (tp *TransactionPayload) Events() iter.Seq[BinlogEvent] {
//	return tp.iterator
//}
