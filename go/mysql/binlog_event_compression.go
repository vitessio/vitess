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
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
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

	// Bytes used to store the internal event length as a uint32 at
	// the end of the binlog event header.
	eventLenBytes = 4
	// Offset from 0 where the eventLenBytes are stored.
	binlogEventLenOffset = 9
	// Length of the binlog event header for internal events within
	// the transaction payload.
	headerLen = binlogEventLenOffset + eventLenBytes

	// At what size should we switch from the in-memory buffer
	// decoding to streaming mode which is much slower, but does
	// not require everything be done in memory.
	zstdInMemoryDecompressorMaxSize = 128 << (10 * 2) // 128MiB
)

var (
	TransactionPayloadCompressionTypes = map[uint64]string{
		TransactionPayloadCompressionZstd: "ZSTD",
		TransactionPayloadCompressionNone: "NONE",
	}

	// Metrics.
	compressedTrxPayloadsInMem       = stats.NewCounter("CompressedTransactionPayloadsInMemory", "The number of compressed binlog transaction payloads that were processed in memory")
	compressedTrxPayloadsUsingStream = stats.NewCounter("CompressedTransactionPayloadsViaStream", "The number of compressed binlog transaction payloads that were processed using a stream")

	// A concurrent stateless decoder that caches decompressors. This is
	// used for smaller payloads that we want to handle entirely using
	// in-memory buffers via DecodeAll.
	statelessDecoder *zstd.Decoder

	// A pool of stateful decoders for larger payloads that we want to
	// stream. The number of large (> zstdInMemoryDecompressorMaxSize)
	// payloads should typically be relatively low, but there may be times
	// where there are many of them -- and users like vstreamer may have
	// N concurrent streams per tablet which could lead to a lot of
	// allocations and GC overhead so this pool allows us to handle
	// concurrent cases better while still scaling to 0 when there's no
	// usage.
	statefulDecoderPool sync.Pool
)

func init() {
	var err error
	statelessDecoder, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
	if err != nil { // Should only happen e.g. due to ENOMEM
		log.Errorf("Error creating stateless decoder: %v", err)
	}
	statefulDecoderPool = sync.Pool{
		New: func() any {
			d, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(zstdInMemoryDecompressorMaxSize))
			if err != nil { // Should only happen e.g. due to ENOMEM
				log.Errorf("Error creating stateful decoder: %v", err)
			}
			return d
		},
	}
}

type TransactionPayload struct {
	size             uint64
	compressionType  uint64
	uncompressedSize uint64
	payload          []byte
	reader           io.Reader
	iterator         func() (BinlogEvent, error)
}

// IsTransactionPayload returns true if a compressed transaction
// payload event is found (binlog_transaction_compression=ON).
func (ev binlogEvent) IsTransactionPayload() bool {
	return ev.Type() == eTransactionPayloadEvent
}

// TransactionPayload processes the payload and provides a GetNextEvent()
// method which should be used in a loop to read BinlogEvents one by one
// that were within the compressed transaction. That function will return
// io.EOF when there are no more events left in the payload. You must
// call Close() when you are done with the TransactionPayload to ensure
// that the underlying reader and related resources are cleaned up.
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
func (ev binlogEvent) TransactionPayload(format BinlogFormat) (*TransactionPayload, error) {
	tp := &TransactionPayload{}
	if err := tp.process(ev.Bytes()[format.HeaderLength:]); err != nil {
		return nil, vterrors.Wrap(err, "error decoding transaction payload event")
	}
	return tp, nil
}

// process reads and decompresses the payload, setting up the iterator
// that can then be used in GetNextEvent() to read the binlog events
// from the uncompressed payload one at a time.
func (tp *TransactionPayload) process(data []byte) error {
	if err := tp.read(data); err != nil {
		return err
	}
	return tp.decode()
}

// read unmarshalls the transaction payload event into the
// TransactionPayload struct. The compressed payload itself will still
// need to be decoded -- meaning decompressing it and setting up the
// iterator that can then be used by GetNextEvent() to extract the
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

// decode decompresses the payload and assigns the iterator to a
// function that can then be used to retrieve the events from the
// uncompressed transaction one at a time.
func (tp *TransactionPayload) decode() error {
	if tp.compressionType != TransactionPayloadCompressionZstd {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL,
			"TransactionPayload has unsupported compression type of %d", tp.compressionType)
	}

	err := tp.decompress()
	if err != nil || tp.reader == nil {
		return vterrors.Wrap(err, "error decompressing transaction payload")
	}

	header := make([]byte, headerLen)
	tp.iterator = func() (ble BinlogEvent, err error) {
		bytesRead, err := io.ReadFull(tp.reader, header)
		if err != nil {
			if err == io.EOF {
				return nil, io.EOF
			}
			return nil, vterrors.Wrap(err, "error reading event header from uncompressed transaction payload")
		}
		if bytesRead != headerLen {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] expected header length of %d but only read %d bytes",
				headerLen, bytesRead)
		}
		eventLen := int64(binary.LittleEndian.Uint32(header[binlogEventLenOffset:headerLen]))
		eventData := make([]byte, eventLen)
		copy(eventData, header) // The event includes the header
		bytesRead, err = io.ReadFull(tp.reader, eventData[headerLen:])
		if err != nil && err != io.EOF {
			return nil, vterrors.Wrap(err, "error reading binlog event data from uncompressed transaction payload")
		}
		if int64(bytesRead+headerLen) != eventLen {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] expected binlog event length of %d but only read %d bytes",
				eventLen, bytesRead)
		}
		return NewMysql56BinlogEvent(eventData), nil
	}
	return nil
}

// decompress decompresses the payload. If the payload is larger than
// zstdInMemoryDecompressorMaxSize then we stream the decompression via
// the package's pool of zstd.Decoders, otherwise we use in-memory
// buffers with the package's concurrent statelessDecoder.
// In either case, we setup the reader that can be used within the
// iterator to read the events one at a time from the decompressed
// payload in GetNextEvent().
func (tp *TransactionPayload) decompress() error {
	if len(tp.payload) == 0 {
		return vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "cannot decompress empty compressed transaction payload")
	}

	// Switch to slower but less memory intensive stream mode for
	// larger payloads.
	if tp.uncompressedSize > zstdInMemoryDecompressorMaxSize {
		in := bytes.NewReader(tp.payload)
		streamDecoder := statefulDecoderPool.Get().(*zstd.Decoder)
		if streamDecoder == nil {
			return vterrors.New(vtrpcpb.Code_INTERNAL, "failed to create stateful stream decoder")
		}
		if err := streamDecoder.Reset(in); err != nil {
			return vterrors.Wrap(err, "error resetting stateful stream decoder")
		}
		compressedTrxPayloadsUsingStream.Add(1)
		tp.reader = streamDecoder
		return nil
	}

	// Process smaller payloads using only in-memory buffers.
	if statelessDecoder == nil { // Should never happen
		return vterrors.New(vtrpcpb.Code_INTERNAL, "failed to create stateless decoder")
	}
	decompressedBytes := make([]byte, 0, tp.uncompressedSize) // Perform a single pre-allocation
	decompressedBytes, err := statelessDecoder.DecodeAll(tp.payload, decompressedBytes[:0])
	if err != nil {
		return err
	}
	if uint64(len(decompressedBytes)) != tp.uncompressedSize {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
			"uncompressed transaction payload size %d does not match expected size %d", len(decompressedBytes), tp.uncompressedSize)
	}
	compressedTrxPayloadsInMem.Add(1)
	tp.reader = bytes.NewReader(decompressedBytes)
	return nil
}

// Close should be called in a defer where the TransactionPayload is
// used to ensure that the underlying reader and related resources
// used are cleaned up.
func (tp *TransactionPayload) Close() {
	switch reader := tp.reader.(type) {
	case *zstd.Decoder:
		if err := reader.Reset(nil); err == nil || err == io.EOF {
			readersPool.Put(reader)
		}
	default:
		reader = nil
	}
	tp.iterator = nil
}

// GetNextEvent returns the next binlog event that was contained within
// the compressed transaction payload. It will return io.EOF when there
// are no more events left in the payload.
func (tp *TransactionPayload) GetNextEvent() (BinlogEvent, error) {
	if tp == nil || tp.iterator == nil {
		return nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "TransactionPayload has been closed")
	}
	return tp.iterator()
}

// Events returns an iterator over the internal binlog events that
// were contained within the compressed transaction payload/event.
// It returns a single-use iterator.
// TODO(mattlord): implement this when main is on go 1.23. See:
// - https://tip.golang.org/wiki/RangefuncExperiment
// - https://github.com/golang/go/blob/release-branch.go1.23/src/iter/iter.go
//func (tp *TransactionPayload) Events() iter.Seq[BinlogEvent] {
//	return tp.iterator
//}
