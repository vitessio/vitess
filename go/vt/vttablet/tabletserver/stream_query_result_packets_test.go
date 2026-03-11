/*
Copyright 2024 The Vitess Authors.

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

package tabletserver

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

// queryMockReader simulates a MySQL connection for testing streamQueryResultPackets.
type queryMockReader struct {
	pr   *io.PipeReader
	pw   *io.PipeWriter
	seq  uint8
	once sync.Once
}

func newQueryMockReader() *queryMockReader {
	pr, pw := io.Pipe()
	return &queryMockReader{pr: pr, pw: pw, seq: 1}
}

func (m *queryMockReader) WritePacket(payload []byte) {
	length := len(payload)
	var header [mysql.PacketHeaderSize]byte
	header[0] = byte(length)
	header[1] = byte(length >> 8)
	header[2] = byte(length >> 16)
	header[3] = m.seq
	m.seq++
	m.pw.Write(header[:])
	if length > 0 {
		m.pw.Write(payload)
	}
}

func (m *queryMockReader) ReadHeaderInto(buf []byte) (int, error) {
	if _, err := io.ReadFull(m.pr, buf[:mysql.PacketHeaderSize]); err != nil {
		return 0, err
	}
	return int(uint32(buf[0]) | uint32(buf[1])<<8 | uint32(buf[2])<<16), nil
}

func (m *queryMockReader) ReadDataInto(buf []byte) error {
	_, err := io.ReadFull(m.pr, buf)
	return err
}

func (m *queryMockReader) Buffered() int {
	return 0
}

func (m *queryMockReader) Close() {
	m.once.Do(func() {
		m.pw.CloseWithError(errors.New("connection closed"))
	})
}

func makeTestColumnDefPayload(name string) []byte {
	var payload []byte

	writeLenEncStr := func(s string) {
		payload = append(payload, byte(len(s)))
		payload = append(payload, []byte(s)...)
	}

	writeLenEncStr("def")            // catalog
	writeLenEncStr("testdb")         // schema
	writeLenEncStr("testtable")      // table
	writeLenEncStr("testtable")      // org_table
	writeLenEncStr(name)             // name
	writeLenEncStr(name)             // org_name
	payload = append(payload, 0x0c)  // length of fixed-length fields
	payload = append(payload, 33, 0) // character set (utf8)
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, 255)
	payload = append(payload, b...) // column length
	payload = append(payload, 0x0f) // type: VARCHAR
	payload = append(payload, 0, 0) // flags
	payload = append(payload, 0)    // decimals
	payload = append(payload, 0, 0) // filler

	return payload
}

func makeTestRowPayload(values ...string) []byte {
	var payload []byte
	for _, v := range values {
		payload = append(payload, byte(len(v)))
		payload = append(payload, []byte(v)...)
	}
	return payload
}

func eofPayload() []byte {
	return []byte{mysql.EOFPacket, 0, 0, 0, 0}
}

func collectRawSender() (func([]byte) error, *[][]byte) {
	var mu sync.Mutex
	var chunks [][]byte
	send := func(raw []byte) error {
		mu.Lock()
		defer mu.Unlock()
		cpy := make([]byte, len(raw))
		copy(cpy, raw)
		chunks = append(chunks, cpy)
		return nil
	}
	return send, &chunks
}

func TestStreamQueryResultPackets_SimpleWithMidEOF(t *testing.T) {
	reader := newQueryMockReader()
	tsv := &TabletServer{}

	send, chunks := collectRawSender()

	go func() {
		// Column count: 1
		reader.WritePacket([]byte{1})
		// Column definition
		reader.WritePacket(makeTestColumnDefPayload("name"))
		// Mid-stream EOF
		reader.WritePacket(eofPayload())
		// Row
		reader.WritePacket(makeTestRowPayload("alice"))
		// Terminal EOF
		reader.WritePacket(eofPayload())
	}()

	err := tsv.streamQueryResultPackets(context.Background(), reader, false, nil, send)
	require.NoError(t, err)

	// We should have received at least one chunk
	require.NotEmpty(t, *chunks)

	// Total bytes should include all packets
	totalBytes := 0
	for _, c := range *chunks {
		totalBytes += len(c)
	}
	assert.Greater(t, totalBytes, 0)
}

func TestStreamQueryResultPackets_DeprecateEOF(t *testing.T) {
	reader := newQueryMockReader()
	tsv := &TabletServer{}

	send, chunks := collectRawSender()

	go func() {
		// Column count: 1
		reader.WritePacket([]byte{1})
		// Column definition
		reader.WritePacket(makeTestColumnDefPayload("id"))
		// No mid-stream EOF (deprecateEOF=true)
		// Row
		reader.WritePacket(makeTestRowPayload("42"))
		// Terminal EOF
		reader.WritePacket(eofPayload())
	}()

	err := tsv.streamQueryResultPackets(context.Background(), reader, true, nil, send)
	require.NoError(t, err)
	require.NotEmpty(t, *chunks)
}

func TestStreamQueryResultPackets_ErrorResponse(t *testing.T) {
	reader := newQueryMockReader()
	tsv := &TabletServer{}

	send, _ := collectRawSender()

	go func() {
		// Error packet instead of column count
		errPayload := []byte{
			mysql.ErrPacket,
			0x48, 0x04, // error code
			'#',
			'H', 'Y', '0', '0', '0',
			'T', 'e', 's', 't',
		}
		reader.WritePacket(errPayload)
	}()

	err := tsv.streamQueryResultPackets(context.Background(), reader, true, nil, send)
	require.NoError(t, err) // Flush should succeed; error is in the raw bytes
}

func TestStreamQueryResultPackets_MultipleRows(t *testing.T) {
	reader := newQueryMockReader()
	tsv := &TabletServer{}

	send, chunks := collectRawSender()

	go func() {
		reader.WritePacket([]byte{2})                        // 2 columns
		reader.WritePacket(makeTestColumnDefPayload("col1")) // Col 1
		reader.WritePacket(makeTestColumnDefPayload("col2")) // Col 2
		reader.WritePacket(eofPayload())                     // Mid-stream EOF
		reader.WritePacket(makeTestRowPayload("a", "b"))     // Row 1
		reader.WritePacket(makeTestRowPayload("c", "d"))     // Row 2
		reader.WritePacket(makeTestRowPayload("e", "f"))     // Row 3
		reader.WritePacket(eofPayload())                     // Terminal EOF
	}()

	err := tsv.streamQueryResultPackets(context.Background(), reader, false, nil, send)
	require.NoError(t, err)
	require.NotEmpty(t, *chunks)

	// Verify the total raw data is correct
	totalBytes := 0
	for _, c := range *chunks {
		totalBytes += len(c)
	}
	// Each packet has header (4 bytes) + payload
	expectedSize := (mysql.PacketHeaderSize + 1) + // col count
		2*(mysql.PacketHeaderSize+len(makeTestColumnDefPayload("col1"))) + // 2 col defs
		(mysql.PacketHeaderSize + 5) + // mid-EOF
		3*(mysql.PacketHeaderSize+len(makeTestRowPayload("a", "b"))) + // 3 rows
		(mysql.PacketHeaderSize + 5) // terminal EOF
	assert.Equal(t, expectedSize, totalBytes)
}

func TestStreamQueryResultPackets_ContextCancellation(t *testing.T) {
	reader := newQueryMockReader()
	tsv := &TabletServer{}

	ctx, cancel := context.WithCancel(context.Background())

	send, _ := collectRawSender()

	go func() {
		// Send column count, then cancel context before sending more
		reader.WritePacket([]byte{1})
		reader.WritePacket(makeTestColumnDefPayload("name"))
		reader.WritePacket(eofPayload())
		// Cancel context to simulate client disconnect
		cancel()
		reader.Close()
	}()

	err := tsv.streamQueryResultPackets(ctx, reader, false, nil, send)
	// Should get an error (either context cancelled or connection closed)
	require.Error(t, err)
}
