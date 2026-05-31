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
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
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

func TestStreamQueryResultPackets_Simple(t *testing.T) {
	reader := newQueryMockReader()
	tsv := &TabletServer{}

	send, chunks := collectRawSender()

	go func() {
		// Column count: 1
		reader.WritePacket([]byte{1})
		// Column definition
		reader.WritePacket(makeTestColumnDefPayload("name"))
		// Row
		reader.WritePacket(makeTestRowPayload("alice"))
		// Terminal EOF
		reader.WritePacket(eofPayload())
	}()

	err := tsv.streamQueryResultPackets(context.Background(), reader, nil, send)
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

func TestStreamQueryResultPackets_SingleRow(t *testing.T) {
	reader := newQueryMockReader()
	tsv := &TabletServer{}

	send, chunks := collectRawSender()

	go func() {
		// Column count: 1
		reader.WritePacket([]byte{1})
		// Column definition
		reader.WritePacket(makeTestColumnDefPayload("id"))
		// Row
		reader.WritePacket(makeTestRowPayload("42"))
		// Terminal EOF
		reader.WritePacket(eofPayload())
	}()

	err := tsv.streamQueryResultPackets(context.Background(), reader, nil, send)
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

	err := tsv.streamQueryResultPackets(context.Background(), reader, nil, send)
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
		reader.WritePacket(makeTestRowPayload("a", "b"))     // Row 1
		reader.WritePacket(makeTestRowPayload("c", "d"))     // Row 2
		reader.WritePacket(makeTestRowPayload("e", "f"))     // Row 3
		reader.WritePacket(eofPayload())                     // Terminal EOF
	}()

	err := tsv.streamQueryResultPackets(context.Background(), reader, nil, send)
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
		// Cancel context to simulate client disconnect
		cancel()
		reader.Close()
	}()

	err := tsv.streamQueryResultPackets(ctx, reader, nil, send)
	// Should get an error (either context cancelled or connection closed)
	require.Error(t, err)
}

// lenEncColumnCount encodes a column count exactly as MySQL frames the
// column-count packet payload (a length-encoded integer).
func lenEncColumnCount(n int) []byte {
	switch {
	case n < 251:
		return []byte{byte(n)}
	case n < 1<<16:
		return []byte{0xfc, byte(n), byte(n >> 8)}
	case n < 1<<24:
		return []byte{0xfd, byte(n), byte(n >> 8), byte(n >> 16)}
	default:
		b := make([]byte, 9)
		b[0] = 0xfe
		binary.LittleEndian.PutUint64(b[1:], uint64(n))
		return b
	}
}

// TestStreamQueryResultPackets_WideResultSet round-trips result sets of varying
// column counts through the producer and the vtgate-side RawResultParser. For
// counts >= 251 the column-count packet is a multi-byte length-encoded integer;
// decoding only its first byte mis-frames the stream, so the parser would see
// the wrong field count or fail. 250 stays single-byte and pins the boundary.
func TestStreamQueryResultPackets_WideResultSet(t *testing.T) {
	for _, numCols := range []int{250, 251, 300, 1000, 4096} {
		t.Run(fmt.Sprintf("cols=%d", numCols), func(t *testing.T) {
			reader := newQueryMockReader()
			tsv := &TabletServer{}
			send, chunks := collectRawSender()

			colNames := make([]string, numCols)
			rowVals := make([]string, numCols)
			for i := range numCols {
				colNames[i] = fmt.Sprintf("c%d", i)
				rowVals[i] = fmt.Sprintf("v%d", i)
			}

			go func() {
				reader.WritePacket(lenEncColumnCount(numCols))
				for _, name := range colNames {
					reader.WritePacket(makeTestColumnDefPayload(name))
				}
				reader.WritePacket(makeTestRowPayload(rowVals...))
				reader.WritePacket(eofPayload())
			}()

			require.NoError(t, tsv.streamQueryResultPackets(context.Background(), reader, nil, send))

			parser := mysql.NewRawResultParser()
			var fields []*querypb.Field
			var rows [][]sqltypes.Value
			for _, c := range *chunks {
				require.NoError(t, parser.Feed(c, func(r *sqltypes.Result) error {
					if r.Fields != nil {
						fields = r.Fields
					}
					rows = append(rows, r.Rows...)
					return nil
				}))
			}

			require.Len(t, fields, numCols)
			require.Equal(t, "c0", fields[0].Name)
			require.Equal(t, fmt.Sprintf("c%d", numCols-1), fields[numCols-1].Name)
			require.Len(t, rows, 1)
			require.Len(t, rows[0], numCols)
			require.Equal(t, "v0", rows[0][0].ToString())
			require.Equal(t, fmt.Sprintf("v%d", numCols-1), rows[0][numCols-1].ToString())
		})
	}
}

// TestStreamQueryResultPackets_WideResultSetNoRows pins the actual failure mode
// of decoding the column count from only its first byte. With 251 columns the
// count packet is 0xfc-prefixed; the single-byte read yields int(0xfc)=252, one
// more than the real count. The producer's column-definition loop then consumes
// the terminal EOF packet as if it were a column definition and reads past the
// end of the result set — in production this hangs on the idle connection. Here
// the reader is closed after the EOF, so the over-read surfaces as an error.
// The fix decodes the count correctly, so the producer stops at the terminal.
func TestStreamQueryResultPackets_WideResultSetNoRows(t *testing.T) {
	const numCols = 251

	reader := newQueryMockReader()
	tsv := &TabletServer{}
	send, chunks := collectRawSender()

	go func() {
		reader.WritePacket(lenEncColumnCount(numCols))
		for i := range numCols {
			reader.WritePacket(makeTestColumnDefPayload(fmt.Sprintf("c%d", i)))
		}
		reader.WritePacket(eofPayload())
		reader.Close() // nothing follows a result set on an idle connection
	}()

	require.NoError(t, tsv.streamQueryResultPackets(context.Background(), reader, nil, send))

	parser := mysql.NewRawResultParser()
	var fields []*querypb.Field
	for _, c := range *chunks {
		require.NoError(t, parser.Feed(c, func(r *sqltypes.Result) error {
			if r.Fields != nil {
				fields = r.Fields
			}
			return nil
		}))
	}
	require.Len(t, fields, numCols)
}

// TestStreamQueryResultPackets_LocalInfile verifies that a LOCAL INFILE request
// packet (first byte 0xfb), which the raw streaming path cannot support, fails
// with a clear error instead of being misread as a column count.
func TestStreamQueryResultPackets_LocalInfile(t *testing.T) {
	reader := newQueryMockReader()
	tsv := &TabletServer{}
	send, _ := collectRawSender()

	go func() {
		// 0xfb followed by a filename, per the LOCAL INFILE request packet.
		reader.WritePacket(append([]byte{0xfb}, []byte("/tmp/data.csv")...))
	}()

	err := tsv.streamQueryResultPackets(context.Background(), reader, nil, send)
	require.ErrorContains(t, err, "LOCAL INFILE")
}
