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

package tabletserver

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// mockPacketReader simulates a MySQL connection for testing streamBinlogPackets.
// Packets are written via WritePacket (which builds proper MySQL headers) and
// consumed by ReadHeaderInto/ReadDataInto. Close() unblocks any pending reads.
type mockPacketReader struct {
	pr   *io.PipeReader
	pw   *io.PipeWriter
	seq  uint8
	once sync.Once
}

func newMockPacketReader() *mockPacketReader {
	pr, pw := io.Pipe()
	return &mockPacketReader{pr: pr, pw: pw, seq: 1}
}

// WritePacket writes a MySQL packet (4-byte header + payload) to the mock connection.
// The header is constructed with the correct length and an incrementing sequence number.
func (m *mockPacketReader) WritePacket(payload []byte) {
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

func (m *mockPacketReader) ReadHeaderInto(buf []byte) (int, error) {
	if _, err := io.ReadFull(m.pr, buf[:mysql.PacketHeaderSize]); err != nil {
		return 0, err
	}
	return int(uint32(buf[0]) | uint32(buf[1])<<8 | uint32(buf[2])<<16), nil
}

func (m *mockPacketReader) ReadDataInto(buf []byte) error {
	_, err := io.ReadFull(m.pr, buf)
	return err
}

func (m *mockPacketReader) Buffered() int {
	return 0
}

// Close unblocks any pending reads with a "connection closed" error.
func (m *mockPacketReader) Close() {
	m.once.Do(func() {
		m.pw.CloseWithError(errors.New("connection closed"))
	})
}

// collectSender returns a send function that collects all sent responses.
// Raw bytes are copied since streamBinlogPackets reuses its internal buffer.
func collectSender() (func(*binlogdatapb.BinlogDumpResponse) error, *[]*binlogdatapb.BinlogDumpResponse) {
	var mu sync.Mutex
	var responses []*binlogdatapb.BinlogDumpResponse
	send := func(resp *binlogdatapb.BinlogDumpResponse) error {
		mu.Lock()
		defer mu.Unlock()
		raw := make([]byte, len(resp.Raw))
		copy(raw, resp.Raw)
		responses = append(responses, &binlogdatapb.BinlogDumpResponse{Raw: raw})
		return nil
	}
	return send, &responses
}

// makePacket builds a raw MySQL packet (header + payload) for expected value comparison.
func makePacket(seq uint8, payload []byte) []byte {
	length := len(payload)
	packet := make([]byte, mysql.PacketHeaderSize+length)
	packet[0] = byte(length)
	packet[1] = byte(length >> 8)
	packet[2] = byte(length >> 16)
	packet[3] = seq
	copy(packet[mysql.PacketHeaderSize:], payload)
	return packet
}

// concatRaw concatenates all Raw bytes from responses into a single slice.
func concatRaw(responses []*binlogdatapb.BinlogDumpResponse) []byte {
	var result []byte
	for _, r := range responses {
		result = append(result, r.Raw...)
	}
	return result
}

func TestStreamBinlogPackets_NormalEOFFromMySQL(t *testing.T) {
	tsv := &TabletServer{}
	reader := newMockPacketReader()
	send, responses := collectSender()

	done := make(chan error, 1)
	go func() {
		done <- tsv.streamBinlogPackets(context.Background(), reader, send)
	}()

	// MySQL sends an EOF packet — this is a terminal packet.
	eofPayload := []byte{mysql.EOFPacket, 0x00, 0x00, 0x00, 0x00}
	reader.WritePacket(eofPayload)

	err := <-done
	require.NoError(t, err)

	// EOF is terminal — buffer flushed immediately with header + payload.
	raw := concatRaw(*responses)
	assert.Equal(t, makePacket(1, eofPayload), raw)
}

func TestStreamBinlogPackets_NormalEventAndEOF(t *testing.T) {
	tsv := &TabletServer{}
	reader := newMockPacketReader()
	send, responses := collectSender()

	done := make(chan error, 1)
	go func() {
		done <- tsv.streamBinlogPackets(context.Background(), reader, send)
	}()

	// Send a normal binlog event (status byte 0x00 = OK)
	eventPayload := []byte{0x00, 0x01, 0x02, 0x03}
	eofPayload := []byte{mysql.EOFPacket, 0x00, 0x00, 0x00, 0x00}
	reader.WritePacket(eventPayload)
	reader.WritePacket(eofPayload)

	err := <-done
	require.NoError(t, err)

	// Both packets batched into response(s), flushed on EOF.
	raw := concatRaw(*responses)
	expected := append(makePacket(1, eventPayload), makePacket(2, eofPayload)...)
	assert.Equal(t, expected, raw)
}

func TestStreamBinlogPackets_MultipleEventsAndEOF(t *testing.T) {
	tsv := &TabletServer{}
	reader := newMockPacketReader()
	send, responses := collectSender()

	done := make(chan error, 1)
	go func() {
		done <- tsv.streamBinlogPackets(context.Background(), reader, send)
	}()

	// Send several events followed by EOF.
	event1 := []byte{0x00, 0xAA}
	event2 := []byte{0x00, 0xBB, 0xCC}
	event3 := []byte{0x00, 0xDD, 0xEE, 0xFF}
	eofPayload := []byte{mysql.EOFPacket, 0x00, 0x00, 0x00, 0x00}
	reader.WritePacket(event1)
	reader.WritePacket(event2)
	reader.WritePacket(event3)
	reader.WritePacket(eofPayload)

	err := <-done
	require.NoError(t, err)

	// All packets batched together (well under 256KB).
	raw := concatRaw(*responses)
	var expected []byte
	expected = append(expected, makePacket(1, event1)...)
	expected = append(expected, makePacket(2, event2)...)
	expected = append(expected, makePacket(3, event3)...)
	expected = append(expected, makePacket(4, eofPayload)...)
	assert.Equal(t, expected, raw)
}

func TestStreamBinlogPackets_ConnectionCloseDuringRead(t *testing.T) {
	tsv := &TabletServer{}
	reader := newMockPacketReader()
	send, responses := collectSender()

	done := make(chan error, 1)
	go func() {
		done <- tsv.streamBinlogPackets(context.Background(), reader, send)
	}()

	// Close immediately — first ReadHeaderInto fails.
	reader.Close()

	err := <-done
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection closed")
	assert.Empty(t, *responses)
}

func TestStreamBinlogPackets_ConnectionCloseBetweenEvents(t *testing.T) {
	tsv := &TabletServer{}
	reader := newMockPacketReader()
	send, responses := collectSender()

	done := make(chan error, 1)
	go func() {
		done <- tsv.streamBinlogPackets(context.Background(), reader, send)
	}()

	// Deliver one event, then close.
	reader.WritePacket([]byte{0x00, 0x01, 0x02, 0x03})
	reader.Close()

	err := <-done
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection closed")

	// The event was buffered but never flushed (no terminal packet).
	assert.Empty(t, *responses)
}

func TestStreamBinlogPackets_ReadError(t *testing.T) {
	tsv := &TabletServer{}
	reader := newMockPacketReader()
	send, _ := collectSender()

	done := make(chan error, 1)
	go func() {
		done <- tsv.streamBinlogPackets(context.Background(), reader, send)
	}()

	// Inject a custom error.
	reader.pw.CloseWithError(errors.New("unexpected network error"))

	err := <-done
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected network error")
}

func TestStreamBinlogPackets_ContextCancelledBeforeAnyData(t *testing.T) {
	tsv := &TabletServer{}
	reader := newMockPacketReader()
	send, responses := collectSender()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately before any data is written.

	err := tsv.streamBinlogPackets(ctx, reader, send)
	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, *responses)
}

func TestStreamBinlogPackets_ContextCancelledBetweenEvents(t *testing.T) {
	tsv := &TabletServer{}
	reader := newMockPacketReader()
	send, _ := collectSender()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- tsv.streamBinlogPackets(ctx, reader, send)
	}()

	// Deliver one event, then cancel the context.
	reader.WritePacket([]byte{0x00, 0x01, 0x02, 0x03})
	cancel()
	// Close the reader so any blocked ReadHeaderInto returns.
	reader.Close()

	err := <-done
	require.ErrorIs(t, err, context.Canceled)
}

func TestStreamBinlogPackets_MaxPacketSizeMessage(t *testing.T) {
	tsv := &TabletServer{}
	reader := newMockPacketReader()
	send, responses := collectSender()

	done := make(chan error, 1)
	go func() {
		done <- tsv.streamBinlogPackets(context.Background(), reader, send)
	}()

	// Send a multi-packet message: MaxPacketSize payload + zero-length continuation + EOF.
	bigPayload := make([]byte, mysql.MaxPacketSize)
	bigPayload[0] = 0x00 // OK status byte
	reader.WritePacket(bigPayload)
	reader.WritePacket(nil) // zero-length continuation terminates the multi-packet message
	eofPayload := []byte{mysql.EOFPacket, 0x00, 0x00, 0x00, 0x00}
	reader.WritePacket(eofPayload)

	err := <-done
	require.NoError(t, err)

	// Verify total raw bytes:
	// header(4) + MaxPacketSize + header(4) + header(4) + 5(EOF) = MaxPacketSize + 17
	raw := concatRaw(*responses)
	assert.Len(t, raw, mysql.MaxPacketSize+17)

	// Multiple responses expected (256KB buffer flushes many times for 16MB payload).
	assert.Greater(t, len(*responses), 1)
}
