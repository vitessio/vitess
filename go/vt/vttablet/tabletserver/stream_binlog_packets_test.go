/*
Copyright 2025 The Vitess Authors.

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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// mockPacketReader simulates a MySQL connection for testing streamBinlogPackets.
// Packets can be enqueued via the packets channel, and Close() unblocks any
// pending ReadOnePacket with an error — mirroring real mysql.Conn.Close() behavior.
type mockPacketReader struct {
	packets chan readResult
	closeCh chan struct{}
	once    sync.Once
}

type readResult struct {
	data []byte
	err  error
}

func newMockPacketReader() *mockPacketReader {
	return &mockPacketReader{
		packets: make(chan readResult),
		closeCh: make(chan struct{}),
	}
}

func (m *mockPacketReader) ReadOnePacket() ([]byte, error) {
	select {
	case r := <-m.packets:
		return r.data, r.err
	case <-m.closeCh:
		return nil, errors.New("connection closed")
	}
}

// Close unblocks any pending ReadOnePacket calls with a "connection closed" error.
func (m *mockPacketReader) Close() {
	m.once.Do(func() { close(m.closeCh) })
}

// collectSender returns a send function that collects all sent responses.
func collectSender() (func(*binlogdatapb.BinlogDumpResponse) error, *[]*binlogdatapb.BinlogDumpResponse) {
	var mu sync.Mutex
	var responses []*binlogdatapb.BinlogDumpResponse
	send := func(resp *binlogdatapb.BinlogDumpResponse) error {
		mu.Lock()
		defer mu.Unlock()
		responses = append(responses, resp)
		return nil
	}
	return send, &responses
}

func TestStreamBinlogPackets_ContextCancelBetweenEvents(t *testing.T) {
	tsv := &TabletServer{}
	reader := newMockPacketReader()
	send, responses := collectSender()
	ctx, cancel := context.WithCancel(context.Background())

	// Simulate the conn-close goroutine from BinlogDump handler
	go func() {
		<-ctx.Done()
		reader.Close()
	}()

	done := make(chan error, 1)
	go func() {
		done <- tsv.streamBinlogPackets(ctx, reader, send)
	}()

	// Deliver one normal binlog event packet (status byte 0x00 = OK, length < MaxPacketSize)
	eventPacket := []byte{0x00, 0x01, 0x02, 0x03}
	reader.packets <- readResult{data: eventPacket}

	// Cancel the context to simulate shutdown.
	// The conn-close goroutine will close the reader, unblocking any pending read.
	cancel()

	err := <-done
	require.NoError(t, err)

	// Should have received: the event packet + an EOF packet
	require.Len(t, *responses, 2)
	assert.Equal(t, eventPacket, (*responses)[0].Raw)
	assert.Equal(t, []byte{mysql.EOFPacket, 0x00, 0x00, 0x00, 0x00}, (*responses)[1].Raw)
}

func TestStreamBinlogPackets_ContextCancelDuringRead(t *testing.T) {
	tsv := &TabletServer{}
	reader := newMockPacketReader()
	send, responses := collectSender()
	ctx, cancel := context.WithCancel(context.Background())

	// Simulate the conn-close goroutine from BinlogDump handler
	go func() {
		<-ctx.Done()
		reader.Close()
	}()

	done := make(chan error, 1)
	go func() {
		done <- tsv.streamBinlogPackets(ctx, reader, send)
	}()

	// Cancel context immediately — the reader will be closed, unblocking the read
	cancel()

	err := <-done
	require.NoError(t, err)

	// Should have received just the EOF packet
	require.Len(t, *responses, 1)
	assert.Equal(t, []byte{mysql.EOFPacket, 0x00, 0x00, 0x00, 0x00}, (*responses)[0].Raw)
}

func TestStreamBinlogPackets_NormalEOFFromMySQL(t *testing.T) {
	tsv := &TabletServer{}
	reader := newMockPacketReader()
	send, responses := collectSender()

	done := make(chan error, 1)
	go func() {
		done <- tsv.streamBinlogPackets(context.Background(), reader, send)
	}()

	// MySQL sends an EOF packet
	eofPacket := []byte{mysql.EOFPacket, 0x00, 0x00, 0x00, 0x00}
	reader.packets <- readResult{data: eofPacket}

	err := <-done
	require.NoError(t, err)

	// Should have received just the EOF packet from MySQL (forwarded as-is)
	require.Len(t, *responses, 1)
	assert.Equal(t, eofPacket, (*responses)[0].Raw)
}

func TestStreamBinlogPackets_ReadErrorWithoutCancel(t *testing.T) {
	tsv := &TabletServer{}
	reader := newMockPacketReader()
	send, _ := collectSender()

	done := make(chan error, 1)
	go func() {
		done <- tsv.streamBinlogPackets(context.Background(), reader, send)
	}()

	// Deliver a real read error (not from shutdown)
	reader.packets <- readResult{err: errors.New("unexpected network error")}

	err := <-done
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected network error")
}

func TestStreamBinlogPackets_ContextCancelDuringContinuation(t *testing.T) {
	tsv := &TabletServer{}
	reader := newMockPacketReader()
	send, responses := collectSender()
	ctx, cancel := context.WithCancel(context.Background())

	// Simulate the conn-close goroutine from BinlogDump handler
	go func() {
		<-ctx.Done()
		reader.Close()
	}()

	done := make(chan error, 1)
	go func() {
		done <- tsv.streamBinlogPackets(ctx, reader, send)
	}()

	// Send a MaxPacketSize packet to trigger continuation loop
	bigPacket := make([]byte, mysql.MaxPacketSize)
	bigPacket[0] = 0x00 // OK status byte
	reader.packets <- readResult{data: bigPacket}

	// Now in continuation loop, cancel — reader.Close() unblocks the read
	cancel()

	err := <-done
	require.NoError(t, err)

	// Should have received: the big packet + EOF
	require.Len(t, *responses, 2)
	assert.Len(t, (*responses)[0].Raw, mysql.MaxPacketSize)
	assert.Equal(t, []byte{mysql.EOFPacket, 0x00, 0x00, 0x00, 0x00}, (*responses)[1].Raw)
}
