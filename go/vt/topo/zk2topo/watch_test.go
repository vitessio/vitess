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

package zk2topo

import (
	"encoding/binary"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Opcodes from the ZooKeeper wire protocol.
const (
	fakeOpGetData    = 4
	fakeOpPing       = 11
	fakeOpSetWatches = 101
	fakeOpClose      = -11
)

// fakeZkServer speaks just enough of the ZooKeeper wire protocol to
// establish and resume sessions, answer getData and pings, and accept
// setWatches. It lets tests control when connections drop and how long a
// reconnect takes, which a real ZooKeeper does not.
type fakeZkServer struct {
	t         *testing.T
	listeners []net.Listener

	// connectDelay is how long the server waits before answering a
	// connect request.
	connectDelay atomic.Int64

	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func startFakeZkServer(t *testing.T, n int) *fakeZkServer {
	s := &fakeZkServer{t: t, conns: make(map[net.Conn]struct{})}
	for range n {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		s.listeners = append(s.listeners, l)
		go s.serve(l)
	}
	t.Cleanup(func() {
		for _, l := range s.listeners {
			l.Close()
		}
		s.dropConnections()
	})
	return s
}

func (s *fakeZkServer) addr() string {
	addrs := make([]string, 0, len(s.listeners))
	for _, l := range s.listeners {
		addrs = append(addrs, l.Addr().String())
	}
	return strings.Join(addrs, ",")
}

// dropConnections closes every client connection, as if the ZooKeeper
// server the clients were talking to had restarted.
func (s *fakeZkServer) dropConnections() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.conns {
		c.Close()
		delete(s.conns, c)
	}
}

func (s *fakeZkServer) serve(l net.Listener) {
	for {
		c, err := l.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns[c] = struct{}{}
		s.mu.Unlock()
		go s.handle(c)
	}
}

func readFrame(c net.Conn) ([]byte, error) {
	var size [4]byte
	if _, err := io.ReadFull(c, size[:]); err != nil {
		return nil, err
	}
	buf := make([]byte, binary.BigEndian.Uint32(size[:]))
	_, err := io.ReadFull(c, buf)
	return buf, err
}

func writeFrame(c net.Conn, body []byte) error {
	buf := binary.BigEndian.AppendUint32(nil, uint32(len(body)))
	_, err := c.Write(append(buf, body...))
	return err
}

func (s *fakeZkServer) handle(c net.Conn) {
	defer func() {
		s.mu.Lock()
		delete(s.conns, c)
		s.mu.Unlock()
		c.Close()
	}()

	// connectRequest: protocolVersion int32, lastZxidSeen int64,
	// timeOut int32, sessionID int64, passwd []byte, readOnly bool.
	req, err := readFrame(c)
	if err != nil {
		return
	}
	sessionID := int64(binary.BigEndian.Uint64(req[16:24]))
	if sessionID == 0 {
		sessionID = 0x1234
	}
	time.Sleep(time.Duration(s.connectDelay.Load()))

	// connectResponse: protocolVersion int32, timeOut int32,
	// sessionID int64, passwd []byte, readOnly bool.
	resp := binary.BigEndian.AppendUint32(nil, 0)
	resp = binary.BigEndian.AppendUint32(resp, 30000)
	resp = binary.BigEndian.AppendUint64(resp, uint64(sessionID))
	resp = binary.BigEndian.AppendUint32(resp, 16)
	resp = append(resp, make([]byte, 16)...)
	resp = append(resp, 0)
	if writeFrame(c, resp) != nil {
		return
	}

	for {
		req, err := readFrame(c)
		if err != nil {
			return
		}
		xid := binary.BigEndian.Uint32(req[0:4])
		opcode := int32(binary.BigEndian.Uint32(req[4:8]))

		// responseHeader: xid int32, zxid int64, err int32.
		resp := binary.BigEndian.AppendUint32(nil, xid)
		resp = binary.BigEndian.AppendUint64(resp, 1)
		resp = binary.BigEndian.AppendUint32(resp, 0)
		switch opcode {
		case fakeOpGetData:
			// getDataResponse: data []byte, then a Stat of 68 bytes.
			resp = binary.BigEndian.AppendUint32(resp, 4)
			resp = append(resp, "data"...)
			resp = append(resp, make([]byte, 68)...)
		case fakeOpPing, fakeOpSetWatches:
		case fakeOpClose:
			writeFrame(c, resp)
			return
		default:
			s.t.Errorf("fake zk server: unexpected opcode %d", opcode)
			return
		}
		if writeFrame(c, resp) != nil {
			return
		}
	}
}

// TestWatchNotifiedWhenConnectionDrops checks that a watch reports an error
// once its ZooKeeper connection drops, even when the client library resumes
// the session on another server before Vitess's Close takes effect. In that
// case the library keeps the session alive but can no longer re-register
// its watches, so without an error the watch would never fire again.
func TestWatchNotifiedWhenConnectionDrops(t *testing.T) {
	fake := startFakeZkServer(t, 2)
	zs := NewServer(fake.addr(), "/")
	defer zs.Close()

	_, changes, err := zs.Watch(t.Context(), "keyspace/SrvVSchema")
	require.NoError(t, err)

	// Resuming the session takes longer than zk.Conn.Close waits to send
	// its close request.
	fake.connectDelay.Store(int64(1500 * time.Millisecond))
	fake.dropConnections()

	select {
	case wd, ok := <-changes:
		require.True(t, ok, "watch channel closed without an error")
		require.Error(t, wd.Err)
	case <-time.After(10 * time.Second):
		t.Fatal("watch was not notified after its connection dropped")
	}
}
