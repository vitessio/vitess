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
	"context"
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
	fakeOpDelete       = 2
	fakeOpExists       = 3
	fakeOpGetData      = 4
	fakeOpPing         = 11
	fakeOpGetChildren2 = 12
	fakeOpSetWatches   = 101
	fakeOpClose        = -11
)

// fakeZkServer speaks just enough of the ZooKeeper wire protocol to
// establish and resume sessions, answer getData, getChildren2, exists and
// pings, and accept delete and setWatches. Every node exists, and has the
// children in children. It lets tests control when connections drop and
// how long a reconnect takes, which a real ZooKeeper does not.
type fakeZkServer struct {
	t         *testing.T
	listeners []net.Listener
	children  []string

	// requests receives the opcode and path of each request with a path.
	requests chan fakeZkRequest

	// connectDelay is how long the server waits before answering a
	// connect request.
	connectDelay atomic.Int64

	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

type fakeZkRequest struct {
	opcode int32
	path   string
}

func startFakeZkServer(t *testing.T, n int, children ...string) *fakeZkServer {
	s := &fakeZkServer{
		t:        t,
		children: children,
		requests: make(chan fakeZkRequest, 100),
		conns:    make(map[net.Conn]struct{}),
	}
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

// waitForRequest waits until the server receives a request with opcode,
// and returns its path.
func (s *fakeZkServer) waitForRequest(opcode int32) string {
	timeout := time.After(10 * time.Second)
	for {
		select {
		case req := <-s.requests:
			if req.opcode == opcode {
				return req.path
			}
		case <-timeout:
			s.t.Fatalf("fake zk server: no request with opcode %d", opcode)
		}
	}
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

func appendString(buf []byte, str string) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(str)))
	return append(buf, str...)
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
		case fakeOpDelete, fakeOpExists, fakeOpGetData, fakeOpGetChildren2:
			// The request starts with the path.
			size := binary.BigEndian.Uint32(req[8:12])
			s.requests <- fakeZkRequest{opcode: opcode, path: string(req[12 : 12+size])}
		}
		// A Stat is 68 bytes.
		stat := make([]byte, 68)
		switch opcode {
		case fakeOpGetData:
			// getDataResponse: data []byte, stat Stat.
			resp = appendString(resp, "data")
			resp = append(resp, stat...)
		case fakeOpGetChildren2:
			// getChildren2Response: children []string, stat Stat.
			resp = binary.BigEndian.AppendUint32(resp, uint32(len(s.children)))
			for _, child := range s.children {
				resp = appendString(resp, child)
			}
			resp = append(resp, stat...)
		case fakeOpExists:
			resp = append(resp, stat...)
		case fakeOpDelete, fakeOpPing, fakeOpSetWatches:
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

// TestObtainQueueLockFailsWhenConnectionDrops checks that waiting for a
// queue lock fails when the connection drops, instead of reading the queue
// again on a new connection: our node belongs to the session of the dropped
// connection, and may go away with it.
func TestObtainQueueLockFailsWhenConnectionDrops(t *testing.T) {
	fake := startFakeZkServer(t, 2, "lock-0000000001", "lock-0000000002")
	conn := Connect(fake.addr())
	defer conn.Close()

	errc := make(chan error, 1)
	go func() {
		errc <- obtainQueueLock(t.Context(), conn, "/locks/lock-0000000002", true)
	}()
	require.Equal(t, "/locks/lock-0000000001", fake.waitForRequest(fakeOpExists))

	fake.connectDelay.Store(int64(1500 * time.Millisecond))
	fake.dropConnections()

	select {
	case err := <-errc:
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("obtainQueueLock did not fail after its connection dropped")
	}
}

// TestLeaderDeletesProposalWhenConnectionDrops checks that a leader that
// stops leading because its connection dropped deletes its proposal, so it
// does not block the other participants if its session stays alive.
func TestLeaderDeletesProposalWhenConnectionDrops(t *testing.T) {
	fake := startFakeZkServer(t, 2)
	zs := NewServer(fake.addr(), "/")
	defer zs.Close()

	mp := &zkLeaderParticipation{
		zs:      zs,
		name:    "election",
		stopCtx: t.Context(),
		done:    make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	go mp.watchLeadership(ctx, zs.conn, "/election/proposal", cancel)
	require.Equal(t, "/election/proposal", fake.waitForRequest(fakeOpGetData))

	fake.connectDelay.Store(int64(1500 * time.Millisecond))
	fake.dropConnections()

	require.Equal(t, "/election/proposal", fake.waitForRequest(fakeOpDelete))
	select {
	case <-ctx.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("leadership was not canceled after its connection dropped")
	}
}
