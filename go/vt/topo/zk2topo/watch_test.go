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
	"github.com/z-division/go-zookeeper/zk"

	"vitess.io/vitess/go/vt/vterrors"
)

// Opcodes from the ZooKeeper wire protocol.
const (
	fakeOpCreate       = 1
	fakeOpDelete       = 2
	fakeOpExists       = 3
	fakeOpGetData      = 4
	fakeOpPing         = 11
	fakeOpGetChildren2 = 12
	fakeOpSetWatches   = 101
	fakeOpClose        = -11

	// fakeErrNoNode is the error code for a node that does not exist.
	fakeErrNoNode = -101
)

// fakeZkServer speaks just enough of the ZooKeeper wire protocol to
// establish and resume sessions, answer create, getData, getChildren2,
// exists and pings, and accept delete and setWatches. Every node exists,
// and has the children in children. Creating a path that ends with a slash
// creates the last of children in it, like a sequential node. It lets
// tests control when connections drop and how long a reconnect takes, and
// hold replies, which a real ZooKeeper does not.
type fakeZkServer struct {
	t         *testing.T
	listeners []net.Listener
	children  []string

	// requests receives the opcode and path of each request with a path,
	// once the server has replied to it.
	requests chan fakeZkRequest

	// connectDelay is how long the server waits before answering a
	// connect request.
	connectDelay atomic.Int64

	// getDataErr is the error code the server answers getData with.
	getDataErr atomic.Int32

	// releaseDeletes, if set, makes the server report delete requests
	// before replying to them, and wait until it is closed to reply.
	releaseDeletes chan struct{}

	// done is closed when the test ends.
	done chan struct{}

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
		requests: make(chan fakeZkRequest, 1000),
		done:     make(chan struct{}),
		conns:    make(map[net.Conn]struct{}),
	}
	for range n {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		s.listeners = append(s.listeners, l)
		go s.serve(l)
	}
	t.Cleanup(func() {
		close(s.done)
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

// waitForRequest waits until the server has replied to a request with
// opcode, and returns its path.
func (s *fakeZkServer) waitForRequest(opcode int32) string {
	var path string
	require.Eventually(s.t, func() bool {
		for {
			select {
			case req := <-s.requests:
				if req.opcode == opcode {
					path = req.path
					return true
				}
			default:
				return false
			}
		}
	}, 30*time.Second, 10*time.Millisecond, "fake zk server: no request with opcode %d", opcode)
	return path
}

// numConns returns the number of open client connections.
func (s *fakeZkServer) numConns() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
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

		var path string
		switch opcode {
		case fakeOpCreate, fakeOpDelete, fakeOpExists, fakeOpGetData, fakeOpGetChildren2:
			// The request starts with the path.
			size := binary.BigEndian.Uint32(req[8:12])
			path = string(req[12 : 12+size])
		}
		var errCode int32
		if opcode == fakeOpGetData {
			errCode = s.getDataErr.Load()
		}
		if opcode == fakeOpDelete && s.releaseDeletes != nil {
			s.requests <- fakeZkRequest{opcode: opcode, path: path}
			path = ""
			select {
			case <-s.releaseDeletes:
			case <-s.done:
				return
			}
		}

		// responseHeader: xid int32, zxid int64, err int32.
		resp := binary.BigEndian.AppendUint32(nil, xid)
		resp = binary.BigEndian.AppendUint64(resp, 1)
		resp = binary.BigEndian.AppendUint32(resp, uint32(errCode))
		if errCode != 0 {
			if writeFrame(c, resp) != nil {
				return
			}
			s.requests <- fakeZkRequest{opcode: opcode, path: path}
			continue
		}
		// A Stat is 68 bytes.
		stat := make([]byte, 68)
		switch opcode {
		case fakeOpCreate:
			// createResponse: the path created.
			created := path
			if strings.HasSuffix(path, "/") && len(s.children) > 0 {
				created += s.children[len(s.children)-1]
			}
			resp = appendString(resp, created)
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
		if path != "" {
			s.requests <- fakeZkRequest{opcode: opcode, path: path}
		}
	}
}

// dropConnectionsAfterSlowReconnect drops the client connections, and makes
// reconnecting take longer than zk.Conn.Close waits to send its close
// request.
func (s *fakeZkServer) dropConnectionsAfterSlowReconnect() {
	s.connectDelay.Store(int64(1500 * time.Millisecond))
	s.dropConnections()
}

// receive waits for a value on ch.
func receive[T any](t *testing.T, ch <-chan T, msg string) T {
	var v T
	require.Eventually(t, func() bool {
		select {
		case v = <-ch:
			return true
		default:
			return false
		}
	}, 30*time.Second, 10*time.Millisecond, msg)
	return v
}

// healthyFor is how long tests check that nothing fails while the
// connection is healthy.
const healthyFor = 500 * time.Millisecond

// TestWatchNotifiedWhenConnectionDrops checks that a watch reports an error
// once its ZooKeeper connection drops, even when the client library resumes
// the session on another server before Vitess's Close takes effect. In that
// case the library keeps the session alive but can no longer set its watches
// again, so without an error the watch would never fire again. It also
// checks that the resumed session is ended.
func TestWatchNotifiedWhenConnectionDrops(t *testing.T) {
	fake := startFakeZkServer(t, 2)
	zs := NewServer(fake.addr(), "/")
	t.Cleanup(zs.Close)

	_, changes, err := zs.Watch(t.Context(), "keyspace/SrvVSchema")
	require.NoError(t, err)
	require.Never(t, func() bool { return len(changes) > 0 }, healthyFor, 10*time.Millisecond,
		"watch fired on a healthy connection")

	fake.dropConnectionsAfterSlowReconnect()

	wd := receive(t, changes, "watch was not notified after its connection dropped")
	require.NotNil(t, wd, "watch channel closed without an error")
	require.ErrorContains(t, wd.Err, zk.ErrClosing.Error())

	require.Eventually(t, func() bool { return fake.numConns() == 0 }, 30*time.Second, 10*time.Millisecond,
		"the resumed session still has a connection")
}

// TestSocketDialerRefusesSocketsOnceClosed checks that closing a
// socketDialer closes a socket that is still being dialed, and that it does
// not dial again. Otherwise a reconnect that is dialing when we drop the
// connection would resume the session after all.
func TestSocketDialerRefusesSocketsOnceClosed(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { server.Close() })

	var dials atomic.Int32
	dialing := make(chan struct{})
	release := make(chan struct{})
	d := &socketDialer{dial: func(network, address string, timeout time.Duration) (net.Conn, error) {
		if dials.Add(1) == 1 {
			close(dialing)
			<-release
		}
		return client, nil
	}}

	errc := make(chan error, 1)
	go func() {
		_, err := d.Dial("tcp", "zk:2181", time.Second)
		errc <- err
	}()
	receive(t, dialing, "dial did not start")
	d.Close()
	close(release)

	require.ErrorIs(t, receive(t, errc, "Dial did not return"), errSocketDialerClosed)
	_, err := server.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF, "the socket dialed during Close was not closed")

	_, err = d.Dial("tcp", "zk:2181", time.Second)
	require.ErrorIs(t, err, errSocketDialerClosed)
	require.EqualValues(t, 1, dials.Load(), "dialed again after Close")
}

// TestObtainQueueLockFailsWhenConnectionDrops checks that waiting for a
// queue lock fails when the connection drops, instead of reading the queue
// again on a new connection: our node belongs to the session of the dropped
// connection, and goes away with it.
func TestObtainQueueLockFailsWhenConnectionDrops(t *testing.T) {
	fake := startFakeZkServer(t, 2, "lock-0000000001", "lock-0000000002")
	conn := Connect(fake.addr())
	t.Cleanup(func() { conn.Close() })

	errc := make(chan error, 1)
	go func() {
		errc <- obtainQueueLock(t.Context(), conn, "/locks/lock-0000000002", true)
	}()
	require.Equal(t, "/locks/lock-0000000001", fake.waitForRequest(fakeOpExists))
	require.Never(t, func() bool { return len(errc) > 0 }, healthyFor, 10*time.Millisecond,
		"obtainQueueLock returned on a healthy connection")

	fake.dropConnectionsAfterSlowReconnect()

	err := receive(t, errc, "obtainQueueLock did not fail after its connection dropped")
	_, cause := vterrors.Unwrap(err)
	require.ErrorIs(t, cause, zk.ErrClosing)
}

// TestLeaderDeletesProposalWhenConnectionDrops checks that a leader stops
// leading when its connection drops, and only then deletes its proposal, so
// the other participants don't wait for the session to expire.
func TestLeaderDeletesProposalWhenConnectionDrops(t *testing.T) {
	fake := startFakeZkServer(t, 2)
	fake.releaseDeletes = make(chan struct{})
	zs := NewServer(fake.addr(), "/")
	t.Cleanup(zs.Close)

	mp := &zkLeaderParticipation{
		zs:      zs,
		name:    "election",
		stopCtx: t.Context(),
		done:    make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(t.Context())
	go mp.watchLeadership(ctx, zs.conn, "/election/proposal", cancel)
	require.Equal(t, "/election/proposal", fake.waitForRequest(fakeOpGetData))
	require.Never(t, func() bool { return ctx.Err() != nil }, healthyFor, 10*time.Millisecond,
		"leadership was canceled on a healthy connection")

	fake.dropConnectionsAfterSlowReconnect()

	// The server holds its reply to the delete, so that leadership can only
	// have been canceled before the delete: no other participant can become
	// leader while we still lead.
	require.Equal(t, "/election/proposal", fake.waitForRequest(fakeOpDelete))
	require.Error(t, ctx.Err(), "leadership was not canceled before deleting the proposal")
	close(fake.releaseDeletes)
}

// TestLeaderDeletesProposalWhenWatchCannotBeSet checks that a new leader
// that cannot watch its proposal stops leading and deletes it.
func TestLeaderDeletesProposalWhenWatchCannotBeSet(t *testing.T) {
	fake := startFakeZkServer(t, 2)
	fake.getDataErr.Store(fakeErrNoNode)
	zs := NewServer(fake.addr(), "/")
	t.Cleanup(zs.Close)

	mp := &zkLeaderParticipation{
		zs:      zs,
		name:    "election",
		stopCtx: t.Context(),
		done:    make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(t.Context())
	go mp.watchLeadership(ctx, zs.conn, "/election/proposal", cancel)

	require.Equal(t, "/election/proposal", fake.waitForRequest(fakeOpDelete))
	require.Eventually(t, func() bool { return ctx.Err() != nil }, 30*time.Second, 10*time.Millisecond,
		"leadership was not canceled")
}

// TestWaitForLeadershipDeletesProposalWhenConnectionDrops checks that a
// participant waiting for leadership deletes its proposal when its
// connection drops, so the participants behind it don't wait for the
// session to expire.
func TestWaitForLeadershipDeletesProposalWhenConnectionDrops(t *testing.T) {
	fake := startFakeZkServer(t, 2, "0000000001", "0000000002")
	zs := NewServer(fake.addr(), "/")
	t.Cleanup(zs.Close)

	mp, err := zs.NewLeaderParticipation("election", "id")
	require.NoError(t, err)
	errc := make(chan error, 1)
	go func() {
		_, err := mp.WaitForLeadership()
		errc <- err
	}()
	require.Equal(t, "/elections/election/0000000001", fake.waitForRequest(fakeOpExists))
	require.Never(t, func() bool { return len(errc) > 0 }, healthyFor, 10*time.Millisecond,
		"WaitForLeadership returned on a healthy connection")

	fake.dropConnectionsAfterSlowReconnect()

	require.Equal(t, "/elections/election/0000000002", fake.waitForRequest(fakeOpDelete))
	require.Error(t, receive(t, errc, "WaitForLeadership did not return"))
}

// TestWaitForLeadershipReturnsWhenServerCloses checks that closing the
// server while a participant waits for leadership makes it return an error,
// and that its cleanup does not connect again.
func TestWaitForLeadershipReturnsWhenServerCloses(t *testing.T) {
	fake := startFakeZkServer(t, 1, "0000000001", "0000000002")
	zs := NewServer(fake.addr(), "/")

	mp, err := zs.NewLeaderParticipation("election", "id")
	require.NoError(t, err)
	errc := make(chan error, 1)
	go func() {
		_, err := mp.WaitForLeadership()
		errc <- err
	}()
	require.Equal(t, "/elections/election/0000000001", fake.waitForRequest(fakeOpExists))

	zs.Close()

	require.Error(t, receive(t, errc, "WaitForLeadership did not return"))
	require.Eventually(t, func() bool { return fake.numConns() == 0 }, 30*time.Second, 10*time.Millisecond,
		"connected again after the server was closed")
}

// TestLockReturnsWhenServerCloses checks that closing the server while
// waiting for a lock makes Lock return an error, and that its cleanup does
// not connect again.
func TestLockReturnsWhenServerCloses(t *testing.T) {
	fake := startFakeZkServer(t, 1, "0000000001", "0000000002")
	zs := NewServer(fake.addr(), "/")

	errc := make(chan error, 1)
	go func() {
		_, err := zs.Lock(t.Context(), "keyspace", "contents")
		errc <- err
	}()
	require.Equal(t, "/keyspace/locks/0000000001", fake.waitForRequest(fakeOpExists))

	zs.Close()

	require.Error(t, receive(t, errc, "Lock did not return"))
	require.Eventually(t, func() bool { return fake.numConns() == 0 }, 30*time.Second, 10*time.Millisecond,
		"connected again after the server was closed")
}
