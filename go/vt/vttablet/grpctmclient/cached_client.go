/*
Copyright 2021 The Vitess Authors.

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

package grpctmclient

import (
	"container/heap"
	"context"
	"flag"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	tabletmanagerservicepb "vitess.io/vitess/go/vt/proto/tabletmanagerservice"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	defaultPoolCapacity = flag.Int("tablet_manager_grpc_connpool_size", 100, "number of tablets to keep tmclient connections open to")
)

func init() {
	tmclient.RegisterTabletManagerClientFactory("grpc-cached", func() tmclient.TabletManagerClient {
		return NewCachedConnClient(*defaultPoolCapacity)
	})
}

// closeFunc allows a standalone function to implement io.Closer, similar to
// how http.HandlerFunc allows standalone functions to implement http.Handler.
type closeFunc func() error

func (fn closeFunc) Close() error {
	return fn()
}

var _ io.Closer = (*closeFunc)(nil)

type cachedConn struct {
	tabletmanagerservicepb.TabletManagerClient
	cc *grpc.ClientConn

	lastAccessTime time.Time
	refs           int

	index int
	key   string
}

// cachedConns provides a priority queue implementation for O(log n) connection
// eviction management. It is a nearly verbatim copy of the priority queue
// sample at https://golang.org/pkg/container/heap/#pkg-overview, with the
// Less function changed such that connections with refs==0 get pushed to the
// front of the queue, because those are the connections we are going to want
// to evict.
type cachedConns []*cachedConn

var _ heap.Interface = (*cachedConns)(nil)

// Len is part of the sort.Interface interface and is used by container/heap
// functions.
func (queue cachedConns) Len() int { return len(queue) }

// Less is part of the sort.Interface interface and is used by container/heap
// functions.
func (queue cachedConns) Less(i, j int) bool {
	left, right := queue[i], queue[j]
	if left.refs == right.refs {
		// break ties by access time.
		// more stale connections have higher priority for removal
		// this condition is equvalent to:
		//		left.lastAccessTime <= right.lastAccessTime
		return !left.lastAccessTime.After(right.lastAccessTime)
	}

	// connections with fewer refs have higher priority for removal
	return left.refs < right.refs
}

// Swap is part of the sort.Interface interface and is used by container/heap
// functions.
func (queue cachedConns) Swap(i, j int) {
	queue[i], queue[j] = queue[j], queue[i]
	queue[i].index = i
	queue[j].index = j
}

// Push is part of the container/heap.Interface interface.
func (queue *cachedConns) Push(x interface{}) {
	n := len(*queue)
	conn := x.(*cachedConn)
	conn.index = n
	*queue = append(*queue, conn)
}

// Pop is part of the container/heap.Interface interface.
func (queue *cachedConns) Pop() interface{} {
	old := *queue
	n := len(old)
	conn := old[n-1]
	old[n-1] = nil  // avoid memory leak
	conn.index = -1 // for safety
	*queue = old[0 : n-1]

	return conn
}

type cachedConnDialer struct {
	m            sync.RWMutex
	conns        map[string]*cachedConn
	qMu          sync.Mutex
	queue        cachedConns
	connWaitSema *sync2.Semaphore
}

var dialerStats = struct {
	ConnReuse            *stats.Gauge
	ConnNew              *stats.Gauge
	DialTimeouts         *stats.Gauge
	DialTimings          *stats.Timings
	EvictionQueueTimings *stats.Timings
}{
	ConnReuse:    stats.NewGauge("tabletmanagerclient_cachedconn_reuse", "number of times a call to dial() was able to reuse an existing connection"),
	ConnNew:      stats.NewGauge("tabletmanagerclient_cachedconn_new", "number of times a call to dial() resulted in a dialing a new grpc clientconn"),
	DialTimeouts: stats.NewGauge("tabletmanagerclient_cachedconn_dial_timeouts", "number of context timeouts during dial()"),
	DialTimings:  stats.NewTimings("tabletmanagerclient_cachedconn_dialtimings", "timings for various dial paths", "path", "rlock_fast", "sema_fast", "sema_poll"),
	// TODO: add timings for heap operations (push, pop, fix)
}

// NewCachedConnClient returns a grpc Client using the priority queue cache
// dialer implementation.
func NewCachedConnClient(capacity int) *Client {
	dialer := &cachedConnDialer{
		conns:        make(map[string]*cachedConn, capacity),
		queue:        make(cachedConns, 0, capacity),
		connWaitSema: sync2.NewSemaphore(capacity, 0),
	}

	heap.Init(&dialer.queue)
	return &Client{dialer}
}

var _ dialer = (*cachedConnDialer)(nil)

func (dialer *cachedConnDialer) dial(ctx context.Context, tablet *topodatapb.Tablet) (tabletmanagerservicepb.TabletManagerClient, io.Closer, error) {
	start := time.Now()

	addr := getTabletAddr(tablet)
	dialer.m.RLock()
	if conn, ok := dialer.conns[addr]; ok {
		defer func() {
			dialerStats.DialTimings.Add("rlock_fast", time.Since(start))
		}()
		defer dialer.m.RUnlock()
		return dialer.redial(conn)
	}
	dialer.m.RUnlock()

	if dialer.connWaitSema.TryAcquire() {
		defer func() {
			dialerStats.DialTimings.Add("sema_fast", time.Since(start))
		}()
		dialer.m.Lock()
		defer dialer.m.Unlock()

		// Check if another goroutine managed to dial a conn for the same addr
		// while we were waiting for the write lock. This is identical to the
		// read-lock section above.
		if conn, ok := dialer.conns[addr]; ok {
			return dialer.redial(conn)
		}

		return dialer.newdial(addr, true /* manage queue lock */)
	}

	defer func() {
		dialerStats.DialTimings.Add("sema_poll", time.Since(start))
	}()

	for {
		select {
		case <-ctx.Done():
			dialerStats.DialTimeouts.Add(1)
			return nil, nil, ctx.Err()
		default:
			dialer.m.Lock()
			if conn, ok := dialer.conns[addr]; ok {
				// Someone else dialed this addr while we were polling. No need
				// to evict anyone else, just reuse the existing conn.
				defer dialer.m.Unlock()
				return dialer.redial(conn)
			}

			dialer.qMu.Lock()
			conn := dialer.queue[0]
			if conn.refs != 0 {
				dialer.qMu.Unlock()
				dialer.m.Unlock()
				continue
			}

			// We're going to return from this point
			defer dialer.m.Unlock()
			defer dialer.qMu.Unlock()
			heap.Pop(&dialer.queue)
			delete(dialer.conns, conn.key)
			conn.cc.Close()

			return dialer.newdial(addr, false /* manage queue lock */)
		}
	}
}

// newdial creates a new cached connection, and updates the cache and eviction
// queue accordingly. This must be called only while holding the write lock on
// dialer.m as well as after having successfully acquired the dialer.connWaitSema. If newdial fails to create the underlying
// gRPC connection, it will make a call to Release the connWaitSema for other
// newdial calls.
//
// It returns the three-tuple of client-interface, closer, and error that the
// main dial func returns.
func (dialer *cachedConnDialer) newdial(addr string, manageQueueLock bool) (tabletmanagerservicepb.TabletManagerClient, io.Closer, error) {
	dialerStats.ConnNew.Add(1)

	opt, err := grpcclient.SecureDialOption(*cert, *key, *ca, *name)
	if err != nil {
		dialer.connWaitSema.Release()
		return nil, nil, err
	}

	cc, err := grpcclient.Dial(addr, grpcclient.FailFast(false), opt)
	if err != nil {
		dialer.connWaitSema.Release()
		return nil, nil, err
	}

	// In the case where dial is evicting a connection from the cache, we
	// already have a lock on the eviction queue. Conversely, in the case where
	// we are able to create a new connection without evicting (because the
	// cache is not yet full), we don't have the queue lock yet.
	if manageQueueLock {
		dialer.qMu.Lock()
		defer dialer.qMu.Unlock()
	}

	conn := &cachedConn{
		TabletManagerClient: tabletmanagerservicepb.NewTabletManagerClient(cc),
		cc:                  cc,
		lastAccessTime:      time.Now(),
		refs:                1,
		index:               -1, // gets set by call to Push
		key:                 addr,
	}
	heap.Push(&dialer.queue, conn)
	dialer.conns[addr] = conn

	return dialer.connWithCloser(conn)
}

// redial takes an already-dialed connection in the cache does all the work of
// lending that connection out to one more caller. this should only ever be
// called while holding at least the RLock on dialer.m (but the write lock is
// fine too), to prevent the connection from getting evicted out from under us.
//
// It returns the three-tuple of client-interface, closer, and error that the
// main dial func returns.
func (dialer *cachedConnDialer) redial(conn *cachedConn) (tabletmanagerservicepb.TabletManagerClient, io.Closer, error) {
	dialerStats.ConnReuse.Add(1)

	dialer.qMu.Lock()
	defer dialer.qMu.Unlock()

	conn.lastAccessTime = time.Now()
	conn.refs++
	heap.Fix(&dialer.queue, conn.index)

	return dialer.connWithCloser(conn)
}

// connWithCloser returns the three-tuple expected by the main dial func, where
// the closer handles the correct state management for updating the conns place
// in the eviction queue.
func (dialer *cachedConnDialer) connWithCloser(conn *cachedConn) (tabletmanagerservicepb.TabletManagerClient, io.Closer, error) {
	return conn, closeFunc(func() error {
		dialer.qMu.Lock()
		defer dialer.qMu.Unlock()

		conn.refs--
		heap.Fix(&dialer.queue, conn.index)
		return nil
	}), nil
}

// Close closes all currently cached connections, ***regardless of whether
// those connections are in use***. Calling Close therefore will fail any RPCs
// using currently lent-out connections, and, furthermore, will invalidate the
// io.Closer that was returned for that connection from dialer.dial().
//
// As a result, it is not safe to reuse a cachedConnDialer after calling Close,
// and you should instead obtain a new one by calling either
// tmclient.TabletManagerClient() with
// TabletManagerProtocol set to "grpc-cached", or by calling
// grpctmclient.NewCachedConnClient directly.
func (dialer *cachedConnDialer) Close() {
	dialer.m.Lock()
	defer dialer.m.Unlock()
	dialer.qMu.Lock()
	defer dialer.qMu.Unlock()

	for dialer.queue.Len() > 0 {
		conn := dialer.queue.Pop().(*cachedConn)
		conn.cc.Close()
		delete(dialer.conns, conn.key)
		dialer.connWaitSema.Release()
	}
}

func getTabletAddr(tablet *topodatapb.Tablet) string {
	return netutil.JoinHostPort(tablet.Hostname, int32(tablet.PortMap["grpc"]))
}
