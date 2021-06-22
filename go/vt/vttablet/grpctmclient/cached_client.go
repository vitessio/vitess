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
	ConnReuse:            stats.NewGauge("tabletmanagerclient_cachedconn_reuse", "number of times a call to dial() was able to reuse an existing connection"),
	ConnNew:              stats.NewGauge("tabletmanagerclient_cachedconn_new", "number of times a call to dial() resulted in a dialing a new grpc clientconn"),
	DialTimeouts:         stats.NewGauge("tabletmanagerclient_cachedconn_dial_timeouts", "number of context timeouts during dial()"),
	DialTimings:          stats.NewTimings("tabletmanagerclient_cachedconn_dialtimings", "timings for various dial paths", "path", "rlock_fast", "sema_fast", "sema_poll"),
	EvictionQueueTimings: stats.NewTimings("tabletmanagerclient_cachedconn_eviction_queue_timings", "timings for eviction queue management operations", "operation", "init", "push", "pop", "fix"),
}

// NewCachedConnClient returns a grpc Client using the priority queue cache
// dialer implementation.
func NewCachedConnClient(capacity int) *Client {
	dialer := &cachedConnDialer{
		conns:        make(map[string]*cachedConn, capacity),
		queue:        make(cachedConns, 0, capacity),
		connWaitSema: sync2.NewSemaphore(capacity, 0),
	}

	dialer.heapInit()
	return &Client{dialer}
}

var _ dialer = (*cachedConnDialer)(nil)

func (dialer *cachedConnDialer) dial(ctx context.Context, tablet *topodatapb.Tablet) (tabletmanagerservicepb.TabletManagerClient, io.Closer, error) {
	start := time.Now()
	addr := getTabletAddr(tablet)

	if client, closer, found, err := dialer.tryFromCache(addr, dialer.m.RLocker()); found {
		dialerStats.DialTimings.Add("rlock_fast", time.Since(start))
		return client, closer, err
	}

	if dialer.connWaitSema.TryAcquire() {
		defer func() {
			dialerStats.DialTimings.Add("sema_fast", time.Since(start))
		}()
		dialer.m.Lock()
		defer dialer.m.Unlock()

		// Check if another goroutine managed to dial a conn for the same addr
		// while we were waiting for the write lock. This is identical to the
		// read-lock section above.
		if client, closer, found, err := dialer.tryFromCache(addr, nil); found {
			return client, closer, err
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
			if client, closer, found, err := dialer.pollOnce(addr); found {
				return client, closer, err
			}
		}
	}
}

// tryFromCache tries to get a connection from the cache, performing a redial
// on that connection if it exists. It returns a TabletManagerClient impl, an
// io.Closer, a flag to indicate whether a connection was found in the cache,
// and an error, which is always nil.
//
// In addition to the addr being dialed, tryFromCache takes a sync.Locker which,
// if not nil, will be used to wrap the lookup and redial in that lock. This
// function can be called in situations where the conns map is locked
// externally (like in pollOnce), so we do not want to manage the locks here. In
// other cases (like in the rlock_fast path of dial()), we pass in the RLocker
// to ensure we have a read lock on the cache for the duration of the call.
func (dialer *cachedConnDialer) tryFromCache(addr string, locker sync.Locker) (client tabletmanagerservicepb.TabletManagerClient, closer io.Closer, found bool, err error) {
	if locker != nil {
		locker.Lock()
		defer locker.Unlock()
	}

	if conn, ok := dialer.conns[addr]; ok {
		client, closer, err := dialer.redial(conn)
		return client, closer, ok, err
	}

	return nil, nil, false, nil
}

// pollOnce is called on each iteration of the polling loop in dial(). It:
// - locks the conns cache for writes
// - attempts to get a connection from the cache. If found, redial() it and exit.
// - locks the queue
// - peeks at the head of the eviction queue. if the peeked conn has no refs, it
//   is unused, and can be evicted to make room for the new connection to addr.
//   If the peeked conn has refs, exit.
// - pops the conn we just peeked from the queue, delete it from the cache, and
//   close the underlying ClientConn for that conn.
// - attempt a newdial. if the newdial fails, it will release a slot on the
//   connWaitSema, so another dial() call can successfully acquire it to dial
//   a new conn. if the newdial succeeds, we will have evicted one conn, but
//   added another, so the net change is 0, and no changes to the connWaitSema
//   are made.
//
// It returns a TabletManagerClient impl, an io.Closer, a flag to indicate
// whether the dial() poll loop should exit, and an error.
func (dialer *cachedConnDialer) pollOnce(addr string) (client tabletmanagerservicepb.TabletManagerClient, closer io.Closer, found bool, err error) {
	dialer.m.Lock()
	defer dialer.m.Unlock()

	if client, closer, found, err := dialer.tryFromCache(addr, nil); found {
		return client, closer, found, err
	}

	dialer.qMu.Lock()
	defer dialer.qMu.Unlock()

	conn := dialer.queue[0]
	if conn.refs != 0 {
		return nil, nil, false, nil
	}

	dialer.heapPop()
	delete(dialer.conns, conn.key)
	conn.cc.Close()

	client, closer, err = dialer.newdial(addr, false /* manage queue lock */)
	return client, closer, true, err
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
	dialer.heapPush(conn)
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
	dialer.heapFix(conn.index)

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
		dialer.heapFix(conn.index)
		return nil
	}), nil
}

// Functions to wrap queue operations to record timings for them.

func (dialer *cachedConnDialer) heapInit() {
	start := time.Now()
	heap.Init(&dialer.queue)
	dialerStats.EvictionQueueTimings.Add("init", time.Since(start))
}

func (dialer *cachedConnDialer) heapFix(index int) {
	start := time.Now()
	heap.Fix(&dialer.queue, index)
	dialerStats.EvictionQueueTimings.Add("fix", time.Since(start))
}

func (dialer *cachedConnDialer) heapPop() interface{} {
	start := time.Now()
	x := heap.Pop(&dialer.queue)
	dialerStats.EvictionQueueTimings.Add("pop", time.Since(start))

	return x
}

func (dialer *cachedConnDialer) heapPush(conn *cachedConn) {
	start := time.Now()
	heap.Push(&dialer.queue, conn)
	dialerStats.EvictionQueueTimings.Add("push", time.Since(start))
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
