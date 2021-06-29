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
	"context"
	"flag"
	"io"
	"sort"
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

	addr           string
	lastAccessTime time.Time
	refs           int
}

type cachedConnDialer struct {
	m            sync.Mutex
	conns        map[string]*cachedConn
	evict        []*cachedConn
	evictSorted  bool
	connWaitSema *sync2.Semaphore
}

var dialerStats = struct {
	ConnReuse    *stats.Gauge
	ConnNew      *stats.Gauge
	DialTimeouts *stats.Gauge
	DialTimings  *stats.Timings
}{
	ConnReuse:    stats.NewGauge("tabletmanagerclient_cachedconn_reuse", "number of times a call to dial() was able to reuse an existing connection"),
	ConnNew:      stats.NewGauge("tabletmanagerclient_cachedconn_new", "number of times a call to dial() resulted in a dialing a new grpc clientconn"),
	DialTimeouts: stats.NewGauge("tabletmanagerclient_cachedconn_dial_timeouts", "number of context timeouts during dial()"),
	DialTimings:  stats.NewTimings("tabletmanagerclient_cachedconn_dialtimings", "timings for various dial paths", "path", "rlock_fast", "sema_fast", "sema_poll"),
}

// NewCachedConnClient returns a grpc Client that caches connections to the different tablets
func NewCachedConnClient(capacity int) *Client {
	dialer := &cachedConnDialer{
		conns:        make(map[string]*cachedConn, capacity),
		evict:        make([]*cachedConn, 0, capacity),
		connWaitSema: sync2.NewSemaphore(capacity, 0),
	}
	return &Client{dialer}
}

var _ dialer = (*cachedConnDialer)(nil)

func (dialer *cachedConnDialer) sortEvictionsLocked() {
	if !dialer.evictSorted {
		sort.Slice(dialer.evict, func(i, j int) bool {
			left, right := dialer.evict[i], dialer.evict[j]
			if left.refs == right.refs {
				return right.lastAccessTime.After(left.lastAccessTime)
			}
			return right.refs > left.refs
		})
		dialer.evictSorted = true
	}
}

func (dialer *cachedConnDialer) dial(ctx context.Context, tablet *topodatapb.Tablet) (tabletmanagerservicepb.TabletManagerClient, io.Closer, error) {
	start := time.Now()
	addr := getTabletAddr(tablet)

	if client, closer, found, err := dialer.tryFromCache(addr, &dialer.m); found {
		dialerStats.DialTimings.Add("rlock_fast", time.Since(start))
		return client, closer, err
	}

	if dialer.connWaitSema.TryAcquire() {
		defer func() {
			dialerStats.DialTimings.Add("sema_fast", time.Since(start))
		}()

		// Check if another goroutine managed to dial a conn for the same addr
		// while we were waiting for the write lock. This is identical to the
		// read-lock section above.
		if client, closer, found, err := dialer.tryFromCache(addr, &dialer.m); found {
			return client, closer, err
		}
		return dialer.newdial(addr)
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
		client, closer, err := dialer.redialLocked(conn)
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

	if client, closer, found, err := dialer.tryFromCache(addr, nil); found {
		dialer.m.Unlock()
		return client, closer, found, err
	}

	dialer.sortEvictionsLocked()

	conn := dialer.evict[0]
	if conn.refs != 0 {
		dialer.m.Unlock()
		return nil, nil, false, nil
	}

	dialer.evict = dialer.evict[1:]
	delete(dialer.conns, conn.addr)
	conn.cc.Close()
	dialer.m.Unlock()

	client, closer, err = dialer.newdial(addr)
	return client, closer, true, err
}

// newdial creates a new cached connection, and updates the cache and eviction
// queue accordingly. If newdial fails to create the underlying
// gRPC connection, it will make a call to Release the connWaitSema for other
// newdial calls.
//
// It returns the three-tuple of client-interface, closer, and error that the
// main dial func returns.
func (dialer *cachedConnDialer) newdial(addr string) (tabletmanagerservicepb.TabletManagerClient, io.Closer, error) {
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

	dialer.m.Lock()
	defer dialer.m.Unlock()

	if conn, existing := dialer.conns[addr]; existing {
		// race condition: some other goroutine has dialed our tablet before we have;
		// this is not great, but shouldn't happen often (if at all), so we're going to
		// close this connection and reuse the existing one. by doing this, we can keep
		// the actual Dial out of the global lock and significantly increase throughput
		cc.Close()
		dialer.connWaitSema.Release()
		return dialer.redialLocked(conn)
	}

	conn := &cachedConn{
		TabletManagerClient: tabletmanagerservicepb.NewTabletManagerClient(cc),
		cc:                  cc,
		lastAccessTime:      time.Now(),
		refs:                1,
		addr:                addr,
	}
	dialer.evict = append(dialer.evict, conn)
	// dialer.evictSorted = false -- no need to do this here
	dialer.conns[addr] = conn

	return dialer.connWithCloser(conn)
}

// redialLocked takes an already-dialed connection in the cache does all the work of
// lending that connection out to one more caller. this should only ever be
// called while holding at least the RLock on dialer.m (but the write lock is
// fine too), to prevent the connection from getting evicted out from under us.
//
// It returns the three-tuple of client-interface, closer, and error that the
// main dial func returns.
func (dialer *cachedConnDialer) redialLocked(conn *cachedConn) (tabletmanagerservicepb.TabletManagerClient, io.Closer, error) {
	dialerStats.ConnReuse.Add(1)
	conn.lastAccessTime = time.Now()
	conn.refs++
	dialer.evictSorted = false
	return dialer.connWithCloser(conn)
}

// connWithCloser returns the three-tuple expected by the main dial func, where
// the closer handles the correct state management for updating the conns place
// in the eviction queue.
func (dialer *cachedConnDialer) connWithCloser(conn *cachedConn) (tabletmanagerservicepb.TabletManagerClient, io.Closer, error) {
	return conn, closeFunc(func() error {
		dialer.m.Lock()
		defer dialer.m.Unlock()
		conn.refs--
		dialer.evictSorted = false
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

	for _, conn := range dialer.evict {
		conn.cc.Close()
		delete(dialer.conns, conn.addr)
		dialer.connWaitSema.Release()
	}
	dialer.evict = nil
}

func getTabletAddr(tablet *topodatapb.Tablet) string {
	return netutil.JoinHostPort(tablet.Hostname, int32(tablet.PortMap["grpc"]))
}
