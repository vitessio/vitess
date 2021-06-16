// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package drpcpool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"storj.io/drpc"
)

type Dialer = func(ctx context.Context) (drpc.Conn, error)

// connReuseStrategy determines how (*DB).conn returns database connections.
type connReuseStrategy uint8

const (
	// alwaysNewConn forces a new connection to the database.
	alwaysNewConn connReuseStrategy = iota
	// cachedOrNewConn returns a cached connection, if available, else waits
	// for one to become available (if MaxOpenConns has been reached) or
	// creates a new database connection.
	cachedOrNewConn
)

// ConnectionPool grabs a connection from the pool for every invoke/stream.
type ConnectionPool struct {
	waitDuration int64

	dialer   Dialer
	closed   bool
	freeConn []*conn
	mu       sync.Mutex

	connRequests map[uint64]chan connRequest

	maxIdleCount int
	maxOpen      int
	numOpen      int
	maxIdleTime  time.Duration
	maxLifetime  time.Duration

	maxIdleClosed     int64
	maxLifetimeClosed int64
	maxIdleTimeClosed int64
	waitCount         int64
	cleanerCh         chan struct{}
	openerCh          chan struct{}

	nextRequest uint64
	stop        func()
}

type connRequest struct {
	conn *conn
	err  error
}

type conn struct {
	pool *ConnectionPool

	ci         drpc.Conn
	createdAt  time.Time
	returnedAt time.Time // Time the connection was created or returned.
	inUse      bool
}

func (dc *conn) expired(timeout time.Duration) bool {
	if timeout <= 0 {
		return false
	}
	return dc.createdAt.Add(timeout).Before(nowFunc())
}

func (dc *conn) Close() error {
	return dc.ci.Close()
}

// nextRequestKeyLocked returns the next connection request key.
// It is assumed that nextRequest will not overflow.
func (pool *ConnectionPool) nextRequestKeyLocked() uint64 {
	next := pool.nextRequest
	pool.nextRequest++
	return next
}

var errDBClosed = errors.New("drpc: database is closed")
var errBadConn = errors.New("drpc: connection has become stale")

// conn returns a newly-opened or cached *driverConn.
func (pool *ConnectionPool) conn(ctx context.Context, strategy connReuseStrategy) (*conn, error) {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return nil, errDBClosed
	}
	// Check if the context is expired.
	select {
	default:
	case <-ctx.Done():
		pool.mu.Unlock()
		return nil, ctx.Err()
	}
	lifetime := pool.maxLifetime

	// Prefer a free connection, if possible.
	numFree := len(pool.freeConn)
	if strategy == cachedOrNewConn && numFree > 0 {
		conn := pool.freeConn[0]
		copy(pool.freeConn, pool.freeConn[1:])
		pool.freeConn = pool.freeConn[:numFree-1]
		conn.inUse = true
		if conn.expired(lifetime) {
			pool.maxLifetimeClosed++
			pool.mu.Unlock()
			conn.Close()
			return nil, errBadConn
		}
		pool.mu.Unlock()

		if conn.ci.Closed() {
			conn.Close()
			return nil, errBadConn
		}
		return conn, nil
	}

	// Out of free connections or we were asked not to use one. If we're not
	// allowed to open any more connections, make a request and wait.
	if pool.maxOpen > 0 && pool.numOpen >= pool.maxOpen {
		// Make the connRequest channel. It's buffered so that the
		// connectionOpener doesn't block while waiting for the req to be read.
		req := make(chan connRequest, 1)
		reqKey := pool.nextRequestKeyLocked()
		pool.connRequests[reqKey] = req
		pool.waitCount++
		pool.mu.Unlock()

		waitStart := nowFunc()

		// Timeout the connection request with the context.
		select {
		case <-ctx.Done():
			// Remove the connection request and ensure no value has been sent
			// on it after removing.
			pool.mu.Lock()
			delete(pool.connRequests, reqKey)
			pool.mu.Unlock()

			atomic.AddInt64(&pool.waitDuration, int64(time.Since(waitStart)))

			select {
			default:
			case ret, ok := <-req:
				if ok && ret.conn != nil {
					pool.putConn(ret.conn, ret.err)
				}
			}
			return nil, ctx.Err()
		case ret, ok := <-req:
			atomic.AddInt64(&pool.waitDuration, int64(time.Since(waitStart)))

			if !ok {
				return nil, errDBClosed
			}
			// Only check if the connection is expired if the strategy is cachedOrNewConns.
			// If we require a new connection, just re-use the connection without looking
			// at the expiry time. If it is expired, it will be checked when it is placed
			// back into the connection pool.
			// This prioritizes giving a valid connection to a client over the exact connection
			// lifetime, which could expire exactly after this point anyway.
			if strategy == cachedOrNewConn && ret.err == nil && ret.conn.expired(lifetime) {
				pool.mu.Lock()
				pool.maxLifetimeClosed++
				pool.mu.Unlock()
				ret.conn.Close()
				return nil, errBadConn
			}

			if ret.conn == nil {
				return nil, ret.err
			}

			if ret.conn.ci.Closed() {
				ret.conn.Close()
				return nil, errBadConn
			}
			return ret.conn, ret.err
		}
	}

	pool.numOpen++ // optimistically
	pool.mu.Unlock()

	ci, err := pool.dialer(ctx)
	if err != nil {
		pool.mu.Lock()
		pool.numOpen-- // correct for earlier optimism
		pool.maybeOpenNewConnections()
		pool.mu.Unlock()
		return nil, err
	}
	dc := &conn{
		pool:       pool,
		createdAt:  nowFunc(),
		returnedAt: nowFunc(),
		ci:         ci,
		inUse:      true,
	}
	return dc, nil
}

func (pool *ConnectionPool) maybeOpenNewConnections() {
	numRequests := len(pool.connRequests)
	if pool.maxOpen > 0 {
		numCanOpen := pool.maxOpen - pool.numOpen
		if numRequests > numCanOpen {
			numRequests = numCanOpen
		}
	}
	for numRequests > 0 {
		pool.numOpen++ // optimistically
		numRequests--
		if pool.closed {
			return
		}
		pool.openerCh <- struct{}{}
	}
}

// Runs in a separate goroutine, opens new connections when requested.
func (pool *ConnectionPool) connectionOpener(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-pool.openerCh:
			pool.openNewConnection(ctx)
		}
	}
}

func (pool *ConnectionPool) openNewConnection(ctx context.Context) {
	// maybeOpenNewConnections has already executed pool.numOpen++ before it sent
	// on pool.openerCh. This function must execute pool.numOpen-- if the
	// connection fails or is closed before returning.
	ci, err := pool.dialer(ctx)
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.closed {
		if err == nil {
			ci.Close()
		}
		pool.numOpen--
		return
	}
	if err != nil {
		pool.numOpen--
		pool.putConnDBLocked(nil, err)
		pool.maybeOpenNewConnections()
		return
	}
	dc := &conn{
		pool:       pool,
		createdAt:  nowFunc(),
		returnedAt: nowFunc(),
		ci:         ci,
	}
	if pool.putConnDBLocked(dc, err) {
		// noop
	} else {
		pool.numOpen--
		ci.Close()
	}
}

func (pool *ConnectionPool) putConnDBLocked(dc *conn, err error) bool {
	if pool.closed {
		return false
	}
	if pool.maxOpen > 0 && pool.numOpen > pool.maxOpen {
		return false
	}
	if c := len(pool.connRequests); c > 0 {
		var req chan connRequest
		var reqKey uint64
		for reqKey, req = range pool.connRequests {
			break
		}
		delete(pool.connRequests, reqKey) // Remove from pending requests.
		if err == nil {
			dc.inUse = true
		}
		req <- connRequest{
			conn: dc,
			err:  err,
		}
		return true
	} else if err == nil && !pool.closed {
		if pool.maxIdleConnsLocked() > len(pool.freeConn) {
			pool.freeConn = append(pool.freeConn, dc)
			pool.startCleanerLocked()
			return true
		}
		pool.maxIdleClosed++
	}
	return false
}

// putConnHook is a hook for testing.
var putConnHook func(*ConnectionPool, *conn)

// putConn adds a connection to the db's free pool.
// err is optionally the last error that occurred on this connection.
func (pool *ConnectionPool) putConn(dc *conn, err error) {
	if err != errBadConn {
		if dc.ci.Closed() {
			err = errBadConn
		}
	}

	pool.mu.Lock()
	if !dc.inUse {
		pool.mu.Unlock()
		panic("sql: connection returned that was never out")
	}

	if err != errBadConn && dc.expired(pool.maxLifetime) {
		pool.maxLifetimeClosed++
		err = errBadConn
	}
	dc.inUse = false
	dc.returnedAt = nowFunc()

	if err == errBadConn {
		// Don't reuse bad connections.
		// Since the conn is considered bad and is being discarded, treat it
		// as closed. Don't decrement the open count here, finalClose will
		// take care of that.
		pool.maybeOpenNewConnections()
		pool.mu.Unlock()
		dc.Close()
		return
	}
	if putConnHook != nil {
		putConnHook(pool, dc)
	}
	added := pool.putConnDBLocked(dc, nil)
	pool.mu.Unlock()

	if !added {
		dc.Close()
		return
	}
}

const defaultMaxIdleConns = 8

func (pool *ConnectionPool) maxIdleConnsLocked() int {
	n := pool.maxIdleCount
	switch {
	case n == 0:
		// TODO(bradfitz): ask driver, if supported, for its default preference
		return defaultMaxIdleConns
	case n < 0:
		return 0
	default:
		return n
	}
}

func (pool *ConnectionPool) shortestIdleTimeLocked() time.Duration {
	if pool.maxIdleTime <= 0 {
		return pool.maxLifetime
	}
	if pool.maxLifetime <= 0 {
		return pool.maxIdleTime
	}

	min := pool.maxIdleTime
	if min > pool.maxLifetime {
		min = pool.maxLifetime
	}
	return min
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool.
//
// If MaxOpenConns is greater than 0 but less than the new MaxIdleConns,
// then the new MaxIdleConns will be reduced to match the MaxOpenConns limit.
//
// If n <= 0, no idle connections are retained.
//
// The default max idle connections is currently 2. This may change in
// a future release.
func (pool *ConnectionPool) SetMaxIdleConns(n int) {
	pool.mu.Lock()
	if n > 0 {
		pool.maxIdleCount = n
	} else {
		// No idle connections.
		pool.maxIdleCount = -1
	}
	// Make sure maxIdle doesn't exceed maxOpen
	if pool.maxOpen > 0 && pool.maxIdleConnsLocked() > pool.maxOpen {
		pool.maxIdleCount = pool.maxOpen
	}
	var closing []*conn
	idleCount := len(pool.freeConn)
	maxIdle := pool.maxIdleConnsLocked()
	if idleCount > maxIdle {
		closing = pool.freeConn[maxIdle:]
		pool.freeConn = pool.freeConn[:maxIdle]
	}
	pool.maxIdleClosed += int64(len(closing))
	pool.mu.Unlock()
	for _, c := range closing {
		c.Close()
	}
}

// SetMaxOpenConns sets the maximum number of open connections to the database.
//
// If MaxIdleConns is greater than 0 and the new MaxOpenConns is less than
// MaxIdleConns, then MaxIdleConns will be reduced to match the new
// MaxOpenConns limit.
//
// If n <= 0, then there is no limit on the number of open connections.
// The default is 0 (unlimited).
func (pool *ConnectionPool) SetMaxOpenConns(n int) {
	pool.mu.Lock()
	pool.maxOpen = n
	if n < 0 {
		pool.maxOpen = 0
	}
	syncMaxIdle := pool.maxOpen > 0 && pool.maxIdleConnsLocked() > pool.maxOpen
	pool.mu.Unlock()
	if syncMaxIdle {
		pool.SetMaxIdleConns(n)
	}
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
//
// Expired connections may be closed lazily before reuse.
//
// If d <= 0, connections are not closed due to a connection's age.
func (pool *ConnectionPool) SetConnMaxLifetime(d time.Duration) {
	if d < 0 {
		d = 0
	}
	pool.mu.Lock()
	// Wake cleaner up when lifetime is shortened.
	if d > 0 && d < pool.maxLifetime && pool.cleanerCh != nil {
		select {
		case pool.cleanerCh <- struct{}{}:
		default:
		}
	}
	pool.maxLifetime = d
	pool.startCleanerLocked()
	pool.mu.Unlock()
}

// SetConnMaxIdleTime sets the maximum amount of time a connection may be idle.
//
// Expired connections may be closed lazily before reuse.
//
// If d <= 0, connections are not closed due to a connection's idle time.
func (pool *ConnectionPool) SetConnMaxIdleTime(d time.Duration) {
	if d < 0 {
		d = 0
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()

	// Wake cleaner up when idle time is shortened.
	if d > 0 && d < pool.maxIdleTime && pool.cleanerCh != nil {
		select {
		case pool.cleanerCh <- struct{}{}:
		default:
		}
	}
	pool.maxIdleTime = d
	pool.startCleanerLocked()
}

// startCleanerLocked starts connectionCleaner if needed.
func (pool *ConnectionPool) startCleanerLocked() {
	if (pool.maxLifetime > 0 || pool.maxIdleTime > 0) && pool.numOpen > 0 && pool.cleanerCh == nil {
		pool.cleanerCh = make(chan struct{}, 1)
		go pool.connectionCleaner(pool.shortestIdleTimeLocked())
	}
}

func (pool *ConnectionPool) connectionCleaner(d time.Duration) {
	const minInterval = time.Second

	if d < minInterval {
		d = minInterval
	}
	t := time.NewTimer(d)

	for {
		select {
		case <-t.C:
		case <-pool.cleanerCh: // maxLifetime was changed or pool was closed.
		}

		pool.mu.Lock()

		d = pool.shortestIdleTimeLocked()
		if pool.closed || pool.numOpen == 0 || d <= 0 {
			pool.cleanerCh = nil
			pool.mu.Unlock()
			return
		}

		closing := pool.connectionCleanerRunLocked()
		pool.mu.Unlock()
		for _, c := range closing {
			c.Close()
		}

		if d < minInterval {
			d = minInterval
		}
		t.Reset(d)
	}
}

var nowFunc = time.Now

func (pool *ConnectionPool) connectionCleanerRunLocked() (closing []*conn) {
	if pool.maxLifetime > 0 {
		expiredSince := nowFunc().Add(-pool.maxLifetime)
		for i := 0; i < len(pool.freeConn); i++ {
			c := pool.freeConn[i]
			if c.createdAt.Before(expiredSince) {
				closing = append(closing, c)
				last := len(pool.freeConn) - 1
				pool.freeConn[i] = pool.freeConn[last]
				pool.freeConn[last] = nil
				pool.freeConn = pool.freeConn[:last]
				i--
			}
		}
		pool.maxLifetimeClosed += int64(len(closing))
	}

	if pool.maxIdleTime > 0 {
		expiredSince := nowFunc().Add(-pool.maxIdleTime)
		var expiredCount int64
		for i := 0; i < len(pool.freeConn); i++ {
			c := pool.freeConn[i]
			if pool.maxIdleTime > 0 && c.returnedAt.Before(expiredSince) {
				closing = append(closing, c)
				expiredCount++
				last := len(pool.freeConn) - 1
				pool.freeConn[i] = pool.freeConn[last]
				pool.freeConn[last] = nil
				pool.freeConn = pool.freeConn[:last]
				i--
			}
		}
		pool.maxIdleTimeClosed += expiredCount
	}
	return
}

// Close marks the ConnectionPool as closed and will not allow future calls to Invoke or NewStream
// to proceed. It does not stop any ongoing calls to Invoke or NewStream.
func (pool *ConnectionPool) Close() (err error) {
	pool.mu.Lock()
	if pool.closed { // Make DB.Close idempotent
		pool.mu.Unlock()
		return nil
	}
	if pool.cleanerCh != nil {
		close(pool.cleanerCh)
	}
	for _, dc := range pool.freeConn {
		dc.Close()
	}
	pool.freeConn = nil
	pool.closed = true
	for _, req := range pool.connRequests {
		close(req)
	}
	pool.mu.Unlock()
	pool.stop()
	return err
}

func OpenConnectionPool(dial Dialer) *ConnectionPool {
	ctx, cancel := context.WithCancel(context.Background())
	db := &ConnectionPool{
		dialer:       dial,
		openerCh:     make(chan struct{}, 64),
		connRequests: make(map[uint64]chan connRequest),
		stop:         cancel,
	}

	go db.connectionOpener(ctx)

	return db
}

// Closed returns true if the ConnectionPool is closed.
func (pool *ConnectionPool) Closed() bool {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	return pool.closed
}

func (pool *ConnectionPool) getConn(ctx context.Context) (*conn, error) {
	const maxBadConnRetries = 2

	var conn *conn
	var err error

	for i := 0; i < maxBadConnRetries; i++ {
		conn, err = pool.conn(ctx, cachedOrNewConn)
		if err != errBadConn {
			break
		}
	}
	if err == errBadConn {
		return pool.conn(ctx, alwaysNewConn)
	}
	return conn, err
}

// Invoke acquires a connection from the pool, dialing if necessary, and issues the Invoke on that
// connection. The connection is replaced into the pool after the invoke finishes.
func (pool *ConnectionPool) Invoke(ctx context.Context, rpc string, enc drpc.Encoding, in, out drpc.Message) error {
	conn, err := pool.getConn(ctx)
	if err != nil {
		return err
	}

	err = conn.ci.Invoke(ctx, rpc, enc, in, out)
	pool.putConn(conn, err)
	return err
}

// NewStream acquires a connection from the pool, dialing if necessary, and issues the NewStream on
// that connection. The connection is replaced into the pool after the stream is finished.
func (pool *ConnectionPool) NewStream(ctx context.Context, rpc string, enc drpc.Encoding) (_ drpc.Stream, err error) {
	conn, err := pool.getConn(ctx)
	if err != nil {
		return nil, err
	}

	stream, err := conn.ci.NewStream(ctx, rpc, enc)
	if err != nil {
		pool.putConn(conn, err)
		return nil, err
	}

	// the stream's done channel is closed when we're sure no reads/writes are
	// coming in for that stream anymore. it has been fully terminated.
	go func() {
		<-stream.Context().Done()
		pool.putConn(conn, nil)
	}()

	return stream, nil
}

// Transport returns nil because it does not have a fixed transport.
func (pool *ConnectionPool) Transport() drpc.Transport {
	return nil
}
