/*
Copyright 2023 The Vitess Authors.

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

package smartconnpool

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/vt/log"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	// ErrTimeout is returned if a connection get times out.
	ErrTimeout = vterrors.New(vtrpcpb.Code_RESOURCE_EXHAUSTED, "connection pool timed out")

	// ErrCtxTimeout is returned if a ctx is already expired by the time the connection pool is used
	ErrCtxTimeout = vterrors.New(vtrpcpb.Code_DEADLINE_EXCEEDED, "connection pool context already expired")

	// ErrConnPoolClosed is returned when trying to get a connection from a closed conn pool
	ErrConnPoolClosed = vterrors.New(vtrpcpb.Code_INTERNAL, "connection pool is closed")

	// ErrPoolWaiterCapReached is returned when the waiter cap has been reached
	ErrPoolWaiterCapReached = vterrors.New(vtrpcpb.Code_RESOURCE_EXHAUSTED, "connection pool waiter cap reached")

	// PoolCloseTimeout is how long to wait for all connections to be returned to the pool during close
	PoolCloseTimeout = 10 * time.Second
)

type Metrics struct {
	maxLifetimeClosed    atomic.Int64
	getCount             atomic.Int64
	getWithSettingsCount atomic.Int64
	waitCount            atomic.Int64
	waitTime             atomic.Int64
	idleClosed           atomic.Int64
	diffSetting          atomic.Int64
	resetSetting         atomic.Int64
	waiterCapRejected    atomic.Int64
}

func (m *Metrics) MaxLifetimeClosed() int64 {
	return m.maxLifetimeClosed.Load()
}

func (m *Metrics) GetCount() int64 {
	return m.getCount.Load()
}

func (m *Metrics) GetSettingCount() int64 {
	return m.getWithSettingsCount.Load()
}

func (m *Metrics) WaitCount() int64 {
	return m.waitCount.Load()
}

func (m *Metrics) WaitTime() time.Duration {
	return time.Duration(m.waitTime.Load())
}

func (m *Metrics) IdleClosed() int64 {
	return m.idleClosed.Load()
}

func (m *Metrics) DiffSettingCount() int64 {
	return m.diffSetting.Load()
}

func (m *Metrics) ResetSettingCount() int64 {
	return m.resetSetting.Load()
}

func (m *Metrics) WaiterCapRejected() int64 {
	return m.waiterCapRejected.Load()
}

type (
	Connector[C Connection] func(ctx context.Context) (C, error)
	RefreshCheck            func() (bool, error)
)

type Config[C Connection] struct {
	Capacity        int64
	MaxIdleCount    int64
	IdleTimeout     time.Duration
	MaxLifetime     time.Duration
	RefreshInterval time.Duration
	MaxWaiters      uint
	LogWait         func(time.Time)
}

// stackMask is the number of connection setting stacks minus one;
// the number of stacks must always be a power of two
const stackMask = 7

type closeCtxState struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// alreadyCanceledCtx is returned by closeContext when the pool is not open;
// it ensures connect callbacks fail fast without checking pool state again.
var alreadyCanceledCtx = func() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}()

// ConnPool is a connection pool for generic connections
type ConnPool[C Connection] struct {
	// clean is a connections stack for connections with no Setting applied
	clean connStack[C]
	// settings are N connection stacks for connections with a Setting applied
	// connections are distributed between stacks based on their Setting.bucket
	settings [stackMask + 1]connStack[C]
	// freshSettingStack is the index in settings to the last stack when a connection
	// was pushed, or -1 if no connection with a Setting has been opened in this pool
	freshSettingsStack atomic.Int64
	// wait is the list of clients waiting for a connection to be returned to the pool
	wait waitlist[C]

	// borrowed is the number of connections that the pool has given out to clients
	// and that haven't been returned yet
	borrowed atomic.Int64
	// active is the number of connections that the pool has opened; this includes connections
	// in the pool and borrowed by clients
	active atomic.Int64
	// capacity is the maximum number of connections that this pool can open
	capacity atomic.Int64
	// maxIdleCount is the maximum idle connections in the pool
	idleCount atomic.Int64

	// workers is a waitgroup for all the currently running worker goroutines
	workers    sync.WaitGroup
	close      atomic.Pointer[chan struct{}]
	capacityMu sync.Mutex
	// generation is bumped on every reopen so that borrowed connections from
	// before the reopen can be identified and retired when they return.
	generation atomic.Int64
	// closeCtx holds a context that's cancelled when the pool starts closing,
	// so background workers and put-driven reconnects abort instead of pinning
	// workers.Wait(). Installed in open(), cancelled and cleared in
	// CloseWithContext. nil means the pool is not open.
	closeCtx atomic.Pointer[closeCtxState]

	config struct {
		// connect is the callback to create a new connection for the pool
		connect Connector[C]
		// refresh is the callback to check whether the pool needs to be refreshed
		refresh RefreshCheck
		// maxCapacity is the maximum value to which capacity can be set; when the pool
		// is re-opened, it defaults to this capacity
		maxCapacity int64
		// maxIdleCount is the maximum idle connections in the pool
		maxIdleCount int64
		// maxLifetime is the maximum time a connection can be open
		maxLifetime atomic.Int64
		// idleTimeout is the maximum time a connection can remain idle
		idleTimeout atomic.Int64
		// refreshInterval is how often to call the refresh check
		refreshInterval atomic.Int64
		// logWait is called every time a client must block waiting for a connection
		logWait func(time.Time)
		// maxWaiters is the maximum number of clients that can be waiting for a connection;
		// 0 means unlimited
		maxWaiters uint
	}

	Metrics Metrics
	Name    string
}

// NewPool creates a new connection pool with the given Config.
// The pool must be ConnPool.Open before it can start giving out connections
func NewPool[C Connection](config *Config[C]) *ConnPool[C] {
	pool := &ConnPool[C]{}
	pool.config.maxCapacity = config.Capacity
	pool.config.maxIdleCount = config.MaxIdleCount
	pool.config.maxLifetime.Store(config.MaxLifetime.Nanoseconds())
	pool.config.idleTimeout.Store(config.IdleTimeout.Nanoseconds())
	pool.config.refreshInterval.Store(config.RefreshInterval.Nanoseconds())
	pool.config.logWait = config.LogWait
	pool.config.maxWaiters = config.MaxWaiters
	pool.wait.init()
	pool.wait.onWait = func() {
		pool.Metrics.waitCount.Add(1)
	}
	pool.wait.onWaiterCapReached = func() {
		pool.Metrics.waiterCapRejected.Add(1)
	}

	return pool
}

func (pool *ConnPool[C]) runWorker(close <-chan struct{}, interval time.Duration, worker func(now time.Time) bool) {
	pool.workers.Add(1)

	go func() {
		tick := time.NewTicker(interval)

		defer tick.Stop()
		defer pool.workers.Done()

		for {
			select {
			case now := <-tick.C:
				if !worker(now) {
					return
				}
			case <-close:
				return
			}
		}
	}()
}

func (pool *ConnPool[C]) open() {
	closeChan := make(chan struct{})
	if !pool.close.CompareAndSwap(nil, &closeChan) {
		// already open
		return
	}

	// Bump generation so that any conn that survived a previous Close (e.g.,
	// a borrowed conn while CloseWithContext timed out) is retired when it
	// returns to this re-opened pool.
	pool.generation.Add(1)

	// Install the close context before starting any workers so they can read
	// it race-free. CloseWithContext cancels it before signalling close-chan,
	// so in-flight worker reconnects abort promptly.
	ctx, cancel := context.WithCancel(context.Background())
	pool.closeCtx.Store(&closeCtxState{ctx: ctx, cancel: cancel})

	pool.capacity.Store(pool.config.maxCapacity)
	pool.setIdleCount()

	idleTimeout := pool.IdleTimeout()
	if idleTimeout != 0 {
		// The idle worker takes care of closing connections that have been idle too long
		pool.runWorker(closeChan, idleTimeout/10, func(now time.Time) bool {
			pool.closeIdleResources(now)
			return true
		})
	}

	refreshInterval := pool.RefreshInterval()
	if refreshInterval != 0 && pool.config.refresh != nil {
		// The refresh worker periodically checks the refresh callback in this pool
		// to decide whether all the connections in the pool need to be cycled
		// (this usually only happens when there's a global DNS change).
		pool.runWorker(closeChan, refreshInterval, func(_ time.Time) bool {
			refresh, err := pool.config.refresh()
			if err != nil {
				log.Error(fmt.Sprint(err))
			}
			if refresh {
				go pool.reopen()
			}
			return true
		})
	}
}

// Open starts the background workers that manage the pool and gets it ready
// to start serving out connections.
//
// Open is NOT safe to call concurrently with Close, CloseWithContext, or
// another Open. The lifecycle pointers (pool.close, pool.closeCtx) and the
// workers sync.WaitGroup are updated without a mutex; racing them violates
// sync.WaitGroup's contract (Add must not be called once a Wait may be
// running concurrently). Callers are responsible for serializing lifecycle
// transitions — in Vitess, the state-manager and tx-engine state locks do
// this for the smartconnpool wrappers.
func (pool *ConnPool[C]) Open(connect Connector[C], refresh RefreshCheck) *ConnPool[C] {
	if pool.close.Load() != nil {
		// already open
		return pool
	}

	pool.config.connect = connect
	pool.config.refresh = refresh
	pool.open()
	return pool
}

// Close shuts down the pool. Once Close returns, every subsequent
// ConnPool.Get call returns ErrConnPoolClosed; Get calls that race with Close
// may briefly succeed in the window before Close finishes draining, and the
// returned connection is then closed on Pooled.Recycle. ConnPool.Put remains
// valid throughout. This function will not return until all of the pool's
// connections have been returned or the default PoolCloseTimeout has elapsed.
//
// Close is NOT safe to call concurrently with Open or another Close — see
// CloseWithContext for details.
func (pool *ConnPool[C]) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), PoolCloseTimeout)
	defer cancel()

	if err := pool.CloseWithContext(ctx); err != nil {
		log.Error(fmt.Sprintf("failed to close pool %q: %v", pool.Name, err))
	}
}

// CloseWithContext behaves like Close but allows passing in a Context to time out the
// pool closing operation.
//
// CloseWithContext is NOT safe to call concurrently with Open or with another
// CloseWithContext (it manipulates the pool's lifecycle pointers and the
// workers sync.WaitGroup without a mutex). Callers are responsible for
// serializing lifecycle transitions — in Vitess this is handled by the
// state-manager / tx-engine state locks.
func (pool *ConnPool[C]) CloseWithContext(ctx context.Context) error {
	// Cancel the pool's close context first, before taking capacityMu. Worker
	// reconnects (closeIdleResources, put) read this context and abort when
	// it fires, so workers.Wait() unblocks promptly inside this function.
	closeState := pool.closeCtx.Swap(nil)
	if closeState == nil {
		// already closed
		return nil
	}
	closeState.cancel()

	closeChan := pool.close.Swap(nil)
	if closeChan != nil {
		close(*closeChan)
	}

	pool.capacityMu.Lock()
	defer pool.capacityMu.Unlock()

	// close all the connections in the pool; if we time out while waiting for
	// users to return our connections, we still want to finish the shutdown
	// for the pool
	err := pool.setCapacity(ctx, 0)

	pool.workers.Wait()
	return err
}

// reopen retires the pool's current connections without changing capacity.
// It's invoked by the refresh worker when the refresh callback reports that
// the pool's connections are no longer trusted (e.g., a DNS change for the
// underlying MySQL endpoint).
//
// The contract is cooperative — there is no atomic way to replace every
// in-flight connection. Each conn category is handled differently:
//
//   - Idle conns in the stacks at reopen-time: bumping the generation makes
//     them stale; the sweep below drains each stack and closes any stale
//     entry. A stale conn pushed onto a stack after the sweep — e.g. by an
//     idle worker that captured the pre-reopen generation — is caught by
//     pop()'s generation guard the next time Get touches it.
//   - Conns borrowed at reopen-time: retired when they return via the
//     generation check in tryReturnConn.
//   - Conns whose connect was in flight at reopen-time: their generation is
//     captured before the connect call, so they land with the pre-reopen
//     generation and are retired by tryReturnConn on Recycle. Capacity is
//     never dipped, so parked waiters are not woken artificially.
func (pool *ConnPool[C]) reopen() {
	pool.capacityMu.Lock()
	defer pool.capacityMu.Unlock()

	if pool.close.Load() == nil {
		return
	}

	// Bump the generation first. Any conn already in the stacks is now
	// stale; borrowed conns retain the previous generation and are retired
	// by tryReturnConn when they return.
	pool.generation.Add(1)

	pool.sweepStaleConns(&pool.clean)
	for i := range pool.settings {
		pool.sweepStaleConns(&pool.settings[i])
	}
}

// sweepStaleConns drains a connection stack, closing every conn whose
// generation predates the pool's current generation. Fresh-generation conns
// — for example, those pushed by closeIdleResources's connReopen after the
// generation bump — are pushed back into the pool instead of being dropped;
// they're valid post-reopen conns and discarding them would leak an active
// slot.
func (pool *ConnPool[C]) sweepStaleConns(stack *connStack[C]) {
	var fresh []*Pooled[C]
	for {
		conn, ok := stack.Pop()
		if !ok {
			break
		}
		if !conn.timeUsed.borrow() {
			// The idle worker already marked this conn as expired and is
			// closing it; let that finish without our interference.
			continue
		}
		if conn.generation != pool.generation.Load() {
			conn.Close()
			pool.closedConn()
			continue
		}
		fresh = append(fresh, conn)
	}
	for _, conn := range fresh {
		// Clear the borrow we took above so the conn is usable again.
		conn.timeUsed.update()
		pool.tryReturnConn(conn)
	}
}

// IsOpen returns whether the pool is open
func (pool *ConnPool[C]) IsOpen() bool {
	return pool.close.Load() != nil
}

// Capacity returns the maximum amount of connections that this pool can maintain open
func (pool *ConnPool[C]) Capacity() int64 {
	return pool.capacity.Load()
}

// MaxCapacity returns the maximum value to which Capacity can be set via ConnPool.SetCapacity
func (pool *ConnPool[C]) MaxCapacity() int64 {
	return pool.config.maxCapacity
}

func (pool *ConnPool[C]) setIdleCount() {
	capacity := pool.Capacity()
	maxIdleCount := pool.config.maxIdleCount
	if maxIdleCount == 0 || maxIdleCount > capacity {
		pool.idleCount.Store(capacity)
	} else {
		pool.idleCount.Store(maxIdleCount)
	}
}

// InUse returns the number of connections that the pool has lent out to clients and that
// haven't been returned yet.
func (pool *ConnPool[C]) InUse() int64 {
	return pool.borrowed.Load()
}

// Available returns the number of connections that the pool can immediately lend out to
// clients without blocking.
func (pool *ConnPool[C]) Available() int64 {
	return pool.capacity.Load() - pool.borrowed.Load()
}

// Active returns the numer of connections that the pool has currently open.
func (pool *ConnPool[C]) Active() int64 {
	return pool.active.Load()
}

func (pool *ConnPool[D]) IdleTimeout() time.Duration {
	return time.Duration(pool.config.idleTimeout.Load())
}

func (pool *ConnPool[C]) SetIdleTimeout(duration time.Duration) {
	pool.config.idleTimeout.Store(duration.Nanoseconds())
}

func (pool *ConnPool[D]) IdleCount() int64 {
	return pool.idleCount.Load()
}

func (pool *ConnPool[D]) RefreshInterval() time.Duration {
	return time.Duration(pool.config.refreshInterval.Load())
}

func (pool *ConnPool[C]) recordWaitDuration(start time.Time) {
	pool.Metrics.waitTime.Add(time.Since(start).Nanoseconds())
	if pool.config.logWait != nil {
		pool.config.logWait(start)
	}
}

// Get returns a connection from the pool with the given Setting applied.
// If there are no connections in the pool to be returned, Get blocks until one
// is returned, or until the given ctx is cancelled.
// The connection must be returned to the pool once it's not needed by calling Pooled.Recycle
//
// ctx is honored on a best-effort basis: Get may return a usable connection
// even if ctx has been cancelled in a narrow window around the return (the
// pool can't atomically observe cancellation that happens after the wait
// completes but before it returns). Callers that need strict cancellation
// must re-check ctx.Err() after Get returns and recycle the conn if so.
func (pool *ConnPool[C]) Get(ctx context.Context, setting *Setting) (*Pooled[C], error) {
	if ctx.Err() != nil {
		return nil, ErrCtxTimeout
	}
	// Closed pools return ErrConnPoolClosed; the close-pointer is cleared
	// before CloseWithContext takes capacityMu, so checking it covers the
	// window where capacity is still > 0 but the pool is on its way out.
	if pool.close.Load() == nil {
		return nil, ErrConnPoolClosed
	}
	// A pool with capacity 0 is paused (e.g., via SetCapacity(0)) but still
	// open. Surface that as ErrTimeout — the same signal callers get when
	// nobody returns a conn in time — so a paused pool can resume without
	// callers treating it as terminally closed.
	if pool.capacity.Load() == 0 {
		return nil, ErrTimeout
	}
	if setting == nil {
		return pool.get(ctx)
	}
	return pool.getWithSetting(ctx, setting)
}

// put returns a connection to the pool. This is a private API.
// Return connections to the pool by calling Pooled.Recycle
func (pool *ConnPool[C]) put(conn *Pooled[C]) {
	pool.borrowed.Add(-1)

	// On a closed pool there's no future use for the conn — Get refuses
	// new requests and no worker will rotate it. Close it outright so we
	// don't leak an open DB connection in the idle stack. (SetCapacity on
	// a closed pool can arm idleCount > 0, which would otherwise make
	// closeOnIdleLimitReached push the returned conn instead of closing
	// it.)
	if pool.close.Load() == nil {
		if conn != nil {
			conn.Close()
		}
		pool.closedConn()
		return
	}

	if conn == nil {
		// Short-circuit if the pool is drained: there's nothing for the
		// new conn to do, and the connector may not honor our cancelled
		// closeContext anyway. Skip the connect outright.
		if pool.capacity.Load() == 0 {
			pool.closedConn()
			return
		}
		var err error
		conn, err = pool.connNew(pool.closeContext())
		if err != nil {
			pool.closedConn()
			return
		}
	} else {
		// A conn whose generation predates the pool's was established before
		// the most recent reopen and must not return to the pool — the reason
		// for reopen is that those conns are no longer trusted.
		if conn.generation != pool.generation.Load() {
			conn.Close()
			pool.closedConn()
			return
		}

		conn.timeUsed.update()

		lifetime := pool.extendedMaxLifetime()
		if lifetime > 0 && conn.timeCreated.elapsed() > lifetime {
			pool.Metrics.maxLifetimeClosed.Add(1)
			conn.Close()
			if err := pool.connReopen(pool.closeContext(), conn, conn.timeUsed.get()); err != nil {
				pool.closedConn()
				return
			}
		}
	}

	pool.tryReturnConn(conn)
}

func (pool *ConnPool[C]) tryReturnConn(conn *Pooled[C]) bool {
	// Ownership gate: a conn whose generation predates the pool's belongs to
	// a previous reopen window. Reject before any handoff (waiter, idle-limit,
	// stack push) so a stale conn never reaches another caller. This catches
	// reopens that race with put()'s initial check, with tryReturnAnyConn's
	// pop, or with closeIdleResources' push-back. The caller must NOT also
	// close — tryReturnConn owns the close + closedConn here.
	if conn.generation != pool.generation.Load() {
		conn.Close()
		pool.closedConn()
		return false
	}

	if pool.wait.tryReturnConn(conn) {
		return true
	}

	if pool.closeOnIdleLimitReached(conn) {
		return false
	}
	connSetting := conn.Conn.Setting()
	if connSetting == nil {
		pool.clean.Push(conn)
	} else {
		stack := connSetting.bucket & stackMask
		pool.settings[stack].Push(conn)
		pool.freshSettingsStack.Store(int64(stack))
	}
	if pool.wait.waiting() != 0 {
		return pool.tryReturnAnyConn()
	}
	return false
}

func (pool *ConnPool[C]) pop(stack *connStack[C]) *Pooled[C] {
	// retry-loop: pop a connection from the stack and atomically check whether
	// its timeout has elapsed. If the timeout has elapsed, the borrow will fail,
	// which means that a background worker has already marked this connection
	// as stale and is in the process of shutting it down. If we successfully mark
	// the timeout as borrowed, we know that background workers will not be able
	// to expire this connection (even if it's still visible to them), so it's
	// safe to return it.
	//
	// A conn whose generation predates the pool's was established before the
	// most recent reopen and must not be handed out. Close it here and keep
	// popping. This guard lets reopen bump the generation first and sweep the
	// stacks after — without it, a Get racing between the bump and the sweep
	// could surface a stale-at-acquisition conn.
	for conn, ok := stack.Pop(); ok; conn, ok = stack.Pop() {
		if !conn.timeUsed.borrow() {
			// Ignore the connection that couldn't be borrowed;
			// it's being closed by the idle worker and replaced by a new connection.
			continue
		}
		if conn.generation != pool.generation.Load() {
			conn.Close()
			pool.closedConn()
			continue
		}

		return conn
	}
	return nil
}

func (pool *ConnPool[C]) tryReturnAnyConn() bool {
	if conn := pool.pop(&pool.clean); conn != nil {
		conn.timeUsed.update()
		return pool.tryReturnConn(conn)
	}
	for u := 0; u <= stackMask; u++ {
		if conn := pool.pop(&pool.settings[u]); conn != nil {
			conn.timeUsed.update()
			return pool.tryReturnConn(conn)
		}
	}
	return false
}

// closeOnIdleLimitReached closes a connection if the number of idle connections (active - inuse) in the pool
// exceeds the idleCount limit. It returns true if the connection is closed, false otherwise.
//
// The Metrics.idleClosed counter is only incremented when the pool is open
// (capacity > 0). A close while capacity == 0 is part of a shutdown or
// reopen drain rather than an idle-policy eviction, and shouldn't show up
// in the idle-eviction metric.
func (pool *ConnPool[C]) closeOnIdleLimitReached(conn *Pooled[C]) bool {
	for {
		open := pool.active.Load()
		idle := open - pool.borrowed.Load()
		if idle <= pool.idleCount.Load() {
			return false
		}
		if pool.active.CompareAndSwap(open, open-1) {
			if pool.capacity.Load() > 0 {
				pool.Metrics.idleClosed.Add(1)
			}
			conn.Close()
			return true
		}
	}
}

// closeContext returns the pool-wide context that is cancelled when the pool
// starts closing. Returns an already-cancelled context if the pool is not
// open, so callers that race with close fail fast.
//
// Used by background workers and by put() so that in-flight reconnects abort
// when CloseWithContext fires, instead of pinning workers.Wait(). The context
// is owned by the pool — do not cancel it.
func (pool *ConnPool[C]) closeContext() context.Context {
	if state := pool.closeCtx.Load(); state != nil {
		return state.ctx
	}
	return alreadyCanceledCtx
}

func (pool *ConnPool[D]) extendedMaxLifetime() time.Duration {
	maxLifetime := pool.config.maxLifetime.Load()
	if maxLifetime <= 0 {
		return 0
	}
	return time.Duration(maxLifetime) + time.Duration(rand.Int64N(maxLifetime))
}

func (pool *ConnPool[C]) connReopen(ctx context.Context, dbconn *Pooled[C], now time.Duration) (err error) {
	// Capture generation before the connect so a reopen that races with this
	// call doesn't mark our stale-server conn as fresh. tryReturnConn will
	// reject the conn on the way back if generation has moved on.
	generation := pool.generation.Load()

	dbconn.Conn, err = pool.config.connect(ctx)
	if err != nil {
		return err
	}

	if setting := dbconn.Conn.Setting(); setting != nil {
		err = dbconn.Conn.ApplySetting(ctx, setting)
		if err != nil {
			dbconn.Close()
			return err
		}
	}

	dbconn.timeCreated.set(now)
	dbconn.timeUsed.set(now)
	dbconn.generation = generation
	return nil
}

func (pool *ConnPool[C]) connNew(ctx context.Context) (*Pooled[C], error) {
	// Capture generation before the connect; see connReopen.
	generation := pool.generation.Load()

	conn, err := pool.config.connect(ctx)
	if err != nil {
		return nil, err
	}
	pooled := &Pooled[C]{
		pool:       pool,
		Conn:       conn,
		generation: generation,
	}
	now := monotonicNow()
	pooled.timeUsed.set(now)
	pooled.timeCreated.set(now)
	return pooled, nil
}

func (pool *ConnPool[C]) getFromSettingsStack(setting *Setting) *Pooled[C] {
	var start uint32
	if setting == nil {
		start = uint32(pool.freshSettingsStack.Load())
	} else {
		start = setting.bucket
	}

	for i := uint32(0); i <= stackMask; i++ {
		pos := (i + start) & stackMask
		if conn := pool.pop(&pool.settings[pos]); conn != nil {
			return conn
		}
	}
	return nil
}

func (pool *ConnPool[C]) discardConn(conn *Pooled[C]) {
	conn.Close()
	pool.closedConn()
}

func (pool *ConnPool[C]) closedConn() {
	_ = pool.active.Add(-1)
	pool.notifyWaitersForAvailableCapacity(1)
}

func (pool *ConnPool[C]) notifyWaitersForAvailableCapacity(limit int64) {
	for range limit {
		if pool.active.Load() >= pool.capacity.Load() {
			return
		}
		if !pool.wait.tryNotifyWaiter() {
			return
		}
	}
}

func (pool *ConnPool[C]) getNew(ctx context.Context) (*Pooled[C], error) {
	for {
		open := pool.active.Load()
		if open >= pool.capacity.Load() {
			return nil, nil
		}

		if pool.active.CompareAndSwap(open, open+1) {
			conn, err := pool.connNew(ctx)
			if err != nil {
				pool.closedConn()
				return nil, err
			}
			return conn, nil
		}
	}
}

func (pool *ConnPool[C]) shouldRetryWait() bool {
	// If the pool was paused (capacity dropped to 0) after we entered the
	// waitlist but before setCapacity's wake-all loop reached our slot, the
	// waiter would otherwise stay parked. Bail out so the get loop can
	// observe capacity == 0 and return ErrTimeout.
	if pool.capacity.Load() == 0 {
		return true
	}
	if pool.active.Load() < pool.capacity.Load() {
		return true
	}
	if pool.clean.Peek() != nil {
		return true
	}
	for i := range pool.settings {
		if pool.settings[i].Peek() != nil {
			return true
		}
	}
	return false
}

// get returns a pooled connection with no Setting applied
func (pool *ConnPool[C]) get(ctx context.Context) (*Pooled[C], error) {
	pool.Metrics.getCount.Add(1)

	for {
		if pool.close.Load() == nil {
			return nil, ErrConnPoolClosed
		}
		if pool.capacity.Load() == 0 {
			return nil, ErrTimeout
		}

		// best case: if there's a connection in the clean stack, return it right away
		if conn := pool.pop(&pool.clean); conn != nil {
			pool.borrowed.Add(1)
			return conn, nil
		}

		// check if we have enough capacity to open a brand-new connection to return
		conn, err := pool.getNew(ctx)
		if err != nil {
			return nil, err
		}
		// if we don't have capacity, try popping a connection from any of the setting stacks
		if conn == nil {
			conn = pool.getFromSettingsStack(nil)
		}
		// if there are no connections in the setting stacks and we've lent out connections
		// to other clients, wait until one of the connections is returned
		if conn == nil {
			start := time.Now()

			closeChan := pool.close.Load()
			if closeChan == nil {
				return nil, ErrConnPoolClosed
			}

			conn, err = pool.wait.waitForConn(ctx, nil, *closeChan, pool.config.maxWaiters, pool.shouldRetryWait)
			// ErrPoolWaiterCapReached is the only error path where we never
			// entered the wait queue, so it's also the only one not to record.
			if !errors.Is(err, ErrPoolWaiterCapReached) {
				pool.recordWaitDuration(start)
			}
			if err != nil {
				if errors.Is(err, ErrPoolWaiterCapReached) || errors.Is(err, ErrConnPoolClosed) {
					return nil, err
				}
				return nil, ErrTimeout
			}
		}
		if conn == nil {
			continue
		}
		if pool.close.Load() == nil {
			pool.discardConn(conn)
			return nil, ErrConnPoolClosed
		}
		if pool.capacity.Load() == 0 {
			pool.discardConn(conn)
			return nil, ErrTimeout
		}

		// if the connection we've acquired has a Setting applied, we must reset it before returning
		if conn.Conn.Setting() != nil {
			pool.Metrics.resetSetting.Add(1)

			err = conn.Conn.ResetSetting(ctx)
			if err != nil {
				conn.Close()
				err = pool.connReopen(ctx, conn, monotonicNow())
				if err != nil {
					pool.closedConn()
					return nil, err
				}
			}
		}

		pool.borrowed.Add(1)
		return conn, nil
	}
}

// getWithSetting returns a connection from the pool with the given Setting applied
func (pool *ConnPool[C]) getWithSetting(ctx context.Context, setting *Setting) (*Pooled[C], error) {
	pool.Metrics.getWithSettingsCount.Add(1)

	for {
		if pool.close.Load() == nil {
			return nil, ErrConnPoolClosed
		}
		if pool.capacity.Load() == 0 {
			return nil, ErrTimeout
		}

		var err error
		// best case: check if there's a connection in the setting stack where our Setting belongs
		conn := pool.pop(&pool.settings[setting.bucket&stackMask])
		// if there's connection with our setting, try popping a clean connection
		if conn == nil {
			conn = pool.pop(&pool.clean)
		}
		// otherwise try opening a brand new connection and we'll apply the setting to it
		if conn == nil {
			conn, err = pool.getNew(ctx)
			if err != nil {
				return nil, err
			}
		}
		// try on the _other_ setting stacks, even if we have to reset the Setting for the returned
		// connection
		if conn == nil {
			conn = pool.getFromSettingsStack(setting)
		}
		// no connections anywhere in the pool; if we've lent out connections to other clients
		// wait for one of them
		if conn == nil {
			start := time.Now()

			closeChan := pool.close.Load()
			if closeChan == nil {
				return nil, ErrConnPoolClosed
			}

			conn, err = pool.wait.waitForConn(ctx, setting, *closeChan, pool.config.maxWaiters, pool.shouldRetryWait)
			// ErrPoolWaiterCapReached is the only error path where we never
			// entered the wait queue, so it's also the only one not to record.
			if !errors.Is(err, ErrPoolWaiterCapReached) {
				pool.recordWaitDuration(start)
			}
			if err != nil {
				if errors.Is(err, ErrPoolWaiterCapReached) || errors.Is(err, ErrConnPoolClosed) {
					return nil, err
				}
				return nil, ErrTimeout
			}
		}
		if conn == nil {
			continue
		}
		if pool.close.Load() == nil {
			pool.discardConn(conn)
			return nil, ErrConnPoolClosed
		}
		if pool.capacity.Load() == 0 {
			pool.discardConn(conn)
			return nil, ErrTimeout
		}

		// ensure that the setting applied to the connection matches the one we want
		connSetting := conn.Conn.Setting()
		if connSetting != setting {
			// if there's another setting applied, reset it before applying our setting
			if connSetting != nil {
				pool.Metrics.diffSetting.Add(1)

				err = conn.Conn.ResetSetting(ctx)
				if err != nil {
					conn.Close()
					err = pool.connReopen(ctx, conn, monotonicNow())
					if err != nil {
						pool.closedConn()
						return nil, err
					}
				}
			}
			// apply our setting now; if we can't we assume that the conn is broken
			// and close it without returning to the pool
			if err := conn.Conn.ApplySetting(ctx, setting); err != nil {
				conn.Close()
				pool.closedConn()
				return nil, err
			}
		}

		pool.borrowed.Add(1)
		return conn, nil
	}
}

// SetCapacity changes the capacity (number of open connections) on the pool.
// If the capacity is smaller than the number of connections that there are
// currently open, we'll close enough connections before returning, even if
// that means waiting for clients to return connections to the pool.
// If the given context times out before we've managed to close enough connections
// an error will be returned.
//
// SetCapacity may be called before Open or after Close — the capacity field
// is configurable independently of lifecycle. A closed pool with capacity > 0
// still refuses Gets (they return ErrConnPoolClosed); the capacity is just
// pre-armed for the next Open. On a closed pool SetCapacity skips the drain
// loop: there are no in-flight Gets to throttle, and any lingering active
// slots from a timed-out CloseWithContext belong to lifecycle teardown, not
// to capacity configuration.
func (pool *ConnPool[C]) SetCapacity(ctx context.Context, newcap int64) error {
	pool.capacityMu.Lock()
	defer pool.capacityMu.Unlock()

	if newcap < 0 {
		panic("negative capacity")
	}

	if pool.close.Load() == nil {
		pool.capacity.Store(newcap)
		pool.setIdleCount()
		return nil
	}
	return pool.setCapacity(ctx, newcap)
}

// setCapacity is the internal implementation for SetCapacity; it must be called
// with pool.capacityMu being held
func (pool *ConnPool[C]) setCapacity(ctx context.Context, newcap int64) error {
	if newcap < 0 {
		panic("negative capacity")
	}

	oldcap := pool.capacity.Swap(newcap)
	if oldcap == newcap {
		return nil
	}
	// Update idleCount synchronously with the capacity Swap. closeOnIdleLimitReached
	// uses idleCount to decide whether a returning borrowed conn should be
	// closed or pushed back to the stack. If idleCount still reflects the
	// pre-shrink value during the drain loop below, a returning conn can be
	// pushed back onto a stack and stranded there if the drain times out —
	// which then leaves a stale-generation conn for the next Get's fast path
	// to pick up after reopen() bumps the generation.
	pool.setIdleCount()

	if newcap == 0 {
		for pool.wait.tryNotifyWaiter() {
		}
	}

	const delay = 10 * time.Millisecond

	// close connections until we're under capacity
	for pool.active.Load() > newcap {
		// try closing from connections which are currently idle in the stacks
		conn := pool.getFromSettingsStack(nil)
		if conn == nil {
			conn = pool.pop(&pool.clean)
		}
		if conn != nil {
			conn.Close()
			pool.closedConn()
			continue
		}

		// Stacks are empty; we have to wait for borrowed conns to return.
		// Honor the caller's ctx only here — popping idle conns is
		// unconditional work, and bailing before draining them would
		// physically leak open MySQL connections.
		if err := ctx.Err(); err != nil {
			return vterrors.Errorf(vtrpcpb.Code_ABORTED,
				"timed out while waiting for connections to be returned to the pool (capacity=%d, active=%d, borrowed=%d)",
				pool.capacity.Load(), pool.active.Load(), pool.borrowed.Load())
		}
		time.Sleep(delay)
	}

	if newcap > oldcap {
		pool.notifyWaitersForAvailableCapacity(newcap - oldcap)
	}

	return nil
}

// closeIdleResources rotates connections that have been idle longer than
// IdleTimeout. It's called on a ticker by the idle worker.
//
// Design note: the rotation is split into two loops on purpose. The close
// loop drops pool.active for each expired conn before the reopen loop tries
// to bring it back up. The visible dip in pool.active is intentional — it's
// the mechanism by which a concurrent Get can claim the freed slot via
// getNew during a slow reopen, instead of waiting for the worker to finish.
// If the Get wins the CAS race for the slot, the reopen-loop's inner check
// `if open >= pool.capacity.Load() { conn.Close() }` discards the worker's
// freshly-opened conn rather than exceeding capacity.
//
// This trades occasional wasted reopens under contention for non-blocking
// Get during idle cycling. The contract is pinned by
// TestIdleTimeoutReopenDoesNotBlockGetsDespiteAvailableCapacity; do not
// "fix" the dip into an in-place rotation without changing that test too.
func (pool *ConnPool[C]) closeIdleResources(now time.Time) {
	timeout := pool.IdleTimeout()
	if timeout == 0 {
		return
	}
	if pool.Capacity() == 0 {
		return
	}

	mono := monotonicFromTime(now)

	// Reconnects must abort when the pool starts closing; otherwise a slow
	// MySQL connect pins workers.Wait() inside CloseWithContext.
	connectCtx := pool.closeContext()

	closeInStack := func(s *connStack[C]) {
		conn, ok := s.Pop()
		if !ok {
			// Early return to skip allocating slices when the stack is empty
			return
		}

		activeConnections := pool.Active()

		// Only expire up to ~half of the active connections at a time. This should
		// prevent us from closing too many connections in one go which could lead to
		// a lot of `.Get` calls being added to the waitlist if there's a sudden spike
		// coming in _after_ connections were popped off the stack but _before_ being
		// returned back to the pool. This is unlikely to happen, but better safe than sorry.
		//
		// We always expire at least one connection per stack per iteration to ensure
		// that idle connections are eventually closed even in small pools.
		//
		// We will expire any additional connections in the next iteration of the idle closer.
		expiredConnections := make([]*Pooled[C], 0, max(activeConnections/2, 1))
		validConnections := make([]*Pooled[C], 0, activeConnections)

		// Pop out connections from the stack until we get a `nil` connection
		for ok {
			if conn.timeUsed.expired(mono, timeout) {
				expiredConnections = append(expiredConnections, conn)

				if len(expiredConnections) == cap(expiredConnections) {
					// We have collected enough connections for this iteration to expire
					break
				}
			} else {
				validConnections = append(validConnections, conn)
			}

			conn, ok = s.Pop()
		}

		// Return all the valid connections back to waiters or the stack
		//
		// The order here is not important - because we can't guarantee to
		// restore the order we got the connections out of the stack anyway.
		//
		// If we return the connections in the order popped off the stack:
		//   * waiters will get the newest connection first
		//   * stack will have the oldest connections at the top of the stack.
		//
		// If we return the connections in reverse order:
		//  * waiters will get the oldest connection first
		//  * stack will have the newest connections at the top of the stack.
		//
		// Neither of these is better or worse than the other.
		for _, conn := range validConnections {
			pool.tryReturnConn(conn)
		}

		// Close all the expired connections and open new ones to replace them
		for _, conn := range expiredConnections {
			pool.Metrics.idleClosed.Add(1)

			conn.Close()
			pool.closedConn()
		}

		for _, conn := range expiredConnections {
			if pool.active.Load() >= pool.capacity.Load() {
				break
			}

			if err := pool.connReopen(connectCtx, conn, mono); err != nil {
				continue
			}

			for {
				open := pool.active.Load()
				if open >= pool.capacity.Load() {
					conn.Close()
					break
				}
				if pool.active.CompareAndSwap(open, open+1) {
					pool.tryReturnConn(conn)
					break
				}
			}
		}
	}

	for i := 0; i <= stackMask; i++ {
		closeInStack(&pool.settings[i])
	}
	closeInStack(&pool.clean)
}

func (pool *ConnPool[C]) StatsJSON() map[string]any {
	return map[string]any{
		"Capacity":          int(pool.Capacity()),
		"Available":         int(pool.Available()),
		"Active":            int(pool.active.Load()),
		"InUse":             int(pool.InUse()),
		"WaitCount":         int(pool.Metrics.WaitCount()),
		"WaitTime":          pool.Metrics.WaitTime(),
		"IdleTimeout":       pool.IdleTimeout(),
		"IdleClosed":        int(pool.Metrics.IdleClosed()),
		"MaxLifetimeClosed": int(pool.Metrics.MaxLifetimeClosed()),
	}
}

// RegisterStats registers this pool's metrics into a stats Exporter
func (pool *ConnPool[C]) RegisterStats(stats *servenv.Exporter, name string) {
	if stats == nil || name == "" {
		return
	}

	pool.Name = name

	stats.NewGaugeFunc(name+"Capacity", "Tablet server conn pool capacity", func() int64 {
		return pool.Capacity()
	})
	stats.NewGaugeFunc(name+"Available", "Tablet server conn pool available", func() int64 {
		return pool.Available()
	})
	stats.NewGaugeFunc(name+"Active", "Tablet server conn pool active", func() int64 {
		return pool.Active()
	})
	stats.NewGaugeFunc(name+"InUse", "Tablet server conn pool in use", func() int64 {
		return pool.InUse()
	})
	stats.NewGaugeFunc(name+"MaxCap", "Tablet server conn pool max cap", func() int64 {
		// the smartconnpool doesn't have a maximum capacity
		return pool.Capacity()
	})
	stats.NewGaugeFunc(name+"IdleAllowed", "Tablet server conn pool idle allowed limit", func() int64 {
		return pool.IdleCount()
	})
	stats.NewCounterFunc(name+"WaitCount", "Tablet server conn pool wait count", func() int64 {
		return pool.Metrics.WaitCount()
	})
	stats.NewCounterDurationFunc(name+"WaitTime", "Tablet server wait time", func() time.Duration {
		return pool.Metrics.WaitTime()
	})
	stats.NewGaugeDurationFunc(name+"IdleTimeout", "Tablet server idle timeout", func() time.Duration {
		return pool.IdleTimeout()
	})
	stats.NewCounterFunc(name+"IdleClosed", "Tablet server conn pool idle closed", func() int64 {
		return pool.Metrics.IdleClosed()
	})
	stats.NewCounterFunc(name+"MaxLifetimeClosed", "Tablet server conn pool refresh closed", func() int64 {
		return pool.Metrics.MaxLifetimeClosed()
	})
	stats.NewCounterFunc(name+"Get", "Tablet server conn pool get count", func() int64 {
		return pool.Metrics.GetCount()
	})
	stats.NewCounterFunc(name+"GetSetting", "Tablet server conn pool get with setting count", func() int64 {
		return pool.Metrics.GetSettingCount()
	})
	stats.NewCounterFunc(name+"WaiterCapRejected", "Number of times a request was rejected due to hitting connection pool waiter capacity", func() int64 {
		return pool.Metrics.WaiterCapRejected()
	})
	stats.NewCounterFunc(name+"DiffSetting", "Number of times pool applied different setting", func() int64 {
		return pool.Metrics.DiffSettingCount()
	})
	stats.NewCounterFunc(name+"ResetSetting", "Number of times pool reset the setting", func() int64 {
		return pool.Metrics.ResetSettingCount()
	})
}
