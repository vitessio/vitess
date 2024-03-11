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
	"math/rand/v2"
	"slices"
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
	ErrTimeout = vterrors.New(vtrpcpb.Code_RESOURCE_EXHAUSTED, "resource pool timed out")

	// ErrCtxTimeout is returned if a ctx is already expired by the time the connection pool is used
	ErrCtxTimeout = vterrors.New(vtrpcpb.Code_DEADLINE_EXCEEDED, "resource pool context already expired")
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

type Connector[C Connection] func(ctx context.Context) (C, error)
type RefreshCheck func() (bool, error)

type Config[C Connection] struct {
	Capacity        int64
	IdleTimeout     time.Duration
	MaxLifetime     time.Duration
	RefreshInterval time.Duration
	LogWait         func(time.Time)
}

// stackMask is the number of connection setting stacks minus one;
// the number of stacks must always be a power of two
const stackMask = 7

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

	// workers is a waitgroup for all the currently running worker goroutines
	workers sync.WaitGroup
	close   chan struct{}

	config struct {
		// connect is the callback to create a new connection for the pool
		connect Connector[C]
		// refresh is the callback to check whether the pool needs to be refreshed
		refresh RefreshCheck

		// maxCapacity is the maximum value to which capacity can be set; when the pool
		// is re-opened, it defaults to this capacity
		maxCapacity int64
		// maxLifetime is the maximum time a connection can be open
		maxLifetime atomic.Int64
		// idleTimeout is the maximum time a connection can remain idle
		idleTimeout atomic.Int64
		// refreshInterval is how often to call the refresh check
		refreshInterval atomic.Int64
		// logWait is called every time a client must block waiting for a connection
		logWait func(time.Time)
	}

	Metrics Metrics
}

// NewPool creates a new connection pool with the given Config.
// The pool must be ConnPool.Open before it can start giving out connections
func NewPool[C Connection](config *Config[C]) *ConnPool[C] {
	pool := &ConnPool[C]{}
	pool.freshSettingsStack.Store(-1)
	pool.config.maxCapacity = config.Capacity
	pool.config.maxLifetime.Store(config.MaxLifetime.Nanoseconds())
	pool.config.idleTimeout.Store(config.IdleTimeout.Nanoseconds())
	pool.config.refreshInterval.Store(config.RefreshInterval.Nanoseconds())
	pool.config.logWait = config.LogWait
	pool.wait.init()

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
	pool.close = make(chan struct{})
	pool.capacity.Store(pool.config.maxCapacity)

	// The expire worker takes care of removing from the waiter list any clients whose
	// context has been cancelled.
	pool.runWorker(pool.close, 1*time.Second, func(_ time.Time) bool {
		pool.wait.expire(false)
		return true
	})

	idleTimeout := pool.IdleTimeout()
	if idleTimeout != 0 {
		// The idle worker takes care of closing connections that have been idle too long
		pool.runWorker(pool.close, idleTimeout/10, func(now time.Time) bool {
			pool.closeIdleResources(now)
			return true
		})
	}

	refreshInterval := pool.RefreshInterval()
	if refreshInterval != 0 && pool.config.refresh != nil {
		// The refresh worker periodically checks the refresh callback in this pool
		// to decide whether all the connections in the pool need to be cycled
		// (this usually only happens when there's a global DNS change).
		pool.runWorker(pool.close, refreshInterval, func(_ time.Time) bool {
			refresh, err := pool.config.refresh()
			if err != nil {
				log.Error(err)
			}
			if refresh {
				go pool.reopen()
				return false
			}
			return true
		})
	}
}

// Open starts the background workers that manage the pool and gets it ready
// to start serving out connections.
func (pool *ConnPool[C]) Open(connect Connector[C], refresh RefreshCheck) *ConnPool[C] {
	if pool.close != nil {
		// already open
		return pool
	}

	pool.config.connect = connect
	pool.config.refresh = refresh
	pool.open()
	return pool
}

// Close shuts down the pool. No connections will be returned from ConnPool.Get after calling this,
// but calling ConnPool.Put is still allowed. This function will not return until all of the pool's
// connections have been returned.
func (pool *ConnPool[C]) Close() {
	if pool.close == nil {
		// already closed
		return
	}

	pool.SetCapacity(0)

	close(pool.close)
	pool.workers.Wait()
	pool.close = nil
}

func (pool *ConnPool[C]) reopen() {
	capacity := pool.capacity.Load()
	if capacity == 0 {
		return
	}

	pool.Close()
	pool.open()
	pool.SetCapacity(capacity)
}

// IsOpen returns whether the pool is open
func (pool *ConnPool[C]) IsOpen() bool {
	return pool.close != nil
}

// Capacity returns the maximum amount of connections that this pool can maintain open
func (pool *ConnPool[C]) Capacity() int64 {
	return pool.capacity.Load()
}

// MaxCapacity returns the maximum value to which Capacity can be set via ConnPool.SetCapacity
func (pool *ConnPool[C]) MaxCapacity() int64 {
	return pool.config.maxCapacity
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

func (pool *ConnPool[D]) RefreshInterval() time.Duration {
	return time.Duration(pool.config.refreshInterval.Load())
}

func (pool *ConnPool[C]) recordWait(start time.Time) {
	pool.Metrics.waitCount.Add(1)
	pool.Metrics.waitTime.Add(time.Since(start).Nanoseconds())
	if pool.config.logWait != nil {
		pool.config.logWait(start)
	}
}

// Get returns a connection from the pool with the given Setting applied.
// If there are no connections in the pool to be returned, Get blocks until one
// is returned, or until the given ctx is cancelled.
// The connection must be returned to the pool once it's not needed by calling Pooled.Recycle
func (pool *ConnPool[C]) Get(ctx context.Context, setting *Setting) (*Pooled[C], error) {
	if ctx.Err() != nil {
		return nil, ErrCtxTimeout
	}
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

	if conn == nil {
		var err error
		conn, err = pool.connNew(context.Background())
		if err != nil {
			pool.closedConn()
			return
		}
	} else {
		conn.timeUsed = time.Now()

		lifetime := pool.extendedMaxLifetime()
		if lifetime > 0 && time.Until(conn.timeCreated.Add(lifetime)) < 0 {
			pool.Metrics.maxLifetimeClosed.Add(1)
			conn.Close()
			if err := pool.connReopen(context.Background(), conn, conn.timeUsed); err != nil {
				pool.closedConn()
				return
			}
		}
	}

	if !pool.wait.tryReturnConn(conn) {
		connSetting := conn.Conn.Setting()
		if connSetting == nil {
			pool.clean.Push(conn)
		} else {
			stack := connSetting.bucket & stackMask
			pool.settings[stack].Push(conn)
			pool.freshSettingsStack.Store(int64(stack))
		}
	}
}

func (pool *ConnPool[D]) extendedMaxLifetime() time.Duration {
	maxLifetime := pool.config.maxLifetime.Load()
	if maxLifetime == 0 {
		return 0
	}
	return time.Duration(maxLifetime) + time.Duration(rand.Uint32N(uint32(maxLifetime)))
}

func (pool *ConnPool[C]) connReopen(ctx context.Context, dbconn *Pooled[C], now time.Time) error {
	var err error
	dbconn.Conn, err = pool.config.connect(ctx)
	if err != nil {
		return err
	}

	dbconn.timeUsed = now
	dbconn.timeCreated = now
	return nil
}

func (pool *ConnPool[C]) connNew(ctx context.Context) (*Pooled[C], error) {
	conn, err := pool.config.connect(ctx)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	return &Pooled[C]{
		timeCreated: now,
		timeUsed:    now,
		pool:        pool,
		Conn:        conn,
	}, nil
}

func (pool *ConnPool[C]) getFromSettingsStack(setting *Setting) *Pooled[C] {
	fresh := pool.freshSettingsStack.Load()
	if fresh < 0 {
		return nil
	}

	var start uint32
	if setting == nil {
		start = uint32(fresh)
	} else {
		start = setting.bucket
	}

	for i := uint32(0); i <= stackMask; i++ {
		pos := (i + start) & stackMask
		if conn, ok := pool.settings[pos].Pop(); ok {
			return conn
		}
	}
	return nil
}

func (pool *ConnPool[C]) closedConn() {
	_ = pool.active.Add(-1)
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

// get returns a pooled connection with no Setting applied
func (pool *ConnPool[C]) get(ctx context.Context) (*Pooled[C], error) {
	pool.Metrics.getCount.Add(1)

	// best case: if there's a connection in the clean stack, return it right away
	if conn, ok := pool.clean.Pop(); ok {
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
		conn, err = pool.wait.waitForConn(ctx, nil)
		if err != nil {
			return nil, ErrTimeout
		}
		pool.recordWait(start)
	}
	// no connections available and no connections to wait for (pool is closed)
	if conn == nil {
		return nil, ErrTimeout
	}

	// if the connection we've acquired has a Setting applied, we must reset it before returning
	if conn.Conn.Setting() != nil {
		pool.Metrics.resetSetting.Add(1)

		err = conn.Conn.ResetSetting(ctx)
		if err != nil {
			conn.Close()
			err = pool.connReopen(ctx, conn, time.Now())
			if err != nil {
				pool.closedConn()
				return nil, err
			}
		}
	}

	pool.borrowed.Add(1)
	return conn, nil
}

// getWithSetting returns a connection from the pool with the given Setting applied
func (pool *ConnPool[C]) getWithSetting(ctx context.Context, setting *Setting) (*Pooled[C], error) {
	pool.Metrics.getWithSettingsCount.Add(1)

	var err error
	// best case: check if there's a connection in the setting stack where our Setting belongs
	conn, _ := pool.settings[setting.bucket&stackMask].Pop()
	// if there's connection with our setting, try popping a clean connection
	if conn == nil {
		conn, _ = pool.clean.Pop()
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
		conn, err = pool.wait.waitForConn(ctx, setting)
		if err != nil {
			return nil, ErrTimeout
		}
		pool.recordWait(start)
	}
	// no connections available and no connections to wait for (pool is closed)
	if conn == nil {
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
				err = pool.connReopen(ctx, conn, time.Now())
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

// SetCapacity changes the capacity (number of open connections) on the pool.
// If the capacity is smaller than the number of connections that there are
// currently open, we'll close enough connections before returning, even if
// that means waiting for clients to return connections to the pool.
func (pool *ConnPool[C]) SetCapacity(newcap int64) {
	if newcap < 0 {
		panic("negative capacity")
	}

	oldcap := pool.capacity.Swap(newcap)
	if oldcap == newcap {
		return
	}

	backoff := 1 * time.Millisecond

	// close connections until we're under capacity
	for pool.active.Load() > newcap {
		// try closing from connections which are currently idle in the stacks
		conn := pool.getFromSettingsStack(nil)
		if conn == nil {
			conn, _ = pool.clean.Pop()
		}
		if conn == nil {
			time.Sleep(backoff)
			backoff += 1 * time.Millisecond
			continue
		}
		conn.Close()
		pool.closedConn()
	}

	// if we're closing down the pool, wake up any blocked waiters because no connections
	// are going to be returned in the future
	if newcap == 0 {
		pool.wait.expire(true)
	}
}

func (pool *ConnPool[C]) closeIdleResources(now time.Time) {
	timeout := pool.IdleTimeout()
	if timeout == 0 {
		return
	}
	if pool.Capacity() == 0 {
		return
	}

	var conns []*Pooled[C]

	closeInStack := func(s *connStack[C]) {
		conns = s.PopAll(conns[:0])
		slices.Reverse(conns)

		for _, conn := range conns {
			if conn.timeUsed.Add(timeout).Sub(now) < 0 {
				pool.Metrics.idleClosed.Add(1)
				conn.Close()
				pool.closedConn()
				continue
			}

			s.Push(conn)
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
	stats.NewCounterFunc(name+"DiffSetting", "Number of times pool applied different setting", func() int64 {
		return pool.Metrics.DiffSettingCount()
	})
	stats.NewCounterFunc(name+"ResetSetting", "Number of times pool reset the setting", func() int64 {
		return pool.Metrics.ResetSettingCount()
	})
}
