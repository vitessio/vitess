package smartconnpool

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
)

const stackMask = 7

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

type ConnPool[C Connection] struct {
	clean              Stack[C]
	settings           [stackMask + 1]Stack[C]
	freshSettingsStack atomic.Int64
	wait               waitlist[C]

	borrowed atomic.Int64
	active   atomic.Int64
	capacity atomic.Int64

	workers sync.WaitGroup
	close   chan struct{}

	config struct {
		connect Connector[C]
		refresh RefreshCheck

		maxCapacity     int64
		maxLifetime     atomic.Int64
		idleTimeout     atomic.Int64
		refreshInterval atomic.Int64
		logWait         func(time.Time)
	}

	Metrics Metrics
}

func (pool *ConnPool[C]) Get(ctx context.Context, setting *Setting) (*Pooled[C], error) {
	if ctx.Err() != nil {
		return nil, ErrCtxTimeout
	}
	if setting == nil {
		return pool.get(ctx)
	}
	return pool.getWithSetting(ctx, setting)
}

func (pool *ConnPool[C]) Put(conn *Pooled[C]) {
	pool.borrowed.Add(-1)

	if conn != nil {
		conn.timeUsed = time.Now()

		lifetime := pool.extendedMaxLifetime()
		if lifetime > 0 && time.Until(conn.timeCreated.Add(lifetime)) < 0 {
			pool.Metrics.maxLifetimeClosed.Add(1)
			conn.Close()
			if err := pool.connReopen(context.Background(), conn, conn.timeUsed); err != nil {
				return
			}
		}
	}
	if conn == nil {
		var err error
		conn, err = pool.connNew(context.Background())
		if err != nil {
			pool.active.Add(-1)
			return
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
	extended := hack.FastRand() % uint32(maxLifetime)
	return time.Duration(maxLifetime) + time.Duration(extended)
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

func (pool *ConnPool[C]) getNew(ctx context.Context) (*Pooled[C], error) {
	for {
		open := pool.active.Load()
		if open >= pool.capacity.Load() {
			return nil, nil
		}

		if pool.active.CompareAndSwap(open, open+1) {
			conn, err := pool.connNew(ctx)
			if err != nil {
				pool.active.Add(-1)
				return nil, err
			}
			return conn, nil
		}
	}
}

func (pool *ConnPool[C]) get(ctx context.Context) (*Pooled[C], error) {
	pool.Metrics.getCount.Add(1)

	if conn, ok := pool.clean.Pop(); ok {
		pool.borrowed.Add(1)
		return conn, nil
	}

	conn, err := pool.getNew(ctx)
	if err != nil {
		return nil, err
	}
	if conn == nil {
		conn = pool.getFromSettingsStack(nil)
	}
	if conn == nil && pool.borrowed.Load() > 0 {
		start := time.Now()
		conn, err = pool.wait.waitForConn(ctx, nil)
		if err != nil {
			if conn != nil {
				// our context expired but we managed to get a conn from
				// the waitlist; put it back!
				pool.Put(conn)
			}
			return nil, ErrTimeout
		}
		pool.recordWait(start)
	}
	if conn == nil {
		return nil, ErrTimeout
	}

	if conn.Conn.Setting() != nil {
		pool.Metrics.resetSetting.Add(1)

		err = conn.Conn.ResetSetting(ctx)
		if err != nil {
			conn.Close()
			err = pool.connReopen(ctx, conn, time.Now())
			if err != nil {
				pool.active.Add(-1)
				return nil, err
			}
		}
	}

	pool.borrowed.Add(1)
	return conn, nil
}

func (pool *ConnPool[C]) getWithSetting(ctx context.Context, setting *Setting) (*Pooled[C], error) {
	pool.Metrics.getWithSettingsCount.Add(1)

	var err error
	conn, _ := pool.settings[setting.bucket&stackMask].Pop()
	if conn == nil {
		conn, _ = pool.clean.Pop()
	}
	if conn == nil {
		conn, err = pool.getNew(ctx)
		if err != nil {
			return nil, err
		}
	}
	if conn == nil {
		conn = pool.getFromSettingsStack(setting)
	}
	if conn == nil && pool.borrowed.Load() > 0 {
		start := time.Now()
		conn, err = pool.wait.waitForConn(ctx, setting)
		if err != nil {
			if conn != nil {
				// our context expired but we managed to get a conn from
				// the waitlist; put it back!
				pool.Put(conn)
			}
			return nil, ErrTimeout
		}
		pool.recordWait(start)
	}
	if conn == nil {
		return nil, ErrTimeout
	}

	connSetting := conn.Conn.Setting()
	if connSetting != setting {
		if connSetting != nil {
			pool.Metrics.diffSetting.Add(1)

			err = conn.Conn.ResetSetting(ctx)
			if err != nil {
				conn.Close()
				err = pool.connReopen(ctx, conn, time.Now())
				if err != nil {
					pool.active.Add(-1)
					return nil, err
				}
			}
		}
		if err := conn.Conn.ApplySetting(ctx, setting); err != nil {
			conn.Close()
			pool.active.Add(-1)
			return nil, err
		}
	}

	pool.borrowed.Add(1)
	return conn, nil
}

func (pool *ConnPool[C]) SetCapacity(newcap int64) {
	if newcap < 0 {
		panic("negative capacity")
	}

	var oldcap int64

	for {
		oldcap = pool.capacity.Load()
		if oldcap == newcap {
			return
		}
		if pool.capacity.CompareAndSwap(oldcap, newcap) {
			break
		}
	}

	for pool.active.Load() > newcap {
		conn := pool.getFromSettingsStack(nil)
		if conn == nil {
			conn, _ = pool.clean.Pop()
		}
		if conn == nil {
			conn, _ = pool.wait.waitForConn(context.Background(), nil)
		}
		if conn == nil {
			continue
		}
		conn.Close()
		pool.active.Add(-1)
	}
}

type RefreshCheck func() (bool, error)

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

func (pool *ConnPool[D]) IdleTimeout() time.Duration {
	return time.Duration(pool.config.idleTimeout.Load())
}

func (pool *ConnPool[D]) RefreshInterval() time.Duration {
	return time.Duration(pool.config.refreshInterval.Load())
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

	closeInStack := func(s *Stack[C]) {
		conns = s.PopAll(conns[:0])
		slices.Reverse(conns)

		for _, conn := range conns {
			if conn.timeUsed.Add(timeout).Sub(now) < 0 {
				pool.Metrics.idleClosed.Add(1)

				conn.Close()
				if err := pool.connReopen(context.Background(), conn, now); err != nil {
					pool.active.Add(-1)
					continue
				}
			}

			s.Push(conn)
		}
	}

	for i := 0; i <= stackMask; i++ {
		closeInStack(&pool.settings[i])
	}
	closeInStack(&pool.clean)
}

type Connector[C Connection] func(ctx context.Context) (C, error)

type Config[C Connection] struct {
	Capacity        int64
	IdleTimeout     time.Duration
	MaxLifetime     time.Duration
	RefreshInterval time.Duration
	LogWait         func(time.Time)
}

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

func (pool *ConnPool[D]) reopen() {
	capacity := pool.capacity.Load()
	if capacity == 0 {
		return
	}

	pool.Close()
	pool.open()
	pool.SetCapacity(capacity)
}

func (pool *ConnPool[D]) Close() {
	if pool.close == nil {
		// already closed
		return
	}

	pool.SetCapacity(0)

	close(pool.close)
	pool.workers.Wait()
	pool.close = nil
}

func (pool *ConnPool[C]) IsOpen() bool {
	return pool.close != nil
}

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

func (pool *ConnPool[C]) open() {
	pool.close = make(chan struct{})
	pool.capacity.Store(pool.config.maxCapacity)

	pool.runWorker(pool.close, 1*time.Second, func(_ time.Time) bool {
		force := pool.capacity.Load() == 0 && pool.borrowed.Load() == 0
		_ = pool.wait.expire(force)
		return true
	})

	idleTimeout := pool.IdleTimeout()
	if idleTimeout != 0 {
		pool.runWorker(pool.close, idleTimeout/10, func(now time.Time) bool {
			pool.closeIdleResources(now)
			return true
		})
	}

	refreshInterval := pool.RefreshInterval()
	if refreshInterval != 0 && pool.config.refresh != nil {
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

func (pool *ConnPool[C]) Capacity() int64 {
	return pool.capacity.Load()
}

func (pool *ConnPool[C]) MaxCapacity() int64 {
	return pool.config.maxCapacity
}

func (pool *ConnPool[C]) InUse() int64 {
	return pool.borrowed.Load()
}

func (pool *ConnPool[C]) Available() int64 {
	return pool.capacity.Load() - pool.borrowed.Load()
}

func (pool *ConnPool[C]) Active() int64 {
	return pool.active.Load()
}

func (pool *ConnPool[C]) recordWait(start time.Time) {
	pool.Metrics.waitCount.Add(1)
	pool.Metrics.waitTime.Add(time.Since(start).Nanoseconds())
	if pool.config.logWait != nil {
		pool.config.logWait(start)
	}
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

func (pool *ConnPool[C]) SetIdleTimeout(duration time.Duration) {
	pool.config.idleTimeout.Store(duration.Nanoseconds())
}
