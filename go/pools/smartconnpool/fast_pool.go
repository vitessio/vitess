package smartconnpool

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type FastPool[C Connection] struct {
	// connectionsMu protects modifying the connections slice
	connectionsMu sync.Mutex
	connections   atomic.Pointer[[]*Pooled[C]] // slice of connections in the pool

	closed   atomic.Bool
	capacity atomic.Int64
	waiters  atomic.Int64

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
	}

	wait waitlist[C]

	// addConnectionChan is a channel to signal that a new connection should be opened
	addConnectionChan chan struct{}

	Metrics Metrics
	Name    string
}

func NewFastPool[C Connection](config *Config[C]) *FastPool[C] {
	pool := &FastPool[C]{}
	pool.config.maxCapacity = config.Capacity
	pool.config.maxIdleCount = config.MaxIdleCount
	pool.config.maxLifetime.Store(config.MaxLifetime.Nanoseconds())
	pool.config.idleTimeout.Store(config.IdleTimeout.Nanoseconds())
	pool.config.refreshInterval.Store(config.RefreshInterval.Nanoseconds())
	pool.config.logWait = config.LogWait

	connections := make([]*Pooled[C], 0, pool.config.maxCapacity)
	pool.connections.Store(&connections)

	pool.addConnectionChan = make(chan struct{}, pool.config.maxCapacity)
	pool.capacity.Store(pool.config.maxCapacity)

	pool.wait.init()

	return pool
}

func (pool *FastPool[C]) Open(connect Connector[C], refresh RefreshCheck) *FastPool[C] {
	pool.config.connect = connect
	pool.config.refresh = refresh

	pool.open()

	return pool
}

func (pool *FastPool[C]) shouldContinueCreating() bool {
	if pool.closed.Load() {
		return false
	}

	// Check if we have reached the maximum capacity
	if int64(len(*pool.connections.Load())) >= pool.capacity.Load() {
		return false
	}

	idleConnectionCount := int64(0)
	connections := *pool.connections.Load()
	for _, conn := range connections {
		if conn.state.Load() == NOT_IN_USE {
			idleConnectionCount += 1
		}
	}

	// If we have too many idle connections, we should stop creating new ones
	if idleConnectionCount > pool.config.maxIdleCount {
		return false
	}

	if idleConnectionCount > pool.waiters.Load() {
		// If we have more idle connections than waiters, we should stop creating new ones
		return false
	}

	return true
}

func (pool *FastPool[C]) open() {
	ctx := context.Background()

	// Connection adder
	go func() {
		for {
			select {
			case <-pool.addConnectionChan:
				for pool.shouldContinueCreating() {
					ctx := context.Background()
					conn, err := pool.connNew(ctx)

					if err != nil {
						// If we couldn't create a new connection, sleep for a bit and then try again
						time.Sleep(10 * time.Millisecond)
						continue
					}

					// Add the new connection to the pool
					conn.state.Store(NOT_IN_USE)

					pool.Metrics.openCount.Add(1)

					pool.add(conn)
				}
			case <-ctx.Done():
				// If the context is done, we stop adding connections
				return
			}
		}
	}()

	// Connection closer

	// Housekeeper
}

func (pool *FastPool[C]) connNew(ctx context.Context) (*Pooled[C], error) {
	conn, err := pool.config.connect(ctx)
	if err != nil {
		return nil, err
	}
	pooled := &Pooled[C]{
		pool: pool,
		Conn: conn,
	}
	now := monotonicNow()
	pooled.timeUsed.set(now)
	pooled.timeCreated.set(now)
	return pooled, nil
}

func (pool *FastPool[C]) add(conn *Pooled[C]) {
	if pool.closed.Load() {
		return
	}

	// First, add the connection to the pool's connections slice
	pool.connectionsMu.Lock()
	connections := *pool.connections.Load()
	connections = append(connections, conn)
	pool.connections.Store(&connections)
	pool.connectionsMu.Unlock()

	// Try to hand the connection to any waiting goroutines
	for pool.waiters.Load() > 0 {
		if !conn.state.CompareAndSwap(NOT_IN_USE, IN_USE) {
			return
		}

		if pool.offer(conn) {
			// If we successfully offered the connection, we can return
			return
		}

		conn.state.Store(NOT_IN_USE)
		runtime.Gosched()
	}
}

func (pool *FastPool[C]) offer(conn *Pooled[C]) bool {
	return pool.wait.tryReturnConn(conn)
}

func (pool *FastPool[C]) put(conn *Pooled[C]) {
	if conn.markedForEviction.Load() {
		pool.closeConnection(conn)
		return
	}

	// Mark the connection as not in use.
	// This will allow other goroutines to grab ownership of this connection.
	conn.state.Store(NOT_IN_USE)

	// If we have waiters waiting for a connection, we can try to offer this connection to them
	for pool.waiters.Load() > 0 {
		if !conn.state.CompareAndSwap(NOT_IN_USE, IN_USE) {
			return
		}

		if pool.offer(conn) {
			// If we successfully offered the connection, we can return
			return
		}

		conn.state.Store(NOT_IN_USE)

		// Allow other goroutines to run
		runtime.Gosched()
	}
}

func (pool *FastPool[C]) closeConnection(conn *Pooled[C]) {
	if !conn.state.CompareAndSwap(IN_USE, REMOVED) {
		return
	}

	// Remove this connection from our connections slice
	pool.connectionsMu.Lock()
	connections := *pool.connections.Load()
	for i, c := range connections {
		if c == conn {
			// Remove the connection from the slice
			connections = append(connections[:i], connections[i+1:]...)
			break
		}
	}
	pool.connections.Store(&connections)
	pool.connectionsMu.Unlock()

	conn.Conn.Close()
	conn.pool = nil
}

func (pool *FastPool[C]) borrow(ctx context.Context, setting *Setting) (*Pooled[C], error) {
	pool.waiters.Add(1)
	defer pool.waiters.Add(-1)

	// See if we have any matching, unused connections in the pool
	connections := *pool.connections.Load()
	for _, conn := range connections {
		if conn.Conn.Setting() != setting {
			// If the connection's setting doesn't match the requested setting, skip it
			continue
		}

		if conn.state.CompareAndSwap(NOT_IN_USE, IN_USE) {
			if conn.Conn.Setting() != setting {
				// Unlikely, but we need to check if the connection's setting has changed,
				// and if it has, we skip this connection
				conn.state.Store(NOT_IN_USE)
				continue
			}

			// We successfully borrowed this connection, so we can return it
			conn.timeUsed.set(monotonicNow())
			return conn, nil
		}
	}

	// If we didn't find a matching connection, try to grab ANY idle connection
	// and change its setting. This prevents waiting forever when all connections
	// are idle but have different settings.
	connections = *pool.connections.Load()
	for _, conn := range connections {
		if conn.state.CompareAndSwap(NOT_IN_USE, IN_USE) {
			// We got a connection! Now check if we need to change its setting
			if conn.Conn.Setting() != setting {
				pool.Metrics.diffSetting.Add(1)

				// Reset the current setting if needed
				if conn.Conn.Setting() != nil {
					err := conn.Conn.ResetSetting(ctx)
					if err != nil {
						// If we couldn't reset the setting, close the connection
						pool.closeConnection(conn)
						continue
					}
				}

				// Apply the new setting
				err := conn.Conn.ApplySetting(ctx, setting)
				if err != nil {
					// If we couldn't apply the setting, close the connection
					pool.closeConnection(conn)
					continue
				}
			}

			// Successfully borrowed and configured the connection
			conn.timeUsed.set(monotonicNow())
			return conn, nil
		}
	}

	// If we have more waiters than in-flight connection add requests,
	// ask for another connection
	if pool.waiters.Load() > int64(len(pool.addConnectionChan)) {
		// Request a new connection to be added to the pool
		select {
		case pool.addConnectionChan <- struct{}{}:
		default:
			// We don't want to block if the channel is already full
		}
	}

	start := time.Now()

	for {
		// Wait for a connection to become available
		conn, err := pool.wait.waitForConn(ctx, setting, pool.closed.Load)
		if err != nil {
			return nil, err
		}

		// Verify that no one has stolen the connection from us
		// We successfully borrowed this connection, so we can return it
		conn.timeUsed.set(monotonicNow())

		// If the connection's setting doesn't match the requested setting,
		// we need to apply the setting to the connection.
		// If the connection's setting is nil, we can skip this step.
		if conn.Conn.Setting() != setting {
			pool.Metrics.diffSetting.Add(1)

			// TODO: Reset setting
			if conn.Conn.Setting() != nil {
				err := conn.Conn.ResetSetting(ctx)
				if err != nil {
					// If we couldn't reset the setting, we need to close the connection
					pool.closeConnection(conn)

					// Wait again
					continue
				}
			}

			err := conn.Conn.ApplySetting(ctx, setting)
			if err != nil {
				// If we couldn't apply the setting, we need to close the connection
				pool.closeConnection(conn)

				// Wait again
				continue
			}
		}

		pool.recordWait(start)

		return conn, nil

		// Otherwise, we need to wait for another connection to become available...
	}
}

func (pool *FastPool[C]) recordWait(start time.Time) {
	pool.Metrics.waitCount.Add(1)
	pool.Metrics.waitTime.Add(time.Since(start).Nanoseconds())
	if pool.config.logWait != nil {
		pool.config.logWait(start)
	}
}

func (pool *FastPool[C]) Get(ctx context.Context, setting *Setting) (*Pooled[C], error) {
	if ctx.Err() != nil {
		return nil, ErrCtxTimeout
	}

	if pool.closed.Load() {
		return nil, ErrConnPoolClosed
	}

	return pool.borrow(ctx, setting)
}

func (pool *FastPool[C]) Close() error {
	if !pool.closed.CompareAndSwap(false, true) {
		return ErrConnPoolClosed
	}

	// Mark all connections for eviction
	connections := *pool.connections.Load()
	for _, conn := range connections {
		conn.markedForEviction.Store(true)
		if conn.state.CompareAndSwap(NOT_IN_USE, IN_USE) {
			// If the connection was not in use, we can close it immediately
			pool.closeConnection(conn)
		}
	}

	return nil
}
