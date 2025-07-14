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
	connections   atomic.Value

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

	pool.connections.Store(make([]*Pooled[C], 0, pool.config.maxCapacity))
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
	if int64(len(pool.connections.Load().([]*Pooled[C]))) >= pool.capacity.Load() {
		return false
	}

	idleConnectionCount := int64(0)
	connections := pool.connections.Load().([]*Pooled[C])
	for _, conn := range connections {
		if conn.state.Load().(ConnectionState).state == NOT_IN_USE {
			idleConnectionCount += 1
		}
	}

	// If we have too many idle connections, we should stop creating new ones
	if idleConnectionCount >= pool.config.maxIdleCount {
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

					//fmt.Println("New connection created:", conn)

					if err != nil {
						// If we couldn't create a new connection, sleep for a bit and then try again
						time.Sleep(10 * time.Millisecond)
						continue
					}

					// Add the new connection to the pool
					conn.state.Store(ConnectionState{
						state:   NOT_IN_USE,
						setting: conn.Conn.Setting(),
					})

					// fmt.Println("Adding connection to pool:", conn, "State:", conn.state.Load())

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

	pool.connectionsMu.Lock()
	connections := pool.connections.Load().([]*Pooled[C])
	pool.connections.Store(append(connections, conn))
	pool.connectionsMu.Unlock()

	for pool.waiters.Load() > 0 {
		if conn.state.Load().(ConnectionState).state != NOT_IN_USE {
			// Someone else stole this connection, so we're no longer the owner
			return
		}

		if pool.offer(conn) {
			// If we successfully offered the connection, we can return
			return
		}

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
	state := conn.state.Load().(ConnectionState)
	conn.state.Store(ConnectionState{
		state:   NOT_IN_USE,
		setting: state.setting,
	})

	// If we have waiters waiting for a connection, we can try to offer this connection to them
	for pool.waiters.Load() > 0 {
		if conn.state.Load().(ConnectionState).state != NOT_IN_USE {
			// Someone else was faster and stole this connection, so we can't offer it to anyone waiting
			return
		}

		if pool.offer(conn) {
			// If we successfully offered the connection, we can return
			return
		}

		// Allow other goroutines to run
		runtime.Gosched()
	}
}

func (pool *FastPool[C]) closeConnection(conn *Pooled[C]) {
	state := conn.state.Load().(ConnectionState)

	if !conn.state.CompareAndSwap(state, ConnectionState{state: REMOVED, setting: state.setting}) {
		return
	}

	// Remove this connection from our connections slice
	pool.connectionsMu.Lock()
	connections := pool.connections.Load().([]*Pooled[C])
	for i, c := range connections {
		if c == conn {
			// Remove the connection from the slice
			connections = append(connections[:i], connections[i+1:]...)
			break
		}
	}
	pool.connections.Store(connections)
	pool.connectionsMu.Unlock()

	conn.Conn.Close()
	conn.pool = nil
}

func (pool *FastPool[C]) borrow(ctx context.Context, setting *Setting) (*Pooled[C], error) {
	requestedState := ConnectionState{
		state:   NOT_IN_USE,
		setting: setting,
	}

	borrowedState := ConnectionState{
		state:   IN_USE,
		setting: setting,
	}

	pool.waiters.Add(1)
	defer pool.waiters.Add(-1)

	// See if we have any matching, unused connections in the pool
	connections := pool.connections.Load().([]*Pooled[C])
	for _, conn := range connections {
		// fmt.Println("Checking connection:", conn, "State:", conn.state.Load())

		if conn.state.CompareAndSwap(requestedState, borrowedState) {
			// We successfully borrowed this connection, so we can return it
			conn.timeUsed.set(monotonicNow())
			return conn, nil
		}
	}

	// fmt.Println("No available connections found in ", len(connections), " connections, waiting for one...")

	// If we have more waiters than in-flight connection add requests,
	// ask for another connection
	if pool.waiters.Load() > int64(len(pool.addConnectionChan)) {
		// fmt.Println("Requesting a new connection to be added to the pool...")
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

		state := conn.state.Load().(ConnectionState)
		if state.state != NOT_IN_USE {
			// Someone has taken this connection from us, so we need to try again
			continue
		}

		// Verify that no one has stolen the connection from us
		if conn.state.CompareAndSwap(state, borrowedState) {
			// We successfully borrowed this connection, so we can return it
			conn.timeUsed.set(monotonicNow())

			// If the connection has a setting, we need to set it
			if state.setting != borrowedState.setting {
				pool.Metrics.diffSetting.Add(1)

				err := conn.Conn.ApplySetting(ctx, borrowedState.setting)
				if err != nil {
					// If we couldn't apply the setting, we need to close the connection
					pool.closeConnection(conn)

					// Wait again
					continue
				}
			}

			pool.recordWait(start)

			return conn, nil
		}

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
