/*
Copyright 2017 Google Inc.

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

/*
Package dbconnpool exposes a single DBConnection object
with wrapped access to a single DB connection, and a ConnectionPool
object to pool these DBConnections.
*/
package dbconnpool

import (
	"errors"
	"net"
	"sync"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
)

var (
	// ErrConnPoolClosed is returned if the connection pool is closed.
	ErrConnPoolClosed = errors.New("connection pool is closed")
	// usedNames is for preventing expvar from panicking. Tests
	// create pool objects multiple time. If a name was previously
	// used, expvar initialization is skipped.
	// TODO(sougou): Find a way to still crash if this happened
	// through non-test code.
	usedNames = make(map[string]bool)
)

// ConnectionPool re-exposes ResourcePool as a pool of
// PooledDBConnection objects.
type ConnectionPool struct {
	mu                  sync.Mutex
	connections         *pools.ResourcePool
	capacity            int
	idleTimeout         time.Duration
	resolutionFrequency time.Duration

	// info and mysqlStats are set at Open() time
	info      *mysql.ConnParams
	addresses []net.IP

	ticker   *time.Ticker
	stop     chan struct{}
	wg       sync.WaitGroup
	hostIsIP bool

	mysqlStats *stats.Timings
}

// NewConnectionPool creates a new ConnectionPool. The name is used
// to publish stats only.
func NewConnectionPool(name string, capacity int, idleTimeout time.Duration, dnsResolutionFrequency time.Duration) *ConnectionPool {
	cp := &ConnectionPool{capacity: capacity, idleTimeout: idleTimeout, resolutionFrequency: dnsResolutionFrequency}
	if name == "" || usedNames[name] {
		return cp
	}
	usedNames[name] = true
	stats.NewGaugeFunc(name+"Capacity", "Connection pool capacity", cp.Capacity)
	stats.NewGaugeFunc(name+"Available", "Connection pool available", cp.Available)
	stats.NewGaugeFunc(name+"Active", "Connection pool active", cp.Active)
	stats.NewGaugeFunc(name+"InUse", "Connection pool in-use", cp.InUse)
	stats.NewGaugeFunc(name+"MaxCap", "Connection pool max cap", cp.MaxCap)
	stats.NewCounterFunc(name+"WaitCount", "Connection pool wait count", cp.WaitCount)
	stats.NewCounterDurationFunc(name+"WaitTime", "Connection pool wait time", cp.WaitTime)
	stats.NewGaugeDurationFunc(name+"IdleTimeout", "Connection pool idle timeout", cp.IdleTimeout)
	stats.NewGaugeFunc(name+"IdleClosed", "Connection pool idle closed", cp.IdleClosed)
	return cp
}

func (cp *ConnectionPool) pool() (p *pools.ResourcePool) {
	cp.mu.Lock()
	p = cp.connections
	cp.mu.Unlock()
	return p
}

func (cp *ConnectionPool) refreshdns() {
	cp.mu.Lock()
	host := cp.info.Host
	cp.mu.Unlock()

	addrs, err := net.LookupHost(host)
	if err != nil {
		log.Errorf("Error refreshing connection dns name: (%v)", err)
		return
	}
	naddr := make([]net.IP, len(addrs))
	for i, a := range addrs {
		naddr[i] = net.ParseIP(a)
	}
	cp.mu.Lock()
	cp.addresses = naddr
	cp.mu.Unlock()
}

func (cp *ConnectionPool) validAddress(addr net.IP) bool {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	// If we have no valid addresses we always return true
	if len(cp.addresses) == 0 {
		return true
	}

	// Check each address to see if the current RemoteAddr is in the set
	for _, a := range cp.addresses {
		if addr.Equal(a) {
			return true
		}
	}
	return false
}

// Open must be call before starting to use the pool.
//
// For instance:
// mysqlStats := stats.NewTimings("Mysql")
// pool := dbconnpool.NewConnectionPool("name", 10, 30*time.Second)
// pool.Open(info, mysqlStats)
// ...
// conn, err := pool.Get()
// ...
func (cp *ConnectionPool) Open(info *mysql.ConnParams, mysqlStats *stats.Timings) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.info = info
	cp.mysqlStats = mysqlStats
	cp.connections = pools.NewResourcePool(cp.connect, cp.capacity, cp.capacity, cp.idleTimeout)
	// Check if we need to resolve a hostname (The Host is not just an IP  address).
	if cp.resolutionFrequency > 0 && net.ParseIP(info.Host) == nil {
		cp.hostIsIP = true
		cp.ticker = time.NewTicker(cp.resolutionFrequency)
		cp.stop = make(chan struct{})
		cp.wg.Add(1)
		go func() {
			defer cp.wg.Done()
			for {
				select {
				case _ = <-cp.ticker.C:
					cp.refreshdns()
				case <-cp.stop:
					return
				}
			}

		}()
	}
}

// connect is used by the resource pool to create a new Resource.
func (cp *ConnectionPool) connect() (pools.Resource, error) {
	c, err := NewDBConnection(cp.info, cp.mysqlStats)
	if err != nil {
		return nil, err
	}
	return &PooledDBConnection{
		DBConnection: c,
		pool:         cp,
	}, nil
}

// Close will close the pool and wait for connections to be returned before
// exiting.
func (cp *ConnectionPool) Close() {
	p := cp.pool()
	if p == nil {
		return
	}
	// We should not hold the lock while calling Close
	// because it waits for connections to be returned.
	p.Close()
	cp.mu.Lock()
	cp.connections = nil
	cp.addresses = nil
	cp.hostIsIP = false
	if cp.ticker != nil {
		cp.ticker.Stop()
		close(cp.stop)
	}
	cp.mu.Unlock()
	cp.wg.Wait()
}

// Get returns a connection.
// You must call Recycle on the PooledDBConnection once done.
func (cp *ConnectionPool) Get(ctx context.Context) (*PooledDBConnection, error) {
	p := cp.pool()
	if p == nil {
		return nil, ErrConnPoolClosed
	}
	r, err := p.Get(ctx)
	if err != nil {
		return nil, err
	}

	// Check that the RemoteAddr is still a valid Address
	if cp.resolutionFrequency > 0 &&
		cp.hostIsIP &&
		!cp.validAddress(net.ParseIP(r.(*PooledDBConnection).RemoteAddr().String())) {
		err := r.(*PooledDBConnection).Reconnect()
		if err != nil {
			p.Put(r)
			return nil, err
		}
	}
	return r.(*PooledDBConnection), nil
}

// Put puts a connection into the pool.
func (cp *ConnectionPool) Put(conn *PooledDBConnection) {
	p := cp.pool()
	if p == nil {
		panic(ErrConnPoolClosed)
	}
	if conn == nil {
		// conn has a type, if we just Put(conn), we end up
		// putting an interface with a nil value, that is not
		// equal to a nil value. So just put a plain nil.
		p.Put(nil)
		return
	}
	p.Put(conn)
}

// SetCapacity alters the size of the pool at runtime.
func (cp *ConnectionPool) SetCapacity(capacity int) (err error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.connections != nil {
		err = cp.connections.SetCapacity(capacity)
		if err != nil {
			return err
		}
	}
	cp.capacity = capacity
	return nil
}

// SetIdleTimeout sets the idleTimeout on the pool.
func (cp *ConnectionPool) SetIdleTimeout(idleTimeout time.Duration) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.connections != nil {
		cp.connections.SetIdleTimeout(idleTimeout)
	}
	cp.idleTimeout = idleTimeout
}

// StatsJSON returns the pool stats as a JSOn object.
func (cp *ConnectionPool) StatsJSON() string {
	p := cp.pool()
	if p == nil {
		return "{}"
	}
	return p.StatsJSON()
}

// Capacity returns the pool capacity.
func (cp *ConnectionPool) Capacity() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.Capacity()
}

// Available returns the number of available connections in the pool
func (cp *ConnectionPool) Available() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.Available()
}

// Active returns the number of active connections in the pool
func (cp *ConnectionPool) Active() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.Active()
}

// InUse returns the number of in-use connections in the pool
func (cp *ConnectionPool) InUse() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.InUse()
}

// MaxCap returns the maximum size of the pool
func (cp *ConnectionPool) MaxCap() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.MaxCap()
}

// WaitCount returns how many clients are waiting for a connection
func (cp *ConnectionPool) WaitCount() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.WaitCount()
}

// WaitTime return the pool WaitTime.
func (cp *ConnectionPool) WaitTime() time.Duration {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.WaitTime()
}

// IdleTimeout returns the idle timeout for the pool.
func (cp *ConnectionPool) IdleTimeout() time.Duration {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.IdleTimeout()
}

// IdleClosed returns the number of closed connections for the pool.
func (cp *ConnectionPool) IdleClosed() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.IdleClosed()
}
