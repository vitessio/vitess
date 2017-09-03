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

package connpool

import (
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/pools"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// ErrConnPoolClosed is returned when the connection pool is closed.
var ErrConnPoolClosed = vterrors.New(vtrpcpb.Code_INTERNAL, "internal error: unexpected: conn pool is closed")

// usedNames is for preventing expvar from panicking. Tests
// create pool objects multiple time. If a name was previously
// used, expvar initialization is skipped.
// TODO(sougou): Find a way to still crash if this happened
// through non-test code.
var usedNames = make(map[string]bool)

// MySQLChecker defines the CheckMySQL interface that lower
// level objects can use to call back into TabletServer.
type MySQLChecker interface {
	CheckMySQL()
}

// Pool implements a custom connection pool for tabletserver.
// It's similar to dbconnpool.ConnPool, but the connections it creates
// come with built-in ability to kill in-flight queries. These connections
// also trigger a CheckMySQL call if we fail to connect to MySQL.
// Other than the connection type, ConnPool maintains an additional
// pool of dba connections that are used to kill connections.
type Pool struct {
	mu             sync.Mutex
	connections    *pools.ResourcePool
	capacity       int
	idleTimeout    time.Duration
	dbaPool        *dbconnpool.ConnectionPool
	checker        MySQLChecker
	appDebugParams *mysql.ConnParams
}

// New creates a new Pool. The name is used
// to publish stats only.
func New(
	name string,
	capacity int,
	idleTimeout time.Duration,
	checker MySQLChecker) *Pool {
	cp := &Pool{
		capacity:    capacity,
		idleTimeout: idleTimeout,
		dbaPool:     dbconnpool.NewConnectionPool("", 1, idleTimeout),
		checker:     checker,
	}
	if name == "" || usedNames[name] {
		return cp
	}
	usedNames[name] = true
	stats.Publish(name+"Capacity", stats.IntFunc(cp.Capacity))
	stats.Publish(name+"Available", stats.IntFunc(cp.Available))
	stats.Publish(name+"MaxCap", stats.IntFunc(cp.MaxCap))
	stats.Publish(name+"WaitCount", stats.IntFunc(cp.WaitCount))
	stats.Publish(name+"WaitTime", stats.DurationFunc(cp.WaitTime))
	stats.Publish(name+"IdleTimeout", stats.DurationFunc(cp.IdleTimeout))
	return cp
}

func (cp *Pool) pool() (p *pools.ResourcePool) {
	cp.mu.Lock()
	p = cp.connections
	cp.mu.Unlock()
	return p
}

// Open must be called before starting to use the pool.
func (cp *Pool) Open(appParams, dbaParams, appDebugParams *mysql.ConnParams) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	f := func() (pools.Resource, error) {
		return NewDBConn(cp, appParams)
	}
	cp.connections = pools.NewResourcePool(f, cp.capacity, cp.capacity, cp.idleTimeout)
	cp.appDebugParams = appDebugParams

	cp.dbaPool.Open(dbaParams, tabletenv.MySQLStats)
}

// Close will close the pool and wait for connections to be returned before
// exiting.
func (cp *Pool) Close() {
	p := cp.pool()
	if p == nil {
		return
	}
	// We should not hold the lock while calling Close
	// because it waits for connections to be returned.
	p.Close()
	cp.mu.Lock()
	cp.connections = nil
	cp.mu.Unlock()
	cp.dbaPool.Close()
}

// Get returns a connection.
// You must call Recycle on DBConn once done.
func (cp *Pool) Get(ctx context.Context) (*DBConn, error) {
	if cp.isCallerIDAppDebug(ctx) {
		return NewDBConnNoPool(cp.appDebugParams)
	}
	p := cp.pool()
	if p == nil {
		return nil, ErrConnPoolClosed
	}
	r, err := p.Get(ctx)
	if err != nil {
		return nil, err
	}
	return r.(*DBConn), nil
}

// Put puts a connection into the pool.
func (cp *Pool) Put(conn *DBConn) {
	p := cp.pool()
	if p == nil {
		panic(ErrConnPoolClosed)
	}
	if conn == nil {
		p.Put(nil)
	} else {
		p.Put(conn)
	}
}

// SetCapacity alters the size of the pool at runtime.
func (cp *Pool) SetCapacity(capacity int) (err error) {
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
func (cp *Pool) SetIdleTimeout(idleTimeout time.Duration) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.connections != nil {
		cp.connections.SetIdleTimeout(idleTimeout)
	}
	cp.dbaPool.SetIdleTimeout(idleTimeout)
	cp.idleTimeout = idleTimeout
}

// StatsJSON returns the pool stats as a JSON object.
func (cp *Pool) StatsJSON() string {
	p := cp.pool()
	if p == nil {
		return "{}"
	}
	return p.StatsJSON()
}

// Capacity returns the pool capacity.
func (cp *Pool) Capacity() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.Capacity()
}

// Available returns the number of available connections in the pool
func (cp *Pool) Available() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.Available()
}

// MaxCap returns the maximum size of the pool
func (cp *Pool) MaxCap() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.MaxCap()
}

// WaitCount returns how many clients are waiting for a connection
func (cp *Pool) WaitCount() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.WaitCount()
}

// WaitTime return the pool WaitTime.
func (cp *Pool) WaitTime() time.Duration {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.WaitTime()
}

// IdleTimeout returns the idle timeout for the pool.
func (cp *Pool) IdleTimeout() time.Duration {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.IdleTimeout()
}

func (cp *Pool) isCallerIDAppDebug(ctx context.Context) bool {
	callerID := callerid.ImmediateCallerIDFromContext(ctx)
	if cp.appDebugParams.Uname == "" {
		return false
	}
	return callerID != nil && callerID.Username == cp.appDebugParams.Uname
}
