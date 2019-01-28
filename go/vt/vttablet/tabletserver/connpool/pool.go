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

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// ErrConnPoolClosed is returned when the connection pool is closed.
var ErrConnPoolClosed = vterrors.New(vtrpcpb.Code_INTERNAL, "internal error: unexpected: conn pool is closed")

// TabletService defines a subset API of TabletServer so that lower
// level objects can call back into it.
type TabletService interface {
	CheckMySQL()
	InstanceName() string
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
	tsv            TabletService
	appDebugParams *mysql.ConnParams
}

// New creates a new Pool. The name is used
// to publish stats only.
func New(
	name string,
	capacity int,
	idleTimeout time.Duration,
	tsv TabletService) *Pool {
	cp := &Pool{
		capacity:    capacity,
		idleTimeout: idleTimeout,
		dbaPool:     dbconnpool.NewConnectionPool("", 1, idleTimeout),
		tsv:         tsv,
	}
	instanceName := tsv.InstanceName()
	servenv.NewGaugeFunc(instanceName, name+"Capacity", "Tablet server conn pool capacity", cp.Capacity)
	servenv.NewGaugeFunc(instanceName, name+"Available", "Tablet server conn pool available", cp.Available)
	servenv.NewGaugeFunc(instanceName, name+"Active", "Tablet server conn pool active", cp.Active)
	servenv.NewGaugeFunc(instanceName, name+"InUse", "Tablet server conn pool in use", cp.InUse)
	servenv.NewGaugeFunc(instanceName, name+"MaxCap", "Tablet server conn pool max cap", cp.MaxCap)
	servenv.NewCounterFunc(instanceName, name+"WaitCount", "Tablet server conn pool wait count", cp.WaitCount)
	servenv.NewCounterDurationFunc(instanceName, name+"WaitTime", "Tablet server wait time", cp.WaitTime)
	servenv.NewGaugeDurationFunc(instanceName, name+"IdleTimeout", "Tablet server idle timeout", cp.IdleTimeout)
	servenv.NewCounterFunc(instanceName, name+"IdleClosed", "Tablet server conn pool idle closed", cp.IdleClosed)
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
		return NewDBConnNoPool(cp.appDebugParams, cp.dbaPool)
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

// Active returns the number of active connections in the pool
func (cp *Pool) Active() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.Active()
}

// InUse returns the number of in-use connections in the pool
func (cp *Pool) InUse() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.InUse()
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

// IdleClosed returns the number of closed connections for the pool.
func (cp *Pool) IdleClosed() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.IdleClosed()
}

func (cp *Pool) isCallerIDAppDebug(ctx context.Context) bool {
	if cp.appDebugParams == nil || cp.appDebugParams.Uname == "" {
		return false
	}
	callerID := callerid.ImmediateCallerIDFromContext(ctx)
	return callerID != nil && callerID.Username == cp.appDebugParams.Uname
}
