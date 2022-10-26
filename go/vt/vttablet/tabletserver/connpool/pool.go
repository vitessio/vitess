/*
Copyright 2019 The Vitess Authors.

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
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// ErrConnPoolClosed is returned when the connection pool is closed.
var ErrConnPoolClosed = vterrors.New(vtrpcpb.Code_INTERNAL, "internal error: unexpected: conn pool is closed")

const (
	getWithoutS = "GetWithoutSettings"
	getWithS    = "GetWithSettings"
)

// Pool implements a custom connection pool for tabletserver.
// It's similar to dbconnpool.ConnPool, but the connections it creates
// come with built-in ability to kill in-flight queries. These connections
// also trigger a CheckMySQL call if we fail to connect to MySQL.
// Other than the connection type, ConnPool maintains an additional
// pool of dba connections that are used to kill connections.
type Pool struct {
	env                tabletenv.Env
	name               string
	mu                 sync.Mutex
	connections        pools.IResourcePool
	capacity           int
	prefillParallelism int
	timeout            time.Duration
	idleTimeout        time.Duration
	waiterCap          int64
	waiterCount        sync2.AtomicInt64
	waiterQueueFull    sync2.AtomicInt64
	dbaPool            *dbconnpool.ConnectionPool
	appDebugParams     dbconfigs.Connector
	getConnTime        *servenv.TimingsWrapper
}

// NewPool creates a new Pool. The name is used
// to publish stats only.
func NewPool(env tabletenv.Env, name string, cfg tabletenv.ConnPoolConfig) *Pool {
	idleTimeout := cfg.IdleTimeoutSeconds.Get()
	cp := &Pool{
		env:                env,
		name:               name,
		capacity:           cfg.Size,
		prefillParallelism: cfg.PrefillParallelism,
		timeout:            cfg.TimeoutSeconds.Get(),
		idleTimeout:        idleTimeout,
		waiterCap:          int64(cfg.MaxWaiters),
		dbaPool:            dbconnpool.NewConnectionPool("", 1, idleTimeout, 0),
	}
	if name == "" {
		return cp
	}
	env.Exporter().NewGaugeFunc(name+"Capacity", "Tablet server conn pool capacity", cp.Capacity)
	env.Exporter().NewGaugeFunc(name+"Available", "Tablet server conn pool available", cp.Available)
	env.Exporter().NewGaugeFunc(name+"Active", "Tablet server conn pool active", cp.Active)
	env.Exporter().NewGaugeFunc(name+"InUse", "Tablet server conn pool in use", cp.InUse)
	env.Exporter().NewGaugeFunc(name+"MaxCap", "Tablet server conn pool max cap", cp.MaxCap)
	env.Exporter().NewCounterFunc(name+"WaitCount", "Tablet server conn pool wait count", cp.WaitCount)
	env.Exporter().NewCounterDurationFunc(name+"WaitTime", "Tablet server wait time", cp.WaitTime)
	env.Exporter().NewGaugeDurationFunc(name+"IdleTimeout", "Tablet server idle timeout", cp.IdleTimeout)
	env.Exporter().NewCounterFunc(name+"IdleClosed", "Tablet server conn pool idle closed", cp.IdleClosed)
	env.Exporter().NewCounterFunc(name+"Exhausted", "Number of times pool had zero available slots", cp.Exhausted)
	env.Exporter().NewCounterFunc(name+"WaiterQueueFull", "Number of times the waiter queue was full", cp.waiterQueueFull.Get)
	env.Exporter().NewCounterFunc(name+"Get", "Tablet server conn pool get count", cp.GetCount)
	env.Exporter().NewCounterFunc(name+"GetSetting", "Tablet server conn pool get with setting count", cp.GetSettingCount)
	env.Exporter().NewCounterFunc(name+"DiffSetting", "Number of times pool applied different setting", cp.DiffSettingCount)
	env.Exporter().NewCounterFunc(name+"ResetSetting", "Number of times pool reset the setting", cp.ResetSettingCount)
	cp.getConnTime = env.Exporter().NewTimings(name+"GetConnTime", "Tracks the amount of time it takes to get a connection", "Settings")

	return cp
}

func (cp *Pool) pool() (p pools.IResourcePool) {
	cp.mu.Lock()
	p = cp.connections
	cp.mu.Unlock()
	return p
}

// Open must be called before starting to use the pool.
func (cp *Pool) Open(appParams, dbaParams, appDebugParams dbconfigs.Connector) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.prefillParallelism != 0 {
		log.Infof("Opening pool: '%s'", cp.name)
		defer log.Infof("Done opening pool: '%s'", cp.name)
	}

	f := func(ctx context.Context) (pools.Resource, error) {
		return NewDBConn(ctx, cp, appParams)
	}

	var refreshCheck pools.RefreshCheck
	if net.ParseIP(appParams.Host()) == nil {
		refreshCheck = netutil.DNSTracker(appParams.Host())
	}

	cp.connections = pools.NewResourcePool(f, cp.capacity, cp.capacity, cp.idleTimeout, cp.getLogWaitCallback(), refreshCheck, mysqlctl.PoolDynamicHostnameResolution)
	cp.appDebugParams = appDebugParams

	cp.dbaPool.Open(dbaParams)
}

func (cp *Pool) getLogWaitCallback() func(time.Time) {
	if cp.name == "" {
		return func(start time.Time) {} // no op
	}
	return func(start time.Time) {
		cp.env.Stats().WaitTimings.Record(cp.name+"ResourceWaitTime", start)
	}
}

// Close will close the pool and wait for connections to be returned before
// exiting.
func (cp *Pool) Close() {
	log.Infof("connpool - started execution of Close")
	p := cp.pool()
	log.Infof("connpool - found the pool")
	if p == nil {
		log.Infof("connpool - pool is empty")
		return
	}
	// We should not hold the lock while calling Close
	// because it waits for connections to be returned.
	log.Infof("connpool - calling close on the pool")
	p.Close()
	log.Infof("connpool - acquiring lock")
	cp.mu.Lock()
	log.Infof("connpool - acquired lock")
	cp.connections = nil
	cp.mu.Unlock()
	log.Infof("connpool - closing dbaPool")
	cp.dbaPool.Close()
	log.Infof("connpool - finished execution of Close")
}

// Get returns a connection.
// You must call Recycle on DBConn once done.
func (cp *Pool) Get(ctx context.Context, setting *pools.Setting) (*DBConn, error) {
	span, ctx := trace.NewSpan(ctx, "Pool.Get")
	defer span.Finish()

	if cp.waiterCap > 0 {
		waiterCount := cp.waiterCount.Add(1)
		defer cp.waiterCount.Add(-1)
		if waiterCount > cp.waiterCap {
			cp.waiterQueueFull.Add(1)
			return nil, vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "pool %s waiter count exceeded", cp.name)
		}
	}

	if cp.isCallerIDAppDebug(ctx) {
		return NewDBConnNoPool(ctx, cp.appDebugParams, cp.dbaPool, setting)
	}
	p := cp.pool()
	if p == nil {
		return nil, ErrConnPoolClosed
	}
	span.Annotate("capacity", p.Capacity())
	span.Annotate("in_use", p.InUse())
	span.Annotate("available", p.Available())
	span.Annotate("active", p.Active())

	if cp.timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cp.timeout)
		defer cancel()
	}

	start := time.Now()
	r, err := p.Get(ctx, setting)
	if err != nil {
		return nil, err
	}
	if cp.getConnTime != nil {
		if setting == nil {
			cp.getConnTime.Record(getWithoutS, start)
		} else {
			cp.getConnTime.Record(getWithS, start)
		}
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
	res := p.StatsJSON()
	closingBraceIndex := strings.LastIndex(res, "}")
	if closingBraceIndex == -1 { // unexpected...
		return res
	}
	return fmt.Sprintf(`%s, "WaiterQueueFull": %v}`, res[:closingBraceIndex], cp.waiterQueueFull.Get())
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

// Exhausted returns the number of times available went to zero for the pool.
func (cp *Pool) Exhausted() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.Exhausted()
}

// GetCount returns the number of times get was called
func (cp *Pool) GetCount() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.GetCount()
}

// GetSettingCount returns the number of times getWithSettings was called
func (cp *Pool) GetSettingCount() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.GetSettingCount()
}

// DiffSettingCount returns the number of times different settings were applied on the resource.
func (cp *Pool) DiffSettingCount() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.DiffSettingCount()
}

// ResetSettingCount returns the number of times settings were reset on the resource.
func (cp *Pool) ResetSettingCount() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.ResetSettingCount()
}

func (cp *Pool) isCallerIDAppDebug(ctx context.Context) bool {
	params, err := cp.appDebugParams.MysqlParams()
	if err != nil {
		return false
	}
	if params == nil || params.Uname == "" {
		return false
	}
	callerID := callerid.ImmediateCallerIDFromContext(ctx)
	return callerID != nil && callerID.Username == params.Uname
}
