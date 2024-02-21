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
	"encoding/json"
	"net"
	"strings"
	"time"

	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/pools/smartconnpool"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/dbconnpool"
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

type PooledConn = smartconnpool.Pooled[*Conn]

// Pool implements a custom connection pool for tabletserver.
// It's similar to dbconnpool.ConnPool, but the connections it creates
// come with built-in ability to kill in-flight queries. These connections
// also trigger a CheckMySQL call if we fail to connect to MySQL.
// Other than the connection type, ConnPool maintains an additional
// pool of dba connections that are used to kill connections.
type Pool struct {
	*smartconnpool.ConnPool[*Conn]
	dbaPool *dbconnpool.ConnectionPool

	timeout time.Duration
	env     tabletenv.Env

	appDebugParams dbconfigs.Connector
	getConnTime    *servenv.TimingsWrapper
}

// NewPool creates a new Pool. The name is used
// to publish stats only.
func NewPool(env tabletenv.Env, name string, cfg tabletenv.ConnPoolConfig) *Pool {
	cp := &Pool{
		timeout: cfg.Timeout,
		env:     env,
	}

	config := smartconnpool.Config[*Conn]{
		Capacity:        int64(cfg.Size),
		IdleTimeout:     cfg.IdleTimeout,
		MaxLifetime:     cfg.MaxLifetime,
		RefreshInterval: mysqlctl.PoolDynamicHostnameResolution,
	}

	if name != "" {
		config.LogWait = func(start time.Time) {
			env.Stats().WaitTimings.Record(name+"ResourceWaitTime", start)
		}

		cp.getConnTime = env.Exporter().NewTimings(name+"GetConnTime", "Tracks the amount of time it takes to get a connection", "Settings")
	}

	cp.ConnPool = smartconnpool.NewPool(&config)
	cp.ConnPool.RegisterStats(env.Exporter(), name)

	cp.dbaPool = dbconnpool.NewConnectionPool("", env.Exporter(), 1, config.IdleTimeout, config.MaxLifetime, 0)

	return cp
}

// Open must be called before starting to use the pool.
func (cp *Pool) Open(appParams, dbaParams, appDebugParams dbconfigs.Connector) {
	cp.appDebugParams = appDebugParams

	var refresh smartconnpool.RefreshCheck
	if net.ParseIP(appParams.Host()) == nil {
		refresh = netutil.DNSTracker(appParams.Host())
	}

	connect := func(ctx context.Context) (*Conn, error) {
		return newPooledConn(ctx, cp, appParams)
	}

	cp.ConnPool.Open(connect, refresh)
	cp.dbaPool.Open(dbaParams)
}

// Close will close the pool and wait for connections to be returned before
// exiting.
func (cp *Pool) Close() {
	cp.ConnPool.Close()
	cp.dbaPool.Close()
}

// Get returns a connection.
// You must call Recycle on DBConn once done.
func (cp *Pool) Get(ctx context.Context, setting *smartconnpool.Setting) (*PooledConn, error) {
	span, ctx := trace.NewSpan(ctx, "Pool.Get")
	defer span.Finish()

	if cp.isCallerIDAppDebug(ctx) {
		conn, err := NewConn(ctx, cp.appDebugParams, cp.dbaPool, setting, cp.env)
		if err != nil {
			return nil, err
		}
		return &smartconnpool.Pooled[*Conn]{Conn: conn}, nil
	}
	span.Annotate("capacity", cp.Capacity())
	span.Annotate("in_use", cp.InUse())
	span.Annotate("available", cp.Available())
	span.Annotate("active", cp.Active())

	if cp.timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cp.timeout)
		defer cancel()
	}

	start := time.Now()
	conn, err := cp.ConnPool.Get(ctx, setting)
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
	return conn, nil
}

// SetIdleTimeout sets the idleTimeout on the pool.
func (cp *Pool) SetIdleTimeout(idleTimeout time.Duration) {
	cp.ConnPool.SetIdleTimeout(idleTimeout)
	cp.dbaPool.SetIdleTimeout(idleTimeout)
}

// StatsJSON returns the pool stats as a JSON object.
func (cp *Pool) StatsJSON() string {
	if !cp.ConnPool.IsOpen() {
		return "{}"
	}

	var buf strings.Builder
	enc := json.NewEncoder(&buf)
	_ = enc.Encode(cp.ConnPool.StatsJSON())
	return buf.String()
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
