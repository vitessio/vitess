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

/*
Package dbconnpool exposes a single DBConnection object
with wrapped access to a single DB connection, and a ConnectionPool
object to pool these DBConnections.
*/
package dbconnpool

import (
	"context"
	"net"
	"time"

	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/pools/smartconnpool"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/servenv"
)

// ConnectionPool re-exposes ResourcePool as a pool of
// PooledDBConnection objects.
type ConnectionPool struct {
	*smartconnpool.ConnPool[*DBConnection]

	name string
}

// usedNames is for preventing expvar from panicking. Tests
// create pool objects multiple time. If a name was previously
// used, expvar initialization is skipped.
// through non-test code.
var usedNames = make(map[string]bool)

// NewConnectionPool creates a new ConnectionPool. The name is used
// to publish stats only.
func NewConnectionPool(name string, stats *servenv.Exporter, capacity int, idleTimeout time.Duration, maxLifetime time.Duration, dnsResolutionFrequency time.Duration) *ConnectionPool {
	config := smartconnpool.Config[*DBConnection]{
		Capacity:        int64(capacity),
		IdleTimeout:     idleTimeout,
		MaxLifetime:     maxLifetime,
		RefreshInterval: dnsResolutionFrequency,
	}
	cp := &ConnectionPool{ConnPool: smartconnpool.NewPool(&config), name: name}
	if name == "" || usedNames[name] {
		return cp
	}
	usedNames[name] = true

	if stats == nil {
		// This is unnamed exported so it will use the stats functions directly when adding to the expvar.
		stats = servenv.NewExporter("", "")
	}
	cp.ConnPool.RegisterStats(stats, name)
	return cp
}

// Open must be called before starting to use the pool.
//
// For instance:
// pool := dbconnpool.NewConnectionPool("name", 10, 30*time.Second)
// pool.Open(info)
// ...
// conn, err := pool.Get()
// ...
func (cp *ConnectionPool) Open(info dbconfigs.Connector) {
	var refresh smartconnpool.RefreshCheck
	if net.ParseIP(info.Host()) == nil {
		refresh = netutil.DNSTracker(info.Host())
	}

	connect := func(ctx context.Context) (*DBConnection, error) {
		return NewDBConnection(ctx, info)
	}

	cp.ConnPool.Open(connect, refresh)
}

func (cp *ConnectionPool) Get(ctx context.Context) (*PooledDBConnection, error) {
	return cp.ConnPool.Get(ctx, nil)
}
