// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"time"

	"github.com/youtube/vitess/go/pools"
)

// InfiniPool is a pool for TabletConn. This pool recycles
// the specified number of connections. If the demand exceeds
// the pool size, InfiniPool creates additional connections that
// it discards when recycled.
type InfiniPool struct {
	connections *pools.ResourcePool
	factory     pools.Factory
}

// NewInfiniPool creates an InfiniPool. factory is the function the pool
// will use to create new connections. capacity specifies the number of
// connections that will be used for recycling. Any connection that was
// not used for idleTimeout will be discarded in favor of a new one.
func NewInfiniPool(factory pools.Factory, capacity int, idleTimeout time.Duration) *InfiniPool {
	return &InfiniPool{
		connections: pools.NewResourcePool(factory, capacity, capacity, idleTimeout),
		factory:     factory,
	}
}

// Get returns a connection from the pool. If no connection
// is available, it returns a new one. Every PooledConnection
// must be either Recycled or Closed.
func (ifp *InfiniPool) Get() (*PooledConn, error) {
	conn, err := ifp.connections.TryGet()
	if err != nil {
		return nil, err
	}
	if conn != nil {
		return &PooledConn{
			TabletConn: conn.(TabletConn),
			pool:       ifp,
		}, nil
	}
	conn, err = ifp.factory()
	if err != nil {
		return nil, err
	}
	// We don't set the pool to ifp because this
	// connection should not be returned to the pool.
	return &PooledConn{
		TabletConn: conn.(TabletConn),
	}, nil
}

// PooledConn re-exposes TabletConn as recyclable connection.
type PooledConn struct {
	TabletConn
	pool *InfiniPool
}

// Recycle returns the connection to the pool. If the connection
// should not be recycled, it's closed and discarded.
func (pc *PooledConn) Recycle() {
	if pc.pool == nil {
		pc.TabletConn.Close()
		return
	}
	pc.pool.connections.Put(pc.TabletConn)
}

// Close closes the connection. If the connection is recyclable,
// an empty token is returned to the pool for future replacement.
func (pc *PooledConn) Close() {
	pc.TabletConn.Close()
	if pc.pool != nil {
		pc.pool.connections.Put(nil)
	}
}
