// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package drpcpool2

import (
	"context"
	"sync/atomic"

	"github.com/zeebo/errs"

	"storj.io/drpc"
)

// poolConn grabs a connection from the pool for every invoke/stream.
type poolConn struct {
	closed int32 // atomic where 1 == true
	pk     poolKey
	dial   Dialer
	pool   *Pool
}

// Close marks the poolConn as closed and will not allow future calls to Invoke or NewStream
// to proceed. It does not stop any ongoing calls to Invoke or NewStream.
func (c *poolConn) Close() (err error) {
	atomic.StoreInt32(&c.closed, 1)
	return nil
}

// Closed returns true if the poolConn is closed.
func (c *poolConn) isClosed() bool {
	return atomic.LoadInt32(&c.closed) != 0
}

func (c *poolConn) Closed() bool {
	return c.isClosed()
}

// Invoke acquires a connection from the pool, dialing if necessary, and issues the Invoke on that
// connection. The connection is replaced into the pool after the invoke finishes.
func (c *poolConn) Invoke(ctx context.Context, rpc string, enc drpc.Encoding, in, out drpc.Message) (err error) {
	if c.isClosed() {
		return errs.New("connection closed")
	}

	conn, err := c.pool.get(ctx, c.pk, c.dial)
	if err != nil {
		return err
	}
	defer c.pool.cache.Put(c.pk, conn)

	return conn.Invoke(ctx, rpc, enc, in, out)
}

// NewStream acquires a connection from the pool, dialing if necessary, and issues the NewStream on
// that connection. The connection is replaced into the pool after the stream is finished.
func (c *poolConn) NewStream(ctx context.Context, rpc string, enc drpc.Encoding) (_ drpc.Stream, err error) {
	if c.isClosed() {
		return nil, errs.New("connection closed")
	}

	conn, err := c.pool.get(ctx, c.pk, c.dial)
	if err != nil {
		return nil, err
	}

	stream, err := conn.NewStream(ctx, rpc, enc)
	if err != nil {
		return nil, err
	}

	// the stream's done channel is closed when we're sure no reads/writes are
	// coming in for that stream anymore. it has been fully terminated.
	go func() {
		<-stream.Context().Done()
		c.pool.cache.Put(c.pk, conn)
	}()

	return stream, nil
}

// Transport returns nil because it does not have a fixed transport.
func (c *poolConn) Transport() drpc.Transport {
	return nil
}
