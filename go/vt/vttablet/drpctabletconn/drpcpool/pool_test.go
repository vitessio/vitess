// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package drpcpool

import (
	"context"
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/drpc"
)

func TestGet(t *testing.T) {
	ctx := context.Background()
	calls := 0
	dial := func(ctx context.Context) (drpc.Conn, error) {
		calls++
		return emptyConn{}, nil
	}

	check := func(t *testing.T, conn *ConnectionPool, counts ...int) {
		calls = 0

		_ = conn.Invoke(ctx, "somerpc", nil, nil, nil)
		require.Equal(t, calls, counts[3])

		_ = conn.Invoke(ctx, "somerpc", nil, nil, nil)
		require.Equal(t, calls, counts[4])

		_ = conn.Invoke(ctx, "somerpc", nil, nil, nil)
		require.Equal(t, calls, counts[5])
	}

	t.Run("Cached", func(t *testing.T) {
		conn := OpenConnectionPool(dial)
		check(t, conn, 1, 1, 2, 2, 2, 2)
	})
}

// fakes for the test

type emptyConn struct{ drpc.Conn }

func (emptyConn) Close() error              { return nil }
func (emptyConn) Closed() <-chan struct{}   { return nil }
func (emptyConn) Transport() drpc.Transport { return emptyTransport{} }

func (emptyConn) Invoke(ctx context.Context, rpc string, enc drpc.Encoding, in, out drpc.Message) error {
	return nil
}

type emptyTransport struct{ drpc.Transport }

func (emptyTransport) ConnectionState() tls.ConnectionState {
	return tls.ConnectionState{}
}
