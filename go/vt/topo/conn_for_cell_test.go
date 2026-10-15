/*
Copyright 2026 The Vitess Authors.

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

package topo_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// TestConnForCellCachesCellInfo verifies that ConnForCell reuses a cell's connection without reading the
// cell's CellInfo from the global topo while the CellInfo is within its cache TTL, re-reads it once the TTL
// has passed and counts the TTL from that read, and picks up this process's own UpdateCellInfoFields
// immediately.
func TestConnForCellCachesCellInfo(t *testing.T) {
	ctx := t.Context()
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	t.Cleanup(ts.Close)
	gets := func() int64 { return factory.GetCallStats().Counts()["Get"] }

	conn, err := ts.ConnForCell(ctx, "zone1")
	require.NoError(t, err)

	t.Run("within the TTL", func(t *testing.T) {
		before := gets()
		for range 3 {
			got, err := ts.ConnForCell(ctx, "zone1")
			require.NoError(t, err)
			assert.Same(t, conn, got)
		}
		assert.Equal(t, before, gets(), "CellInfo was read from the global topo")
	})

	t.Run("after the TTL", func(t *testing.T) {
		ts.SetCellInfoCacheTTL(0)
		t.Cleanup(func() { ts.SetCellInfoCacheTTL(time.Hour) })
		before := gets()
		for range 3 {
			got, err := ts.ConnForCell(ctx, "zone1")
			require.NoError(t, err)
			assert.Same(t, conn, got)
		}
		assert.Equal(t, before+3, gets(), "CellInfo was not re-read on every call")
	})

	t.Run("the TTL counts from the latest read", func(t *testing.T) {
		ts.SetCellInfoCacheTTL(time.Second)
		t.Cleanup(func() { ts.SetCellInfoCacheTTL(time.Hour) })
		// Wait for the TTL to pass and the CellInfo to be read again.
		assert.Eventually(t, func() bool {
			before := gets()
			_, err := ts.ConnForCell(ctx, "zone1")
			return err == nil && gets() > before
		}, 30*time.Second, 10*time.Millisecond)
		before := gets()
		got, err := ts.ConnForCell(ctx, "zone1")
		require.NoError(t, err)
		assert.Same(t, conn, got)
		assert.Equal(t, before, gets(), "CellInfo was read again within the TTL of the latest read")
	})

	t.Run("re-pointed in this process", func(t *testing.T) {
		ts.SetCellInfoCacheTTL(time.Hour)
		require.NoError(t, ts.UpdateCellInfoFields(ctx, "zone1", func(ci *topodatapb.CellInfo) error {
			ci.Root = "/zone1-moved"
			return nil
		}))
		got, err := ts.ConnForCell(ctx, "zone1")
		require.NoError(t, err)
		assert.NotSame(t, conn, got, "the connection to the old root was reused")
	})
}

// pausingFactory wraps a memorytopo factory. Once armed, the next global read of a CellInfo reads it and then
// waits for release, or for its ctx to be done like a real topo read, before returning.
type pausingFactory struct {
	*memorytopo.Factory
	armed   chan struct{}
	paused  chan struct{}
	release chan struct{}
	used    atomic.Bool
	// returned is closed when the paused read returns.
	returned chan struct{}
	// failRoot makes connecting to a cell at this root fail.
	failRoot string
}

func newPausingFactory(memFactory *memorytopo.Factory) *pausingFactory {
	return &pausingFactory{
		Factory:  memFactory,
		armed:    make(chan struct{}),
		paused:   make(chan struct{}),
		release:  make(chan struct{}),
		returned: make(chan struct{}),
	}
}

func (f *pausingFactory) Create(cell, serverAddr, root string) (topo.Conn, error) {
	if f.failRoot != "" && root == f.failRoot {
		return nil, errors.New("topo server unreachable")
	}
	conn, err := f.Factory.Create(cell, serverAddr, root)
	if err != nil || cell != topo.GlobalCell {
		return conn, err
	}
	return &pausingConn{Conn: conn, factory: f}, nil
}

type pausingConn struct {
	topo.Conn
	factory *pausingFactory
}

func (c *pausingConn) Get(ctx context.Context, filePath string) ([]byte, topo.Version, error) {
	contents, version, err := c.Conn.Get(ctx, filePath)
	if strings.HasPrefix(filePath, topo.CellsPath+"/") {
		select {
		case <-c.factory.armed:
			if c.factory.used.CompareAndSwap(false, true) {
				defer close(c.factory.returned)
				close(c.factory.paused)
				select {
				case <-c.factory.release:
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				}
			}
		default:
		}
	}
	return contents, version, err
}

// TestConnForCellExpiryDuringRead verifies that when this process changes a cell's CellInfo while a
// ConnForCell is reading the old one, that ConnForCell does not mark the old CellInfo as fresh, so the
// next ConnForCell reads the change instead of reusing the old connection for another TTL.
func TestConnForCellExpiryDuringRead(t *testing.T) {
	ctx := t.Context()
	_, memFactory := memorytopo.NewServerAndFactory(ctx, "zone1")
	factory := newPausingFactory(memFactory)
	ts, err := topo.NewWithFactory(factory, "", "")
	require.NoError(t, err)
	t.Cleanup(ts.Close)
	ts.SetCellInfoCacheTTL(time.Hour)

	conn, err := ts.ConnForCell(ctx, "zone1")
	require.NoError(t, err)

	// Make the next ConnForCell read CellInfo, and pause that read after it has the old CellInfo.
	ts.ExpireCellInfo("zone1")
	close(factory.armed)
	done := make(chan topo.Conn)
	go func() {
		got, err := ts.ConnForCell(ctx, "zone1")
		assert.NoError(t, err)
		done <- got
	}()
	<-factory.paused

	require.NoError(t, ts.UpdateCellInfoFields(ctx, "zone1", func(ci *topodatapb.CellInfo) error {
		ci.Root = "/zone1-moved"
		return nil
	}))
	close(factory.release)
	assert.Same(t, conn, <-done, "the paused read returned the CellInfo it read")

	got, err := ts.ConnForCell(ctx, "zone1")
	require.NoError(t, err)
	assert.NotSame(t, conn, got, "the connection to the old root was reused after the read raced the change")
}

// TestDeleteCellInfoFailureKeepsCache verifies that a DeleteCellInfo that fails to delete the CellInfo
// returns the error and leaves the cell's cached connection in place.
func TestDeleteCellInfoFailureKeepsCache(t *testing.T) {
	ctx := t.Context()
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	t.Cleanup(ts.Close)

	conn, err := ts.ConnForCell(ctx, "zone1")
	require.NoError(t, err)

	errTopo := errors.New("topo unavailable")
	factory.SetError(errTopo)
	require.ErrorIs(t, ts.DeleteCellInfo(ctx, "zone1", true /*force*/), errTopo)
	factory.SetError(nil)

	got, err := ts.ConnForCell(ctx, "zone1")
	require.NoError(t, err)
	assert.Same(t, conn, got)
}

// TestConnForCellOlderReadDoesNotReplaceNewer verifies that when a ConnForCell read of the old CellInfo is
// still in flight after another call has read the cell's CellInfo again and applied it, the older read never
// replaces what the newer one applied: it returns the newer read's connection, or, if connecting to what the
// newer read found failed, reads again rather than connect to a topo server that may have been superseded.
// That holds whether or not this process expired the cell in between, which expiry leaves the order of reads
// in place for.
func TestConnForCellOlderReadDoesNotReplaceNewer(t *testing.T) {
	const unreachableRoot = "/zone1-unreachable"
	tcases := []struct {
		name string
		// newRoot is where another process re-points the cell before the newer read; empty leaves it.
		newRoot string
		// updateAgain has this process change the cell again before the older read completes.
		updateAgain bool
	}{
		{name: "not re-pointed"},
		{name: "re-pointed", newRoot: "/zone1-moved"},
		{name: "re-pointed, then changed again here", newRoot: "/zone1-moved", updateAgain: true},
		{name: "re-pointed to an unreachable topo server", newRoot: unreachableRoot},
	}
	for _, tcase := range tcases {
		for _, expireBetween := range []bool{false, true} {
			name := tcase.name
			if expireBetween {
				name += ", expired here in between"
			}
			t.Run(name, func(t *testing.T) {
				ctx := t.Context()
				_, memFactory := memorytopo.NewServerAndFactory(ctx, "zone1")
				factory := newPausingFactory(memFactory)
				factory.failRoot = unreachableRoot
				ts, err := topo.NewWithFactory(factory, "", "")
				require.NoError(t, err)
				t.Cleanup(ts.Close)
				other, err := topo.NewWithFactory(memFactory, "", "")
				require.NoError(t, err)
				t.Cleanup(other.Close)
				ts.SetCellInfoCacheTTL(time.Hour)

				conn, err := ts.ConnForCell(ctx, "zone1")
				require.NoError(t, err)

				// The older read gets the old CellInfo and pauses.
				ts.ExpireCellInfo("zone1")
				close(factory.armed)
				type result struct {
					conn topo.Conn
					err  error
				}
				older := make(chan result)
				go func() {
					got, err := ts.ConnForCell(ctx, "zone1")
					older <- result{got, err}
				}()
				<-factory.paused

				// The newer read gets the current CellInfo and is applied.
				if tcase.newRoot != "" {
					require.NoError(t, other.UpdateCellInfoFields(ctx, "zone1", func(ci *topodatapb.CellInfo) error {
						ci.Root = tcase.newRoot
						return nil
					}))
				}
				if expireBetween {
					ts.ExpireCellInfo("zone1")
				}
				unreachable := tcase.newRoot == unreachableRoot
				newer, err := ts.ConnForCell(ctx, "zone1")
				if unreachable {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
				if tcase.newRoot == "" {
					assert.Same(t, conn, newer)
				}
				if tcase.updateAgain {
					require.NoError(t, ts.UpdateCellInfoFields(ctx, "zone1", func(ci *topodatapb.CellInfo) error {
						ci.Root = "/zone1-moved-again"
						return nil
					}))
				}

				close(factory.release)
				res := <-older
				if unreachable {
					require.Error(t, res.err, "the older read connected to the topo server the cell was re-pointed from")
				} else {
					require.NoError(t, res.err)
					assert.Same(t, newer, res.conn, "the older read replaced the connection built from the newer one")
				}

				got, err := ts.ConnForCell(ctx, "zone1")
				switch {
				case unreachable:
					require.Error(t, err, "a call after the failed connection reused a connection to the old topo server")
				case tcase.updateAgain:
					require.NoError(t, err)
					assert.NotSame(t, newer, got, "the expired entry was reused instead of being read again")
				default:
					require.NoError(t, err)
					assert.Same(t, newer, got, "the cached connection does not point at the cell's current topo server")
				}
			})
		}
	}
}

// TestDeleteCellInfoRereadsCellInfo verifies that DeleteCellInfo reads the cell's CellInfo before it checks
// the cell for serving keyspaces, rather than checking through a cached connection that another process's
// re-point may have left pointing at the old topo server.
func TestDeleteCellInfoRereadsCellInfo(t *testing.T) {
	ctx := t.Context()
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	t.Cleanup(ts.Close)
	gets := func() int64 { return factory.GetCallStats().Counts()["Get"] }

	_, err := ts.ConnForCell(ctx, "zone1")
	require.NoError(t, err)
	ts.SetCellInfoCacheTTL(time.Hour)

	before := gets()
	require.NoError(t, ts.DeleteCellInfo(ctx, "zone1", false /*force*/))
	assert.Equal(t, before+1, gets(), "the safety check did not re-read CellInfo")
}

// TestConnForCellAfterRecreate verifies that after another process deletes a cell and this process creates
// a cell of the same name at a different root, ConnForCell does not reuse the connection it cached for the
// old cell.
func TestConnForCellAfterRecreate(t *testing.T) {
	ctx := t.Context()
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	t.Cleanup(ts.Close)
	other, err := topo.NewWithFactory(factory, "", "")
	require.NoError(t, err)
	t.Cleanup(other.Close)
	ts.SetCellInfoCacheTTL(time.Hour)

	conn, err := ts.ConnForCell(ctx, "zone1")
	require.NoError(t, err)

	// Creating a cell that already exists fails and leaves the cached connection in place.
	require.True(t, topo.IsErrType(ts.CreateCellInfo(ctx, "zone1", &topodatapb.CellInfo{}), topo.NodeExists))
	got, err := ts.ConnForCell(ctx, "zone1")
	require.NoError(t, err)
	require.Same(t, conn, got)

	require.NoError(t, other.DeleteCellInfo(ctx, "zone1", true /*force*/))
	require.NoError(t, ts.CreateCellInfo(ctx, "zone1", &topodatapb.CellInfo{Root: "/zone1-recreated"}))

	got, err = ts.ConnForCell(ctx, "zone1")
	require.NoError(t, err)
	assert.NotSame(t, conn, got, "the connection cached for the deleted cell was reused")
}

// TestConnForCellReadEndsWithCaller verifies that the CellInfo read a ConnForCell does has ended by the time
// that ConnForCell returns, even when it returns because its ctx was canceled, so that no read is left running
// against a Server that its owner may close next.
func TestConnForCellReadEndsWithCaller(t *testing.T) {
	ctx := t.Context()
	_, memFactory := memorytopo.NewServerAndFactory(ctx, "zone1")
	factory := newPausingFactory(memFactory)
	ts, err := topo.NewWithFactory(factory, "", "")
	require.NoError(t, err)
	t.Cleanup(ts.Close)

	ts.ExpireCellInfo("zone1")
	close(factory.armed)
	callCtx, cancel := context.WithCancel(ctx)
	done := make(chan error)
	go func() {
		_, err := ts.ConnForCell(callCtx, "zone1")
		done <- err
	}()
	<-factory.paused
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	select {
	case <-factory.returned:
	default:
		assert.Fail(t, "the CellInfo read was still running after ConnForCell returned")
	}
	close(factory.release)
}

// TestConnForCellConcurrentWithReadOnly verifies that refreshing cell connections in ConnForCell is safe
// alongside SetReadOnly and IsReadOnly, which srvtopo's read-only server calls on every GetTopoServer, and
// alongside operations on a connection that check its read-only state. Run with -race.
func TestConnForCellConcurrentWithReadOnly(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "zone1")
	t.Cleanup(ts.Close)
	_, err := ts.ConnForCell(ctx, "zone1")
	require.NoError(t, err)
	// Every call refreshes the cell's cached connection.
	ts.SetCellInfoCacheTTL(0)

	const iterations = 500
	var wg sync.WaitGroup
	var connErr, readOnlyErr error
	wg.Go(func() {
		for range iterations {
			conn, err := ts.ConnForCell(ctx, "zone1")
			if err != nil {
				connErr = err
				return
			}
			// Checks the connection's read-only state; the outcome depends on timing.
			_, _ = conn.Create(ctx, "read_only_probe", nil)
		}
	})
	wg.Go(func() {
		for range iterations {
			if err := ts.SetReadOnly(true); err != nil {
				readOnlyErr = err
				return
			}
			if _, err := ts.IsReadOnly(); err != nil {
				readOnlyErr = err
				return
			}
		}
	})
	wg.Wait()
	require.NoError(t, connErr)
	require.NoError(t, readOnlyErr)

	readOnly, err := ts.IsReadOnly()
	require.NoError(t, err)
	assert.True(t, readOnly)
}

// TestConnForCellNewConnKeepsReadOnly verifies that once the Server is read-only, a connection ConnForCell
// creates afterwards is read-only too, both for a cell re-pointed by another process and for a cell this
// Server connects to for the first time.
func TestConnForCellNewConnKeepsReadOnly(t *testing.T) {
	ctx := t.Context()
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1", "zone2")
	t.Cleanup(ts.Close)
	other, err := topo.NewWithFactory(factory, "", "")
	require.NoError(t, err)
	t.Cleanup(other.Close)
	ts.SetCellInfoCacheTTL(0)

	conn, err := ts.ConnForCell(ctx, "zone1")
	require.NoError(t, err)
	require.NoError(t, ts.SetReadOnly(true))

	assertReadOnly := func(t *testing.T, conn topo.Conn) {
		_, err := conn.Create(ctx, "read_only_probe", nil)
		assert.Equal(t, vtrpcpb.Code_READ_ONLY, vterrors.Code(err), "a write through the connection was not rejected: %v", err)
	}

	t.Run("re-pointed cell", func(t *testing.T) {
		require.NoError(t, other.UpdateCellInfoFields(ctx, "zone1", func(ci *topodatapb.CellInfo) error {
			ci.Root = "/zone1-moved"
			return nil
		}))
		got, err := ts.ConnForCell(ctx, "zone1")
		require.NoError(t, err)
		require.NotSame(t, conn, got)
		assertReadOnly(t, got)
	})

	t.Run("first connection to a cell", func(t *testing.T) {
		got, err := ts.ConnForCell(ctx, "zone2")
		require.NoError(t, err)
		assertReadOnly(t, got)
	})
}

// trackingFactory wraps a memorytopo factory so that each cell connection reports, through rootProbePath, the
// root it was made for and when it was closed, and so that connecting to a root under /down fails.
type trackingFactory struct {
	*memorytopo.Factory
	closes atomic.Int64
}

const rootProbePath = "__root_probe__"

func (f *trackingFactory) Create(cell, serverAddr, root string) (topo.Conn, error) {
	if cell != topo.GlobalCell && strings.HasPrefix(root, "/down") {
		return nil, errors.New("topo server unreachable")
	}
	conn, err := f.Factory.Create(cell, serverAddr, root)
	if err != nil || cell == topo.GlobalCell {
		return conn, err
	}
	return &trackingConn{Conn: conn, factory: f, root: root}, nil
}

type trackingConn struct {
	topo.Conn
	factory *trackingFactory
	root    string
	// closedAt is the factory's close count when this connection was closed, or zero while it is open.
	closedAt atomic.Int64
}

func (c *trackingConn) Close() {
	c.closedAt.CompareAndSwap(0, c.factory.closes.Add(1))
	c.Conn.Close()
}

func (c *trackingConn) Get(ctx context.Context, filePath string) ([]byte, topo.Version, error) {
	if filePath == rootProbePath {
		return fmt.Appendf(nil, "%d %s", c.closedAt.Load(), c.root), nil, nil
	}
	return c.Conn.Get(ctx, filePath)
}

// probeConn returns the root a cell connection from ConnForCell was made for, and when it was closed.
func probeConn(ctx context.Context, conn topo.Conn) (root string, closedAt int64, err error) {
	contents, _, err := conn.Get(ctx, rootProbePath)
	if err != nil {
		return "", 0, err
	}
	closed, root, _ := strings.Cut(string(contents), " ")
	closedAt, err = strconv.ParseInt(closed, 10, 64)
	return root, closedAt, err
}

// TestConnForCellConcurrentRepoints runs ConnForCell from several goroutines while another process keeps
// re-pointing the cell, sometimes to a topo server that can't be reached, and this process keeps expiring it.
// It verifies that ConnForCell never returns a connection that was closed before the call started, and that
// once the cell settles on a reachable topo server, ConnForCell connects to it.
func TestConnForCellConcurrentRepoints(t *testing.T) {
	ctx := t.Context()
	_, memFactory := memorytopo.NewServerAndFactory(ctx, "zone1")
	factory := &trackingFactory{Factory: memFactory}
	ts, err := topo.NewWithFactory(factory, "", "")
	require.NoError(t, err)
	t.Cleanup(ts.Close)
	other, err := topo.NewWithFactory(memFactory, "", "")
	require.NoError(t, err)
	t.Cleanup(other.Close)
	ts.SetCellInfoCacheTTL(20 * time.Millisecond)
	repoint := func(root string) {
		require.NoError(t, other.UpdateCellInfoFields(ctx, "zone1", func(ci *topodatapb.CellInfo) error {
			ci.Root = root
			return nil
		}))
	}

	var stop atomic.Bool
	var calls, closedReturns atomic.Int64
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for !stop.Load() {
				before := factory.closes.Load()
				conn, err := ts.ConnForCell(ctx, "zone1")
				calls.Add(1)
				if err != nil {
					continue
				}
				// A connection closed during this call may have been closed by another goroutine after this
				// call got it; one closed before this call started was returned closed.
				if _, closedAt, err := probeConn(ctx, conn); err != nil || closedAt != 0 && closedAt <= before {
					closedReturns.Add(1)
				}
			}
		})
	}
	for i := range 200 {
		root := fmt.Sprintf("/up%d", i)
		if i%3 == 0 {
			root = fmt.Sprintf("/down%d", i)
		}
		repoint(root)
		if i%5 == 0 {
			ts.ExpireCellInfo("zone1")
		}
		// Let the callers run against each state.
		target := calls.Load() + 20
		for calls.Load() < target {
			runtime.Gosched()
		}
	}
	stop.Store(true)
	wg.Wait()
	assert.Zero(t, closedReturns.Load(), "ConnForCell returned a connection that was already closed")

	repoint("/final")
	assert.Eventually(t, func() bool {
		conn, err := ts.ConnForCell(ctx, "zone1")
		if err != nil {
			return false
		}
		root, _, err := probeConn(ctx, conn)
		return err == nil && root == "/final"
	}, 30*time.Second, 5*time.Millisecond, "ConnForCell did not settle on the cell's current topo server")
}
