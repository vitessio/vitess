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

// pausingFactory wraps a trackingFactory. Once armed, the next global read of a CellInfo waits for release, or
// for its ctx to be done like a real topo read, after it reads the CellInfo, or before if beforeRead is set.
type pausingFactory struct {
	*trackingFactory
	armed   chan struct{}
	paused  chan struct{}
	release chan struct{}
	used    atomic.Bool
	// beforeRead pauses the read before it reads the CellInfo rather than after.
	beforeRead bool
}

func newPausingFactory(memFactory *memorytopo.Factory) *pausingFactory {
	return &pausingFactory{
		trackingFactory: &trackingFactory{Factory: memFactory},
		armed:           make(chan struct{}),
		paused:          make(chan struct{}),
		release:         make(chan struct{}),
	}
}

func (f *pausingFactory) Create(cell, serverAddr, root string) (topo.Conn, error) {
	conn, err := f.trackingFactory.Create(cell, serverAddr, root)
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
	if !strings.HasPrefix(filePath, topo.CellsPath+"/") {
		return c.Conn.Get(ctx, filePath)
	}
	if c.factory.beforeRead {
		if err := c.pause(ctx); err != nil {
			return nil, nil, err
		}
		return c.Conn.Get(ctx, filePath)
	}
	contents, version, err := c.Conn.Get(ctx, filePath)
	if pauseErr := c.pause(ctx); pauseErr != nil {
		return nil, nil, pauseErr
	}
	return contents, version, err
}

// pause waits for release if the factory is armed and this is the first read since.
func (c *pausingConn) pause(ctx context.Context) error {
	select {
	case <-c.factory.armed:
	default:
		return nil
	}
	if !c.factory.used.CompareAndSwap(false, true) {
		return nil
	}
	close(c.factory.paused)
	select {
	case <-c.factory.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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

// connResult is what a ConnForCell run in a goroutine returned.
type connResult struct {
	conn topo.Conn
	err  error
}

func goConnForCell(ctx context.Context, ts *topo.Server, cell string) chan connResult {
	done := make(chan connResult, 1)
	go func() {
		conn, err := ts.ConnForCell(ctx, cell)
		done <- connResult{conn, err}
	}()
	return done
}

// TestConnForCellRefreshesOneAtATime verifies that while a ConnForCell is refreshing a cell, another that finds
// the cell's entry stale waits for it rather than reading the cell's CellInfo too, and then uses what it
// applied, without reading it again. If the two reads overlapped, the one that started first could get the newer CellInfo, for example
// when it is delayed before it reaches the topo server and the cell is re-pointed meanwhile.
func TestConnForCellRefreshesOneAtATime(t *testing.T) {
	ctx := t.Context()
	_, memFactory := memorytopo.NewServerAndFactory(ctx, "zone1")
	factory := newPausingFactory(memFactory)
	factory.beforeRead = true
	ts, err := topo.NewWithFactory(factory, "", "")
	require.NoError(t, err)
	t.Cleanup(ts.Close)
	other, err := topo.NewWithFactory(memFactory, "", "")
	require.NoError(t, err)
	t.Cleanup(other.Close)
	ts.SetCellInfoCacheTTL(time.Hour)

	_, err = ts.ConnForCell(ctx, "zone1")
	require.NoError(t, err)

	// The first call pauses before it reads CellInfo.
	ts.ExpireCellInfo("zone1")
	close(factory.armed)
	first := goConnForCell(ctx, ts, "zone1")
	<-factory.paused

	second := goConnForCell(ctx, ts, "zone1")
	assert.Never(t, func() bool { return len(second) > 0 }, 500*time.Millisecond, 10*time.Millisecond,
		"a second call refreshed the cell while the first was refreshing it")

	// The cell is re-pointed before the first call reads CellInfo.
	require.NoError(t, other.UpdateCellInfoFields(ctx, "zone1", func(ci *topodatapb.CellInfo) error {
		ci.Root = "/zone1-moved"
		return nil
	}))
	gets := func() int64 { return memFactory.GetCallStats().Counts()["Get"] }
	before := gets()
	close(factory.release)
	res := <-first
	require.NoError(t, res.err)
	root, _, err := probeConn(ctx, res.conn)
	require.NoError(t, err)
	assert.Equal(t, "/zone1-moved", root, "the first call did not apply the CellInfo it read")
	res2 := <-second
	require.NoError(t, res2.err)
	assert.Same(t, res.conn, res2.conn, "the second call did not use what the first applied")
	assert.Equal(t, before+1, gets(), "the second call read CellInfo again after the first had refreshed the cell")
}

// TestConnForCellWaitEndsWithCaller verifies that a ConnForCell waiting for another call's refresh of the cell
// returns when its own ctx is done, rather than waiting as long as that refresh takes.
func TestConnForCellWaitEndsWithCaller(t *testing.T) {
	ctx := t.Context()
	_, memFactory := memorytopo.NewServerAndFactory(ctx, "zone1")
	factory := newPausingFactory(memFactory)
	ts, err := topo.NewWithFactory(factory, "", "")
	require.NoError(t, err)
	t.Cleanup(ts.Close)

	close(factory.armed)
	first := goConnForCell(ctx, ts, "zone1")
	<-factory.paused

	callCtx, cancel := context.WithCancel(ctx)
	second := goConnForCell(callCtx, ts, "zone1")
	assert.Never(t, func() bool { return len(second) > 0 }, 200*time.Millisecond, 10*time.Millisecond,
		"a second call did not wait for the first call's refresh")
	cancel()
	require.Eventually(t, func() bool { return len(second) > 0 }, 30*time.Second, 10*time.Millisecond,
		"a call kept waiting for another call's refresh after its ctx was canceled")
	require.ErrorIs(t, (<-second).err, context.Canceled)

	close(factory.release)
	require.NoError(t, (<-first).err)
}

// TestConnForCellPointedBackAfterFailedReplacement verifies that when a cell is re-pointed at a topo server
// that can't be reached and then pointed back, ConnForCell connects again rather than returning the
// connection it closed when it found the cell re-pointed.
func TestConnForCellPointedBackAfterFailedReplacement(t *testing.T) {
	ctx := t.Context()
	_, memFactory := memorytopo.NewServerAndFactory(ctx, "zone1")
	ts, err := topo.NewWithFactory(&trackingFactory{Factory: memFactory}, "", "")
	require.NoError(t, err)
	t.Cleanup(ts.Close)
	other, err := topo.NewWithFactory(memFactory, "", "")
	require.NoError(t, err)
	t.Cleanup(other.Close)
	ts.SetCellInfoCacheTTL(time.Hour)
	repoint := func(root string) {
		require.NoError(t, other.UpdateCellInfoFields(ctx, "zone1", func(ci *topodatapb.CellInfo) error {
			ci.Root = root
			return nil
		}))
		// Stand in for the TTL passing.
		ts.ExpireCellInfo("zone1")
	}

	conn, err := ts.ConnForCell(ctx, "zone1")
	require.NoError(t, err)
	oldRoot, _, err := probeConn(ctx, conn)
	require.NoError(t, err)

	repoint("/down")
	_, err = ts.ConnForCell(ctx, "zone1")
	require.Error(t, err)

	repoint(oldRoot)
	got, err := ts.ConnForCell(ctx, "zone1")
	require.NoError(t, err)
	root, closedAt, err := probeConn(ctx, got)
	require.NoError(t, err)
	assert.Equal(t, oldRoot, root)
	assert.Zero(t, closedAt, "ConnForCell returned the connection it closed")
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
