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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// cellInfoFactory wraps a memorytopo.Factory, counting the global reads of CellInfo. Once armed, the next such
// read waits for release, or for its ctx to be done like a real topo read, after it reads the CellInfo, or
// before if beforeRead is set.
type cellInfoFactory struct {
	*memorytopo.Factory
	reads   atomic.Int64
	armed   chan struct{}
	paused  chan struct{}
	release chan struct{}
	used    atomic.Bool
	// beforeRead pauses the read before it reads the CellInfo rather than after.
	beforeRead bool
	// readOnlyLags gives the Server a GlobalReadOnlyCell, whose CellInfo reads return what the first one
	// returned, like a replica of the global topo that has stopped catching up.
	readOnlyLags bool
}

// newCellInfoServer returns a Server on a cellInfoFactory with the cell zone1 and the given CellInfo cache
// TTL, and another Server on the same topo that stands in for another process.
func newCellInfoServer(t *testing.T, ttl time.Duration, opts ...func(*cellInfoFactory)) (ts, other *topo.Server, factory *cellInfoFactory) {
	_, memFactory := memorytopo.NewServerAndFactory(t.Context(), "zone1")
	factory = &cellInfoFactory{
		Factory: memFactory,
		armed:   make(chan struct{}),
		paused:  make(chan struct{}),
		release: make(chan struct{}),
	}
	for _, opt := range opts {
		opt(factory)
	}
	ts, err := topo.NewWithFactory(factory, "", "")
	require.NoError(t, err)
	t.Cleanup(ts.Close)
	ts.SetCellInfoCacheTTL(ttl)
	other, err = topo.NewWithFactory(memFactory, "", "")
	require.NoError(t, err)
	t.Cleanup(other.Close)
	return ts, other, factory
}

func (f *cellInfoFactory) HasGlobalReadOnlyCell(serverAddr, root string) bool {
	return f.readOnlyLags
}

func (f *cellInfoFactory) Create(cell, serverAddr, root string) (topo.Conn, error) {
	switch cell {
	case topo.GlobalCell:
		conn, err := f.Factory.Create(cell, serverAddr, root)
		if err != nil {
			return nil, err
		}
		return &cellInfoConn{Conn: conn, factory: f}, nil
	case topo.GlobalReadOnlyCell:
		conn, err := f.Factory.Create(topo.GlobalCell, serverAddr, root)
		if err != nil {
			return nil, err
		}
		return &laggingConn{Conn: conn, cellInfos: make(map[string][]byte)}, nil
	}
	return f.Factory.Create(cell, serverAddr, root)
}

// laggingConn returns, for each CellInfo, what its first read of it returned.
type laggingConn struct {
	topo.Conn
	mu        sync.Mutex
	cellInfos map[string][]byte
}

func (c *laggingConn) Get(ctx context.Context, filePath string) ([]byte, topo.Version, error) {
	if !strings.HasPrefix(filePath, topo.CellsPath+"/") {
		return c.Conn.Get(ctx, filePath)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if contents, ok := c.cellInfos[filePath]; ok {
		return contents, nil, nil
	}
	contents, version, err := c.Conn.Get(ctx, filePath)
	if err == nil {
		c.cellInfos[filePath] = contents
	}
	return contents, version, err
}

type cellInfoConn struct {
	topo.Conn
	factory *cellInfoFactory
}

func (c *cellInfoConn) Get(ctx context.Context, filePath string) ([]byte, topo.Version, error) {
	if !strings.HasPrefix(filePath, topo.CellsPath+"/") {
		return c.Conn.Get(ctx, filePath)
	}
	c.factory.reads.Add(1)
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
func (c *cellInfoConn) pause(ctx context.Context) error {
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

func repointCell(t *testing.T, ts *topo.Server, root string) {
	require.NoError(t, ts.UpdateCellInfoFields(t.Context(), "zone1", func(ci *topodatapb.CellInfo) error {
		ci.Root = root
		return nil
	}))
}

// readerResult is what a CellReader run in a goroutine returned.
type readerResult struct {
	reader any
	err    error
}

func goCellReader(ctx context.Context, ts *topo.Server, cell string) chan readerResult {
	done := make(chan readerResult, 1)
	go func() {
		reader, err := ts.CellReader(ctx, cell)
		done <- readerResult{reader, err}
	}()
	return done
}

// TestConnForCellIgnoresCellInfoCache verifies that ConnForCell reads the cell's CellInfo on every call, as
// on main, whatever the CellInfo cache TTL, and also after the StaleTolerantView has cached it.
func TestConnForCellIgnoresCellInfoCache(t *testing.T) {
	ctx := t.Context()
	ts, _, factory := newCellInfoServer(t, time.Hour)

	_, err := ts.CellReader(ctx, "zone1")
	require.NoError(t, err)
	before := factory.reads.Load()
	for range 3 {
		_, err := ts.ConnForCell(ctx, "zone1")
		require.NoError(t, err)
	}
	assert.Equal(t, before+3, factory.reads.Load(), "ConnForCell did not read CellInfo on every call")
}

// TestStaleTolerantViewWithoutCache verifies that with the default CellInfo cache TTL of 0, the
// StaleTolerantView reads through ConnForCell: it reads the cell's CellInfo on every call, and calls for a
// cell don't wait for each other's reads.
func TestStaleTolerantViewWithoutCache(t *testing.T) {
	ctx := t.Context()
	ts, _, factory := newCellInfoServer(t, 0)

	conn, err := ts.ConnForCell(ctx, "zone1")
	require.NoError(t, err)
	before := factory.reads.Load()
	for range 3 {
		got, err := ts.CellReader(ctx, "zone1")
		require.NoError(t, err)
		assert.Same(t, conn, got)
	}
	require.Equal(t, before+3, factory.reads.Load(), "CellInfo was not read on every call")

	close(factory.armed)
	first := goCellReader(ctx, ts, "zone1")
	<-factory.paused
	second := goCellReader(ctx, ts, "zone1")
	require.Eventually(t, func() bool { return len(second) > 0 }, 30*time.Second, 10*time.Millisecond,
		"a call waited for another call's read of the cell's CellInfo")
	require.NoError(t, (<-second).err)
	close(factory.release)
	require.NoError(t, (<-first).err)
}

// TestStaleTolerantViewCachesCellInfo verifies that the StaleTolerantView reuses a cell's connection without
// reading the cell's CellInfo from the global topo while the CellInfo is within its cache TTL, re-reads it
// once the TTL has passed and counts the TTL from that read, and picks up this process's own
// UpdateCellInfoFields immediately.
func TestStaleTolerantViewCachesCellInfo(t *testing.T) {
	ctx := t.Context()
	ts, _, factory := newCellInfoServer(t, time.Hour)

	conn, err := ts.CellReader(ctx, "zone1")
	require.NoError(t, err)
	connForCell, err := ts.ConnForCell(ctx, "zone1")
	require.NoError(t, err)
	require.Same(t, connForCell, conn, "the view did not share ConnForCell's connection")

	t.Run("within the TTL", func(t *testing.T) {
		before := factory.reads.Load()
		for range 3 {
			got, err := ts.CellReader(ctx, "zone1")
			require.NoError(t, err)
			assert.Same(t, conn, got)
		}
		assert.Equal(t, before, factory.reads.Load(), "CellInfo was read from the global topo")
	})

	t.Run("after the TTL", func(t *testing.T) {
		ts.SetCellInfoCacheTTL(time.Nanosecond)
		t.Cleanup(func() { ts.SetCellInfoCacheTTL(time.Hour) })
		before := factory.reads.Load()
		for range 3 {
			got, err := ts.CellReader(ctx, "zone1")
			require.NoError(t, err)
			assert.Same(t, conn, got)
		}
		assert.Equal(t, before+3, factory.reads.Load(), "CellInfo was not re-read on every call")
	})

	t.Run("the TTL counts from the latest read", func(t *testing.T) {
		ts.SetCellInfoCacheTTL(time.Second)
		t.Cleanup(func() { ts.SetCellInfoCacheTTL(time.Hour) })
		// Wait for the TTL to pass and the CellInfo to be read again.
		assert.Eventually(t, func() bool {
			before := factory.reads.Load()
			_, err := ts.CellReader(ctx, "zone1")
			return err == nil && factory.reads.Load() > before
		}, 30*time.Second, 10*time.Millisecond)
		before := factory.reads.Load()
		got, err := ts.CellReader(ctx, "zone1")
		require.NoError(t, err)
		assert.Same(t, conn, got)
		assert.Equal(t, before, factory.reads.Load(), "CellInfo was read again within the TTL of the latest read")
	})

	t.Run("the global cell", func(t *testing.T) {
		global, err := ts.ConnForCell(ctx, topo.GlobalCell)
		require.NoError(t, err)
		got, err := ts.CellReader(ctx, topo.GlobalCell)
		require.NoError(t, err)
		assert.Same(t, global, got)
	})

	t.Run("re-pointed in this process", func(t *testing.T) {
		repointCell(t, ts, "/zone1-moved")
		got, err := ts.CellReader(ctx, "zone1")
		require.NoError(t, err)
		assert.NotSame(t, conn, got, "the connection to the old root was reused")
	})
}

// TestStaleTolerantViewFollowsConnForCell verifies that once ConnForCell has found a cell re-pointed by
// another process and replaced the cell's connection, the StaleTolerantView reads the cell's CellInfo again
// rather than keep using the CellInfo it cached for the closed connection.
func TestStaleTolerantViewFollowsConnForCell(t *testing.T) {
	ctx := t.Context()
	ts, other, factory := newCellInfoServer(t, time.Hour)

	conn, err := ts.CellReader(ctx, "zone1")
	require.NoError(t, err)

	repointCell(t, other, "/zone1-moved")
	got, err := ts.CellReader(ctx, "zone1")
	require.NoError(t, err)
	require.Same(t, conn, got, "the view read CellInfo within the TTL")

	moved, err := ts.ConnForCell(ctx, "zone1")
	require.NoError(t, err)
	require.NotSame(t, conn, moved)
	before := factory.reads.Load()
	got, err = ts.CellReader(ctx, "zone1")
	require.NoError(t, err)
	assert.Same(t, moved, got, "the view did not use ConnForCell's new connection")
	assert.Equal(t, before+1, factory.reads.Load(), "the view did not read CellInfo again")
}

// TestStaleTolerantViewExpiryDuringRead verifies that when this process changes a cell's CellInfo while the
// StaleTolerantView is reading the old one, the view does not mark the old CellInfo as fresh, so the next read
// reads the change instead of reusing the old connection for another TTL.
func TestStaleTolerantViewExpiryDuringRead(t *testing.T) {
	ctx := t.Context()
	ts, _, factory := newCellInfoServer(t, time.Hour)

	conn, err := ts.CellReader(ctx, "zone1")
	require.NoError(t, err)

	// Make the next read read CellInfo, and pause that read after it has the old CellInfo.
	ts.ExpireCellInfo("zone1")
	close(factory.armed)
	done := goCellReader(ctx, ts, "zone1")
	<-factory.paused

	repointCell(t, ts, "/zone1-moved")
	close(factory.release)
	res := <-done
	require.NoError(t, res.err)
	assert.Same(t, conn, res.reader, "the paused read returned the CellInfo it read")

	got, err := ts.CellReader(ctx, "zone1")
	require.NoError(t, err)
	assert.NotSame(t, conn, got, "the connection to the old root was reused after the read raced the change")
}

// TestStaleTolerantViewRefreshesOneAtATime verifies that while the StaleTolerantView is refreshing a cell,
// another call that finds the cell's CellInfo stale waits for it rather than reading the cell's CellInfo too,
// and then uses what it read, without reading it again. If the two reads overlapped, the one that started
// first could get the newer CellInfo, for example when it is delayed before it reaches the topo server and the
// cell is re-pointed meanwhile, and the older one would then be cached for the TTL.
func TestStaleTolerantViewRefreshesOneAtATime(t *testing.T) {
	ctx := t.Context()
	ts, other, factory := newCellInfoServer(t, time.Hour)
	factory.beforeRead = true

	conn, err := ts.CellReader(ctx, "zone1")
	require.NoError(t, err)

	// The first call pauses before it reads CellInfo.
	ts.ExpireCellInfo("zone1")
	close(factory.armed)
	first := goCellReader(ctx, ts, "zone1")
	<-factory.paused

	second := goCellReader(ctx, ts, "zone1")
	assert.Never(t, func() bool { return len(second) > 0 }, 500*time.Millisecond, 10*time.Millisecond,
		"a second call refreshed the cell while the first was refreshing it")

	// The cell is re-pointed before the first call reads CellInfo.
	repointCell(t, other, "/zone1-moved")
	before := factory.reads.Load()
	close(factory.release)
	res := <-first
	require.NoError(t, res.err)
	assert.NotSame(t, conn, res.reader, "the first call did not use the CellInfo it read")
	res2 := <-second
	require.NoError(t, res2.err)
	assert.Same(t, res.reader, res2.reader, "the second call did not use what the first read")
	assert.Equal(t, before, factory.reads.Load(), "the second call read CellInfo again after the first had refreshed the cell")
}

// TestStaleTolerantViewWaitEndsWithCaller verifies that a call waiting for another call's refresh of the cell
// returns when its own ctx is done, rather than waiting as long as that refresh takes.
func TestStaleTolerantViewWaitEndsWithCaller(t *testing.T) {
	ctx := t.Context()
	ts, _, factory := newCellInfoServer(t, time.Hour)

	close(factory.armed)
	first := goCellReader(ctx, ts, "zone1")
	<-factory.paused

	callCtx, cancel := context.WithCancel(ctx)
	second := goCellReader(callCtx, ts, "zone1")
	assert.Never(t, func() bool { return len(second) > 0 }, 200*time.Millisecond, 10*time.Millisecond,
		"a second call did not wait for the first call's refresh")
	cancel()
	require.Eventually(t, func() bool { return len(second) > 0 }, 30*time.Second, 10*time.Millisecond,
		"a call kept waiting for another call's refresh after its ctx was canceled")
	require.ErrorIs(t, (<-second).err, context.Canceled)

	close(factory.release)
	require.NoError(t, (<-first).err)
}

// TestStaleTolerantViewRefreshReadTimesOut verifies that the StaleTolerantView bounds its CellInfo read by
// RemoteOperationTimeout even when its ctx has no deadline, so that a read that hangs doesn't keep the other
// calls for the cell waiting after the global topo recovers.
func TestStaleTolerantViewRefreshReadTimesOut(t *testing.T) {
	ctx := t.Context()
	ts, _, factory := newCellInfoServer(t, time.Hour)
	timeout := topo.RemoteOperationTimeout
	topo.RemoteOperationTimeout = 100 * time.Millisecond
	t.Cleanup(func() { topo.RemoteOperationTimeout = timeout })

	// The first call's read hangs; ctx has no deadline.
	_, hasDeadline := ctx.Deadline()
	require.False(t, hasDeadline)
	close(factory.armed)
	t.Cleanup(func() { close(factory.release) })
	first := goCellReader(ctx, ts, "zone1")
	<-factory.paused
	second := goCellReader(ctx, ts, "zone1")

	require.Eventually(t, func() bool { return len(second) > 0 }, 30*time.Second, 10*time.Millisecond,
		"a call for the cell kept waiting behind a read that hung")
	require.NoError(t, (<-second).err)
	require.ErrorIs(t, (<-first).err, context.DeadlineExceeded)
}

// TestStaleTolerantViewAfterRecreate verifies that after another process deletes a cell and this process
// creates a cell of the same name at a different root, the StaleTolerantView does not reuse the connection it
// cached for the old cell, and that a CreateCellInfo that fails leaves the cache in place.
func TestStaleTolerantViewAfterRecreate(t *testing.T) {
	ctx := t.Context()
	ts, other, factory := newCellInfoServer(t, time.Hour)

	conn, err := ts.CellReader(ctx, "zone1")
	require.NoError(t, err)

	require.True(t, topo.IsErrType(ts.CreateCellInfo(ctx, "zone1", &topodatapb.CellInfo{}), topo.NodeExists))
	before := factory.reads.Load()
	got, err := ts.CellReader(ctx, "zone1")
	require.NoError(t, err)
	require.Same(t, conn, got)
	require.Equal(t, before, factory.reads.Load(), "a failed CreateCellInfo expired the cache")

	require.NoError(t, other.DeleteCellInfo(ctx, "zone1", true /*force*/))
	require.NoError(t, ts.CreateCellInfo(ctx, "zone1", &topodatapb.CellInfo{Root: "/zone1-recreated"}))
	got, err = ts.CellReader(ctx, "zone1")
	require.NoError(t, err)
	assert.NotSame(t, conn, got, "the connection cached for the deleted cell was reused")
}

// TestStaleTolerantViewDeletedInThisProcess verifies that once this process deletes a cell, the
// StaleTolerantView no longer reads it.
func TestStaleTolerantViewDeletedInThisProcess(t *testing.T) {
	ctx := t.Context()
	ts, _, _ := newCellInfoServer(t, time.Hour)

	_, err := ts.CellReader(ctx, "zone1")
	require.NoError(t, err)
	require.NoError(t, ts.DeleteCellInfo(ctx, "zone1", true /*force*/))
	_, err = ts.CellReader(ctx, "zone1")
	assert.True(t, topo.IsErrType(err, topo.NoNode), "got %v", err)
}

// TestStaleTolerantViewAfterClose verifies that once the Server is closed, the StaleTolerantView fails
// rather than reading from the closed global connection or connecting to the cell again.
func TestStaleTolerantViewAfterClose(t *testing.T) {
	ctx := t.Context()
	ts, _, factory := newCellInfoServer(t, time.Hour)

	_, err := ts.CellReader(ctx, "zone1")
	require.NoError(t, err)
	ts.Close()
	before := factory.reads.Load()
	_, err = ts.CellReader(ctx, "zone1")
	assert.True(t, topo.IsErrType(err, topo.Interrupted), "got %v", err)
	assert.Equal(t, before, factory.reads.Load())
}

// TestStaleTolerantViewConcurrentRepoints runs StaleTolerantView reads, ConnForCell calls and re-points of
// the cell concurrently, for the race detector, and verifies that the view settles on the cell's current
// topo server.
func TestStaleTolerantViewConcurrentRepoints(t *testing.T) {
	ctx := t.Context()
	ts, other, _ := newCellInfoServer(t, 5*time.Millisecond)

	var stop atomic.Bool
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			for !stop.Load() {
				if i%2 == 0 {
					_, _ = ts.CellReader(ctx, "zone1")
				} else {
					_, _ = ts.ConnForCell(ctx, "zone1")
				}
			}
		})
	}
	for i := range 100 {
		repointCell(t, other, fmt.Sprintf("/up%d", i))
		if i%5 == 0 {
			ts.ExpireCellInfo("zone1")
		}
		time.Sleep(time.Millisecond)
	}
	stop.Store(true)
	wg.Wait()

	// ConnForCell, which reads the current CellInfo, keeps the view's connection only if it is to the cell's
	// current topo server.
	repointCell(t, other, "/final")
	assert.Eventually(t, func() bool {
		reader, err := ts.CellReader(ctx, "zone1")
		if err != nil {
			return false
		}
		conn, err := ts.ConnForCell(ctx, "zone1")
		return err == nil && reader == conn
	}, 30*time.Second, 5*time.Millisecond, "the view did not settle on the cell's current topo server")
}

// TestStaleTolerantViewReads verifies that the StaleTolerantView's GetTablet and FindAllTabletAliasesInShard
// return what the Server's do, and with the CellInfo cache read no CellInfo once it is cached.
func TestStaleTolerantViewReads(t *testing.T) {
	ctx := t.Context()
	ts, _, factory := newCellInfoServer(t, time.Hour)
	require.NoError(t, ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{}))
	view := ts.StaleTolerantView()
	shards := map[string]*topodatapb.TabletAlias{
		// A primary that is also in the shard's replication graph.
		"-80": {Cell: "zone1", Uid: 1},
		// A primary that is only in the shard record.
		"80-": {Cell: "zone1", Uid: 100},
		// A primary in a cell that does not exist.
		"0": {Cell: "nocell", Uid: 1},
		// No primary, and no replication graph.
		"1": nil,
	}
	for shard, primary := range shards {
		require.NoError(t, ts.CreateShard(ctx, "ks", shard))
		_, err := ts.UpdateShardFields(ctx, "ks", shard, func(si *topo.ShardInfo) error {
			si.PrimaryAlias = primary
			return nil
		})
		require.NoError(t, err)
	}
	for uid := range uint32(3) {
		require.NoError(t, ts.CreateTablet(ctx, &topodatapb.Tablet{
			Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: uid + 1},
			Keyspace: "ks",
			Shard:    "-80",
			Type:     topodatapb.TabletType_REPLICA,
		}))
	}

	for shard := range shards {
		want, err := ts.FindAllTabletAliasesInShard(ctx, "ks", shard)
		require.NoError(t, err)
		got, err := view.FindAllTabletAliasesInShard(ctx, "ks", shard)
		require.NoError(t, err)
		assert.Equal(t, want, got, "shard %s", shard)
	}
	want, err := ts.FindAllTabletAliasesInShard(ctx, "ks", "-80")
	require.NoError(t, err)
	require.Len(t, want, 3)
	for _, alias := range want {
		wantTablet, err := ts.GetTablet(ctx, alias)
		require.NoError(t, err)
		gotTablet, err := view.GetTablet(ctx, alias)
		require.NoError(t, err)
		assert.Equal(t, wantTablet.Tablet, gotTablet.Tablet)
		assert.Equal(t, wantTablet.Version(), gotTablet.Version())
	}

	before := factory.reads.Load()
	_, err = view.FindAllTabletAliasesInShard(ctx, "ks", "-80")
	require.NoError(t, err)
	for _, alias := range want {
		_, err := view.GetTablet(ctx, alias)
		require.NoError(t, err)
	}
	assert.Equal(t, before, factory.reads.Load(), "the view read CellInfo while it was cached")

	_, err = view.GetTablet(ctx, &topodatapb.TabletAlias{Cell: "zone1", Uid: 100})
	assert.True(t, topo.IsErrType(err, topo.NoNode), "got %v", err)
	_, err = view.FindAllTabletAliasesInShard(ctx, "ks", "nosuchshard")
	assert.True(t, topo.IsErrType(err, topo.NoNode), "got %v", err)
}

// TestStaleTolerantViewPartialResult verifies that, like the Server's, the StaleTolerantView's
// FindAllTabletAliasesInShard returns what it found with a PartialResult error when a cell's replication
// graph can't be read.
func TestStaleTolerantViewPartialResult(t *testing.T) {
	ctx := t.Context()
	_, memFactory := memorytopo.NewServerAndFactory(ctx, "zone1", "zone2")
	ts, err := topo.NewWithFactory(memFactory, "", "")
	require.NoError(t, err)
	t.Cleanup(ts.Close)
	ts.SetCellInfoCacheTTL(time.Hour)
	require.NoError(t, ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{}))
	require.NoError(t, ts.CreateShard(ctx, "ks", "0"))
	for _, cell := range []string{"zone1", "zone2"} {
		require.NoError(t, ts.CreateTablet(ctx, &topodatapb.Tablet{
			Alias:    &topodatapb.TabletAlias{Cell: cell, Uid: 1},
			Keyspace: "ks",
			Shard:    "0",
			Type:     topodatapb.TabletType_REPLICA,
		}))
	}
	// zone2's topo server is unreachable: reads from it block until their ctx is done.
	require.NoError(t, ts.UpdateCellInfoFields(ctx, "zone2", func(ci *topodatapb.CellInfo) error {
		ci.ServerAddress = memorytopo.UnreachableServerAddr
		return nil
	}))
	find := func(f func(ctx context.Context, keyspace, shard string) ([]*topodatapb.TabletAlias, error)) ([]*topodatapb.TabletAlias, error) {
		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		return f(ctx, "ks", "0")
	}

	want, wantErr := find(ts.FindAllTabletAliasesInShard)
	got, err := find(ts.StaleTolerantView().FindAllTabletAliasesInShard)
	assert.Equal(t, want, got)
	require.True(t, topo.IsErrType(wantErr, topo.PartialResult), "got %v", wantErr)
	assert.True(t, topo.IsErrType(err, topo.PartialResult), "got %v", err)
	assert.Equal(t, []*topodatapb.TabletAlias{{Cell: "zone1", Uid: 1}}, got)
}

// TestStaleTolerantViewStrongRead verifies that the StaleTolerantView reads CellInfo from the global cell
// rather than the GlobalReadOnlyCell: what it reads is trusted for the TTL, so it must not come from a replica
// that lags behind.
func TestStaleTolerantViewStrongRead(t *testing.T) {
	ctx := t.Context()
	ts, other, _ := newCellInfoServer(t, time.Hour, func(f *cellInfoFactory) { f.readOnlyLags = true })

	conn, err := ts.CellReader(ctx, "zone1")
	require.NoError(t, err)
	// Make the GlobalReadOnlyCell lag behind.
	_, err = ts.ConnForCell(ctx, "zone1")
	require.NoError(t, err)

	repointCell(t, other, "/zone1-moved")
	// Stand in for the TTL passing.
	ts.ExpireCellInfo("zone1")
	got, err := ts.CellReader(ctx, "zone1")
	require.NoError(t, err)
	assert.NotSame(t, conn, got, "the view read the lagging GlobalReadOnlyCell")
}

// TestStaleTolerantViewCloseDuringRead verifies that when the Server is closed while the StaleTolerantView
// reads a cell's CellInfo, the view fails rather than connecting to the cell after Close has closed every
// connection.
func TestStaleTolerantViewCloseDuringRead(t *testing.T) {
	ctx := t.Context()
	ts, _, factory := newCellInfoServer(t, time.Hour)

	close(factory.armed)
	done := goCellReader(ctx, ts, "zone1")
	<-factory.paused
	ts.Close()
	close(factory.release)
	res := <-done
	assert.True(t, topo.IsErrType(res.err, topo.Interrupted), "got %v", res.err)
	assert.Nil(t, res.reader)
}
