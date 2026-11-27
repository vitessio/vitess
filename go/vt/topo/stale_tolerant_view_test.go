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
// read waits for release, or for its ctx to be done like a real topo read, after it reads the CellInfo.
type cellInfoFactory struct {
	*memorytopo.Factory
	reads   atomic.Int64
	armed   chan struct{}
	paused  chan struct{}
	release chan struct{}
	used    atomic.Bool
}

// newCellInfoServer returns a Server on a cellInfoFactory with the cell zone1 and the given CellInfo cache
// TTL, and another Server on the same topo that stands in for another process.
func newCellInfoServer(t *testing.T, ttl time.Duration) (ts, other *topo.Server, factory *cellInfoFactory) {
	_, memFactory := memorytopo.NewServerAndFactory(t.Context(), "zone1")
	factory = &cellInfoFactory{
		Factory: memFactory,
		armed:   make(chan struct{}),
		paused:  make(chan struct{}),
		release: make(chan struct{}),
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

func (f *cellInfoFactory) Create(cell, serverAddr, root string) (topo.Conn, error) {
	conn, err := f.Factory.Create(cell, serverAddr, root)
	if err != nil || cell != topo.GlobalCell {
		return conn, err
	}
	return &cellInfoConn{Conn: conn, factory: f}, nil
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
		repointCell(t, ts, "zone1", "/zone1-moved")
		got, err := ts.CellReader(ctx, "zone1")
		require.NoError(t, err)
		assert.NotSame(t, conn, got, "the connection to the old root was reused")
	})
}

// TestStaleTolerantViewFollowsConnForCell verifies how the StaleTolerantView follows ConnForCell's handling of
// a cell re-pointed by another process. Once ConnForCell has found the new topo server and replaced the cell's
// connection, whether the cell's root or only its address changed, the view reads the cell's CellInfo again
// rather than keep using the CellInfo it cached for the replaced connection. When the new topo server can't be
// connected to, ConnForCell keeps the old connection open, and the view reads through it within the TTL, fails
// like ConnForCell once the TTL has passed, and reads through the original connection again once the cell is
// pointed back.
func TestStaleTolerantViewFollowsConnForCell(t *testing.T) {
	ctx := t.Context()
	ts, other, factory := newCellInfoServer(t, time.Hour)

	repoints := []struct {
		name   string
		update func(ci *topodatapb.CellInfo)
	}{
		{"root", func(ci *topodatapb.CellInfo) { ci.Root = "/zone1-moved" }},
		{"address only", func(ci *topodatapb.CellInfo) { ci.ServerAddress = "zone1-moved:2379" }},
	}
	for _, repoint := range repoints {
		t.Run("re-pointed to a reachable topo server, by "+repoint.name, func(t *testing.T) {
			ts.ExpireCellInfo("zone1")
			conn, err := ts.CellReader(ctx, "zone1")
			require.NoError(t, err)

			require.NoError(t, other.UpdateCellInfoFields(ctx, "zone1", func(ci *topodatapb.CellInfo) error {
				repoint.update(ci)
				return nil
			}))
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
		})
	}

	t.Run("re-pointed to an unreachable topo server", func(t *testing.T) {
		// A downFactory can't connect to a root under /down, and tells whether a connection was closed.
		_, memFactory := memorytopo.NewServerAndFactory(ctx, "zone1")
		ts, err := topo.NewWithFactory(&downFactory{Factory: memFactory, t: t}, "", "")
		require.NoError(t, err)
		t.Cleanup(ts.Close)
		ts.SetCellInfoCacheTTL(time.Hour)
		other, err := topo.NewWithFactory(memFactory, "", "")
		require.NoError(t, err)
		t.Cleanup(other.Close)
		original, err := ts.GetCellInfo(ctx, "zone1", true /*strongRead*/)
		require.NoError(t, err)

		conn, err := ts.CellReader(ctx, "zone1")
		require.NoError(t, err)

		repointCell(t, other, "zone1", "/down")
		_, wantErr := ts.ConnForCell(ctx, "zone1")
		require.Error(t, wantErr)
		got, err := ts.CellReader(ctx, "zone1")
		require.NoError(t, err, "the view did not read through the open connection within the TTL")
		assert.Same(t, conn, got)
		assertClosed(t, conn.(topo.Conn), false, "the connection the view reads through was closed")

		// Stand in for the TTL passing.
		ts.ExpireCellInfo("zone1")
		_, err = ts.CellReader(ctx, "zone1")
		require.Error(t, err, "the view read from a cell whose topo server can't be connected to after the TTL")
		assert.Equal(t, wantErr.Error(), err.Error(), "the view did not fail like ConnForCell")

		repointCell(t, other, "zone1", original.Root)
		got, err = ts.CellReader(ctx, "zone1")
		require.NoError(t, err)
		assert.Same(t, conn, got, "the view did not read through the original connection once the cell was pointed back")
		assertClosed(t, got.(topo.Conn), false, "the view returned a closed connection")
	})
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

	repointCell(t, ts, "zone1", "/zone1-moved")
	close(factory.release)
	res := <-done
	require.NoError(t, res.err)
	assert.Same(t, conn, res.reader, "the paused read returned the CellInfo it read")

	got, err := ts.CellReader(ctx, "zone1")
	require.NoError(t, err)
	assert.NotSame(t, conn, got, "the connection to the old root was reused after the read raced the change")
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

// TestStaleTolerantViewConcurrentRepoints runs StaleTolerantView reads, ConnForCell calls and re-points of
// the cell concurrently, for the race detector, and verifies that the view settles on the cell's current
// topo server.
func TestStaleTolerantViewConcurrentRepoints(t *testing.T) {
	ctx := t.Context()
	ts, other, _ := newCellInfoServer(t, 5*time.Millisecond)

	var stop atomic.Bool
	var wg sync.WaitGroup
	// Stop the readers before the topo servers close, also when the test fails on the way.
	t.Cleanup(func() {
		stop.Store(true)
		wg.Wait()
	})
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
		repointCell(t, other, "zone1", fmt.Sprintf("/up%d", i))
		if i%5 == 0 {
			ts.ExpireCellInfo("zone1")
		}
		time.Sleep(time.Millisecond)
	}
	stop.Store(true)
	wg.Wait()

	// ConnForCell, which reads the current CellInfo, keeps the view's connection only if it is to the cell's
	// current topo server.
	repointCell(t, other, "zone1", "/final")
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
// return what the Server's do, and with the CellInfo cache read no CellInfo once it is cached, while the
// Server's own GetTablet, GetShardReplication and FindAllTabletAliasesInShardByCell still read it on every call.
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

	// The Server's own reads, which share their code with the view's, read CellInfo on every call, as on main,
	// however warm the cache: they feed decisions that must not see a cell's old topo server.
	serverReads := map[string]func() error{
		"GetTablet": func() error {
			_, err := ts.GetTablet(ctx, want[0])
			return err
		},
		"GetShardReplication": func() error {
			_, err := ts.GetShardReplication(ctx, "zone1", "ks", "-80")
			return err
		},
		"FindAllTabletAliasesInShardByCell": func() error {
			_, err := ts.FindAllTabletAliasesInShardByCell(ctx, "ks", "-80", []string{"zone1"})
			return err
		},
	}
	for name, read := range serverReads {
		before := factory.reads.Load()
		require.NoError(t, read())
		assert.Equal(t, before+1, factory.reads.Load(), "Server.%s did not read CellInfo with the cache warm", name)
	}

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
