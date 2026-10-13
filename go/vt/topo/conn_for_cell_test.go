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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestConnForCellCachesCellInfo verifies that ConnForCell reuses a cell's connection without reading the
// cell's CellInfo from the global topo while the CellInfo is within its cache TTL, re-reads it once the TTL
// has passed, and picks up this process's own UpdateCellInfoFields immediately.
func TestConnForCellCachesCellInfo(t *testing.T) {
	ctx := t.Context()
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()
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

// pausingFactory wraps a memorytopo factory. Once armed, the next global read of a CellInfo returns what it
// read and then waits for release before returning.
type pausingFactory struct {
	*memorytopo.Factory
	armed   chan struct{}
	paused  chan struct{}
	release chan struct{}
	used    atomic.Bool
}

func (f *pausingFactory) Create(cell, serverAddr, root string) (topo.Conn, error) {
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
				close(c.factory.paused)
				<-c.factory.release
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
	factory := &pausingFactory{Factory: memFactory, armed: make(chan struct{}), paused: make(chan struct{}), release: make(chan struct{})}
	ts, err := topo.NewWithFactory(factory, "", "")
	require.NoError(t, err)
	defer ts.Close()
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
	defer ts.Close()

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
