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
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// downFactory wraps a memorytopo factory so that connecting a cell to a root under /down fails, so that
// closedProbePath on a cell connection reports whether the connection was closed, and so that closing a cell
// connection twice fails the test, as it panics on etcd2topo.
type downFactory struct {
	*memorytopo.Factory
	t *testing.T
}

const closedProbePath = "__closed_probe__"

func (f *downFactory) Create(cell, serverAddr, root string) (topo.Conn, error) {
	if cell != topo.GlobalCell && strings.HasPrefix(root, "/down") {
		return nil, errors.New("topo server unreachable")
	}
	conn, err := f.Factory.Create(cell, serverAddr, root)
	if err != nil || cell == topo.GlobalCell {
		return conn, err
	}
	return &closeTrackingConn{Conn: conn, t: f.t}, nil
}

type closeTrackingConn struct {
	topo.Conn
	t      *testing.T
	closed atomic.Bool
}

func (c *closeTrackingConn) Close() {
	if c.closed.Swap(true) {
		c.t.Errorf("cell connection closed twice")
		return
	}
	c.Conn.Close()
}

func (c *closeTrackingConn) Get(ctx context.Context, filePath string) ([]byte, topo.Version, error) {
	if filePath == closedProbePath {
		return fmt.Append(nil, c.closed.Load()), nil, nil
	}
	return c.Conn.Get(ctx, filePath)
}

func repointCell(t *testing.T, ts *topo.Server, cell, root string) {
	require.NoError(t, ts.UpdateCellInfoFields(t.Context(), cell, func(ci *topodatapb.CellInfo) error {
		ci.Root = root
		return nil
	}))
}

// TestConnForCellPointedBackAfterFailedReplacement verifies that when a cell is re-pointed at a topo server
// that can't be reached, ConnForCell doesn't close the connection it closed when it found the cell re-pointed
// again on a later call, and that once the cell is pointed back, ConnForCell connects again rather than
// returning that closed connection.
func TestConnForCellPointedBackAfterFailedReplacement(t *testing.T) {
	ctx := t.Context()
	_, memFactory := memorytopo.NewServerAndFactory(ctx, "zone1")
	ts, err := topo.NewWithFactory(&downFactory{Factory: memFactory, t: t}, "", "")
	require.NoError(t, err)
	t.Cleanup(ts.Close)
	ci, err := ts.GetCellInfo(ctx, "zone1", true /*strongRead*/)
	require.NoError(t, err)

	conn, err := ts.ConnForCell(ctx, "zone1")
	require.NoError(t, err)

	repointCell(t, ts, "zone1", "/down")
	_, err = ts.ConnForCell(ctx, "zone1")
	require.Error(t, err)
	_, err = ts.ConnForCell(ctx, "zone1")
	require.Error(t, err)

	repointCell(t, ts, "zone1", ci.Root)
	got, err := ts.ConnForCell(ctx, "zone1")
	require.NoError(t, err)
	assert.NotSame(t, conn, got, "ConnForCell returned the connection it closed")
	closed, _, err := got.Get(ctx, closedProbePath)
	require.NoError(t, err)
	assert.Equal(t, "false", string(closed), "ConnForCell returned a closed connection")
}

// TestConnForCellConcurrentWithReadOnly verifies that SetReadOnly and IsReadOnly, which srvtopo's read-only
// server calls on every GetTopoServer, are safe alongside ConnForCell replacing cell connections and writes
// through them checking their read-only state.
func TestConnForCellConcurrentWithReadOnly(t *testing.T) {
	ctx := t.Context()
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	t.Cleanup(ts.Close)
	other, err := topo.NewWithFactory(factory, "", "")
	require.NoError(t, err)
	t.Cleanup(other.Close)

	const iterations = 200
	var wg sync.WaitGroup
	var connErr, readOnlyErr error
	wg.Go(func() {
		for i := range iterations {
			// Another process re-points the cell, so ConnForCell replaces its connection.
			if err := other.UpdateCellInfoFields(ctx, "zone1", func(ci *topodatapb.CellInfo) error {
				ci.Root = fmt.Sprintf("/zone1-%d", i)
				return nil
			}); err != nil {
				connErr = err
				return
			}
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
}

// TestConnForCellNewConnKeepsReadOnly verifies that once the Server is read-only, a connection ConnForCell
// creates afterwards is read-only too, both for a cell re-pointed by another process and for a cell this
// Server connects to for the first time, and that SetReadOnly(false) makes those connections writable again.
func TestConnForCellNewConnKeepsReadOnly(t *testing.T) {
	ctx := t.Context()
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1", "zone2")
	t.Cleanup(ts.Close)
	other, err := topo.NewWithFactory(factory, "", "")
	require.NoError(t, err)
	t.Cleanup(other.Close)

	conn, err := ts.ConnForCell(ctx, "zone1")
	require.NoError(t, err)
	require.NoError(t, ts.SetReadOnly(true))

	assertReadOnly := func(t *testing.T, conn topo.Conn) {
		_, err := conn.Create(ctx, "read_only_probe", nil)
		assert.Equal(t, vtrpcpb.Code_READ_ONLY, vterrors.Code(err), "a write through the connection was not rejected: %v", err)
	}

	t.Run("re-pointed cell", func(t *testing.T) {
		repointCell(t, other, "zone1", "/zone1-moved")
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

	t.Run("SetReadOnly(false)", func(t *testing.T) {
		require.NoError(t, ts.SetReadOnly(false))
		for _, cell := range []string{"zone1", "zone2"} {
			got, err := ts.ConnForCell(ctx, cell)
			require.NoError(t, err)
			_, err = got.Create(ctx, "read_only_probe", nil)
			assert.NoError(t, err, "a write through the %s connection was rejected", cell)
		}
	})
}
