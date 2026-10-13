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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
