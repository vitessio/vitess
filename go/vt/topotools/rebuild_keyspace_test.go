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

package topotools

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestRebuildKeyspaceOfDeletedKeyspace checks that rebuilding a keyspace that
// doesn't exist, without naming cells, deletes its leftover files in every cell
// and returns NoNode without taking the keyspace lock.
func TestRebuildKeyspaceOfDeletedKeyspace(t *testing.T) {
	ctx := t.Context()
	cells := []string{"zone-1", "zone-2"}
	ts, factory := memorytopo.NewServerAndFactory(ctx, cells...)
	for _, cell := range cells {
		require.NoError(t, ts.UpdateSrvKeyspace(ctx, cell, "ks", &topodatapb.SrvKeyspace{}))
	}
	factory.GetCallStats().ResetAll()

	err := RebuildKeyspace(ctx, logutil.NewMemoryLogger(), ts, "ks", nil, false)
	require.True(t, topo.IsErrType(err, topo.NoNode), "RebuildKeyspace returned %v", err)

	require.Zero(t, factory.GetCallStats().Counts()["Lock"])
	for _, cell := range cells {
		keyspaces, err := ts.GetSrvKeyspaceNames(ctx, cell)
		require.NoError(t, err)
		require.Empty(t, keyspaces, "cell %v", cell)
	}
}
