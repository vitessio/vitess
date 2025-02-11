/*
Copyright 2025 The Vitess Authors.

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

package logic

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/inst"
)

func TestRefreshCells(t *testing.T) {
	db.ClearVTOrcDatabase()
	oldTs := ts
	defer func() {
		db.ClearVTOrcDatabase()
		ts = oldTs
	}()

	cells := []string{"zone1", "zone2", "zone3"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts = memorytopo.NewServer(ctx, cells...)

	require.NoError(t, RefreshCells(ctx))
	cellsRead, err := inst.ReadCells()
	require.NoError(t, err)
	require.Equal(t, cells, cellsRead)

	require.NoError(t, ts.DeleteCellInfo(context.Background(), "zone3", true))
	require.NoError(t, RefreshCells(ctx))
	cellsRead, err = inst.ReadCells()
	require.NoError(t, err)
	require.Equal(t, []string{"zone1", "zone2"}, cellsRead)
}
