/*
Copyright 2019 The Vitess Authors.

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

package topotests

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// This file tests the CellInfo part of the topo.Server API.

func TestCellInfo(t *testing.T) {
	cell := "cell1"
	ctx := context.Background()
	ts := memorytopo.NewServer(cell)

	// Check GetCellInfo returns what memorytopo created.
	ci, err := ts.GetCellInfo(ctx, cell, true /*strongRead*/)
	if err != nil {
		t.Fatalf("GetCellInfo failed: %v", err)
	}
	if ci.Root != "" {
		t.Fatalf("unexpected CellInfo: %v", ci)
	}

	var cells []string
	cells, err = ts.ExpandCells(ctx, cell)
	require.NoError(t, err)
	require.EqualValues(t, []string{"cell1"}, cells)

	// Update the Server Address.
	if err := ts.UpdateCellInfoFields(ctx, cell, func(ci *topodatapb.CellInfo) error {
		ci.ServerAddress = "new address"
		return nil
	}); err != nil {
		t.Fatalf("UpdateCellInfoFields failed: %v", err)
	}
	ci, err = ts.GetCellInfo(ctx, cell, true /*strongRead*/)
	if err != nil {
		t.Fatalf("GetCellInfo failed: %v", err)
	}
	if ci.ServerAddress != "new address" {
		t.Fatalf("unexpected CellInfo: %v", ci)
	}

	// Test update with no change.
	if err := ts.UpdateCellInfoFields(ctx, cell, func(ci *topodatapb.CellInfo) error {
		ci.ServerAddress = "bad address"
		return topo.NewError(topo.NoUpdateNeeded, cell)
	}); err != nil {
		t.Fatalf("UpdateCellInfoFields failed: %v", err)
	}
	ci, err = ts.GetCellInfo(ctx, cell, true /*strongRead*/)
	if err != nil {
		t.Fatalf("GetCellInfo failed: %v", err)
	}
	if ci.ServerAddress != "new address" {
		t.Fatalf("unexpected CellInfo: %v", ci)
	}

	// Test failing update.
	updateErr := fmt.Errorf("inside error")
	if err := ts.UpdateCellInfoFields(ctx, cell, func(ci *topodatapb.CellInfo) error {
		return updateErr
	}); err != updateErr {
		t.Fatalf("UpdateCellInfoFields failed: %v", err)
	}

	// Test update on non-existing object.
	newCell := "new_cell"
	if err := ts.UpdateCellInfoFields(ctx, newCell, func(ci *topodatapb.CellInfo) error {
		ci.Root = "/"
		ci.ServerAddress = "good address"
		return nil
	}); err != nil {
		t.Fatalf("UpdateCellInfoFields failed: %v", err)
	}
	ci, err = ts.GetCellInfo(ctx, newCell, true /*strongRead*/)
	if err != nil {
		t.Fatalf("GetCellInfo failed: %v", err)
	}
	if ci.ServerAddress != "good address" || ci.Root != "/" {
		t.Fatalf("unexpected CellInfo: %v", ci)
	}

	// Add a record that should block CellInfo deletion for safety reasons.
	if err := ts.UpdateSrvKeyspace(ctx, cell, "keyspace", &topodatapb.SrvKeyspace{}); err != nil {
		t.Fatalf("UpdateSrvKeyspace failed: %v", err)
	}
	srvKeyspaces, err := ts.GetSrvKeyspaceNames(ctx, cell)
	if err != nil {
		t.Fatalf("GetSrvKeyspaceNames failed: %v", err)
	}
	if len(srvKeyspaces) == 0 {
		t.Fatalf("UpdateSrvKeyspace did not add SrvKeyspace.")
	}

	// Try to delete without force; it should fail.
	if err := ts.DeleteCellInfo(ctx, cell, false); err == nil {
		t.Fatalf("DeleteCellInfo should have failed without -force")
	}

	// Use the force.
	if err := ts.DeleteCellInfo(ctx, cell, true); err != nil {
		t.Fatalf("DeleteCellInfo failed even with -force: %v", err)
	}
	if _, err := ts.GetCellInfo(ctx, cell, true /*strongRead*/); !topo.IsErrType(err, topo.NoNode) {
		t.Fatalf("GetCellInfo(non-existing cell) failed: %v", err)
	}
}

func TestExpandCells(t *testing.T) {
	ctx := context.Background()
	var cells []string
	var err error
	var allCells = "cell1,cell2,cell3"
	type testCase struct {
		name      string
		cellsIn   string
		cellsOut  []string
		errString string
	}

	testCases := []testCase{
		{"single", "cell1", []string{"cell1"}, ""},
		{"multiple", "cell1,cell2,cell3", []string{"cell1", "cell2", "cell3"}, ""},
		{"empty", "", []string{"cell1", "cell2", "cell3"}, ""},
		{"bad", "unknown", nil, "node doesn't exist"},
	}

	for _, tCase := range testCases {
		t.Run(tCase.name, func(t *testing.T) {
			cellsIn := tCase.cellsIn
			if cellsIn == "" {
				cellsIn = allCells
			}
			topoCells := strings.Split(cellsIn, ",")
			var ts *topo.Server
			if tCase.name == "bad" {
				ts = memorytopo.NewServer()
			} else {
				ts = memorytopo.NewServer(topoCells...)
			}
			cells, err = ts.ExpandCells(ctx, cellsIn)
			if tCase.errString != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tCase.errString)
			} else {
				require.NoError(t, err)
			}
			require.EqualValues(t, tCase.cellsOut, cells)
		})
	}

	t.Run("aliases", func(t *testing.T) {
		cells := []string{"cell1", "cell2", "cell3"}
		ts := memorytopo.NewServer(cells...)
		err := ts.CreateCellsAlias(ctx, "alias", &topodatapb.CellsAlias{Cells: cells})
		require.NoError(t, err)

		tests := []struct {
			name      string
			in        string
			out       []string
			shouldErr bool
		}{
			{
				name: "alias only",
				in:   "alias",
				out:  []string{"cell1", "cell2", "cell3"},
			},
			{
				name: "alias and cell in alias", // test deduping logic
				in:   "alias,cell1",
				out:  []string{"cell1", "cell2", "cell3"},
			},
			{
				name: "just cells",
				in:   "cell1",
				out:  []string{"cell1"},
			},
			{
				name:      "missing alias",
				in:        "not_an_alias",
				shouldErr: true,
			},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				expanded, err := ts.ExpandCells(ctx, tt.in)
				if tt.shouldErr {
					assert.Error(t, err)
					return
				}

				require.NoError(t, err)
				assert.ElementsMatch(t, expanded, tt.out)
			})
		}
	})
}

func TestDeleteCellInfo(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("zone1", "unreachable")

	err := ts.UpdateCellInfoFields(ctx, "unreachable", func(ci *topodatapb.CellInfo) error {
		ci.ServerAddress = memorytopo.UnreachableServerAddr
		return nil
	})
	require.NoError(t, err, "failed to update cell to point at unreachable addr")

	tests := []struct {
		force       bool
		shouldErr   bool
		shouldExist bool
	}{
		{
			force:       false,
			shouldErr:   true,
			shouldExist: true,
		},
		{
			force:       true,
			shouldErr:   false,
			shouldExist: false,
		},
	}
	for _, tt := range tests {
		func() {
			ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
			defer cancel()

			err := ts.DeleteCellInfo(ctx, "unreachable", tt.force)
			if tt.shouldErr {
				assert.Error(t, err, "force=%t", tt.force)
			} else {
				assert.NoError(t, err, "force=%t", tt.force)
			}

			ci, err := ts.GetCellInfo(ctx, "unreachable", true /* strongRead */)
			if tt.shouldExist {
				assert.NoError(t, err)
				assert.NotNil(t, ci)
			} else {
				assert.True(t, topo.IsErrType(err, topo.NoNode), "expected cell %q to not exist", "unreachable")
			}
		}()
	}
}
