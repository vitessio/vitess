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
	"fmt"
	"testing"

	"golang.org/x/net/context"

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

	// Might as well test DeleteCellInfo.
	if err := ts.DeleteCellInfo(ctx, newCell); err != nil {
		t.Fatalf("DeleteCellInfo failed: %v", err)
	}
	if _, err := ts.GetCellInfo(ctx, newCell, true /*strongRead*/); !topo.IsErrType(err, topo.NoNode) {
		t.Fatalf("GetCellInfo(non-existing cell) failed: %v", err)
	}
}
