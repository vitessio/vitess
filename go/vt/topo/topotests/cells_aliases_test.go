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
	"reflect"
	"sort"
	"testing"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/topo/memorytopo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// This file tests the CellsAliases part of the topo.Server API.

func TestCellsAliases(t *testing.T) {
	// Create an alias

	cell := "cell1"
	ctx := context.Background()
	ts := memorytopo.NewServer(cell)

	if err := ts.CreateCellsAlias(ctx, "alias", &topodatapb.CellsAlias{Cells: []string{"cell1", "cell2"}}); err != nil {
		t.Fatalf("CreateCellsAlias failed: %v", err)
	}

	if err := ts.CreateCellsAlias(ctx, "aliasb", &topodatapb.CellsAlias{Cells: []string{"cell3", "cell4"}}); err != nil {
		t.Fatalf("CreateCellsAlias failed: %v", err)
	}

	aliases, err := ts.GetCellsAliases(ctx, true /*strongRead*/)
	if err != nil {
		t.Fatalf("GetCellsAliases failed: %v", err)
	}

	var aliasesName []string
	for aliasName := range aliases {
		aliasesName = append(aliasesName, aliasName)
	}
	sort.Strings(aliasesName)

	if len(aliasesName) != 2 {
		t.Fatalf("Expected to have 2 aliases. Got %v", len(aliasesName))

	}

	if aliasesName[0] != "alias" {
		t.Fatalf("Expected alias name to be alias, got: %v", aliasesName[0])
	}

	if aliasesName[1] != "aliasb" {
		t.Fatalf("Expected alias name to be aliasb, got: %v", aliasesName[0])
	}

	want := []string{"cell1", "cell2"}

	if !reflect.DeepEqual(aliases[aliasesName[0]].Cells, want) {
		t.Fatalf("Expected alias to be: %v, got %v", want, aliases[aliasesName[0]])
	}

	want = []string{"cell3", "cell4"}

	if !reflect.DeepEqual(aliases[aliasesName[1]].Cells, want) {
		t.Fatalf("Expected aliasb to be: %v, got %v", want, aliases[aliasesName[1]])
	}

	// Test update on non-existing object.

	want = []string{"newcell"}

	if err := ts.UpdateCellsAlias(ctx, "newalias", func(ca *topodatapb.CellsAlias) error {
		ca.Cells = want
		return nil
	}); err != nil {
		t.Fatalf("UpdateCellsAlias failed: %v", err)
	}

	aliases, err = ts.GetCellsAliases(ctx, true /*strongRead*/)
	if err != nil {
		t.Fatalf("GetCellsAliases failed: %v", err)
	}

	if !reflect.DeepEqual(aliases["newalias"].Cells, want) {
		t.Fatalf("Expected newalias to be: %v, got %v", want, aliases["newalias"])
	}

	// Test update on existing object.

	want = []string{"newcell2"}

	if err := ts.UpdateCellsAlias(ctx, "newalias", func(ca *topodatapb.CellsAlias) error {
		ca.Cells = want
		return nil
	}); err != nil {
		t.Fatalf("UpdateCellsAlias failed: %v", err)
	}

	aliases, err = ts.GetCellsAliases(ctx, true /*strongRead*/)
	if err != nil {
		t.Fatalf("GetCellsAliases failed: %v", err)
	}

	if !reflect.DeepEqual(aliases["newalias"].Cells, want) {
		t.Fatalf("Expected newalias to be: %v, got %v", want, aliases["newalias"])
	}

	// Test delete alias

	if err := ts.DeleteCellsAlias(ctx, "newalias"); err != nil {
		t.Fatalf("UpdateCellsAlias failed: %v", err)
	}

	aliases, err = ts.GetCellsAliases(ctx, true /*strongRead*/)
	if err != nil {
		t.Fatalf("GetCellsAliases failed: %v", err)
	}

	if aliases["newalias"] != nil {
		t.Fatalf("Expected newalias to be: nil, got %v", aliases["newalias"])
	}

	// Create an alias that adds an overlapping cell is not supported
	if err := ts.CreateCellsAlias(ctx, "invalid", &topodatapb.CellsAlias{Cells: []string{"cell1", "cell2"}}); err == nil {
		t.Fatal("CreateCellsAlias should fail, got nil")
	}

	// Update an alias that adds an overlapping cell is not supported
	if err := ts.UpdateCellsAlias(ctx, "aliasb", func(ca *topodatapb.CellsAlias) error {
		ca.Cells = []string{"cell1"}
		return nil
	}); err == nil {
		t.Fatalf("UpdateCellsAlias should fail, got nil")
	}
}
