/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package test

import (
	"reflect"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

// checkDirectory tests the directory part of the topo.Conn API.
func checkDirectory(t *testing.T, ts *topo.Server) {
	ctx := context.Background()

	// global cell
	t.Logf("===   checkDirectoryInCell global")
	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		t.Fatalf("ConnForCell(global) failed: %v", err)
	}
	checkDirectoryInCell(t, conn)

	// local cell
	t.Logf("===   checkDirectoryInCell test")
	conn, err = ts.ConnForCell(ctx, LocalCellName)
	if err != nil {
		t.Fatalf("ConnForCell(test) failed: %v", err)
	}
	checkDirectoryInCell(t, conn)
}

// entriesWithoutCells removes 'cells' from the global directory.
// 'cells' may be present on some topo implementations but not in others.
func entriesWithoutCells(entries []string) []string {
	var result []string
	for _, e := range entries {
		if e != "cells" {
			result = append(result, e)
		}
	}
	return result
}

func checkListDir(ctx context.Context, t *testing.T, conn topo.Conn, dirPath string, expected []string) {
	entries, err := conn.ListDir(ctx, dirPath)
	switch err {
	case topo.ErrNoNode:
		if len(expected) != 0 {
			t.Errorf("ListDir(%v) returned ErrNoNode but was expecting %v", dirPath, expected)
		}
	case nil:
		// The 'cells' directory may be present in some implementations
		// but not others. It will eventually be in all of them,
		// as it is where CellInfo records are stored, but for now,
		// 'zookeeper' doesn't have it.
		entries = entriesWithoutCells(entries)
		if !reflect.DeepEqual(entries, expected) {
			t.Errorf("ListDir(%v) returned %v but was expecting %v", dirPath, entries, expected)
		}
	default:
		t.Errorf("ListDir(%v) returned unexpected error: %v", dirPath, err)
	}
}

func checkDirectoryInCell(t *testing.T, conn topo.Conn) {
	ctx := context.Background()

	// ListDir root: nothing
	checkListDir(ctx, t, conn, "/", nil)

	// Create a topolevel entry
	version, err := conn.Create(ctx, "/MyFile", []byte{'a'})
	if err != nil {
		t.Fatalf("cannot create toplevel file: %v", err)
	}

	// ListDir should return it.
	checkListDir(ctx, t, conn, "/", []string{"MyFile"})

	// Delete it, it should be gone.
	if err := conn.Delete(ctx, "/MyFile", version); err != nil {
		t.Fatalf("cannot delete toplevel file: %v", err)
	}
	checkListDir(ctx, t, conn, "/", nil)

	// Create a file 3 layers down.
	version, err = conn.Create(ctx, "/types/name/MyFile", []byte{'a'})
	if err != nil {
		t.Fatalf("cannot create deep file: %v", err)
	}

	// Check listing at all levels.
	checkListDir(ctx, t, conn, "/", []string{"types"})
	checkListDir(ctx, t, conn, "/types/", []string{"name"})
	checkListDir(ctx, t, conn, "/types/name/", []string{"MyFile"})

	// Add a second file
	version2, err := conn.Create(ctx, "/types/othername/MyFile", []byte{'a'})
	if err != nil {
		t.Fatalf("cannot create deep file2: %v", err)
	}

	// Check entries at all levels
	checkListDir(ctx, t, conn, "/", []string{"types"})
	checkListDir(ctx, t, conn, "/types/", []string{"name", "othername"})
	checkListDir(ctx, t, conn, "/types/name/", []string{"MyFile"})
	checkListDir(ctx, t, conn, "/types/othername/", []string{"MyFile"})

	// Delete the first file, expect all lists to return the second one.
	if err := conn.Delete(ctx, "/types/name/MyFile", version); err != nil {
		t.Fatalf("cannot delete deep file: %v", err)
	}
	checkListDir(ctx, t, conn, "/", []string{"types"})
	checkListDir(ctx, t, conn, "/types/", []string{"othername"})
	checkListDir(ctx, t, conn, "/types/name/", nil)
	checkListDir(ctx, t, conn, "/types/othername/", []string{"MyFile"})

	// Delete the second file, expect all lists to return nothing.
	if err := conn.Delete(ctx, "/types/othername/MyFile", version2); err != nil {
		t.Fatalf("cannot delete second deep file: %v", err)
	}
	for _, dir := range []string{"/", "/types/", "/types/name/", "/types/othername/"} {
		checkListDir(ctx, t, conn, dir, nil)
	}
}
