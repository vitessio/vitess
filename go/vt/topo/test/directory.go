package test

import (
	"reflect"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

// checkDirectory tests the directory part of the Backend API.
// It does not use the pre-Backend API paths, to really
// test the new functions.
func checkDirectory(t *testing.T, ts topo.Impl) {
	ctx := context.Background()

	// global cell
	checkDirectoryInCell(t, ts, topo.GlobalCell)

	// local cell
	cell := getLocalCell(ctx, t, ts)
	checkDirectoryInCell(t, ts, cell)
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

func checkListDir(ctx context.Context, t *testing.T, ts topo.Impl, cell string, dirPath string, expected []string) {
	entries, err := ts.ListDir(ctx, cell, dirPath)
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

func checkDirectoryInCell(t *testing.T, ts topo.Impl, cell string) {
	t.Logf("===   checkDirectoryInCell %v", cell)
	ctx := context.Background()

	// ListDir root: nothing
	checkListDir(ctx, t, ts, cell, "/", nil)

	// Create a topolevel entry
	version, err := ts.Create(ctx, cell, "/MyFile", []byte{'a'})
	if err != nil {
		t.Fatalf("cannot create toplevel file: %v", err)
	}

	// ListDir should return it.
	checkListDir(ctx, t, ts, cell, "/", []string{"MyFile"})

	// Delete it, it should be gone.
	if err := ts.Delete(ctx, cell, "/MyFile", version); err != nil {
		t.Fatalf("cannot delete toplevel file: %v", err)
	}
	checkListDir(ctx, t, ts, cell, "/", nil)

	// Create a file 3 layers down.
	version, err = ts.Create(ctx, cell, "/types/name/MyFile", []byte{'a'})
	if err != nil {
		t.Fatalf("cannot create deep file: %v", err)
	}

	// Check listing at all levels.
	checkListDir(ctx, t, ts, cell, "/", []string{"types"})
	checkListDir(ctx, t, ts, cell, "/types/", []string{"name"})
	checkListDir(ctx, t, ts, cell, "/types/name/", []string{"MyFile"})

	// Add a second file
	version2, err := ts.Create(ctx, cell, "/types/othername/MyFile", []byte{'a'})
	if err != nil {
		t.Fatalf("cannot create deep file2: %v", err)
	}

	// Check entries at all levels
	checkListDir(ctx, t, ts, cell, "/", []string{"types"})
	checkListDir(ctx, t, ts, cell, "/types/", []string{"name", "othername"})
	checkListDir(ctx, t, ts, cell, "/types/name/", []string{"MyFile"})
	checkListDir(ctx, t, ts, cell, "/types/othername/", []string{"MyFile"})

	// Delete the first file, expect all lists to return the second one.
	if err := ts.Delete(ctx, cell, "/types/name/MyFile", version); err != nil {
		t.Fatalf("cannot delete deep file: %v", err)
	}
	checkListDir(ctx, t, ts, cell, "/", []string{"types"})
	checkListDir(ctx, t, ts, cell, "/types/", []string{"othername"})
	checkListDir(ctx, t, ts, cell, "/types/name/", nil)
	checkListDir(ctx, t, ts, cell, "/types/othername/", []string{"MyFile"})

	// Delete the second file, expect all lists to return nothing.
	if err := ts.Delete(ctx, cell, "/types/othername/MyFile", version2); err != nil {
		t.Fatalf("cannot delete second deep file: %v", err)
	}
	for _, dir := range []string{"/", "/types/", "/types/name/", "/types/othername/"} {
		checkListDir(ctx, t, ts, cell, dir, nil)
	}
}
