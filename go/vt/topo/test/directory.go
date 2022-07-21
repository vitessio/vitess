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

package test

import (
	"reflect"
	"testing"

	"context"

	"vitess.io/vitess/go/vt/topo"
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
	checkDirectoryInCell(t, conn, true /*hasCells*/)

	// local cell
	t.Logf("===   checkDirectoryInCell test")
	conn, err = ts.ConnForCell(ctx, LocalCellName)
	if err != nil {
		t.Fatalf("ConnForCell(test) failed: %v", err)
	}
	checkDirectoryInCell(t, conn, false /*hasCells*/)
}

func checkListDir(ctx context.Context, t *testing.T, conn topo.Conn, dirPath string, expected []topo.DirEntry) {
	t.Helper()

	// Build the shallow expected list, when full=false.
	se := make([]topo.DirEntry, len(expected))
	for i, e := range expected {
		se[i].Name = e.Name
	}

	// Test with full=false.
	entries, err := conn.ListDir(ctx, dirPath, false /*full*/)
	switch {
	case topo.IsErrType(err, topo.NoNode):
		if len(se) != 0 {
			t.Errorf("ListDir(%v, false) returned ErrNoNode but was expecting %v", dirPath, se)
		}
	case err == nil:
		if len(se) != 0 || len(entries) != 0 {
			if !reflect.DeepEqual(entries, se) {
				t.Errorf("ListDir(%v, false) returned %v but was expecting %v", dirPath, entries, se)
			}
		}
	default:
		t.Errorf("ListDir(%v, false) returned unexpected error: %v", dirPath, err)
	}

	// Test with full=true.
	entries, err = conn.ListDir(ctx, dirPath, true /*full*/)
	switch {
	case topo.IsErrType(err, topo.NoNode):
		if len(expected) != 0 {
			t.Errorf("ListDir(%v, true) returned ErrNoNode but was expecting %v", dirPath, expected)
		}
	case err == nil:
		if len(expected) != 0 || len(entries) != 0 {
			if !reflect.DeepEqual(entries, expected) {
				t.Errorf("ListDir(%v, true) returned %v but was expecting %v", dirPath, entries, expected)
			}
		}
	default:
		t.Errorf("ListDir(%v, true) returned unexpected error: %v", dirPath, err)
	}
}

func checkDirectoryInCell(t *testing.T, conn topo.Conn, hasCells bool) {
	ctx := context.Background()

	// ListDir root: nothing
	var expected []topo.DirEntry
	if hasCells {
		expected = append(expected, topo.DirEntry{
			Name: "cells",
			Type: topo.TypeDirectory,
		})
	}
	checkListDir(ctx, t, conn, "/", expected)

	// Create a topolevel entry
	version, err := conn.Create(ctx, "/MyFile", []byte{'a'})
	if err != nil {
		t.Fatalf("cannot create toplevel file: %v", err)
	}

	// ListDir should return it.
	expected = append([]topo.DirEntry{
		{
			Name: "MyFile",
			Type: topo.TypeFile,
		},
	}, expected...)
	checkListDir(ctx, t, conn, "/", expected)

	// Delete it, it should be gone.
	if err := conn.Delete(ctx, "/MyFile", version); err != nil {
		t.Fatalf("cannot delete toplevel file: %v", err)
	}
	expected = expected[1:]
	checkListDir(ctx, t, conn, "/", expected)

	// Create a file 3 layers down.
	version, err = conn.Create(ctx, "/types/name/MyFile", []byte{'a'})
	if err != nil {
		t.Fatalf("cannot create deep file: %v", err)
	}
	expected = append(expected, topo.DirEntry{
		Name: "types",
		Type: topo.TypeDirectory,
	})

	// Check listing at all levels.
	checkListDir(ctx, t, conn, "/", expected)
	checkListDir(ctx, t, conn, "/types/", []topo.DirEntry{
		{
			Name: "name",
			Type: topo.TypeDirectory,
		},
	})
	checkListDir(ctx, t, conn, "/types/name/", []topo.DirEntry{
		{
			Name: "MyFile",
			Type: topo.TypeFile,
		},
	})

	// Add a second file
	version2, err := conn.Create(ctx, "/types/othername/MyFile", []byte{'a'})
	if err != nil {
		t.Fatalf("cannot create deep file2: %v", err)
	}

	// Check entries at all levels
	checkListDir(ctx, t, conn, "/", expected)
	checkListDir(ctx, t, conn, "/types/", []topo.DirEntry{
		{
			Name: "name",
			Type: topo.TypeDirectory,
		},
		{
			Name: "othername",
			Type: topo.TypeDirectory,
		},
	})
	checkListDir(ctx, t, conn, "/types/name/", []topo.DirEntry{
		{
			Name: "MyFile",
			Type: topo.TypeFile,
		},
	})
	checkListDir(ctx, t, conn, "/types/othername/", []topo.DirEntry{
		{
			Name: "MyFile",
			Type: topo.TypeFile,
		},
	})

	// Delete the first file, expect all lists to return the second one.
	if err := conn.Delete(ctx, "/types/name/MyFile", version); err != nil {
		t.Fatalf("cannot delete deep file: %v", err)
	}
	checkListDir(ctx, t, conn, "/", expected)
	checkListDir(ctx, t, conn, "/types/", []topo.DirEntry{
		{
			Name: "othername",
			Type: topo.TypeDirectory,
		},
	})
	checkListDir(ctx, t, conn, "/types/name/", nil)
	checkListDir(ctx, t, conn, "/types/othername/", []topo.DirEntry{
		{
			Name: "MyFile",
			Type: topo.TypeFile,
		},
	})

	// Delete the second file, expect all lists to return nothing.
	if err := conn.Delete(ctx, "/types/othername/MyFile", version2); err != nil {
		t.Fatalf("cannot delete second deep file: %v", err)
	}
	for _, dir := range []string{"/types/", "/types/name/", "/types/othername/"} {
		checkListDir(ctx, t, conn, dir, nil)
	}
	expected = expected[:len(expected)-1]
	checkListDir(ctx, t, conn, "/", expected)
}
