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
	"context"
	"reflect"
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/topo"
)

// checkFile tests the file part of the Conn API.
func checkFile(t *testing.T, ts *topo.Server) {
	ctx := context.Background()

	// global cell
	t.Logf("===   checkFileInCell global")
	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		t.Fatalf("ConnForCell(global) failed: %v", err)
	}
	checkFileInCell(t, conn, true /*hasCells*/)

	// local cell
	t.Logf("===   checkFileInCell global")
	conn, err = ts.ConnForCell(ctx, LocalCellName)
	if err != nil {
		t.Fatalf("ConnForCell(test) failed: %v", err)
	}
	checkFileInCell(t, conn, false /*hasCells*/)
}

func checkFileInCell(t *testing.T, conn topo.Conn, hasCells bool) {
	ctx := context.Background()

	// ListDir root: nothing.
	var expected []topo.DirEntry
	if hasCells {
		expected = append(expected, topo.DirEntry{
			Name: "cells",
			Type: topo.TypeDirectory,
		})
	}
	checkListDir(ctx, t, conn, "/", expected)

	// Get with no file -> ErrNoNode.
	_, _, err := conn.Get(ctx, "/myfile")
	if !topo.IsErrType(err, topo.NoNode) {
		t.Errorf("Get(non-existent) didn't return ErrNoNode but: %v", err)
	}

	// Create a file.
	version, err := conn.Create(ctx, "/myfile", []byte{'a'})
	if err != nil {
		t.Fatalf("Create('/myfile') failed: %v", err)
	}

	// See it in the listing now.
	expected = append(expected, topo.DirEntry{
		Name: "myfile",
		Type: topo.TypeFile,
	})
	checkListDir(ctx, t, conn, "/", expected)

	// Get should work, get the right contents and version.
	contents, getVersion, err := conn.Get(ctx, "/myfile")
	if err != nil {
		t.Errorf("Get('/myfile') returned an error: %v", err)
	} else {
		if len(contents) != 1 || contents[0] != 'a' {
			t.Errorf("Get('/myfile') returned bad content: %v", contents)
		}
		if !reflect.DeepEqual(getVersion, version) {
			t.Errorf("Get('/myfile') returned bad version: got %v expected %v", getVersion, version)
		}
	}

	// Update it, make sure version changes.
	newVersion, err := conn.Update(ctx, "/myfile", []byte{'b'}, version)
	if err != nil {
		t.Fatalf("Update('/myfile') failed: %v", err)
	}
	if reflect.DeepEqual(version, newVersion) {
		t.Errorf("Version didn't change, stayed %v", newVersion)
	}

	// Get should work, get the right contents and version.
	contents, getVersion, err = conn.Get(ctx, "/myfile")
	if err != nil {
		t.Errorf("Get('/myfile') returned an error: %v", err)
	} else {
		if len(contents) != 1 || contents[0] != 'b' {
			t.Errorf("Get('/myfile') returned bad content: %v", contents)
		}
		if !reflect.DeepEqual(getVersion, newVersion) {
			t.Errorf("Get('/myfile') returned bad version: got %v expected %v", getVersion, newVersion)
		}
	}

	// Try to update again with wrong version, should fail.
	if _, err = conn.Update(ctx, "/myfile", []byte{'b'}, version); !topo.IsErrType(err, topo.BadVersion) {
		t.Errorf("Update(bad version) didn't return ErrBadVersion but: %v", err)
	}

	// Try to update again with nil version, should work.
	newVersion, err = conn.Update(ctx, "/myfile", []byte{'c'}, nil)
	if err != nil {
		t.Errorf("Update(nil version) should have worked but got: %v", err)
	}

	// Get should work, get the right contents and version.
	contents, getVersion, err = conn.Get(ctx, "/myfile")
	if err != nil {
		t.Errorf("Get('/myfile') returned an error: %v", err)
	} else {
		if len(contents) != 1 || contents[0] != 'c' {
			t.Errorf("Get('/myfile') returned bad content: %v", contents)
		}
		if !reflect.DeepEqual(getVersion, newVersion) {
			t.Errorf("Get('/myfile') returned bad version: got %v expected %v", getVersion, newVersion)
		}
	}

	// Try to update again with empty content, should work.
	newVersion, err = conn.Update(ctx, "/myfile", nil, newVersion)
	if err != nil {
		t.Fatalf("Update(empty content) should have worked but got: %v", err)
	}
	contents, getVersion, err = conn.Get(ctx, "/myfile")
	if err != nil || len(contents) != 0 || !reflect.DeepEqual(getVersion, newVersion) {
		t.Errorf("Get('/myfile') expecting empty content got bad result: %v %v %v", contents, getVersion, err)
	}

	// Try to delete with wrong version, should fail.
	if err = conn.Delete(ctx, "/myfile", version); !topo.IsErrType(err, topo.BadVersion) {
		t.Errorf("Delete('/myfile', wrong version) returned bad error: %v", err)
	}

	// Now delete it.
	if err = conn.Delete(ctx, "/myfile", newVersion); err != nil {
		t.Fatalf("Delete('/myfile') failed: %v", err)
	}

	// ListDir root: nothing.
	expected = expected[:len(expected)-1]
	checkListDir(ctx, t, conn, "/", expected)

	// Try to delete again, should fail.
	if err = conn.Delete(ctx, "/myfile", newVersion); !topo.IsErrType(err, topo.NoNode) {
		t.Errorf("Delete(already gone) returned bad error: %v", err)
	}

	// Create again, with unconditional update.
	version, err = conn.Update(ctx, "/myfile", []byte{'d'}, nil)
	if err != nil {
		t.Fatalf("Update('/myfile', nil) failed: %v", err)
	}

	// Check contents.
	contents, getVersion, err = conn.Get(ctx, "/myfile")
	if err != nil {
		t.Errorf("Get('/myfile') returned an error: %v", err)
	} else {
		if len(contents) != 1 || contents[0] != 'd' {
			t.Errorf("Get('/myfile') returned bad content: %v", contents)
		}
		if !reflect.DeepEqual(getVersion, version) {
			t.Errorf("Get('/myfile') returned bad version: got %v expected %v", getVersion, version)
		}
	}

	// See it in the listing now.
	expected = append(expected, topo.DirEntry{
		Name: "myfile",
		Type: topo.TypeFile,
	})
	checkListDir(ctx, t, conn, "/", expected)

	// Unconditional delete.
	if err = conn.Delete(ctx, "/myfile", nil); err != nil {
		t.Errorf("Delete('/myfile', nil) failed: %v", err)
	}

	// ListDir root: nothing.
	expected = expected[:len(expected)-1]
	checkListDir(ctx, t, conn, "/", expected)
}

// checkList tests the file part of the Conn API.
func checkList(t *testing.T, ts *topo.Server) {
	ctx := context.Background()
	// global cell
	conn, err := ts.ConnForCell(ctx, LocalCellName)
	if err != nil {
		t.Fatalf("ConnForCell(LocalCellName) failed: %v", err)
	}

	_, err = conn.Create(ctx, "/some/arbitrary/file", []byte{'a'})
	if err != nil {
		t.Fatalf("Create('/myfile') failed: %v", err)
	}

	_, err = conn.List(ctx, "/")
	if topo.IsErrType(err, topo.NoImplementation) {
		// If this is not supported, skip the test
		t.Skipf("%T does not support List()", conn)
		return
	}
	if err != nil {
		t.Fatalf("List(test) failed: %v", err)
	}

	_, err = conn.Create(ctx, "/toplevel/nested/myfile", []byte{'a'})
	if err != nil {
		t.Fatalf("Create('/myfile') failed: %v", err)
	}

	for _, path := range []string{"/top", "/toplevel", "/toplevel/", "/toplevel/nes", "/toplevel/nested/myfile"} {
		entries, err := conn.List(ctx, path)
		if err != nil {
			t.Fatalf("List failed(path: %q): %v", path, err)
		}

		if len(entries) != 1 {
			t.Fatalf("List(test) returned incorrect number of elements for path %q. Expected 1, got %d: %v", path, len(entries), entries)
		}

		if !strings.HasSuffix(string(entries[0].Key), "/toplevel/nested/myfile") {
			t.Fatalf("found entry doesn't end with /toplevel/nested/myfile for path %q: %s", path, string(entries[0].Key))
		}

		if string(entries[0].Value) != "a" {
			t.Fatalf("found entry doesn't have value \"a\" for path %q: %s", path, string(entries[0].Value))
		}
	}
}
