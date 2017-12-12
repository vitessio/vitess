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
	contents, version, err := conn.Get(ctx, "/myfile")
	if err != topo.ErrNoNode {
		t.Errorf("Get(non-existent) didn't return ErrNoNode but: %v", err)
	}

	// Create a file.
	version, err = conn.Create(ctx, "/myfile", []byte{'a'})
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
	if _, err = conn.Update(ctx, "/myfile", []byte{'b'}, version); err != topo.ErrBadVersion {
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
	if err = conn.Delete(ctx, "/myfile", version); err != topo.ErrBadVersion {
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
	if err = conn.Delete(ctx, "/myfile", newVersion); err != topo.ErrNoNode {
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
