package test

import (
	"reflect"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

// checkFile tests the file part of the Backend API.
// It does not use the pre-Backend API paths, to really
// test the new functions.
func checkFile(t *testing.T, ts topo.Impl) {
	ctx := context.Background()

	// global cell
	checkFileInCell(t, ts, topo.GlobalCell)

	// local cell
	cell := getLocalCell(ctx, t, ts)
	checkFileInCell(t, ts, cell)
}

func checkFileInCell(t *testing.T, ts topo.Impl, cell string) {
	t.Logf("===   checkFileInCell %v", cell)
	ctx := context.Background()

	// ListDir root: nothing.
	checkListDir(ctx, t, ts, cell, "/", nil)

	// Get with no file -> ErrNoNode.
	contents, version, err := ts.Get(ctx, cell, "/myfile")
	if err != topo.ErrNoNode {
		t.Errorf("Get(non-existent) didn't return ErrNoNode but: %v", err)
	}

	// Create a file.
	version, err = ts.Create(ctx, cell, "/myfile", []byte{'a'})
	if err != nil {
		t.Fatalf("Create('/myfile') failed: %v", err)
	}

	// See it in the listing now.
	checkListDir(ctx, t, ts, cell, "/", []string{"myfile"})

	// Get should work, get the right contents and version.
	contents, getVersion, err := ts.Get(ctx, cell, "/myfile")
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
	newVersion, err := ts.Update(ctx, cell, "/myfile", []byte{'b'}, version)
	if err != nil {
		t.Fatalf("Update('/myfile') failed: %v", err)
	}
	if reflect.DeepEqual(version, newVersion) {
		t.Errorf("Version didn't change, stayed %v", newVersion)
	}

	// Get should work, get the right contents and version.
	contents, getVersion, err = ts.Get(ctx, cell, "/myfile")
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
	if _, err = ts.Update(ctx, cell, "/myfile", []byte{'b'}, version); err != topo.ErrBadVersion {
		t.Errorf("Update(bad version) didn't return ErrBadVersion but: %v", err)
	}

	// Try to update again with nil version, should work.
	newVersion, err = ts.Update(ctx, cell, "/myfile", []byte{'c'}, nil)
	if err != nil {
		t.Errorf("Update(nil version) should have worked but got: %v", err)
	}

	// Get should work, get the right contents and version.
	contents, getVersion, err = ts.Get(ctx, cell, "/myfile")
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
	newVersion, err = ts.Update(ctx, cell, "/myfile", nil, newVersion)
	if err != nil {
		t.Fatalf("Update(empty content) should have worked but got: %v", err)
	}
	contents, getVersion, err = ts.Get(ctx, cell, "/myfile")
	if err != nil || len(contents) != 0 || !reflect.DeepEqual(getVersion, newVersion) {
		t.Errorf("Get('/myfile') expecting empty content got bad result: %v %v %v", contents, getVersion, err)
	}

	// Try to delete with wrong version, should fail.
	if err = ts.Delete(ctx, cell, "/myfile", version); err != topo.ErrBadVersion {
		t.Errorf("Delete('/myfile', wrong version) returned bad error: %v", err)
	}

	// Now delete it.
	if err = ts.Delete(ctx, cell, "/myfile", newVersion); err != nil {
		t.Fatalf("Delete('/myfile') failed: %v", err)
	}

	// ListDir root: nothing.
	checkListDir(ctx, t, ts, cell, "/", nil)

	// Try to delete again, should fail.
	if err = ts.Delete(ctx, cell, "/myfile", newVersion); err != topo.ErrNoNode {
		t.Errorf("Delete(already gone) returned bad error: %v", err)
	}

	// Create again, with unconditional update.
	version, err = ts.Update(ctx, cell, "/myfile", []byte{'d'}, nil)
	if err != nil {
		t.Fatalf("Update('/myfile', nil) failed: %v", err)
	}

	// Check contents.
	contents, getVersion, err = ts.Get(ctx, cell, "/myfile")
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
	checkListDir(ctx, t, ts, cell, "/", []string{"myfile"})

	// Unconditional delete.
	if err = ts.Delete(ctx, cell, "/myfile", nil); err != nil {
		t.Errorf("Delete('/myfile', nil) failed: %v", err)
	}

	// ListDir root: nothing.
	checkListDir(ctx, t, ts, cell, "/", nil)
}
