// package test contains utilities to test topo.Server
// implementations. If you are testing your implementation, you will
// want to call CheckAll in your test method. For an example, look at
// the tests in github.com/youtube/vitess/go/vt/zktopo.
package test

import (
	"testing"

	"github.com/youtube/vitess/go/vt/topo"
)

func CheckKeyspace(t *testing.T, ts topo.Server) {
	keyspaces, err := ts.GetKeyspaces()
	if err != nil {
		t.Errorf("GetKeyspaces(empty): %v", err)
	}
	if len(keyspaces) != 0 {
		t.Errorf("len(GetKeyspaces()) != 0: %v", keyspaces)
	}

	if err := ts.CreateKeyspace("test_keyspace"); err != nil {
		t.Errorf("CreateKeyspace: %v", err)
	}
	if err := ts.CreateKeyspace("test_keyspace"); err != topo.ErrNodeExists {
		t.Errorf("CreateKeyspace(again) is not ErrNodeExists: %v", err)
	}

	keyspaces, err = ts.GetKeyspaces()
	if err != nil {
		t.Errorf("GetKeyspaces: %v", err)
	}
	if len(keyspaces) != 1 || keyspaces[0] != "test_keyspace" {
		t.Errorf("GetKeyspaces: want %v, got %v", []string{"test_keyspace"}, keyspaces)
	}

	if err := ts.CreateKeyspace("test_keyspace2"); err != nil {
		t.Errorf("CreateKeyspace: %v", err)
	}
	keyspaces, err = ts.GetKeyspaces()
	if err != nil {
		t.Errorf("GetKeyspaces: %v", err)
	}
	if len(keyspaces) != 2 || keyspaces[0] != "test_keyspace" || keyspaces[1] != "test_keyspace2" {
		t.Errorf("GetKeyspaces: want %v, got %v", []string{"test_keyspace", "test_keyspace2"}, keyspaces)
	}
}
