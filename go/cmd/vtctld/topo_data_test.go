package main

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/zktopo"
)

func TestKnownCellsCache(t *testing.T) {
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	kcc := newKnownCellsCache(ts)
	var kc KnownCells
	expectedKc := KnownCells{
		Cells: []string{"cell1", "cell2"},
	}

	testVersionedObjectCache(t, kcc, &kc, &expectedKc)
}

func TestKeyspacesCache(t *testing.T) {
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	if err := ts.CreateKeyspace("ks1", &topo.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}
	if err := ts.CreateKeyspace("ks2", &topo.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}
	kc := newKeyspacesCache(ts)
	var k Keyspaces
	expectedK := Keyspaces{
		Keyspaces: []string{"ks1", "ks2"},
	}

	testVersionedObjectCache(t, kc, &k, &expectedK)
}

func testVersionedObjectCache(t *testing.T, voc *VersionedObjectCache, vo VersionedObject, expectedVO VersionedObject) {
	result, err := voc.Get()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := json.Unmarshal(result, vo); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	if vo.GetVersion() != 1 {
		t.Fatalf("Got wrong initial version: %v", vo.GetVersion())
	}
	expectedVO.SetVersion(1)
	if !reflect.DeepEqual(vo, expectedVO) {
		t.Fatalf("Got bad result: %v expected: %v", vo, expectedVO)
	}

	result2, err := voc.Get()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(result, result2) {
		t.Fatalf("Bad content from cache: %v != %v", string(result), string(result2))
	}

	// force a re-get with same content, version shouldn't change
	voc.timestamp = time.Time{}
	result2, err = voc.Get()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(result, result2) {
		t.Fatalf("Bad content from cache: %v != %v", string(result), string(result2))
	}

	// force a reget with different content, version should change
	voc.timestamp = time.Time{}
	voc.versionedObject.Reset() // poking inside the object here
	result, err = voc.Get()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := json.Unmarshal(result, vo); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	if vo.GetVersion() != 2 {
		t.Fatalf("Got wrong second version: %v", vo.GetVersion())
	}
	expectedVO.SetVersion(2)
	if !reflect.DeepEqual(vo, expectedVO) {
		t.Fatalf("Got bad result: %v expected: %v", vo, expectedVO)
	}

	// force a flush and see the version increase again
	voc.Flush()
	result, err = voc.Get()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := json.Unmarshal(result, vo); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	if vo.GetVersion() != 3 {
		t.Fatalf("Got wrong third version: %v", vo.GetVersion())
	}
	expectedVO.SetVersion(3)
	if !reflect.DeepEqual(vo, expectedVO) {
		t.Fatalf("Got bad result: %v expected: %v", vo, expectedVO)
	}
}
