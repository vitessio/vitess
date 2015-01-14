package main

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/zktopo"
)

func TestKnownCells(t *testing.T) {
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})

	kcc := newKnownCellsCache(ts)
	result, err := kcc.get()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var kc KnownCells
	if err := json.Unmarshal(result, &kc); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	if kc.Version != 1 {
		t.Fatalf("Got wrong initial version: %v", kc.Version)
	}
	if !reflect.DeepEqual(kc.Cells, []string{"cell1", "cell2"}) {
		t.Fatalf("Bad cells: %v", kc.Cells)
	}

	result2, err := kcc.get()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(result, result2) {
		t.Fatalf("Bad content from cache: %v != %v", string(result), string(result2))
	}

	// force a re-get with same content, version shouldn't change
	kcc.timestamp = time.Time{}
	result2, err = kcc.get()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(result, result2) {
		t.Fatalf("Bad content from cache: %v != %v", string(result), string(result2))
	}

	// force a reget with different content, version should change
	kcc.timestamp = time.Time{}
	kcc.knownCells.Cells = []string{"cell1"}
	result, err = kcc.get()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := json.Unmarshal(result, &kc); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	if kc.Version != 2 {
		t.Fatalf("Got wrong second version: %v", kc.Version)
	}
	if !reflect.DeepEqual(kc.Cells, []string{"cell1", "cell2"}) {
		t.Fatalf("Bad cells: %v", kc.Cells)
	}
}
