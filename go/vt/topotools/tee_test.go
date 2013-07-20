// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topotools

import (
	"testing"

	//	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"

//	"github.com/youtube/vitess/go/vt/zktopo"
//	"github.com/youtube/vitess/go/zk"
//	"github.com/youtube/vitess/go/zk/fakezk"
//	"launchpad.net/gozk/zookeeper"
)

func testStringArrays(t *testing.T, expected, value []string) {
	if len(expected) != len(value) {
		t.Fatalf("Different arrays:\n%v\n%v", expected, value)
	}
	for i, e := range expected {
		if e != value[i] {
			t.Fatalf("Different arrays:\n%v\n%v", expected, value)
		}
	}
}

func TestTee(t *testing.T) {

	// create the setup, copy the data
	fromTS, toTS := createSetup(t)
	CopyKeyspaces(fromTS, toTS)
	CopyShards(fromTS, toTS, true)
	CopyTablets(fromTS, toTS)

	// create a tee and check it implements the interface
	tee := NewTee(fromTS, toTS, true)
	var _ topo.Server = tee

	// create a keyspace, make sure it is on both sides
	if err := tee.CreateKeyspace("keyspace2"); err != nil {
		t.Fatalf("tee.CreateKeyspace(keyspace2) failed: %v", err)
	}
	teeKeyspaces, err := tee.GetKeyspaces()
	if err != nil {
		t.Fatalf("tee.GetKeyspaces() failed: %v", err)
	}
	testStringArrays(t, []string{"keyspace2", "test_keyspace"}, teeKeyspaces)
	fromKeyspaces, err := fromTS.GetKeyspaces()
	if err != nil {
		t.Fatalf("fromTS.GetKeyspaces() failed: %v", err)
	}
	testStringArrays(t, []string{"keyspace2", "test_keyspace"}, fromKeyspaces)
	toKeyspaces, err := toTS.GetKeyspaces()
	if err != nil {
		t.Fatalf("toTS.GetKeyspaces() failed: %v", err)
	}
	testStringArrays(t, []string{"keyspace2", "test_keyspace"}, toKeyspaces)

}
