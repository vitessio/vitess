// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package helpers

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestTee(t *testing.T) {
	ctx := context.Background()

	// create the setup, copy the data
	fromTS, toTS := createSetup(ctx, t)
	CopyKeyspaces(ctx, fromTS, toTS)
	CopyShards(ctx, fromTS, toTS)
	CopyTablets(ctx, fromTS, toTS)

	// create a tee and check it implements the interface
	tee := NewTee(fromTS, toTS, true)
	var _ topo.Impl = tee

	// create a keyspace, make sure it is on both sides
	if err := tee.CreateKeyspace(ctx, "keyspace2", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("tee.CreateKeyspace(keyspace2) failed: %v", err)
	}
	teeKeyspaces, err := tee.GetKeyspaces(ctx)
	if err != nil {
		t.Fatalf("tee.GetKeyspaces() failed: %v", err)
	}
	expected := []string{"keyspace2", "test_keyspace"}
	if !reflect.DeepEqual(expected, teeKeyspaces) {
		t.Errorf("teeKeyspaces mismatch, got %+v, want %+v", teeKeyspaces, expected)
	}
	fromKeyspaces, err := fromTS.GetKeyspaces(ctx)
	if err != nil {
		t.Fatalf("fromTS.GetKeyspaces() failed: %v", err)
	}
	expected = []string{"keyspace2", "test_keyspace"}
	if !reflect.DeepEqual(expected, fromKeyspaces) {
		t.Errorf("fromKeyspaces mismatch, got %+v, want %+v", fromKeyspaces, expected)
	}
	toKeyspaces, err := toTS.GetKeyspaces(ctx)
	if err != nil {
		t.Fatalf("toTS.GetKeyspaces() failed: %v", err)
	}
	expected = []string{"keyspace2", "test_keyspace"}
	if !reflect.DeepEqual(expected, toKeyspaces) {
		t.Errorf("toKeyspaces mismatch, got %+v, want %+v", toKeyspaces, expected)
	}
}
