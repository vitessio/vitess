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

package helpers

import (
	"reflect"
	"testing"

	"context"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestTee(t *testing.T) {
	ctx := context.Background()

	// create the setup, copy the data
	fromTS, toTS := createSetup(ctx, t)
	CopyKeyspaces(ctx, fromTS, toTS)
	CopyShards(ctx, fromTS, toTS)
	CopyTablets(ctx, fromTS, toTS)

	// create a tee and check it implements the interface.
	teeTS, err := NewTee(fromTS, toTS, true)

	// create a keyspace, make sure it is on both sides
	if err := teeTS.CreateKeyspace(ctx, "keyspace2", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("tee.CreateKeyspace(keyspace2) failed: %v", err)
	}
	teeKeyspaces, err := teeTS.GetKeyspaces(ctx)
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

	// Read the keyspace from the tee, update it, and make sure
	// both sides have the updated value.
	lockCtx, unlock, err := teeTS.LockKeyspace(ctx, "test_keyspace", "fake-action")
	if err != nil {
		t.Fatalf("LockKeyspaceForAction: %v", err)
	}
	ki, err := teeTS.GetKeyspace(ctx, "test_keyspace")
	if err != nil {
		t.Fatalf("tee.GetKeyspace(test_keyspace) failed: %v", err)
	}
	ki.Keyspace.ShardingColumnName = "toChangeIt"
	if err := teeTS.UpdateKeyspace(lockCtx, ki); err != nil {
		t.Fatalf("tee.UpdateKeyspace(test_keyspace) failed: %v", err)
	}
	unlock(&err)
	if err != nil {
		t.Fatalf("unlock(test_keyspace): %v", err)
	}

	fromKi, err := fromTS.GetKeyspace(ctx, "test_keyspace")
	if err != nil || fromKi.Keyspace.ShardingColumnName != "toChangeIt" {
		t.Errorf("invalid keyspace data in fromTTS: %v %v", fromKi, err)
	}
	toKi, err := toTS.GetKeyspace(ctx, "test_keyspace")
	if err != nil || toKi.Keyspace.ShardingColumnName != "toChangeIt" {
		t.Errorf("invalid keyspace data in toTTS: %v %v", toKi, err)
	}
}
