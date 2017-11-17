/*
Copyright 2017 Google Inc.

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

	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestTee(t *testing.T) {
	ctx := context.Background()

	// create the setup, copy the data
	fromTS, toTS := createSetup(ctx, t)
	fromTTS := topo.Server{Impl: fromTS}
	toTTS := topo.Server{Impl: toTS}
	CopyKeyspaces(ctx, fromTTS, toTTS)
	CopyShards(ctx, fromTS, toTS)
	CopyTablets(ctx, fromTS, toTS)

	// create a tee and check it implements the interface
	tee := NewTee(fromTS, toTS, true)
	teeTTS := topo.Server{Impl: tee}

	// create a keyspace, make sure it is on both sides
	if err := teeTTS.CreateKeyspace(ctx, "keyspace2", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("tee.CreateKeyspace(keyspace2) failed: %v", err)
	}
	teeKeyspaces, err := teeTTS.GetKeyspaces(ctx)
	if err != nil {
		t.Fatalf("tee.GetKeyspaces() failed: %v", err)
	}
	expected := []string{"keyspace2", "test_keyspace"}
	if !reflect.DeepEqual(expected, teeKeyspaces) {
		t.Errorf("teeKeyspaces mismatch, got %+v, want %+v", teeKeyspaces, expected)
	}
	fromKeyspaces, err := fromTTS.GetKeyspaces(ctx)
	if err != nil {
		t.Fatalf("fromTS.GetKeyspaces() failed: %v", err)
	}
	expected = []string{"keyspace2", "test_keyspace"}
	if !reflect.DeepEqual(expected, fromKeyspaces) {
		t.Errorf("fromKeyspaces mismatch, got %+v, want %+v", fromKeyspaces, expected)
	}
	toKeyspaces, err := toTTS.GetKeyspaces(ctx)
	if err != nil {
		t.Fatalf("toTS.GetKeyspaces() failed: %v", err)
	}
	expected = []string{"keyspace2", "test_keyspace"}
	if !reflect.DeepEqual(expected, toKeyspaces) {
		t.Errorf("toKeyspaces mismatch, got %+v, want %+v", toKeyspaces, expected)
	}
}
