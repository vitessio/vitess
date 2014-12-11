// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package rpc contains RPC-related structs shared between many components.
package rpc

import (
	"testing"

	"github.com/henryanand/vitess/go/bson"
)

func TestUnmarshalStringIntoUnused(t *testing.T) {
	// Previous versions would marshal a naked empty string for unused args.
	str := ""
	buf, err := bson.Marshal(&str)
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}

	// Check that a new-style server expecting Unused{} can handle a naked empty
	// string sent by an old-style client.
	var unused Unused
	if err := bson.Unmarshal(buf, &unused); err != nil {
		t.Errorf("bson.Unmarshal: %v", err)
	}
}

func TestUnmarshalEmptyStructIntoUnused(t *testing.T) {
	buf, err := bson.Marshal(&struct{}{})
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}

	// Check that a new-style server expecting Unused{} can handle an empty struct.
	var unused Unused
	if err := bson.Unmarshal(buf, &unused); err != nil {
		t.Errorf("bson.Unmarshal: %v", err)
	}
}

func TestUnmarshalStructIntoUnused(t *testing.T) {
	buf, err := bson.Marshal(&struct{ A, B string }{"A", "B"})
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}

	// Check that a new-style server expecting Unused{} can handle an actual
	// struct being sent.
	var unused Unused
	if err := bson.Unmarshal(buf, &unused); err != nil {
		t.Errorf("bson.Unmarshal: %v", err)
	}
}

func TestUnmarshalUnusedIntoString(t *testing.T) {
	buf, err := bson.Marshal(&Unused{})
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}

	// Check that it's safe for new clients to send Unused{} to an old server
	// expecting the naked empty string convention.
	var str string
	if err := bson.Unmarshal(buf, &str); err != nil {
		t.Errorf("bson.Unmarshal: %v", err)
	}
}

func TestUnmarshalStructIntoString(t *testing.T) {
	buf, err := bson.Marshal(&struct{ A, B string }{"A", "B"})
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}

	// This fails. That's why you can't upgrade a method from unused (either old
	// or new style) to a real struct with public fields, if there are still
	// servers around that expect old-style string.
	var str string
	if err := bson.Unmarshal(buf, &str); err == nil {
		t.Errorf("expected error from bson.Unmarshal, got none")
	}
}

func TestUnmarshalStringIntoStruct(t *testing.T) {
	str := ""
	buf, err := bson.Marshal(&str)
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}

	// This fails. That's why you can't have old-style (empty string) clients
	// talking to servers that already expect a real struct (not Unused).
	var out struct{ A, B string }
	if err := bson.Unmarshal(buf, &out); err == nil {
		t.Errorf("expected error from bson.Unmarshal, got none")
	}
}

func TestUnmarshalEmptyStructIntoStruct(t *testing.T) {
	buf, err := bson.Marshal(&struct{}{})
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}

	// It should always be possible to add fields to something that's already a
	// struct. The struct name is irrelevant since it's never encoded.
	var out struct{ A, B string }
	if err := bson.Unmarshal(buf, &out); err != nil {
		t.Errorf("bson.Unmarshal: %v", err)
	}
}
