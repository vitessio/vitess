// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"strings"
	"testing"

	"github.com/youtube/vitess/go/bson"
)

func TestParseGTID(t *testing.T) {
	flavor := "fake flavor"
	GTIDParsers[flavor] = func(s string) (GTID, error) {
		return fakeGTID{value: s}, nil
	}
	input := "12345"
	want := fakeGTID{value: "12345"}

	got, err := ParseGTID(flavor, input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("ParseGTID(%#v, %#v) = %#v, want %#v", flavor, input, got, want)
	}
}

func TestParseUnknownFlavor(t *testing.T) {
	want := "ParseGTID: unknown flavor 'foobar8675309'"

	_, err := ParseGTID("foobar8675309", "foo")
	if !strings.HasPrefix(err.Error(), want) {
		t.Errorf("wrong error, got '%v', want '%v'", err, want)
	}
}

func TestEncodeGTID(t *testing.T) {
	input := fakeGTID{
		flavor: "myflav",
		value:  "1:2:3-4-5-6",
	}
	want := "myflav/1:2:3-4-5-6"

	if got := EncodeGTID(input); got != want {
		t.Errorf("EncodeGTID(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestDecodeGTID(t *testing.T) {
	GTIDParsers["flavorflav"] = func(s string) (GTID, error) {
		return fakeGTID{value: s}, nil
	}
	input := "flavorflav/123-456:789"
	want := fakeGTID{value: "123-456:789"}

	got, err := DecodeGTID(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("DecodeGTID(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestDecodeNoFlavor(t *testing.T) {
	GTIDParsers[""] = func(s string) (GTID, error) {
		return fakeGTID{value: s}, nil
	}
	input := "12345"
	want := fakeGTID{value: "12345"}

	got, err := DecodeGTID(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("DecodeGTID(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestDecodeGTIDWithSeparator(t *testing.T) {
	GTIDParsers["moobar"] = func(s string) (GTID, error) {
		return fakeGTID{value: s}, nil
	}
	input := "moobar/GTID containing / a slash"
	want := fakeGTID{value: "GTID containing / a slash"}

	got, err := DecodeGTID(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("DecodeGTID(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestGTIDFieldEqual(t *testing.T) {
	input1 := GTIDField{fakeGTID{flavor: "poo", value: "bah"}}
	input2 := GTIDField{fakeGTID{flavor: "poo", value: "bah"}}
	want := true

	if got := (input1 == input2); got != want {
		t.Errorf("(%#v == %#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestGTIDFieldNotEqual(t *testing.T) {
	input1 := GTIDField{fakeGTID{flavor: "poo", value: "bah"}}
	input2 := GTIDField{fakeGTID{flavor: "foo", value: "bah"}}
	want := false

	if got := (input1 == input2); got != want {
		t.Errorf("(%#v == %#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestBsonMarshalUnmarshalGTIDField(t *testing.T) {
	GTIDParsers["golf"] = func(s string) (GTID, error) {
		return fakeGTID{flavor: "golf", value: s}, nil
	}
	input := fakeGTID{flavor: "golf", value: "par"}
	want := fakeGTID{flavor: "golf", value: "par"}

	buf, err := bson.Marshal(GTIDField{input})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	var gotField GTIDField
	if err = bson.Unmarshal(buf, &gotField); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if got := gotField.GTID; got != want {
		t.Errorf("marshal->unmarshal mismatch, got %#v, want %#v", got, want)
	}
}

func TestBsonMarshalUnmarshalGTIDFieldPointer(t *testing.T) {
	GTIDParsers["golf"] = func(s string) (GTID, error) {
		return fakeGTID{flavor: "golf", value: s}, nil
	}
	input := fakeGTID{flavor: "golf", value: "par"}
	want := fakeGTID{flavor: "golf", value: "par"}

	buf, err := bson.Marshal(&GTIDField{input})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	var gotField GTIDField
	if err = bson.Unmarshal(buf, &gotField); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if got := gotField.GTID; got != want {
		t.Errorf("marshal->unmarshal mismatch, got %#v, want %#v", got, want)
	}
}

func TestBsonMarshalUnmarshalGTIDFieldInStruct(t *testing.T) {
	GTIDParsers["golf"] = func(s string) (GTID, error) {
		return fakeGTID{flavor: "golf", value: s}, nil
	}
	input := fakeGTID{flavor: "golf", value: "par"}
	want := fakeGTID{flavor: "golf", value: "par"}

	type mystruct struct {
		GTIDField
	}

	buf, err := bson.Marshal(&mystruct{GTIDField{input}})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	var gotStruct mystruct
	if err = bson.Unmarshal(buf, &gotStruct); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if got := gotStruct.GTIDField.GTID; got != want {
		t.Errorf("marshal->unmarshal mismatch, got %#v, want %#v", got, want)
	}
}

type fakeGTID struct {
	flavor, value string
}

func (f fakeGTID) String() string             { return f.value }
func (f fakeGTID) Flavor() string             { return f.flavor }
func (fakeGTID) TryCompare(GTID) (int, error) { return 0, nil }
