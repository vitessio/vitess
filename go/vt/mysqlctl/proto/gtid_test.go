// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/bson"
)

func TestParseGTID(t *testing.T) {
	flavor := "fake flavor"
	gtidParsers[flavor] = func(s string) (GTID, error) {
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

func TestMustParseGTID(t *testing.T) {
	flavor := "fake flavor"
	gtidParsers[flavor] = func(s string) (GTID, error) {
		return fakeGTID{value: s}, nil
	}
	input := "12345"
	want := fakeGTID{value: "12345"}

	got := MustParseGTID(flavor, input)
	if got != want {
		t.Errorf("ParseGTID(%#v, %#v) = %#v, want %#v", flavor, input, got, want)
	}
}

func TestMustParseGTIDError(t *testing.T) {
	defer func() {
		want := "ParseGTID: unknown flavor"
		err := recover()
		if err == nil {
			t.Errorf("wrong error, got %#v, want %#v", err, want)
		}
		got, ok := err.(error)
		if !ok || !strings.HasPrefix(got.Error(), want) {
			t.Errorf("wrong error, got %#v, want %#v", got, want)
		}
	}()

	MustParseGTID("unknown flavor !@$!@", "yowzah")
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
	gtidParsers["flavorflav"] = func(s string) (GTID, error) {
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

func TestEncodeNilGTID(t *testing.T) {
	input := GTID(nil)
	want := ""

	if got := EncodeGTID(input); got != want {
		t.Errorf("EncodeGTID(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestDecodeNilGTID(t *testing.T) {
	input := ""
	want := GTID(nil)

	got, err := DecodeGTID(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("DecodeGTID(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestDecodeNoFlavor(t *testing.T) {
	gtidParsers[""] = func(s string) (GTID, error) {
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
	gtidParsers["moobar"] = func(s string) (GTID, error) {
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

func TestGTIDFieldString(t *testing.T) {
	input := GTIDField{fakeGTID{flavor: "gahgah", value: "googoo"}}
	want := "googoo"
	if got := input.String(); got != want {
		t.Errorf("%#v.String() = %#v, want %#v", input, got, want)
	}
}

func TestGTIDFieldStringNil(t *testing.T) {
	input := GTIDField{nil}
	want := "<nil>"
	if got := input.String(); got != want {
		t.Errorf("%#v.String() = %#v, want %#v", input, got, want)
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
	gtidParsers["golf"] = func(s string) (GTID, error) {
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
	gtidParsers["golf"] = func(s string) (GTID, error) {
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
	gtidParsers["golf"] = func(s string) (GTID, error) {
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

func TestBsonMarshalUnmarshalNilGTID(t *testing.T) {
	gtidParsers["golf"] = func(s string) (GTID, error) {
		return fakeGTID{flavor: "golf", value: s}, nil
	}
	input := GTID(nil)
	want := GTID(nil)

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

func TestJsonMarshalGTIDField(t *testing.T) {
	input := GTIDField{fakeGTID{flavor: "golf", value: "par"}}
	want := `"golf/par"`

	buf, err := json.Marshal(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if got := string(buf); got != want {
		t.Errorf("json.Marshal(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestJsonMarshalGTIDFieldPointer(t *testing.T) {
	input := GTIDField{fakeGTID{flavor: "golf", value: "par"}}
	want := `"golf/par"`

	buf, err := json.Marshal(&input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if got := string(buf); got != want {
		t.Errorf("json.Marshal(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestJsonUnmarshalGTIDField(t *testing.T) {
	gtidParsers["golf"] = func(s string) (GTID, error) {
		return fakeGTID{flavor: "golf", value: s}, nil
	}
	input := `"golf/par"`
	want := GTIDField{fakeGTID{flavor: "golf", value: "par"}}

	var got GTIDField
	err := json.Unmarshal([]byte(input), &got)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("json.Unmarshal(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestJsonMarshalGTIDFieldInStruct(t *testing.T) {
	input := GTIDField{fakeGTID{flavor: "golf", value: "par"}}
	want := `{"GTID":"golf/par"}`

	type mystruct struct {
		GTID GTIDField
	}

	buf, err := json.Marshal(&mystruct{input})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if got := string(buf); got != want {
		t.Errorf("json.Marshal(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestJsonUnmarshalGTIDFieldInStruct(t *testing.T) {
	gtidParsers["golf"] = func(s string) (GTID, error) {
		return fakeGTID{flavor: "golf", value: s}, nil
	}
	input := `{"GTID":"golf/par"}`
	want := GTIDField{fakeGTID{flavor: "golf", value: "par"}}

	var gotStruct struct {
		GTID GTIDField
	}
	err := json.Unmarshal([]byte(input), &gotStruct)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if got := gotStruct.GTID; got != want {
		t.Errorf("json.Unmarshal(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestJsonMarshalNilGTID(t *testing.T) {
	input := GTIDField{nil}
	want := `""`

	buf, err := json.Marshal(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if got := string(buf); got != want {
		t.Errorf("json.Marshal(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestJsonUnmarshalNilGTID(t *testing.T) {
	input := `""`
	want := GTIDField{nil}

	var got GTIDField
	err := json.Unmarshal([]byte(input), &got)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("json.Unmarshal(%#v) = %#v, want %#v", input, got, want)
	}
}

type fakeGTID struct {
	flavor, value string
}

func (f fakeGTID) String() string             { return f.value }
func (f fakeGTID) Flavor() string             { return f.flavor }
func (fakeGTID) TryCompare(GTID) (int, error) { return 0, nil }
