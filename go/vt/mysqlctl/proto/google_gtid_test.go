// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"strings"
	"testing"
)

func TestParseGoogleGTID(t *testing.T) {
	input := "1758283"
	want := GoogleGTID{GroupID: 1758283}

	got, err := parseGoogleGTID(input)
	if err != nil {
		t.Errorf("%v", err)
	}
	if got.(GoogleGTID) != want {
		t.Errorf("ParseGTID(%v) = %v, want %v", input, got, want)
	}
}

func TestParseInvalidGoogleGTID(t *testing.T) {
	input := "1-2-3"
	want := "invalid Google MySQL group_id"

	_, err := parseGoogleGTID(input)
	if err == nil {
		t.Errorf("expected error for invalid input (%v)", input)
	}
	if !strings.HasPrefix(err.Error(), want) {
		t.Errorf("wrong error message, got '%v', want '%v'", err, want)
	}
}

func TestGoogleGTIDString(t *testing.T) {
	input := GoogleGTID{GroupID: 1857273}
	want := "1857273"

	got := input.String()
	if got != want {
		t.Errorf("%#v.String() = '%v', want '%v'", input, got, want)
	}
}

func TestGoogleGTIDFlavor(t *testing.T) {
	input := GoogleGTID{GroupID: 123}
	want := "GoogleMysql"

	got := input.Flavor()
	if got != want {
		t.Errorf("%#v.Flavor() = '%v', want '%v'", input, got, want)
	}
}

func TestGoogleGTIDCompareLess(t *testing.T) {
	input1 := GoogleGTID{GroupID: 12345}
	input2 := GoogleGTID{GroupID: 54321}

	cmp, err := input1.TryCompare(input2)
	if err != nil {
		t.Errorf("unexpected error for %#v.TryCompare(%#v): %v", input1, input2, err)
	}
	if !(cmp < 0) {
		t.Errorf("%#v.TryCompare(%#v) = %v, want < 0", input1, input2, cmp)
	}
}

func TestGoogleGTIDCompareGreater(t *testing.T) {
	input1 := GoogleGTID{GroupID: 98765}
	input2 := GoogleGTID{GroupID: 56789}

	cmp, err := input1.TryCompare(input2)
	if err != nil {
		t.Errorf("unexpected error for %#v.TryCompare(%#v): %v", input2, input1, err)
	}
	if !(cmp > 0) {
		t.Errorf("%#v.TryCompare(%#v) = %v, want > 0", input2, input1, cmp)
	}
}

func TestGoogleGTIDCompareEqual(t *testing.T) {
	input1 := GoogleGTID{GroupID: 41234}
	input2 := GoogleGTID{GroupID: 41234}

	cmp, err := input1.TryCompare(input2)
	if err != nil {
		t.Errorf("unexpected error for %#v.TryCompare(%#v): %v", input1, input2, err)
	}
	if cmp != 0 {
		t.Errorf("%#v.TryCompare(%#v) = %v, want 0", input1, input2, cmp)
	}
}

func TestGoogleGTIDCompareWrongType(t *testing.T) {
	input1 := GoogleGTID{GroupID: 123}
	input2 := fakeGTID{}
	want := "can't compare GTID, wrong type"

	_, err := input1.TryCompare(input2)
	if err == nil {
		t.Errorf("expected error for %#v.TryCompare(%#v)", input1, input2)
	}
	if !strings.HasPrefix(err.Error(), want) {
		t.Errorf("wrong error message for %#v.TryCompare(%#v), got %v, want %v", input1, input2, err, want)
	}
}

func TestGoogleGTIDCompareNil(t *testing.T) {
	input1 := GoogleGTID{GroupID: 123}
	input2 := GTID(nil)
	want := "can't compare GTID"

	_, err := input1.TryCompare(input2)
	if err == nil {
		t.Errorf("expected error for %#v.TryCompare(%#v)", input1, input2)
	}
	if !strings.HasPrefix(err.Error(), want) {
		t.Errorf("wrong error message for %#v.TryCompare(%#v), got %v, want %v", input1, input2, err, want)
	}
}

func TestGoogleGTIDEqual(t *testing.T) {
	input1 := GTID(GoogleGTID{GroupID: 41234})
	input2 := GTID(GoogleGTID{GroupID: 41234})
	want := true

	cmp := input1 == input2
	if cmp != want {
		t.Errorf("(%#v == %#v) = %v, want %v", input1, input2, cmp, want)
	}
}

func TestGoogleGTIDNotEqual(t *testing.T) {
	input1 := GTID(GoogleGTID{GroupID: 41234})
	input2 := GTID(GoogleGTID{GroupID: 51234})
	want := false

	cmp := input1 == input2
	if cmp != want {
		t.Errorf("(%#v == %#v) = %v, want %v", input1, input2, cmp, want)
	}
}
