// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"strings"
	"testing"
)

func TestParseGoogleGTID(t *testing.T) {
	input := "41983-1758283"
	want := GoogleGTID{ServerID: 41983, GroupID: 1758283}

	got, err := parseGoogleGTID(input)
	if err != nil {
		t.Errorf("%v", err)
	}
	if got.(GoogleGTID) != want {
		t.Errorf("parseGoogleGTID(%v) = %v, want %v", input, got, want)
	}
}

func TestParseGoogleGTIDSet(t *testing.T) {
	input := "41983-1758283"
	want := GoogleGTID{ServerID: 41983, GroupID: 1758283}

	got, err := parseGoogleGTIDSet(input)
	if err != nil {
		t.Errorf("%v", err)
	}
	if got.(GoogleGTID) != want {
		t.Errorf("parseGoogleGTIDSet(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestParseInvalidGoogleGTIDSet(t *testing.T) {
	input := "12-3456d78"
	want := "invalid Google MySQL group_id"

	_, err := parseGoogleGTIDSet(input)
	if err == nil {
		t.Errorf("expected error for invalid input (%#v)", input)
		return
	}
	if got := err.Error(); !strings.HasPrefix(got, want) {
		t.Errorf("parseGoogleGTIDSet(%#v) error = %#v, want %#v", input, got, want)
	}
}

func TestParseInvalidGoogleGTID(t *testing.T) {
	input := "12345"
	want := "invalid Google MySQL GTID"

	_, err := parseGoogleGTID(input)
	if err == nil {
		t.Errorf("expected error for invalid input (%v)", input)
	}
	if !strings.HasPrefix(err.Error(), want) {
		t.Errorf("wrong error message, got '%v', want '%v'", err, want)
	}
}

func TestParseInvalidGoogleServerID(t *testing.T) {
	input := "1d3-45"
	want := "invalid Google MySQL server_id"

	_, err := parseGoogleGTID(input)
	if err == nil {
		t.Errorf("expected error for invalid input (%v)", input)
	}
	if !strings.HasPrefix(err.Error(), want) {
		t.Errorf("wrong error message, got '%v', want '%v'", err, want)
	}
}

func TestParseInvalidGoogleGroupID(t *testing.T) {
	input := "1-2d3"
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
	input := GoogleGTID{ServerID: 41983, GroupID: 1857273}
	want := "41983-1857273"

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

func TestGoogleGTIDSequenceDomain(t *testing.T) {
	input := GoogleGTID{ServerID: 41983, GroupID: 1857273}
	want := ""

	got := input.SequenceDomain()
	if got != want {
		t.Errorf("%#v.SequenceDomain() = '%v', want '%v'", input, got, want)
	}
}

func TestGoogleGTIDSourceServer(t *testing.T) {
	input := GoogleGTID{ServerID: 41983, GroupID: 1857273}
	want := "41983"

	got := input.SourceServer()
	if got != want {
		t.Errorf("%#v.SourceServer() = '%v', want '%v'", input, got, want)
	}
}

func TestGoogleGTIDSequenceNumber(t *testing.T) {
	input := GoogleGTID{ServerID: 41983, GroupID: 1857273}
	want := uint64(1857273)

	got := input.SequenceNumber()
	if got != want {
		t.Errorf("%#v.SequenceNumber() = %v, want %v", input, got, want)
	}
}

func TestGoogleGTIDGTIDSet(t *testing.T) {
	input := GoogleGTID{ServerID: 41983, GroupID: 1857273}
	want := GTIDSet(input)

	got := input.GTIDSet()
	if got != want {
		t.Errorf("%#v.GTIDSet() = %#v, want %#v", input, got, want)
	}
}

func TestGoogleGTIDLast(t *testing.T) {
	input := GoogleGTID{ServerID: 41983, GroupID: 1857273}
	want := GTID(input)

	got := input.Last()
	if got != want {
		t.Errorf("%#v.Last() = %#v, want %#v", input, got, want)
	}
}

func TestGoogleGTIDContainsLess(t *testing.T) {
	input1 := GoogleGTID{GroupID: 12345}
	input2 := GoogleGTID{GroupID: 54321}
	want := false

	if got := input1.Contains(input2); got != want {
		t.Errorf("%#v.Contains(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestGoogleGTIDContainsGreater(t *testing.T) {
	input1 := GoogleGTID{GroupID: 54321}
	input2 := GoogleGTID{GroupID: 12345}
	want := true

	if got := input1.Contains(input2); got != want {
		t.Errorf("%#v.Contains(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestGoogleGTIDContainsEqual(t *testing.T) {
	input1 := GoogleGTID{GroupID: 12345}
	input2 := GoogleGTID{GroupID: 12345}
	want := true

	if got := input1.Contains(input2); got != want {
		t.Errorf("%#v.Contains(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestGoogleGTIDContainsWrongType(t *testing.T) {
	input1 := GoogleGTID{GroupID: 123}
	input2 := fakeGTID{}
	want := false

	if got := input1.Contains(input2); got != want {
		t.Errorf("%#v.Contains(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestGoogleGTIDContainsNil(t *testing.T) {
	input1 := GoogleGTID{GroupID: 123}
	input2 := GTIDSet(nil)
	want := true

	if got := input1.Contains(input2); got != want {
		t.Errorf("%#v.Contains(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestGoogleGTIDContainsGTIDLess(t *testing.T) {
	input1 := GoogleGTID{GroupID: 12345}
	input2 := GoogleGTID{GroupID: 54321}
	want := false

	if got := input1.ContainsGTID(input2); got != want {
		t.Errorf("%#v.ContainsGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestGoogleGTIDContainsGTIDGreater(t *testing.T) {
	input1 := GoogleGTID{GroupID: 54321}
	input2 := GoogleGTID{GroupID: 12345}
	want := true

	if got := input1.ContainsGTID(input2); got != want {
		t.Errorf("%#v.ContainsGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestGoogleGTIDContainsGTIDEqual(t *testing.T) {
	input1 := GoogleGTID{GroupID: 12345}
	input2 := GoogleGTID{GroupID: 12345}
	want := true

	if got := input1.ContainsGTID(input2); got != want {
		t.Errorf("%#v.ContainsGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestGoogleGTIDContainsGTIDWrongType(t *testing.T) {
	input1 := GoogleGTID{GroupID: 123}
	input2 := fakeGTID{}
	want := false

	if got := input1.ContainsGTID(input2); got != want {
		t.Errorf("%#v.ContainsGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestGoogleGTIDContainsGTIDNil(t *testing.T) {
	input1 := GoogleGTID{GroupID: 123}
	input2 := GTID(nil)
	want := true

	if got := input1.ContainsGTID(input2); got != want {
		t.Errorf("%#v.ContainsGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestGoogleGTIDEqual(t *testing.T) {
	input1 := GoogleGTID{GroupID: 41234}
	input2 := GoogleGTID{GroupID: 41234}
	want := true

	if got := input1.Equal(input2); got != want {
		t.Errorf("%#v.Equal(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestGoogleGTIDNotEqual(t *testing.T) {
	input1 := GoogleGTID{GroupID: 41234}
	input2 := GoogleGTID{GroupID: 51234}
	want := false

	if got := input1.Equal(input2); got != want {
		t.Errorf("%#v.Equal(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestGoogleGTIDEqualWrongType(t *testing.T) {
	input1 := GoogleGTID{GroupID: 41234}
	input2 := fakeGTID{}
	want := false

	if got := input1.Equal(input2); got != want {
		t.Errorf("%#v.Equal(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestGoogleGTIDEqualNil(t *testing.T) {
	input1 := GoogleGTID{GroupID: 41234}
	input2 := GTIDSet(nil)
	want := false

	if got := input1.Equal(input2); got != want {
		t.Errorf("%#v.Equal(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestGoogleGTIDAddGTIDEqual(t *testing.T) {
	input1 := GoogleGTID{GroupID: 41234}
	input2 := GoogleGTID{GroupID: 41234}
	want := GoogleGTID{GroupID: 41234}

	if got := input1.AddGTID(input2); got != want {
		t.Errorf("%#v.AddGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestGoogleGTIDAddGTIDGreater(t *testing.T) {
	input1 := GoogleGTID{GroupID: 41234}
	input2 := GoogleGTID{GroupID: 51234}
	want := GoogleGTID{GroupID: 51234}

	if got := input1.AddGTID(input2); got != want {
		t.Errorf("%#v.AddGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestGoogleGTIDAddGTIDLess(t *testing.T) {
	input1 := GoogleGTID{GroupID: 51234}
	input2 := GoogleGTID{GroupID: 41234}
	want := GoogleGTID{GroupID: 51234}

	if got := input1.AddGTID(input2); got != want {
		t.Errorf("%#v.AddGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestGoogleGTIDAddGTIDWrongType(t *testing.T) {
	input1 := GoogleGTID{GroupID: 41234}
	input2 := fakeGTID{}
	want := input1

	if got := input1.AddGTID(input2); got != want {
		t.Errorf("%#v.AddGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestGoogleGTIDAddGTIDNil(t *testing.T) {
	input1 := GoogleGTID{GroupID: 41234}
	input2 := GTID(nil)
	want := input1

	if got := input1.AddGTID(input2); got != want {
		t.Errorf("%#v.AddGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestGoogleGTIDAddGTIDDifferentServer(t *testing.T) {
	input1 := GoogleGTID{ServerID: 1, GroupID: 41234}
	input2 := GoogleGTID{ServerID: 2, GroupID: 51234}
	want := GoogleGTID{ServerID: 2, GroupID: 51234}

	if got := input1.AddGTID(input2); got != want {
		t.Errorf("%#v.AddGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}
