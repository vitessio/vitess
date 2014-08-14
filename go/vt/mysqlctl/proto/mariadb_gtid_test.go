// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"strings"
	"testing"
)

func TestParseMariaGTID(t *testing.T) {
	input := "12-345-6789"
	want := MariadbGTID{Domain: 12, Server: 345, Sequence: 6789}

	got, err := parseMariadbGTID(input)
	if err != nil {
		t.Errorf("%v", err)
	}
	if got.(MariadbGTID) != want {
		t.Errorf("ParseGTID(%v) = %v, want %v", input, got, want)
	}
}

func TestParseInvalidMariaGTID(t *testing.T) {
	input := "12345"
	want := "invalid MariaDB GTID"

	_, err := parseMariadbGTID(input)
	if err == nil {
		t.Errorf("expected error for invalid input (%v)", input)
	}
	if !strings.HasPrefix(err.Error(), want) {
		t.Errorf("wrong error message, got '%v', want '%v'", err, want)
	}
}

func TestParseMariaGTIDInvalidDomain(t *testing.T) {
	input := "1x-33-142"
	want := "invalid MariaDB GTID Domain ID"

	_, err := parseMariadbGTID(input)
	if err == nil {
		t.Errorf("expected error for invalid input (%v)", input)
	}
	if !strings.HasPrefix(err.Error(), want) {
		t.Errorf("wrong error message, got '%v', want '%v'", err, want)
	}
}

func TestParseMariaGTIDInvalidServer(t *testing.T) {
	input := "1-2c3-142"
	want := "invalid MariaDB GTID Server ID"

	_, err := parseMariadbGTID(input)
	if err == nil {
		t.Errorf("expected error for invalid input (%v)", input)
	}
	if !strings.HasPrefix(err.Error(), want) {
		t.Errorf("wrong error message, got '%v', want '%v'", err, want)
	}
}

func TestParseMariaGTIDInvalidSequence(t *testing.T) {
	input := "1-33-a142"
	want := "invalid MariaDB GTID Sequence number"

	_, err := parseMariadbGTID(input)
	if err == nil {
		t.Errorf("expected error for invalid input (%v)", input)
	}
	if !strings.HasPrefix(err.Error(), want) {
		t.Errorf("wrong error message, got '%v', want '%v'", err, want)
	}
}

func TestMariaGTIDString(t *testing.T) {
	input := MariadbGTID{Domain: 5, Server: 4727, Sequence: 1737373}
	want := "5-4727-1737373"

	got := input.String()
	if got != want {
		t.Errorf("%#v.String() = '%v', want '%v'", input, got, want)
	}
}

func TestMariaGTIDFlavor(t *testing.T) {
	input := MariadbGTID{Domain: 1, Server: 2, Sequence: 123}
	want := "MariaDB"

	got := input.Flavor()
	if got != want {
		t.Errorf("%#v.Flavor() = '%v', want '%v'", input, got, want)
	}
}

func TestMariaGTIDCompareLess(t *testing.T) {
	input1 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 300}
	input2 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 700}

	cmp, err := input1.TryCompare(input2)
	if err != nil {
		t.Errorf("unexpected error for %#v.TryCompare(%#v): %v", input1, input2, err)
	}
	if !(cmp < 0) {
		t.Errorf("%#v.TryCompare(%#v) = %v, want < 0", input1, input2, cmp)
	}
}

func TestMariaGTIDCompareGreater(t *testing.T) {
	input1 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 9000}
	input2 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 100}

	cmp, err := input1.TryCompare(input2)
	if err != nil {
		t.Errorf("unexpected error for %#v.TryCompare(%#v): %v", input2, input1, err)
	}
	if !(cmp > 0) {
		t.Errorf("%#v.TryCompare(%#v) = %v, want > 0", input2, input1, cmp)
	}
}

func TestMariaGTIDCompareEqual(t *testing.T) {
	input1 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 1234}
	input2 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 1234}

	cmp, err := input1.TryCompare(input2)
	if err != nil {
		t.Errorf("unexpected error for %#v.TryCompare(%#v): %v", input1, input2, err)
	}
	if cmp != 0 {
		t.Errorf("%#v.TryCompare(%#v) = %v, want 0", input1, input2, cmp)
	}
}

func TestMariaGTIDCompareNil(t *testing.T) {
	input1 := MariadbGTID{Domain: 1, Server: 2, Sequence: 123}
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

func TestMariaGTIDCompareWrongType(t *testing.T) {
	input1 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 1234}
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

func TestMariaGTIDCompareWrongDomain(t *testing.T) {
	input1 := MariadbGTID{Domain: 3, Server: 4727, Sequence: 1234}
	input2 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 1234}
	want := "can't compare GTID, MariaDB Domain doesn't match"

	_, err := input1.TryCompare(input2)
	if err == nil {
		t.Errorf("expected error for %#v.TryCompare(%#v)", input1, input2)
	}
	if !strings.HasPrefix(err.Error(), want) {
		t.Errorf("wrong error message for %#v.TryCompare(%#v), got %v, want %v", input1, input2, err, want)
	}
}

func TestMariaGTIDCompareWrongServer(t *testing.T) {
	input1 := MariadbGTID{Domain: 3, Server: 4727, Sequence: 1234}
	input2 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}
	want := "can't compare GTID, MariaDB Server doesn't match"

	_, err := input1.TryCompare(input2)
	if err == nil {
		t.Errorf("expected error for %#v.TryCompare(%#v)", input1, input2)
	}
	if !strings.HasPrefix(err.Error(), want) {
		t.Errorf("wrong error message for %#v.TryCompare(%#v), got %v, want %v", input1, input2, err, want)
	}
}

func TestMariaGTIDEqual(t *testing.T) {
	input1 := GTID(MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234})
	input2 := GTID(MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234})
	want := true

	cmp := input1 == input2
	if cmp != want {
		t.Errorf("(%#v == %#v) = %v, want %v", input1, input2, cmp, want)
	}
}

func TestMariaGTIDNotEqual(t *testing.T) {
	input1 := GTID(MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234})
	input2 := GTID(MariadbGTID{Domain: 3, Server: 4555, Sequence: 1234})
	want := false

	cmp := input1 == input2
	if cmp != want {
		t.Errorf("(%#v == %#v) = %v, want %v", input1, input2, cmp, want)
	}
}
