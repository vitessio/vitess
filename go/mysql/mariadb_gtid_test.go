/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mysql

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
		t.Errorf("parseMariadbGTID(%v) = %v, want %v", input, got, want)
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

func TestParseMariaGTIDSet(t *testing.T) {
	input := "12-34-5678"
	want := MariadbGTID{Domain: 12, Server: 34, Sequence: 5678}

	got, err := parseMariadbGTIDSet(input)
	if err != nil {
		t.Errorf("%v", err)
	}
	if got.(MariadbGTID) != want {
		t.Errorf("parseMariadbGTIDSet(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestParseInvalidMariaGTIDSet(t *testing.T) {
	input := "12-34-56d78"
	want := "invalid MariaDB GTID Sequence number"

	_, err := parseMariadbGTIDSet(input)
	if err == nil {
		t.Errorf("expected error for invalid input (%#v)", input)
		return
	}
	if got := err.Error(); !strings.HasPrefix(got, want) {
		t.Errorf("parseMariadbGTIDSet(%#v) error = %#v, want %#v", input, got, want)
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

func TestMariaGTIDSequenceDomain(t *testing.T) {
	input := MariadbGTID{Domain: 12, Server: 345, Sequence: 6789}
	want := interface{}(uint32(12))

	got := input.SequenceDomain()
	if got != want {
		t.Errorf("%#v.SequenceDomain() = %#v, want %#v", input, got, want)
	}
}

func TestMariaGTIDSourceServer(t *testing.T) {
	input := MariadbGTID{Domain: 12, Server: 345, Sequence: 6789}
	want := interface{}(uint32(345))

	got := input.SourceServer()
	if got != want {
		t.Errorf("%#v.SourceServer() = %#v, want %#v", input, got, want)
	}
}

func TestMariaGTIDSequenceNumber(t *testing.T) {
	input := MariadbGTID{Domain: 12, Server: 345, Sequence: 6789}
	want := interface{}(uint64(6789))

	got := input.SequenceNumber()
	if got != want {
		t.Errorf("%#v.SequenceNumber() = %#v, want %#v", input, got, want)
	}
}

func TestMariaGTIDGTIDSet(t *testing.T) {
	input := MariadbGTID{Domain: 12, Server: 345, Sequence: 6789}
	want := GTIDSet(input)

	got := input.GTIDSet()
	if got != want {
		t.Errorf("%#v.GTIDSet() = %#v, want %#v", input, got, want)
	}
}

func TestMariaGTIDContainsLess(t *testing.T) {
	input1 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 300}
	input2 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 700}
	want := false

	if got := input1.Contains(input2); got != want {
		t.Errorf("%#v.Contains(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDContainsGreater(t *testing.T) {
	input1 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 9000}
	input2 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 100}
	want := true

	if got := input1.Contains(input2); got != want {
		t.Errorf("%#v.Contains(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDContainsEqual(t *testing.T) {
	input1 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 1234}
	input2 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 1234}
	want := true

	if got := input1.Contains(input2); got != want {
		t.Errorf("%#v.Contains(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDContainsNil(t *testing.T) {
	input1 := MariadbGTID{Domain: 1, Server: 2, Sequence: 123}
	input2 := GTIDSet(nil)
	want := true

	if got := input1.Contains(input2); got != want {
		t.Errorf("%#v.Contains(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDContainsWrongType(t *testing.T) {
	input1 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 1234}
	input2 := fakeGTID{}
	want := false

	if got := input1.Contains(input2); got != want {
		t.Errorf("%#v.Contains(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDContainsDifferentDomain(t *testing.T) {
	input1 := MariadbGTID{Domain: 3, Server: 4727, Sequence: 1235}
	input2 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 1234}
	want := false

	if got := input1.Contains(input2); got != want {
		t.Errorf("%#v.Contains(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDContainsDifferentServer(t *testing.T) {
	input1 := MariadbGTID{Domain: 3, Server: 4727, Sequence: 1235}
	input2 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}
	want := true

	if got := input1.Contains(input2); got != want {
		t.Errorf("%#v.Contains(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDContainsGTIDLess(t *testing.T) {
	input1 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 300}
	input2 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 700}
	want := false

	if got := input1.ContainsGTID(input2); got != want {
		t.Errorf("%#v.ContainsGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDContainsGTIDGreater(t *testing.T) {
	input1 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 9000}
	input2 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 100}
	want := true

	if got := input1.ContainsGTID(input2); got != want {
		t.Errorf("%#v.ContainsGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDContainsGTIDEqual(t *testing.T) {
	input1 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 1234}
	input2 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 1234}
	want := true

	if got := input1.ContainsGTID(input2); got != want {
		t.Errorf("%#v.ContainsGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDContainsGTIDNil(t *testing.T) {
	input1 := MariadbGTID{Domain: 1, Server: 2, Sequence: 123}
	input2 := GTID(nil)
	want := true

	if got := input1.ContainsGTID(input2); got != want {
		t.Errorf("%#v.ContainsGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDContainsGTIDWrongType(t *testing.T) {
	input1 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 1234}
	input2 := fakeGTID{}
	want := false

	if got := input1.ContainsGTID(input2); got != want {
		t.Errorf("%#v.ContainsGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDContainsGTIDDifferentDomain(t *testing.T) {
	input1 := MariadbGTID{Domain: 3, Server: 4727, Sequence: 1235}
	input2 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 1234}
	want := false

	if got := input1.ContainsGTID(input2); got != want {
		t.Errorf("%#v.ContainsGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDContainsGTIDDifferentServer(t *testing.T) {
	input1 := MariadbGTID{Domain: 3, Server: 4727, Sequence: 1235}
	input2 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}
	want := true

	if got := input1.ContainsGTID(input2); got != want {
		t.Errorf("%#v.ContainsGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDEqual(t *testing.T) {
	input1 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}
	input2 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}
	want := true

	if got := input1.Equal(input2); got != want {
		t.Errorf("%#v.Equal(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDNotEqual(t *testing.T) {
	input1 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}
	input2 := MariadbGTID{Domain: 3, Server: 4555, Sequence: 1234}
	want := false

	if got := input1.Equal(input2); got != want {
		t.Errorf("%#v.Equal(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDEqualWrongType(t *testing.T) {
	input1 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}
	input2 := fakeGTID{}
	want := false

	if got := input1.Equal(input2); got != want {
		t.Errorf("%#v.Equal(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDEqualNil(t *testing.T) {
	input1 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}
	input2 := GTIDSet(nil)
	want := false

	if got := input1.Equal(input2); got != want {
		t.Errorf("%#v.Equal(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDAddGTIDEqual(t *testing.T) {
	input1 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}
	input2 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}
	want := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}

	if got := input1.AddGTID(input2); got != want {
		t.Errorf("%#v.AddGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDAddGTIDGreater(t *testing.T) {
	input1 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 5234}
	input2 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}
	want := MariadbGTID{Domain: 3, Server: 5555, Sequence: 5234}

	if got := input1.AddGTID(input2); got != want {
		t.Errorf("%#v.AddGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDAddGTIDLess(t *testing.T) {
	input1 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}
	input2 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 5234}
	want := MariadbGTID{Domain: 3, Server: 5555, Sequence: 5234}

	if got := input1.AddGTID(input2); got != want {
		t.Errorf("%#v.AddGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDAddGTIDWrongType(t *testing.T) {
	input1 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}
	input2 := fakeGTID{}
	want := input1

	if got := input1.AddGTID(input2); got != want {
		t.Errorf("%#v.AddGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDAddGTIDNil(t *testing.T) {
	input1 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}
	input2 := GTID(nil)
	want := input1

	if got := input1.AddGTID(input2); got != want {
		t.Errorf("%#v.AddGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDAddGTIDDifferentServer(t *testing.T) {
	input1 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}
	input2 := MariadbGTID{Domain: 3, Server: 4444, Sequence: 5234}
	want := MariadbGTID{Domain: 3, Server: 4444, Sequence: 5234}

	if got := input1.AddGTID(input2); got != want {
		t.Errorf("%#v.AddGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDAddGTIDDifferentDomain(t *testing.T) {
	input1 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}
	input2 := MariadbGTID{Domain: 5, Server: 5555, Sequence: 5234}
	want := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}

	if got := input1.AddGTID(input2); got != want {
		t.Errorf("%#v.AddGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}
