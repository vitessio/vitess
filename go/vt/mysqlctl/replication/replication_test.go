// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package replication

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestPositionEqual(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	input2 := Position{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	want := true

	if got := input1.Equal(input2); got != want {
		t.Errorf("%#v.Equal(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionNotEqual(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	input2 := Position{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 12345}}
	want := false

	if got := input1.Equal(input2); got != want {
		t.Errorf("%#v.Equal(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionEqualZero(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	input2 := Position{}
	want := false

	if got := input1.Equal(input2); got != want {
		t.Errorf("%#v.Equal(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionZeroEqualZero(t *testing.T) {
	input1 := Position{}
	input2 := Position{}
	want := true

	if got := input1.Equal(input2); got != want {
		t.Errorf("%#v.Equal(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionAtLeastLess(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1233}}
	input2 := Position{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	want := false

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionAtLeastEqual(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	input2 := Position{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	want := true

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionAtLeastGreater(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1235}}
	input2 := Position{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	want := true

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionAtLeastDifferentServer(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1235}}
	input2 := Position{GTIDSet: MariadbGTID{Domain: 3, Server: 4444, Sequence: 1234}}
	want := true

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionAtLeastDifferentDomain(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1235}}
	input2 := Position{GTIDSet: MariadbGTID{Domain: 4, Server: 5555, Sequence: 1234}}
	want := false

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionZeroAtLeast(t *testing.T) {
	input1 := Position{}
	input2 := Position{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	want := false

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionAtLeastZero(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	input2 := Position{}
	want := true

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionZeroAtLeastZero(t *testing.T) {
	input1 := Position{}
	input2 := Position{}
	want := true

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionString(t *testing.T) {
	input := Position{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	want := "3-5555-1234"

	if got := input.String(); got != want {
		t.Errorf("%#v.String() = %#v, want %#v", input, got, want)
	}
}

func TestPositionStringNil(t *testing.T) {
	input := Position{}
	want := "<nil>"

	if got := input.String(); got != want {
		t.Errorf("%#v.String() = %#v, want %#v", input, got, want)
	}
}

func TestPositionIsZero(t *testing.T) {
	input := Position{}
	want := true

	if got := input.IsZero(); got != want {
		t.Errorf("%#v.IsZero() = %#v, want %#v", input, got, want)
	}
}

func TestPositionIsNotZero(t *testing.T) {
	input := Position{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	want := false

	if got := input.IsZero(); got != want {
		t.Errorf("%#v.IsZero() = %#v, want %#v", input, got, want)
	}
}

func TestPositionAppend(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	input2 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1235}
	want := Position{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1235}}

	if got := AppendGTID(input1, input2); !got.Equal(want) {
		t.Errorf("AppendGTID(%#v, %#v) = %#v, want %#v", input1, input2, got, want)
	}
}

func TestPositionAppendNil(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	input2 := GTID(nil)
	want := Position{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}

	if got := AppendGTID(input1, input2); !got.Equal(want) {
		t.Errorf("AppendGTID(%#v, %#v) = %#v, want %#v", input1, input2, got, want)
	}
}

func TestPositionAppendToZero(t *testing.T) {
	input1 := Position{}
	input2 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}
	want := Position{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}

	if got := AppendGTID(input1, input2); !got.Equal(want) {
		t.Errorf("AppendGTID(%#v, %#v) = %#v, want %#v", input1, input2, got, want)
	}
}

func TestMustParsePosition(t *testing.T) {
	flavor := "fake flavor"
	gtidSetParsers[flavor] = func(s string) (GTIDSet, error) {
		return fakeGTID{value: s}, nil
	}
	input := "12345"
	want := Position{GTIDSet: fakeGTID{value: "12345"}}

	if got := MustParsePosition(flavor, input); !got.Equal(want) {
		t.Errorf("MustParsePosition(%#v, %#v) = %#v, want %#v", flavor, input, got, want)
	}
}

func TestMustParsePositionError(t *testing.T) {
	defer func() {
		want := `parse error: unknown GTIDSet flavor "unknown flavor !@$!@"`
		err := recover()
		if err == nil {
			t.Errorf("wrong error, got %#v, want %#v", err, want)
		}
		got, ok := err.(error)
		if !ok || !strings.HasPrefix(got.Error(), want) {
			t.Errorf("wrong error, got %#v, want %#v", got, want)
		}
	}()

	MustParsePosition("unknown flavor !@$!@", "yowzah")
}

func TestEncodePosition(t *testing.T) {
	input := Position{GTIDSet: fakeGTID{
		flavor: "myflav",
		value:  "1:2:3-4-5-6",
	}}
	want := "myflav/1:2:3-4-5-6"

	if got := EncodePosition(input); got != want {
		t.Errorf("EncodePosition(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestEncodePositionZero(t *testing.T) {
	input := Position{}
	want := ""

	if got := EncodePosition(input); got != want {
		t.Errorf("EncodePosition(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestDecodePosition(t *testing.T) {
	gtidSetParsers["flavorflav"] = func(s string) (GTIDSet, error) {
		return fakeGTID{value: s}, nil
	}
	input := "flavorflav/123-456:789"
	want := Position{GTIDSet: fakeGTID{value: "123-456:789"}}

	got, err := DecodePosition(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !got.Equal(want) {
		t.Errorf("DecodePosition(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestDecodePositionZero(t *testing.T) {
	input := ""
	want := Position{}

	got, err := DecodePosition(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !got.Equal(want) {
		t.Errorf("DecodePosition(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestDecodePositionNoFlavor(t *testing.T) {
	gtidSetParsers[""] = func(s string) (GTIDSet, error) {
		return fakeGTID{value: s}, nil
	}
	input := "12345"
	want := Position{GTIDSet: fakeGTID{value: "12345"}}

	got, err := DecodePosition(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !got.Equal(want) {
		t.Errorf("DecodePosition(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestJsonMarshalPosition(t *testing.T) {
	input := Position{GTIDSet: fakeGTID{flavor: "golf", value: "par"}}
	want := `"golf/par"`

	buf, err := json.Marshal(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if got := string(buf); got != want {
		t.Errorf("json.Marshal(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestJsonMarshalPositionPointer(t *testing.T) {
	input := Position{GTIDSet: fakeGTID{flavor: "golf", value: "par"}}
	want := `"golf/par"`

	buf, err := json.Marshal(&input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if got := string(buf); got != want {
		t.Errorf("json.Marshal(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestJsonUnmarshalPosition(t *testing.T) {
	gtidSetParsers["golf"] = func(s string) (GTIDSet, error) {
		return fakeGTID{flavor: "golf", value: s}, nil
	}
	input := `"golf/par"`
	want := Position{GTIDSet: fakeGTID{flavor: "golf", value: "par"}}

	var got Position
	err := json.Unmarshal([]byte(input), &got)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !got.Equal(want) {
		t.Errorf("json.Unmarshal(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestJsonMarshalPositionInStruct(t *testing.T) {
	input := Position{GTIDSet: fakeGTID{flavor: "golf", value: "par"}}
	want := `{"Position":"golf/par"}`

	type mystruct struct {
		Position Position
	}

	buf, err := json.Marshal(&mystruct{input})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if got := string(buf); got != want {
		t.Errorf("json.Marshal(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestJsonUnmarshalPositionInStruct(t *testing.T) {
	gtidSetParsers["golf"] = func(s string) (GTIDSet, error) {
		return fakeGTID{flavor: "golf", value: s}, nil
	}
	input := `{"Position":"golf/par"}`
	want := Position{GTIDSet: fakeGTID{flavor: "golf", value: "par"}}

	var gotStruct struct {
		Position Position
	}
	err := json.Unmarshal([]byte(input), &gotStruct)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if got := gotStruct.Position; !got.Equal(want) {
		t.Errorf("json.Unmarshal(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestJsonMarshalPositionZero(t *testing.T) {
	input := Position{}
	want := `""`

	buf, err := json.Marshal(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if got := string(buf); got != want {
		t.Errorf("json.Marshal(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestJsonUnmarshalPositionZero(t *testing.T) {
	input := `""`
	want := Position{}

	var got Position
	err := json.Unmarshal([]byte(input), &got)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !got.Equal(want) {
		t.Errorf("json.Unmarshal(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestStatusSlaveRunning(t *testing.T) {
	input := &Status{
		SlaveIORunning:  true,
		SlaveSQLRunning: true,
	}
	want := true
	if got := input.SlaveRunning(); got != want {
		t.Errorf("%#v.SlaveRunning() = %v, want %v", input, got, want)
	}
}

func TestStatusSlaveIONotRunning(t *testing.T) {
	input := &Status{
		SlaveIORunning:  false,
		SlaveSQLRunning: true,
	}
	want := false
	if got := input.SlaveRunning(); got != want {
		t.Errorf("%#v.SlaveRunning() = %v, want %v", input, got, want)
	}
}

func TestStatusSlaveSQLNotRunning(t *testing.T) {
	input := &Status{
		SlaveIORunning:  true,
		SlaveSQLRunning: false,
	}
	want := false
	if got := input.SlaveRunning(); got != want {
		t.Errorf("%#v.SlaveRunning() = %v, want %v", input, got, want)
	}
}

func TestStatusMasterAddr(t *testing.T) {
	table := map[string]*Status{
		"master-host:1234": {
			MasterHost: "master-host",
			MasterPort: 1234,
		},
		"[::1]:4321": {
			MasterHost: "::1",
			MasterPort: 4321,
		},
	}
	for want, input := range table {
		if got := input.MasterAddr(); got != want {
			t.Errorf("%#v.MasterAddr() = %v, want %v", input, got, want)
		}
	}
}

func TestNewStatus(t *testing.T) {
	table := map[string]*Status{
		"master-host:1234": {
			MasterHost: "master-host",
			MasterPort: 1234,
		},
		"[::1]:4321": {
			MasterHost: "::1",
			MasterPort: 4321,
		},
	}
	for input, want := range table {
		got, err := NewStatus(input)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if got.MasterHost != want.MasterHost || got.MasterPort != want.MasterPort {
			t.Errorf("NewStatus(%#v) = %#v, want %#v", input, got, want)
		}
	}
}
