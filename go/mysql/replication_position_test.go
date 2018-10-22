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
	"encoding/json"
	"strings"
	"testing"
)

func TestPositionEqual(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTIDSet{MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
	input2 := Position{GTIDSet: MariadbGTIDSet{MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
	want := true

	if got := input1.Equal(input2); got != want {
		t.Errorf("%#v.Equal(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionNotEqual(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTIDSet{MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
	input2 := Position{GTIDSet: MariadbGTIDSet{MariadbGTID{Domain: 3, Server: 5555, Sequence: 12345}}}
	want := false

	if got := input1.Equal(input2); got != want {
		t.Errorf("%#v.Equal(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionEqualZero(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTIDSet{MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
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
	input1 := Position{GTIDSet: MariadbGTIDSet{MariadbGTID{Domain: 3, Server: 5555, Sequence: 1233}}}
	input2 := Position{GTIDSet: MariadbGTIDSet{MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
	want := false

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionAtLeastEqual(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTIDSet{MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
	input2 := Position{GTIDSet: MariadbGTIDSet{MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
	want := true

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionAtLeastGreater(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTIDSet{MariadbGTID{Domain: 3, Server: 5555, Sequence: 1235}}}
	input2 := Position{GTIDSet: MariadbGTIDSet{MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
	want := true

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionAtLeastDifferentServer(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTIDSet{MariadbGTID{Domain: 3, Server: 5555, Sequence: 1235}}}
	input2 := Position{GTIDSet: MariadbGTIDSet{MariadbGTID{Domain: 3, Server: 4444, Sequence: 1234}}}
	want := true

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionAtLeastDifferentDomain(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTIDSet{MariadbGTID{Domain: 3, Server: 5555, Sequence: 1235}}}
	input2 := Position{GTIDSet: MariadbGTIDSet{MariadbGTID{Domain: 4, Server: 5555, Sequence: 1234}}}
	want := false

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionZeroAtLeast(t *testing.T) {
	input1 := Position{}
	input2 := Position{GTIDSet: MariadbGTIDSet{MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
	want := false

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionAtLeastZero(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTIDSet{MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
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
	input := Position{GTIDSet: MariadbGTIDSet{MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
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
	input := Position{GTIDSet: MariadbGTIDSet{MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
	want := false

	if got := input.IsZero(); got != want {
		t.Errorf("%#v.IsZero() = %#v, want %#v", input, got, want)
	}
}

func TestPositionAppend(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTIDSet{MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
	input2 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1235}
	want := Position{GTIDSet: MariadbGTIDSet{MariadbGTID{Domain: 3, Server: 5555, Sequence: 1235}}}

	if got := AppendGTID(input1, input2); !got.Equal(want) {
		t.Errorf("AppendGTID(%#v, %#v) = %#v, want %#v", input1, input2, got, want)
	}
}

func TestPositionAppendNil(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTIDSet{MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
	input2 := GTID(nil)
	want := Position{GTIDSet: MariadbGTIDSet{MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}

	if got := AppendGTID(input1, input2); !got.Equal(want) {
		t.Errorf("AppendGTID(%#v, %#v) = %#v, want %#v", input1, input2, got, want)
	}
}

func TestPositionAppendToZero(t *testing.T) {
	input1 := Position{}
	input2 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}
	want := Position{GTIDSet: MariadbGTIDSet{MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}

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
