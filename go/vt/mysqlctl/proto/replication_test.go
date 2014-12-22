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

func TestReplicationPositionEqual(t *testing.T) {
	input1 := ReplicationPosition{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	input2 := ReplicationPosition{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	want := true

	if got := input1.Equal(input2); got != want {
		t.Errorf("%#v.Equal(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestReplicationPositionNotEqual(t *testing.T) {
	input1 := ReplicationPosition{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	input2 := ReplicationPosition{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 12345}}
	want := false

	if got := input1.Equal(input2); got != want {
		t.Errorf("%#v.Equal(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestReplicationPositionEqualZero(t *testing.T) {
	input1 := ReplicationPosition{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	input2 := ReplicationPosition{}
	want := false

	if got := input1.Equal(input2); got != want {
		t.Errorf("%#v.Equal(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestReplicationPositionZeroEqualZero(t *testing.T) {
	input1 := ReplicationPosition{}
	input2 := ReplicationPosition{}
	want := true

	if got := input1.Equal(input2); got != want {
		t.Errorf("%#v.Equal(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestReplicationPositionAtLeastLess(t *testing.T) {
	input1 := ReplicationPosition{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1233}}
	input2 := ReplicationPosition{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	want := false

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestReplicationPositionAtLeastEqual(t *testing.T) {
	input1 := ReplicationPosition{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	input2 := ReplicationPosition{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	want := true

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestReplicationPositionAtLeastGreater(t *testing.T) {
	input1 := ReplicationPosition{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1235}}
	input2 := ReplicationPosition{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	want := true

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestReplicationPositionAtLeastDifferentServer(t *testing.T) {
	input1 := ReplicationPosition{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1235}}
	input2 := ReplicationPosition{GTIDSet: MariadbGTID{Domain: 3, Server: 4444, Sequence: 1234}}
	want := true

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestReplicationPositionAtLeastDifferentDomain(t *testing.T) {
	input1 := ReplicationPosition{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1235}}
	input2 := ReplicationPosition{GTIDSet: MariadbGTID{Domain: 4, Server: 5555, Sequence: 1234}}
	want := false

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestReplicationPositionZeroAtLeast(t *testing.T) {
	input1 := ReplicationPosition{}
	input2 := ReplicationPosition{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	want := false

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestReplicationPositionAtLeastZero(t *testing.T) {
	input1 := ReplicationPosition{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	input2 := ReplicationPosition{}
	want := true

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestReplicationPositionZeroAtLeastZero(t *testing.T) {
	input1 := ReplicationPosition{}
	input2 := ReplicationPosition{}
	want := true

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestReplicationPositionString(t *testing.T) {
	input := ReplicationPosition{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	want := "3-5555-1234"

	if got := input.String(); got != want {
		t.Errorf("%#v.String() = %#v, want %#v", input, got, want)
	}
}

func TestReplicationPositionStringNil(t *testing.T) {
	input := ReplicationPosition{}
	want := "<nil>"

	if got := input.String(); got != want {
		t.Errorf("%#v.String() = %#v, want %#v", input, got, want)
	}
}

func TestReplicationPositionIsZero(t *testing.T) {
	input := ReplicationPosition{}
	want := true

	if got := input.IsZero(); got != want {
		t.Errorf("%#v.IsZero() = %#v, want %#v", input, got, want)
	}
}

func TestReplicationPositionIsNotZero(t *testing.T) {
	input := ReplicationPosition{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	want := false

	if got := input.IsZero(); got != want {
		t.Errorf("%#v.IsZero() = %#v, want %#v", input, got, want)
	}
}

func TestReplicationPositionAppend(t *testing.T) {
	input1 := ReplicationPosition{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	input2 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1235}
	want := ReplicationPosition{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1235}}

	if got := AppendGTID(input1, input2); !got.Equal(want) {
		t.Errorf("AppendGTID(%#v, %#v) = %#v, want %#v", input1, input2, got, want)
	}
}

func TestReplicationPositionAppendNil(t *testing.T) {
	input1 := ReplicationPosition{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	input2 := GTID(nil)
	want := ReplicationPosition{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}

	if got := AppendGTID(input1, input2); !got.Equal(want) {
		t.Errorf("AppendGTID(%#v, %#v) = %#v, want %#v", input1, input2, got, want)
	}
}

func TestReplicationPositionAppendToZero(t *testing.T) {
	input1 := ReplicationPosition{}
	input2 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}
	want := ReplicationPosition{GTIDSet: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}

	if got := AppendGTID(input1, input2); !got.Equal(want) {
		t.Errorf("AppendGTID(%#v, %#v) = %#v, want %#v", input1, input2, got, want)
	}
}

func TestMustParseReplicationPosition(t *testing.T) {
	flavor := "fake flavor"
	gtidSetParsers[flavor] = func(s string) (GTIDSet, error) {
		return fakeGTID{value: s}, nil
	}
	input := "12345"
	want := ReplicationPosition{GTIDSet: fakeGTID{value: "12345"}}

	if got := MustParseReplicationPosition(flavor, input); !got.Equal(want) {
		t.Errorf("MustParseReplicationPosition(%#v, %#v) = %#v, want %#v", flavor, input, got, want)
	}
}

func TestMustParseReplicationPositionError(t *testing.T) {
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

	MustParseReplicationPosition("unknown flavor !@$!@", "yowzah")
}

func TestEncodeReplicationPosition(t *testing.T) {
	input := ReplicationPosition{GTIDSet: fakeGTID{
		flavor: "myflav",
		value:  "1:2:3-4-5-6",
	}}
	want := "myflav/1:2:3-4-5-6"

	if got := EncodeReplicationPosition(input); got != want {
		t.Errorf("EncodeReplicationPosition(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestEncodeReplicationPositionZero(t *testing.T) {
	input := ReplicationPosition{}
	want := ""

	if got := EncodeReplicationPosition(input); got != want {
		t.Errorf("EncodeReplicationPosition(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestDecodeReplicationPosition(t *testing.T) {
	gtidSetParsers["flavorflav"] = func(s string) (GTIDSet, error) {
		return fakeGTID{value: s}, nil
	}
	input := "flavorflav/123-456:789"
	want := ReplicationPosition{GTIDSet: fakeGTID{value: "123-456:789"}}

	got, err := DecodeReplicationPosition(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !got.Equal(want) {
		t.Errorf("DecodeReplicationPosition(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestDecodeReplicationPositionZero(t *testing.T) {
	input := ""
	want := ReplicationPosition{}

	got, err := DecodeReplicationPosition(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !got.Equal(want) {
		t.Errorf("DecodeReplicationPosition(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestDecodeReplicationPositionNoFlavor(t *testing.T) {
	gtidSetParsers[""] = func(s string) (GTIDSet, error) {
		return fakeGTID{value: s}, nil
	}
	input := "12345"
	want := ReplicationPosition{GTIDSet: fakeGTID{value: "12345"}}

	got, err := DecodeReplicationPosition(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !got.Equal(want) {
		t.Errorf("DecodeReplicationPosition(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestBsonMarshalUnmarshalReplicationPosition(t *testing.T) {
	gtidSetParsers["golf"] = func(s string) (GTIDSet, error) {
		return fakeGTID{flavor: "golf", value: s}, nil
	}
	input := ReplicationPosition{GTIDSet: fakeGTID{flavor: "golf", value: "par"}}
	want := ReplicationPosition{GTIDSet: fakeGTID{flavor: "golf", value: "par"}}

	buf, err := bson.Marshal(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	var got ReplicationPosition
	if err = bson.Unmarshal(buf, &got); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !got.Equal(want) {
		t.Errorf("marshal->unmarshal mismatch, got %#v, want %#v", got, want)
	}
}

func TestBsonMarshalUnmarshalReplicationPositionPointer(t *testing.T) {
	gtidSetParsers["golf"] = func(s string) (GTIDSet, error) {
		return fakeGTID{flavor: "golf", value: s}, nil
	}
	input := ReplicationPosition{GTIDSet: fakeGTID{flavor: "golf", value: "par"}}
	want := ReplicationPosition{GTIDSet: fakeGTID{flavor: "golf", value: "par"}}

	buf, err := bson.Marshal(&input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	var got ReplicationPosition
	if err = bson.Unmarshal(buf, &got); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if !got.Equal(want) {
		t.Errorf("marshal->unmarshal mismatch, got %#v, want %#v", got, want)
	}
}

func TestBsonMarshalUnmarshalReplicationPositionInStruct(t *testing.T) {
	gtidSetParsers["golf"] = func(s string) (GTIDSet, error) {
		return fakeGTID{flavor: "golf", value: s}, nil
	}
	input := ReplicationPosition{GTIDSet: fakeGTID{flavor: "golf", value: "par"}}
	want := ReplicationPosition{GTIDSet: fakeGTID{flavor: "golf", value: "par"}}

	type mystruct struct {
		ReplicationPosition
	}

	buf, err := bson.Marshal(&mystruct{ReplicationPosition: input})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	var gotStruct mystruct
	if err = bson.Unmarshal(buf, &gotStruct); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if got := gotStruct.ReplicationPosition; !got.Equal(want) {
		t.Errorf("marshal->unmarshal mismatch, got %#v, want %#v", got, want)
	}
}

func TestBsonMarshalUnmarshalReplicationPositionZero(t *testing.T) {
	gtidSetParsers["golf"] = func(s string) (GTIDSet, error) {
		return fakeGTID{flavor: "golf", value: s}, nil
	}
	input := ReplicationPosition{}
	want := ReplicationPosition{}

	buf, err := bson.Marshal(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	var got ReplicationPosition
	if err = bson.Unmarshal(buf, &got); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if !got.Equal(want) {
		t.Errorf("marshal->unmarshal mismatch, got %#v, want %#v", got, want)
	}
}

func TestJsonMarshalReplicationPosition(t *testing.T) {
	input := ReplicationPosition{GTIDSet: fakeGTID{flavor: "golf", value: "par"}}
	want := `"golf/par"`

	buf, err := json.Marshal(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if got := string(buf); got != want {
		t.Errorf("json.Marshal(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestJsonMarshalReplicationPositionPointer(t *testing.T) {
	input := ReplicationPosition{GTIDSet: fakeGTID{flavor: "golf", value: "par"}}
	want := `"golf/par"`

	buf, err := json.Marshal(&input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if got := string(buf); got != want {
		t.Errorf("json.Marshal(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestJsonUnmarshalReplicationPosition(t *testing.T) {
	gtidSetParsers["golf"] = func(s string) (GTIDSet, error) {
		return fakeGTID{flavor: "golf", value: s}, nil
	}
	input := `"golf/par"`
	want := ReplicationPosition{GTIDSet: fakeGTID{flavor: "golf", value: "par"}}

	var got ReplicationPosition
	err := json.Unmarshal([]byte(input), &got)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !got.Equal(want) {
		t.Errorf("json.Unmarshal(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestJsonMarshalReplicationPositionInStruct(t *testing.T) {
	input := ReplicationPosition{GTIDSet: fakeGTID{flavor: "golf", value: "par"}}
	want := `{"ReplicationPosition":"golf/par"}`

	type mystruct struct {
		ReplicationPosition ReplicationPosition
	}

	buf, err := json.Marshal(&mystruct{input})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if got := string(buf); got != want {
		t.Errorf("json.Marshal(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestJsonUnmarshalReplicationPositionInStruct(t *testing.T) {
	gtidSetParsers["golf"] = func(s string) (GTIDSet, error) {
		return fakeGTID{flavor: "golf", value: s}, nil
	}
	input := `{"ReplicationPosition":"golf/par"}`
	want := ReplicationPosition{GTIDSet: fakeGTID{flavor: "golf", value: "par"}}

	var gotStruct struct {
		ReplicationPosition ReplicationPosition
	}
	err := json.Unmarshal([]byte(input), &gotStruct)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if got := gotStruct.ReplicationPosition; !got.Equal(want) {
		t.Errorf("json.Unmarshal(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestJsonMarshalReplicationPositionZero(t *testing.T) {
	input := ReplicationPosition{}
	want := `""`

	buf, err := json.Marshal(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if got := string(buf); got != want {
		t.Errorf("json.Marshal(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestJsonUnmarshalReplicationPositionZero(t *testing.T) {
	input := `""`
	want := ReplicationPosition{}

	var got ReplicationPosition
	err := json.Unmarshal([]byte(input), &got)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !got.Equal(want) {
		t.Errorf("json.Unmarshal(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestReplicationStatusSlaveRunning(t *testing.T) {
	input := &ReplicationStatus{
		SlaveIORunning:  true,
		SlaveSQLRunning: true,
	}
	want := true
	if got := input.SlaveRunning(); got != want {
		t.Errorf("%#v.SlaveRunning() = %v, want %v", input, got, want)
	}
}

func TestReplicationStatusSlaveIONotRunning(t *testing.T) {
	input := &ReplicationStatus{
		SlaveIORunning:  false,
		SlaveSQLRunning: true,
	}
	want := false
	if got := input.SlaveRunning(); got != want {
		t.Errorf("%#v.SlaveRunning() = %v, want %v", input, got, want)
	}
}

func TestReplicationStatusSlaveSQLNotRunning(t *testing.T) {
	input := &ReplicationStatus{
		SlaveIORunning:  true,
		SlaveSQLRunning: false,
	}
	want := false
	if got := input.SlaveRunning(); got != want {
		t.Errorf("%#v.SlaveRunning() = %v, want %v", input, got, want)
	}
}

func TestReplicationStatusMasterAddr(t *testing.T) {
	input := &ReplicationStatus{
		MasterHost: "master-host",
		MasterPort: 1234,
	}
	want := "master-host:1234"
	if got := input.MasterAddr(); got != want {
		t.Errorf("%#v.MasterAddr() = %v, want %v", input, got, want)
	}
}

func TestNewReplicationStatus(t *testing.T) {
	table := map[string]*ReplicationStatus{
		"master-host:1234": &ReplicationStatus{
			MasterHost: "master-host",
			MasterPort: 1234,
		},
		"[::1]:4321": &ReplicationStatus{
			MasterHost: "::1",
			MasterPort: 4321,
		},
	}
	for input, want := range table {
		got, err := NewReplicationStatus(input)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if got.MasterHost != want.MasterHost || got.MasterPort != want.MasterPort {
			t.Errorf("NewReplicationStatus(%#v) = %#v, want %#v", input, got, want)
		}
	}
}
