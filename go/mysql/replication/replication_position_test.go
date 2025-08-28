/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package replication

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPositionEqual(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
	input2 := Position{GTIDSet: MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
	want := true

	if got := input1.Equal(input2); got != want {
		t.Errorf("%#v.Equal(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionNotEqual(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
	input2 := Position{GTIDSet: MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 12345}}}
	want := false

	if got := input1.Equal(input2); got != want {
		t.Errorf("%#v.Equal(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionEqualZero(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
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
	input1 := Position{GTIDSet: MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1233}}}
	input2 := Position{GTIDSet: MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
	want := false

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionAtLeastEqual(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
	input2 := Position{GTIDSet: MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
	want := true

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionAtLeastGreater(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1235}}}
	input2 := Position{GTIDSet: MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
	want := true

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionAtLeastDifferentServer(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1235}}}
	input2 := Position{GTIDSet: MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 4444, Sequence: 1234}}}
	want := true

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionAtLeastDifferentDomain(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1235}}}
	input2 := Position{GTIDSet: MariadbGTIDSet{4: MariadbGTID{Domain: 4, Server: 5555, Sequence: 1234}}}
	want := false

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionZeroAtLeast(t *testing.T) {
	input1 := Position{}
	input2 := Position{GTIDSet: MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
	want := false

	if got := input1.AtLeast(input2); got != want {
		t.Errorf("%#v.AtLeast(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestPositionAtLeastZero(t *testing.T) {
	input1 := Position{GTIDSet: MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
	input2 := Position{GTIDSet: nil}
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
	input := Position{GTIDSet: MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
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
	input := Position{GTIDSet: MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
	want := false

	if got := input.IsZero(); got != want {
		t.Errorf("%#v.IsZero() = %#v, want %#v", input, got, want)
	}
}

func TestPositionAppend(t *testing.T) {
	t.Run("MariaDB", func(t *testing.T) {
		input1 := Position{GTIDSet: MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
		input2 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1235}
		want := Position{GTIDSet: MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1235}}}

		if got := AppendGTID(input1, input2); !got.Equal(want) {
			t.Errorf("AppendGTID(%#v, %#v) = %#v, want %#v", input1, input2, got, want)
		}
	})
	t.Run("MySQL56", func(t *testing.T) {
		gtidset, err := ParseMysql56GTIDSet("16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615")
		require.NoError(t, err)
		gtid, err := parseMysql56GTID("16b1039f-22b6-11ed-b765-0a43f95f28a3:616")
		require.NoError(t, err)
		want, err := ParseMysql56GTIDSet("16b1039f-22b6-11ed-b765-0a43f95f28a3:1-616")
		require.NoError(t, err)

		pos := Position{GTIDSet: gtidset}
		wantPos := Position{GTIDSet: want}

		gotPos := AppendGTID(pos, gtid)
		assert.Equal(t, wantPos, gotPos, "got=%v", gotPos)
		assert.NotEqual(t, pos, gotPos)

		gotPos = AppendGTIDInPlace(pos, gtid)
		assert.Equal(t, wantPos, gotPos)
		assert.Equal(t, wantPos, pos)
	})
}

func TestPositionAppendBefore(t *testing.T) {
	t.Run("MySQL56", func(t *testing.T) {
		gtidset, err := ParseMysql56GTIDSet("16b1039f-22b6-11ed-b765-0a43f95f28a3:10-615")
		require.NoError(t, err)
		gtid, err := parseMysql56GTID("16b1039f-22b6-11ed-b765-0a43f95f28a3:9")
		require.NoError(t, err)
		want, err := ParseMysql56GTIDSet("16b1039f-22b6-11ed-b765-0a43f95f28a3:9-615")
		require.NoError(t, err)

		pos := Position{GTIDSet: gtidset}
		wantPos := Position{GTIDSet: want}

		gotPos := AppendGTID(pos, gtid)
		assert.Equal(t, wantPos, gotPos, "got=%v", gotPos)
		assert.NotEqual(t, pos, gotPos)

		gotPos = AppendGTIDInPlace(pos, gtid)
		assert.Equal(t, wantPos, gotPos)
		assert.Equal(t, wantPos, pos)
	})
}

func TestPositionAppendNewInterval(t *testing.T) {
	t.Run("MySQL56", func(t *testing.T) {
		gtidset, err := ParseMysql56GTIDSet("16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615")
		require.NoError(t, err)
		gtid, err := parseMysql56GTID("16b1039f-22b6-11ed-b765-0a43f95f28a3:620")
		require.NoError(t, err)
		want, err := ParseMysql56GTIDSet("16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615:620")
		require.NoError(t, err)

		pos := Position{GTIDSet: gtidset}
		wantPos := Position{GTIDSet: want}

		gotPos := AppendGTID(pos, gtid)
		assert.Equal(t, wantPos, gotPos, "got=%v", gotPos)
		assert.NotEqual(t, pos, gotPos)

		gotPos = AppendGTIDInPlace(pos, gtid)
		assert.Equal(t, wantPos, gotPos)
		assert.Equal(t, wantPos, pos)
	})
}

func TestPositionAppendContains(t *testing.T) {
	t.Run("MySQL56", func(t *testing.T) {
		gtidset, err := ParseMysql56GTIDSet("16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615")
		require.NoError(t, err)
		gtid, err := parseMysql56GTID("16b1039f-22b6-11ed-b765-0a43f95f28a3:600")
		require.NoError(t, err)
		want, err := ParseMysql56GTIDSet("16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615")
		require.NoError(t, err)

		pos := Position{GTIDSet: gtidset}
		wantPos := Position{GTIDSet: want}

		gotPos := AppendGTID(pos, gtid)
		assert.Equal(t, wantPos, gotPos, "got=%v", gotPos)
		assert.Equal(t, pos, gotPos)

		gotPos = AppendGTIDInPlace(pos, gtid)
		assert.Equal(t, wantPos, gotPos)
		assert.Equal(t, wantPos, pos)
	})
}

func TestPositionAppendNil(t *testing.T) {
	t.Run("MariaDB", func(t *testing.T) {
		input1 := Position{GTIDSet: MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}
		input2 := GTID(nil)
		want := Position{GTIDSet: MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}

		if got := AppendGTID(input1, input2); !got.Equal(want) {
			t.Errorf("AppendGTID(%#v, %#v) = %#v, want %#v", input1, input2, got, want)
		}
	})
	t.Run("MySQL56", func(t *testing.T) {
		gtidset, err := ParseMysql56GTIDSet("16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615")
		require.NoError(t, err)
		gtid := GTID(nil)
		require.NoError(t, err)
		want, err := ParseMysql56GTIDSet("16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615")
		require.NoError(t, err)

		pos := Position{GTIDSet: gtidset}
		wantPos := Position{GTIDSet: want}

		gotPos := AppendGTID(pos, gtid)
		assert.Equal(t, wantPos, gotPos, "got=%v", gotPos)
		assert.Equal(t, pos, gotPos)

		gotPos = AppendGTIDInPlace(pos, gtid)
		assert.Equal(t, wantPos, gotPos)
		assert.Equal(t, wantPos, pos)
	})
}

func TestPositionAppendToZero(t *testing.T) {
	t.Run("MariaDB", func(t *testing.T) {
		input1 := Position{}
		input2 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}
		want := Position{GTIDSet: MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}}

		if got := AppendGTID(input1, input2); !got.Equal(want) {
			t.Errorf("AppendGTID(%#v, %#v) = %#v, want %#v", input1, input2, got, want)
		}
	})
	t.Run("MySQL56", func(t *testing.T) {
		gtid, err := parseMysql56GTID("16b1039f-22b6-11ed-b765-0a43f95f28a3:616")
		require.NoError(t, err)
		want, err := ParseMysql56GTIDSet("16b1039f-22b6-11ed-b765-0a43f95f28a3:616")
		require.NoError(t, err)

		pos := Position{}
		wantPos := Position{GTIDSet: want}

		gotPos := AppendGTID(pos, gtid)
		assert.Equal(t, wantPos, gotPos, "got=%v", gotPos)
		assert.NotEqual(t, pos, gotPos)

		gotPos = AppendGTIDInPlace(pos, gtid)
		assert.Equal(t, wantPos, gotPos)
	})
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
		assert.NotNil(t, err, "wrong error, got %#v, want %#v", err, want)

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
	assert.NoError(t, err, "unexpected error: %v", err)
	assert.True(t, got.Equal(want), "DecodePosition(%#v) = %#v, want %#v", input, got, want)

}

func TestDecodePositionDefaultFlavor(t *testing.T) {
	gtidSetParsers[Mysql56FlavorID] = func(s string) (GTIDSet, error) {
		return ParseMysql56GTIDSet(s)
	}
	{
		pos := "MySQL56/16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615"
		rp, err := DecodePositionDefaultFlavor(pos, "foo")
		assert.NoError(t, err)
		assert.Equal(t, "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615", rp.GTIDSet.String())
	}
	{
		pos := "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615"
		rp, err := DecodePositionDefaultFlavor(pos, Mysql56FlavorID)
		assert.NoError(t, err)
		assert.Equal(t, "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615", rp.GTIDSet.String())
	}
}

func TestDecodePositionZero(t *testing.T) {
	input := ""
	want := Position{}

	got, err := DecodePosition(input)
	assert.NoError(t, err, "unexpected error: %v", err)
	assert.True(t, got.Equal(want), "DecodePosition(%#v) = %#v, want %#v", input, got, want)

}

func TestDecodePositionNoFlavor(t *testing.T) {
	gtidSetParsers[""] = func(s string) (GTIDSet, error) {
		return fakeGTID{value: s}, nil
	}
	input := "12345"
	want := Position{GTIDSet: fakeGTID{value: "12345"}}

	got, err := DecodePosition(input)
	assert.NoError(t, err, "unexpected error: %v", err)
	assert.True(t, got.Equal(want), "DecodePosition(%#v) = %#v, want %#v", input, got, want)

}

func TestJsonMarshalPosition(t *testing.T) {
	input := Position{GTIDSet: fakeGTID{flavor: "golf", value: "par"}}
	want := `"golf/par"`

	buf, err := json.Marshal(input)
	assert.NoError(t, err, "unexpected error: %v", err)

	if got := string(buf); got != want {
		t.Errorf("json.Marshal(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestJsonMarshalPositionPointer(t *testing.T) {
	input := Position{GTIDSet: fakeGTID{flavor: "golf", value: "par"}}
	want := `"golf/par"`

	buf, err := json.Marshal(&input)
	assert.NoError(t, err, "unexpected error: %v", err)

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
	assert.NoError(t, err, "unexpected error: %v", err)
	assert.True(t, got.Equal(want), "json.Unmarshal(%#v) = %#v, want %#v", input, got, want)

}

func TestJsonMarshalPositionInStruct(t *testing.T) {
	input := Position{GTIDSet: fakeGTID{flavor: "golf", value: "par"}}
	want := `{"Position":"golf/par"}`

	type mystruct struct {
		Position Position
	}

	buf, err := json.Marshal(&mystruct{input})
	assert.NoError(t, err, "unexpected error: %v", err)

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
	assert.NoError(t, err, "unexpected error: %v", err)

	if got := gotStruct.Position; !got.Equal(want) {
		t.Errorf("json.Unmarshal(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestJsonMarshalPositionZero(t *testing.T) {
	input := Position{}
	want := `""`

	buf, err := json.Marshal(input)
	assert.NoError(t, err, "unexpected error: %v", err)

	if got := string(buf); got != want {
		t.Errorf("json.Marshal(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestJsonUnmarshalPositionZero(t *testing.T) {
	input := `""`
	want := Position{}

	var got Position
	err := json.Unmarshal([]byte(input), &got)
	assert.NoError(t, err, "unexpected error: %v", err)
	assert.True(t, got.Equal(want), "json.Unmarshal(%#v) = %#v, want %#v", input, got, want)

}

func TestComparePositionsSortStable(t *testing.T) {
	sid, _ := ParseSID("3e11fa47-71ca-11e1-9e33-c80aa9429562")
	positions := []Position{
		{GTIDSet: Mysql56GTIDSet{sid: []interval{{start: 1, end: 5}}}},
		{GTIDSet: Mysql56GTIDSet{sid: []interval{{start: 1, end: 5}}}},
		{GTIDSet: Mysql56GTIDSet{sid: []interval{{start: 1, end: 6}}}},
		{GTIDSet: Mysql56GTIDSet{sid: []interval{{start: 1, end: 2}}}},
		{GTIDSet: Mysql56GTIDSet{sid: []interval{{start: 1, end: 7}}}},
		{GTIDSet: Mysql56GTIDSet{sid: []interval{{start: 1, end: 6}}}},
	}

	wantedStrings := []string{
		"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-7",
		"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6",
		"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-6",
		"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
		"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
		"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-2",
	}

	slices.SortStableFunc(positions, func(a, b Position) int {
		return ComparePositions(a, b)
	})

	for i, wanted := range wantedStrings {
		require.Equal(t, wanted, positions[i].String())
	}
}
