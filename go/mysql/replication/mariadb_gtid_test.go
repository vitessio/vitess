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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseMariaGTID(t *testing.T) {
	input := "12-345-6789"
	want := MariadbGTID{Domain: 12, Server: 345, Sequence: 6789}

	got, err := parseMariadbGTID(input)
	assert.NoError(t, err, "%v", err)
	assert.Equal(t, want, got.(MariadbGTID), "parseMariadbGTID(%v) = %v, want %v", input, got, want)

}

func TestParseInvalidMariaGTID(t *testing.T) {
	input := "12345"
	want := "invalid MariaDB GTID"

	_, err := parseMariadbGTID(input)
	assert.Error(t, err, "expected error for invalid input (%v)", input)
	assert.True(t, strings.HasPrefix(err.Error(), want), "wrong error message, got '%v', want '%v'", err, want)

}

func TestParseMariaGTIDInvalidDomain(t *testing.T) {
	input := "1x-33-142"
	want := "invalid MariaDB GTID Domain ID"

	_, err := parseMariadbGTID(input)
	assert.Error(t, err, "expected error for invalid input (%v)", input)
	assert.True(t, strings.HasPrefix(err.Error(), want), "wrong error message, got '%v', want '%v'", err, want)

}

func TestParseMariaGTIDInvalidServer(t *testing.T) {
	input := "1-2c3-142"
	want := "invalid MariaDB GTID Server ID"

	_, err := parseMariadbGTID(input)
	assert.Error(t, err, "expected error for invalid input (%v)", input)
	assert.True(t, strings.HasPrefix(err.Error(), want), "wrong error message, got '%v', want '%v'", err, want)

}

func TestParseMariaGTIDInvalidSequence(t *testing.T) {
	input := "1-33-a142"
	want := "invalid MariaDB GTID Sequence number"

	_, err := parseMariadbGTID(input)
	assert.Error(t, err, "expected error for invalid input (%v)", input)
	assert.True(t, strings.HasPrefix(err.Error(), want), "wrong error message, got '%v', want '%v'", err, want)

}

func TestParseMariaGTIDSet(t *testing.T) {
	input := "12-34-5678,11-22-3333"
	want := MariadbGTIDSet{
		12: MariadbGTID{Domain: 12, Server: 34, Sequence: 5678},
		11: MariadbGTID{Domain: 11, Server: 22, Sequence: 3333},
	}

	got, err := ParseMariadbGTIDSet(input)
	assert.NoError(t, err, "%v", err)
	assert.True(t, got.Equal(want), "ParseMariadbGTIDSet(%#v) = %#v, want %#v", input, got, want)

}

func TestParseInvalidMariaGTIDSet(t *testing.T) {
	input := "12-34-5678,11-22-33e33"
	want := "invalid MariaDB GTID Sequence number"

	_, err := ParseMariadbGTIDSet(input)
	if err == nil {
		t.Errorf("expected error for invalid input (%#v)", input)
		return
	}
	if got := err.Error(); !strings.HasPrefix(got, want) {
		t.Errorf("ParseMariadbGTIDSet(%#v) error = %#v, want %#v", input, got, want)
	}
}

func TestMariaGTIDString(t *testing.T) {
	input := MariadbGTID{Domain: 5, Server: 4727, Sequence: 1737373}
	want := "5-4727-1737373"

	got := input.String()
	assert.Equal(t, want, got, "%#v.String() = '%v', want '%v'", input, got, want)

}

func TestMariaGTIDFlavor(t *testing.T) {
	input := MariadbGTID{Domain: 1, Server: 2, Sequence: 123}
	want := "MariaDB"

	got := input.Flavor()
	assert.Equal(t, want, got, "%#v.Flavor() = '%v', want '%v'", input, got, want)

}

func TestMariaGTIDSequenceDomain(t *testing.T) {
	input := MariadbGTID{Domain: 12, Server: 345, Sequence: 6789}
	want := any(uint32(12))

	got := input.SequenceDomain()
	assert.Equal(t, want, got, "%#v.SequenceDomain() = %#v, want %#v", input, got, want)

}

func TestMariaGTIDSourceServer(t *testing.T) {
	input := MariadbGTID{Domain: 12, Server: 345, Sequence: 6789}
	want := any(uint32(345))

	got := input.SourceServer()
	assert.Equal(t, want, got, "%#v.SourceServer() = %#v, want %#v", input, got, want)

}

func TestMariaGTIDSequenceNumber(t *testing.T) {
	input := MariadbGTID{Domain: 12, Server: 345, Sequence: 6789}
	want := any(uint64(6789))

	got := input.SequenceNumber()
	assert.Equal(t, want, got, "%#v.SequenceNumber() = %#v, want %#v", input, got, want)

}

func TestMariaGTIDGTIDSet(t *testing.T) {
	input := MariadbGTID{Domain: 12, Server: 345, Sequence: 6789}
	want := MariadbGTIDSet{12: input}

	got := input.GTIDSet()
	assert.True(t, got.Equal(want), "%#v.GTIDSet() = %#v, want %#v", input, got, want)

}

func TestMariaGTIDSetString(t *testing.T) {
	input := MariadbGTIDSet{
		5: MariadbGTID{Domain: 5, Server: 4727, Sequence: 1737373},
		3: MariadbGTID{Domain: 3, Server: 4321, Sequence: 9876},
		1: MariadbGTID{Domain: 1, Server: 1234, Sequence: 5678},
	}
	want := "1-1234-5678,3-4321-9876,5-4727-1737373"

	got := input.String()
	assert.Equal(t, want, got, "%#v.String() = '%v', want '%v'", input, got, want)

}

func TestMariaGTIDSetContainsLess(t *testing.T) {
	input1 := MariadbGTIDSet{5: MariadbGTID{Domain: 5, Server: 4727, Sequence: 300}}
	input2 := MariadbGTIDSet{5: MariadbGTID{Domain: 5, Server: 4727, Sequence: 700}}
	want := false

	if got := input1.Contains(input2); got != want {
		t.Errorf("%#v.Contains(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetContainsGreater(t *testing.T) {
	input1 := MariadbGTIDSet{5: MariadbGTID{Domain: 5, Server: 4727, Sequence: 9000}}
	input2 := MariadbGTIDSet{5: MariadbGTID{Domain: 5, Server: 5555, Sequence: 100}}
	want := true

	if got := input1.Contains(input2); got != want {
		t.Errorf("%#v.Contains(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetContainsEqual(t *testing.T) {
	input1 := MariadbGTIDSet{
		1: MariadbGTID{Domain: 1, Server: 4321, Sequence: 100},
		5: MariadbGTID{Domain: 5, Server: 4727, Sequence: 1234},
	}
	input2 := MariadbGTIDSet{
		1: MariadbGTID{Domain: 1, Server: 4321, Sequence: 100},
		5: MariadbGTID{Domain: 5, Server: 4727, Sequence: 1234},
	}
	want := true

	if got := input1.Contains(input2); got != want {
		t.Errorf("%#v.Contains(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetMultipleContainsLess(t *testing.T) {
	input1 := MariadbGTIDSet{
		4: MariadbGTID{Domain: 4, Server: 1, Sequence: 123},
		5: MariadbGTID{Domain: 5, Server: 5, Sequence: 24601},
	}
	input2 := MariadbGTIDSet{
		4: MariadbGTID{Domain: 4, Server: 1, Sequence: 124},
	}
	want := false

	if got := input1.Contains(input2); got != want {
		t.Errorf("%#v.Contains(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetMultipleContainsGreater(t *testing.T) {
	input1 := MariadbGTIDSet{
		4: MariadbGTID{Domain: 4, Server: 1, Sequence: 123},
		5: MariadbGTID{Domain: 5, Server: 5, Sequence: 24601},
	}
	input2 := MariadbGTIDSet{
		4: MariadbGTID{Domain: 4, Server: 2, Sequence: 122},
	}
	want := true

	if got := input1.Contains(input2); got != want {
		t.Errorf("%#v.Contains(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetMultipleContainsMultipleGreater(t *testing.T) {
	input1 := MariadbGTIDSet{
		4: MariadbGTID{Domain: 4, Server: 1, Sequence: 123},
		5: MariadbGTID{Domain: 5, Server: 5, Sequence: 24601},
	}
	input2 := MariadbGTIDSet{
		4: MariadbGTID{Domain: 4, Server: 1, Sequence: 122},
		5: MariadbGTID{Domain: 5, Server: 4, Sequence: 1999},
	}
	want := true

	if got := input1.Contains(input2); got != want {
		t.Errorf("%#v.Contains(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetMultipleContainsOneLess(t *testing.T) {
	input1 := MariadbGTIDSet{
		4: MariadbGTID{Domain: 4, Server: 1, Sequence: 123},
		5: MariadbGTID{Domain: 5, Server: 5, Sequence: 24601},
	}
	input2 := MariadbGTIDSet{
		4: MariadbGTID{Domain: 4, Server: 1, Sequence: 122},
		5: MariadbGTID{Domain: 5, Server: 4, Sequence: 24602},
	}
	want := false

	if got := input1.Contains(input2); got != want {
		t.Errorf("%#v.Contains(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetSingleContainsMultiple(t *testing.T) {
	input1 := MariadbGTIDSet{
		4: MariadbGTID{Domain: 4, Server: 1, Sequence: 123},
	}
	input2 := MariadbGTIDSet{
		4: MariadbGTID{Domain: 4, Server: 1, Sequence: 122},
		5: MariadbGTID{Domain: 5, Server: 4, Sequence: 24602},
	}
	want := false

	if got := input1.Contains(input2); got != want {
		t.Errorf("%#v.Contains(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetContainsConcreteNil(t *testing.T) {
	input1 := MariadbGTIDSet{1: MariadbGTID{Domain: 1, Server: 2, Sequence: 123}}
	input2 := MariadbGTIDSet(nil)
	want := true

	if got := input1.Contains(input2); got != want {
		t.Errorf("%#v.Contains(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetFailsNil(t *testing.T) {
	input1 := MariadbGTIDSet{6: MariadbGTID{Domain: 23, Server: 1, Sequence: 456}}
	input2 := GTIDSet(nil)
	want := false

	if got := input1.Contains(input2); got != want {
		t.Errorf("%#v.Contains(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetContainsWrongType(t *testing.T) {
	input1 := MariadbGTIDSet{5: MariadbGTID{Domain: 5, Server: 4727, Sequence: 1234}}
	input2 := fakeGTID{}
	want := false

	if got := input1.Contains(input2); got != want {
		t.Errorf("%#v.Contains(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetContainsDifferentDomain(t *testing.T) {
	input1 := MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 4727, Sequence: 1235}}
	input2 := MariadbGTIDSet{5: MariadbGTID{Domain: 5, Server: 4727, Sequence: 1234}}
	want := false

	if got := input1.Contains(input2); got != want {
		t.Errorf("%#v.Contains(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetContainsDifferentServer(t *testing.T) {
	input1 := MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 4727, Sequence: 1235}}
	input2 := MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	want := true

	if got := input1.Contains(input2); got != want {
		t.Errorf("%#v.Contains(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetContainsGTIDLess(t *testing.T) {
	input1 := MariadbGTIDSet{5: MariadbGTID{Domain: 5, Server: 4727, Sequence: 300}}
	input2 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 700}
	want := false

	if got := input1.ContainsGTID(input2); got != want {
		t.Errorf("%#v.ContainsGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetContainsGTIDGreater(t *testing.T) {
	input1 := MariadbGTIDSet{5: MariadbGTID{Domain: 5, Server: 4727, Sequence: 9000}}
	input2 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 100}
	want := true

	if got := input1.ContainsGTID(input2); got != want {
		t.Errorf("%#v.ContainsGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetContainsGTIDEqual(t *testing.T) {
	input1 := MariadbGTIDSet{5: MariadbGTID{Domain: 5, Server: 4727, Sequence: 1234}}
	input2 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 1234}
	want := true

	if got := input1.ContainsGTID(input2); got != want {
		t.Errorf("%#v.ContainsGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetContainsGTIDNil(t *testing.T) {
	input1 := MariadbGTIDSet{1: MariadbGTID{Domain: 1, Server: 2, Sequence: 123}}
	input2 := GTID(nil)
	want := true

	if got := input1.ContainsGTID(input2); got != want {
		t.Errorf("%#v.ContainsGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetContainsGTIDWrongType(t *testing.T) {
	input1 := MariadbGTIDSet{5: MariadbGTID{Domain: 5, Server: 4727, Sequence: 1234}}
	input2 := fakeGTID{}
	want := false

	if got := input1.ContainsGTID(input2); got != want {
		t.Errorf("%#v.ContainsGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetContainsGTIDDifferentDomain(t *testing.T) {
	input1 := MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 4727, Sequence: 1235}}
	input2 := MariadbGTID{Domain: 5, Server: 4727, Sequence: 1234}
	want := false

	if got := input1.ContainsGTID(input2); got != want {
		t.Errorf("%#v.ContainsGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetContainsGTIDDifferentServer(t *testing.T) {
	input1 := MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 4727, Sequence: 1235}}
	input2 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}
	want := true

	if got := input1.ContainsGTID(input2); got != want {
		t.Errorf("%#v.ContainsGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetEqual(t *testing.T) {
	input1 := MariadbGTIDSet{
		1: MariadbGTID{Domain: 1, Server: 1234, Sequence: 5678},
		3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234},
	}
	input2 := MariadbGTIDSet{
		1: MariadbGTID{Domain: 1, Server: 1234, Sequence: 5678},
		3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234},
	}
	want := true

	if got := input1.Equal(input2); got != want {
		t.Errorf("%#v.Equal(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetNotEqual(t *testing.T) {
	input1 := MariadbGTIDSet{
		3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234},
		4: MariadbGTID{Domain: 4, Server: 4555, Sequence: 1234},
	}
	input2 := MariadbGTIDSet{
		3: MariadbGTID{Domain: 3, Server: 4555, Sequence: 1234},
		4: MariadbGTID{Domain: 4, Server: 4555, Sequence: 1234},
	}
	want := false

	if got := input1.Equal(input2); got != want {
		t.Errorf("%#v.Equal(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetNotEqualDifferentDomain(t *testing.T) {
	input1 := MariadbGTIDSet{
		3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234},
		4: MariadbGTID{Domain: 4, Server: 4555, Sequence: 1234},
	}
	input2 := MariadbGTIDSet{
		3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234},
		5: MariadbGTID{Domain: 5, Server: 4555, Sequence: 1234},
	}
	want := false

	if got := input1.Equal(input2); got != want {
		t.Errorf("%#v.Equal(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetNotEqualExtraDomain(t *testing.T) {
	input1 := MariadbGTIDSet{
		3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234},
	}
	input2 := MariadbGTIDSet{
		3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234},
		4: MariadbGTID{Domain: 4, Server: 4555, Sequence: 1234},
	}
	want := false

	if got := input1.Equal(input2); got != want {
		t.Errorf("%#v.Equal(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetEqualWrongType(t *testing.T) {
	input1 := MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	input2 := fakeGTID{}
	want := false

	if got := input1.Equal(input2); got != want {
		t.Errorf("%#v.Equal(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetEqualNil(t *testing.T) {
	input1 := MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	input2 := GTIDSet(nil)
	want := false

	if got := input1.Equal(input2); got != want {
		t.Errorf("%#v.Equal(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetAddGTIDEqual(t *testing.T) {
	input1 := MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	input2 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}
	want := input1

	if got := input1.AddGTID(input2); !got.Equal(want) {
		t.Errorf("%#v.AddGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetAddGTIDGreater(t *testing.T) {
	input1 := MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 5234}}
	input2 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}
	want := input1

	if got := input1.AddGTID(input2); !got.Equal(want) {
		t.Errorf("%#v.AddGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetAddGTIDLess(t *testing.T) {
	input1 := MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	input2 := MariadbGTID{Domain: 3, Server: 5555, Sequence: 5234}
	want := MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 5234}}

	if got := input1.AddGTID(input2); !got.Equal(want) {
		t.Errorf("%#v.AddGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetAddGTIDWrongType(t *testing.T) {
	input1 := MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	input2 := fakeGTID{}
	want := input1

	if got := input1.AddGTID(input2); !got.Equal(want) {
		t.Errorf("%#v.AddGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetAddGTIDNil(t *testing.T) {
	input1 := MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	input2 := GTID(nil)
	want := input1

	if got := input1.AddGTID(input2); !got.Equal(want) {
		t.Errorf("%#v.AddGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetAddGTIDDifferentServer(t *testing.T) {
	input1 := MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	input2 := MariadbGTID{Domain: 3, Server: 4444, Sequence: 5234}
	want := MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 4444, Sequence: 5234}}

	if got := input1.AddGTID(input2); !got.Equal(want) {
		t.Errorf("%#v.AddGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetAddGTIDDifferentDomain(t *testing.T) {
	input1 := MariadbGTIDSet{3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234}}
	input2 := MariadbGTID{Domain: 5, Server: 5555, Sequence: 5234}
	want := MariadbGTIDSet{
		3: MariadbGTID{Domain: 3, Server: 5555, Sequence: 1234},
		5: MariadbGTID{Domain: 5, Server: 5555, Sequence: 5234},
	}

	if got := input1.AddGTID(input2); !got.Equal(want) {
		t.Errorf("%#v.AddGTID(%#v) = %v, want %v", input1, input2, got, want)
	}
}

func TestMariaGTIDSetUnion(t *testing.T) {
	set1 := MariadbGTIDSet{
		3: MariadbGTID{Domain: 3, Server: 1, Sequence: 1},
		4: MariadbGTID{Domain: 4, Server: 1, Sequence: 2},
		5: MariadbGTID{Domain: 5, Server: 1, Sequence: 3},
	}
	set2 := MariadbGTIDSet{
		3: MariadbGTID{Domain: 3, Server: 1, Sequence: 2},
		4: MariadbGTID{Domain: 4, Server: 1, Sequence: 1},
		5: MariadbGTID{Domain: 5, Server: 1, Sequence: 4},
	}

	got := set1.Union(set2)

	want := MariadbGTIDSet{
		3: MariadbGTID{Domain: 3, Server: 1, Sequence: 2},
		4: MariadbGTID{Domain: 4, Server: 1, Sequence: 2},
		5: MariadbGTID{Domain: 5, Server: 1, Sequence: 4},
	}
	assert.True(t, got.Equal(want), "set1: %#v, set1.Union(%#v) = %#v, want %#v", set1, set2, got, want)

}

func TestMariaGTIDSetUnionNewDomain(t *testing.T) {
	set1 := MariadbGTIDSet{
		3: MariadbGTID{Domain: 3, Server: 1, Sequence: 1},
		4: MariadbGTID{Domain: 4, Server: 1, Sequence: 2},
		5: MariadbGTID{Domain: 5, Server: 1, Sequence: 3},
	}
	set2 := MariadbGTIDSet{
		3: MariadbGTID{Domain: 3, Server: 1, Sequence: 2},
		4: MariadbGTID{Domain: 4, Server: 1, Sequence: 1},
		5: MariadbGTID{Domain: 5, Server: 1, Sequence: 4},
		6: MariadbGTID{Domain: 6, Server: 1, Sequence: 7},
	}

	got := set1.Union(set2)

	want := MariadbGTIDSet{
		3: MariadbGTID{Domain: 3, Server: 1, Sequence: 2},
		4: MariadbGTID{Domain: 4, Server: 1, Sequence: 2},
		5: MariadbGTID{Domain: 5, Server: 1, Sequence: 4},
		6: MariadbGTID{Domain: 6, Server: 1, Sequence: 7},
	}
	assert.True(t, got.Equal(want), "set1: %#v, set1.Union(%#v) = %#v, want %#v", set1, set2, got, want)

	switch g := got.(type) {
	case MariadbGTIDSet:
		// Enforce order of want. Results should be sorted by domain.
		if g[0] != want[0] || g[1] != want[1] || g[2] != want[2] || g[3] != want[3] {
			t.Error("Set was not sorted by domain when returned.")
		}
	default:
		t.Error("Union result was not of type MariadbGTIDSet.")
	}
}

func TestMariaGTIDSetLast(t *testing.T) {

	testCases := map[string]string{
		"12-34-5678,11-22-3333,24-52-4523": "24-52-4523",
		"12-34-5678":                       "12-34-5678",
	}
	for input, want := range testCases {
		got, err := ParseMariadbGTIDSet(input)
		require.NoError(t, err)
		assert.Equal(t, want, got.Last())
	}
}
