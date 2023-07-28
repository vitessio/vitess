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

package mysql

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSortSIDList(t *testing.T) {
	input := []SID{
		{1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16},
		{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
	}
	want := []SID{
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16},
		{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		{1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
	}
	sortSIDs(input)
	assert.True(t, reflect.DeepEqual(input, want), "got %#v, want %#v", input, want)
}

func TestParseMysql56GTIDSet(t *testing.T) {
	sid1 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	sid2 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 255}

	table := map[string]Mysql56GTIDSet{
		// Empty
		"": {},
		// Simple case
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-5": {
			sid1: []interval{{1, 5}},
		},
		// Capital hex chars
		"00010203-0405-0607-0809-0A0B0C0D0E0F:1-5": {
			sid1: []interval{{1, 5}},
		},
		// Interval with same start and end
		"00010203-0405-0607-0809-0a0b0c0d0e0f:12": {
			sid1: []interval{{12, 12}},
		},
		// Multiple intervals
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20": {
			sid1: []interval{{1, 5}, {10, 20}},
		},
		// Multiple intervals, out of order
		"00010203-0405-0607-0809-0a0b0c0d0e0f:10-20:1-5": {
			sid1: []interval{{1, 5}, {10, 20}},
		},
		// Intervals with end < start are discarded by MySQL 5.6
		"00010203-0405-0607-0809-0a0b0c0d0e0f:8-7": {},
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:8-7:10-20": {
			sid1: []interval{{1, 5}, {10, 20}},
		},
		// Same repeating SIDs
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-5,00010203-0405-0607-0809-0a0b0c0d0e0f:10-20": {
			sid1: []interval{{1, 5}, {10, 20}},
		},
		// Same repeating SIDs, backwards order
		"00010203-0405-0607-0809-0a0b0c0d0e0f:10-20,00010203-0405-0607-0809-0a0b0c0d0e0f:1-5": {
			sid1: []interval{{1, 5}, {10, 20}},
		},
		// Multiple SIDs
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20,00010203-0405-0607-0809-0a0b0c0d0eff:1-5:50": {
			sid1: []interval{{1, 5}, {10, 20}},
			sid2: []interval{{1, 5}, {50, 50}},
		},
		// Multiple SIDs with space around the comma
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20, 00010203-0405-0607-0809-0a0b0c0d0eff:1-5:50": {
			sid1: []interval{{1, 5}, {10, 20}},
			sid2: []interval{{1, 5}, {50, 50}},
		},
	}

	for input, want := range table {
		t.Run(input, func(t *testing.T) {
			got, err := ParseMysql56GTIDSet(input)
			require.NoError(t, err)
			assert.Equal(t, want, got)
		})
	}
}

func TestParseMysql56GTIDSetInvalid(t *testing.T) {
	table := []string{
		// No intervals
		"00010203-0405-0607-0809-0a0b0c0d0e0f",
		// Invalid SID
		"00010203-0405-060X-0809-0a0b0c0d0e0f:1-5",
		// Invalid intervals
		"00010203-0405-0607-0809-0a0b0c0d0e0f:0-5",
		"00010203-0405-0607-0809-0a0b0c0d0e0f:-5",
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-2-3",
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-",
	}

	for _, input := range table {
		_, err := ParseMysql56GTIDSet(input)
		assert.Error(t, err, "ParseMysql56GTIDSet(%#v) expected error, got none", err)
	}
}

func TestMysql56GTIDSetString(t *testing.T) {
	sid1 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	sid2 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 255}

	table := map[string]Mysql56GTIDSet{
		// Simple case
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-5": {
			sid1: []interval{{1, 5}},
		},
		// Interval with same start and end
		"00010203-0405-0607-0809-0a0b0c0d0e0f:12": {
			sid1: []interval{{12, 12}},
		},
		// Multiple intervals
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20": {
			sid1: []interval{{1, 5}, {10, 20}},
		},
		// Multiple SIDs
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20,00010203-0405-0607-0809-0a0b0c0d0eff:1-5:50": {
			sid1: []interval{{1, 5}, {10, 20}},
			sid2: []interval{{1, 5}, {50, 50}},
		},
	}

	for want, input := range table {
		got := strings.ToLower(input.String())
		assert.Equal(t, want, got, "%#v.String() = %#v, want %#v", input, got, want)

	}
}

func TestMysql56GTIDSetFlavor(t *testing.T) {
	input := Mysql56GTIDSet{}
	if got, want := input.Flavor(), "MySQL56"; got != want {
		t.Errorf("%#v.Flavor() = %#v, want %#v", input, got, want)
	}
}

func TestMysql56GTIDSetContainsGTID(t *testing.T) {
	sid1 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	sid2 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16}
	sid3 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 17}

	set := Mysql56GTIDSet{
		sid1: []interval{{20, 30}, {35, 40}},
		sid2: []interval{{1, 5}, {50, 50}},
	}

	table := map[GTID]bool{
		fakeGTID{}: false,

		Mysql56GTID{sid1, 1}:  false,
		Mysql56GTID{sid1, 19}: false,
		Mysql56GTID{sid1, 20}: true,
		Mysql56GTID{sid1, 23}: true,
		Mysql56GTID{sid1, 30}: true,
		Mysql56GTID{sid1, 31}: false,

		Mysql56GTID{sid2, 1}:  true,
		Mysql56GTID{sid2, 10}: false,
		Mysql56GTID{sid2, 50}: true,
		Mysql56GTID{sid2, 51}: false,

		Mysql56GTID{sid3, 1}: false,
	}

	for input, want := range table {
		if got := set.ContainsGTID(input); got != want {
			t.Errorf("ContainsGTID(%#v) = %#v, want %#v", input, got, want)
		}
	}
}

func TestMysql56GTIDSetContains(t *testing.T) {
	sid1 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	sid2 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16}
	sid3 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 17}

	// The set to test against.
	set := Mysql56GTIDSet{
		sid1: []interval{{20, 30}, {35, 40}},
		sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
	}

	// Test cases that should return Contains() = true.
	contained := []Mysql56GTIDSet{
		// The set should contain itself.
		set,
		// Every set contains the empty set.
		{},

		// Simple case
		{sid1: []interval{{25, 30}}},
		// Multiple intervals
		{sid2: []interval{{1, 2}, {4, 5}, {60, 70}}},
		// Multiple SIDs
		{
			sid1: []interval{{25, 30}, {35, 37}},
			sid2: []interval{{1, 5}},
		},
	}

	for _, other := range contained {
		assert.True(t, set.Contains(other), "Contains(%#v) = false, want true", other)

	}

	// Test cases that should return Contains() = false.
	notContained := []GTIDSet{
		// Wrong flavor is not contained.
		fakeGTID{},

		// Simple cases
		Mysql56GTIDSet{sid1: []interval{{1, 5}}},
		Mysql56GTIDSet{sid1: []interval{{10, 19}}},
		// Overlapping intervals
		Mysql56GTIDSet{sid1: []interval{{10, 20}}},
		Mysql56GTIDSet{sid1: []interval{{10, 25}}},
		Mysql56GTIDSet{sid1: []interval{{25, 31}}},
		Mysql56GTIDSet{sid1: []interval{{30, 31}}},
		// Multiple intervals
		Mysql56GTIDSet{sid1: []interval{{20, 30}, {34, 34}}},
		// Multiple SIDs
		Mysql56GTIDSet{
			sid1: []interval{{20, 30}, {36, 36}},
			sid2: []interval{{3, 5}, {55, 60}},
		},
		// SID is missing entirely
		Mysql56GTIDSet{sid3: []interval{{1, 5}}},
	}

	for _, other := range notContained {
		if set.Contains(other) {
			t.Errorf("Contains(%#v) = true, want false", other)
		}
	}
}

func TestMysql56GTIDSetContains2(t *testing.T) {
	set1, err := ParseMysql56GTIDSet("16b1039f-22b6-11ed-b765-0a43f95f28a3:1-243")
	require.NoError(t, err)
	set2, err := ParseMysql56GTIDSet("16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615")
	require.NoError(t, err)
	set3, err := ParseMysql56GTIDSet("16b1039f-22b6-11ed-b765-0a43f95f28a3:1-632")
	require.NoError(t, err)
	set4, err := ParseMysql56GTIDSet("16b1039f-22b6-11ed-b765-0a43f95f28a3:20-664")
	require.NoError(t, err)
	set5, err := ParseMysql56GTIDSet("16b1039f-22b6-11ed-b765-0a43f95f28a3:20-243")
	require.NoError(t, err)

	compareSet, err := ParseMysql56GTIDSet("16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615")
	require.NoError(t, err)

	assert.True(t, compareSet.Contains(set1))
	assert.True(t, compareSet.Contains(set2))
	assert.False(t, compareSet.Contains(set3))
	assert.False(t, compareSet.Contains(set4))
	assert.True(t, compareSet.Contains(set5))
}

func TestMysql56GTIDSetEqual(t *testing.T) {
	sid1 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	sid2 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16}
	sid3 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 17}

	// The set to test against.
	set := Mysql56GTIDSet{
		sid1: []interval{{20, 30}, {35, 40}},
		sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
	}

	// Test cases that should return Equal() = true.
	equal := []Mysql56GTIDSet{
		// Same underlying map instance
		set,
		// Different instance, same data
		{
			sid1: []interval{{20, 30}, {35, 40}},
			sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
		},
	}

	for _, other := range equal {
		assert.True(t, set.Equal(other), "%#v.Equal(%#v) = false, want true", set, other)
		// Equality should be transitive.
		assert.True(t, other.Equal(set), "%#v.Equal(%#v) = false, want true", other, set)

	}

	// Test cases that should return Equal() = false.
	notEqual := []GTIDSet{
		// Wrong flavor is not equal.
		fakeGTID{},
		// Empty set
		Mysql56GTIDSet{},
		// Interval changed
		Mysql56GTIDSet{
			sid1: []interval{{20, 31}, {35, 40}},
			sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
		},
		// Interval added
		Mysql56GTIDSet{
			sid1: []interval{{20, 30}, {32, 33}, {35, 40}},
			sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
		},
		// Interval removed
		Mysql56GTIDSet{
			sid1: []interval{{20, 30}, {35, 40}},
			sid2: []interval{{1, 5}, {60, 70}},
		},
		// Different SID, same intervals
		Mysql56GTIDSet{
			sid1: []interval{{20, 30}, {35, 40}},
			sid3: []interval{{1, 5}, {50, 50}, {60, 70}},
		},
		// SID added
		Mysql56GTIDSet{
			sid1: []interval{{20, 30}, {35, 40}},
			sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
			sid3: []interval{{1, 5}},
		},
		// SID removed
		Mysql56GTIDSet{
			sid1: []interval{{20, 30}, {35, 40}},
		},
	}

	for _, other := range notEqual {
		if set.Equal(other) {
			t.Errorf("%#v.Equal(%#v) = true, want false", set, other)
		}
		// Equality should be transitive.
		if other.Equal(set) {
			t.Errorf("%#v.Equal(%#v) = true, want false", other, set)
		}
	}
}

func TestMysql56GTIDSetAddGTID(t *testing.T) {
	sid1 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	sid2 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16}
	sid3 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 17}

	// The set to test against.
	set := Mysql56GTIDSet{
		sid1: []interval{{20, 30}, {35, 40}, {42, 45}},
		sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
	}

	table := map[GTID]Mysql56GTIDSet{
		// Adding wrong flavor is a no-op.
		fakeGTID{}: set,

		// Adding GTIDs that are already in the set
		Mysql56GTID{Server: sid1, Sequence: 20}: {
			sid1: []interval{{20, 30}, {35, 40}, {42, 45}},
			sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
		},
		Mysql56GTID{Server: sid1, Sequence: 30}: {
			sid1: []interval{{20, 30}, {35, 40}, {42, 45}},
			sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
		},
		Mysql56GTID{Server: sid1, Sequence: 25}: {
			sid1: []interval{{20, 30}, {35, 40}, {42, 45}},
			sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
		},
		// New interval beginning
		Mysql56GTID{Server: sid1, Sequence: 1}: {
			sid1: []interval{{1, 1}, {20, 30}, {35, 40}, {42, 45}},
			sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
		},
		// New interval middle
		Mysql56GTID{Server: sid1, Sequence: 32}: {
			sid1: []interval{{20, 30}, {32, 32}, {35, 40}, {42, 45}},
			sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
		},
		// New interval end
		Mysql56GTID{Server: sid1, Sequence: 50}: {
			sid1: []interval{{20, 30}, {35, 40}, {42, 45}, {50, 50}},
			sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
		},
		// Extend interval start
		Mysql56GTID{Server: sid2, Sequence: 49}: {
			sid1: []interval{{20, 30}, {35, 40}, {42, 45}},
			sid2: []interval{{1, 5}, {49, 50}, {60, 70}},
		},
		// Extend interval end
		Mysql56GTID{Server: sid2, Sequence: 51}: {
			sid1: []interval{{20, 30}, {35, 40}, {42, 45}},
			sid2: []interval{{1, 5}, {50, 51}, {60, 70}},
		},
		// Merge intervals
		Mysql56GTID{Server: sid1, Sequence: 41}: {
			sid1: []interval{{20, 30}, {35, 45}},
			sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
		},
		// Different SID
		Mysql56GTID{Server: sid3, Sequence: 1}: {
			sid1: []interval{{20, 30}, {35, 40}, {42, 45}},
			sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
			sid3: []interval{{1, 1}},
		},
	}

	for input, want := range table {
		if got := set.AddGTID(input); !got.Equal(want) {
			t.Errorf("AddGTID(%#v) = %#v, want %#v", input, got, want)
		}
	}
}

func TestMysql56GTIDSetUnion(t *testing.T) {
	sid1 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	sid2 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16}
	sid3 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 17}

	set1 := Mysql56GTIDSet{
		sid1: []interval{{20, 30}, {35, 40}, {42, 45}},
		sid2: []interval{{1, 5}, {20, 50}, {60, 70}},
	}

	set2 := Mysql56GTIDSet{
		sid1: []interval{{20, 31}, {35, 37}, {41, 46}},
		sid2: []interval{{3, 6}, {22, 49}, {67, 72}},
		sid3: []interval{{1, 45}},
	}

	got := set1.Union(set2)

	want := Mysql56GTIDSet{
		sid1: []interval{{20, 31}, {35, 46}},
		sid2: []interval{{1, 6}, {20, 50}, {60, 72}},
		sid3: []interval{{1, 45}},
	}
	assert.True(t, got.Equal(want), "set1: %#v, set1.Union(%#v) = %#v, want %#v", set1, set2, got, want)

}

func TestMysql56GTIDSetDifference(t *testing.T) {
	sid1 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	sid2 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16}
	sid3 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 17}
	sid4 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18}
	sid5 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 19}

	set1 := Mysql56GTIDSet{
		sid1: []interval{{20, 30}, {35, 39}, {40, 53}, {55, 75}},
		sid2: []interval{{1, 7}, {20, 50}, {60, 70}},
		sid4: []interval{{1, 30}},
		sid5: []interval{{1, 7}, {20, 30}},
	}

	set2 := Mysql56GTIDSet{
		sid1: []interval{{20, 30}, {35, 37}, {50, 60}},
		sid2: []interval{{3, 5}, {22, 25}, {32, 37}, {67, 70}},
		sid3: []interval{{1, 45}},
		sid5: []interval{{2, 6}, {15, 40}},
	}

	got := set1.Difference(set2)

	want := Mysql56GTIDSet{
		sid1: []interval{{38, 39}, {40, 49}, {61, 75}},
		sid2: []interval{{1, 2}, {6, 7}, {20, 21}, {26, 31}, {38, 50}, {60, 66}},
		sid4: []interval{{1, 30}},
		sid5: []interval{{1, 1}, {7, 7}},
	}
	assert.True(t, got.Equal(want), "got %#v; want %#v", got, want)

	sid10 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	sid11 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	set10 := Mysql56GTIDSet{
		sid10: []interval{{1, 30}},
	}
	set11 := Mysql56GTIDSet{
		sid11: []interval{{1, 30}},
	}
	got = set10.Difference(set11)
	want = Mysql56GTIDSet{}
	assert.True(t, got.Equal(want), "got %#v; want %#v", got, want)

}

func TestMysql56GTIDSetSIDBlock(t *testing.T) {
	sid1 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	sid2 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16}

	input := Mysql56GTIDSet{
		sid1: []interval{{20, 30}, {35, 40}},
		sid2: []interval{{1, 5}},
	}
	want := []byte{
		// n_sids
		2, 0, 0, 0, 0, 0, 0, 0,
		// sid1
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
		// sid1: n_intervals
		2, 0, 0, 0, 0, 0, 0, 0,
		// sid1: interval 1 start
		20, 0, 0, 0, 0, 0, 0, 0,
		// sid1: interval 1 end
		31, 0, 0, 0, 0, 0, 0, 0,
		// sid1: interval 2 start
		35, 0, 0, 0, 0, 0, 0, 0,
		// sid1: interval 2 end
		41, 0, 0, 0, 0, 0, 0, 0,
		// sid2
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16,
		// sid2: n_intervals
		1, 0, 0, 0, 0, 0, 0, 0,
		// sid2: interval 1 start
		1, 0, 0, 0, 0, 0, 0, 0,
		// sid2: interval 1 end
		6, 0, 0, 0, 0, 0, 0, 0,
	}
	got := input.SIDBlock()
	assert.True(t, reflect.DeepEqual(got, want), "%#v.SIDBlock() = %#v, want %#v", input, got, want)

	// Testing the conversion back.
	set, err := NewMysql56GTIDSetFromSIDBlock(want)
	require.NoError(t, err, "Reconstructing Mysql56GTIDSet from SID block failed: %v", err)
	assert.True(t, reflect.DeepEqual(set, input), "NewMysql56GTIDSetFromSIDBlock(%#v) = %#v, want %#v", want, set, input)

}

func TestMySQL56GTIDSetLast(t *testing.T) {
	sid1 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	sid2 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 255}

	table := map[string]Mysql56GTIDSet{
		// Simple case
		"00010203-0405-0607-0809-0a0b0c0d0e0f:5": {
			sid1: []interval{{1, 5}},
		},
		"00010203-0405-0607-0809-0a0b0c0d0e0f:3": {
			sid1: []interval{{end: 3}},
		},
		// Interval with same start and end
		"00010203-0405-0607-0809-0a0b0c0d0e0f:12": {
			sid1: []interval{{12, 12}},
		},
		// Multiple intervals
		"00010203-0405-0607-0809-0a0b0c0d0e0f:20": {
			sid1: []interval{{1, 5}, {10, 20}},
		},
		// Multiple SIDs
		"00010203-0405-0607-0809-0a0b0c0d0eff:50": {
			sid1: []interval{{1, 5}, {10, 20}},
			sid2: []interval{{1, 5}, {50, 50}},
		},
	}

	for want, input := range table {
		got := strings.ToLower(input.Last())
		assert.Equal(t, want, got)
	}
}

func TestSubtract(t *testing.T) {
	tests := []struct {
		name       string
		lhs        string
		rhs        string
		difference string
		wantErr    string
	}{
		{
			name:       "Extra GTID set on left side",
			lhs:        "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8,8bc65cca-3fe4-11ed-bbfb-091034d48b3e:1",
			rhs:        "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8",
			difference: "8bc65cca-3fe4-11ed-bbfb-091034d48b3e:1",
		}, {
			name:       "Extra GTID set on right side",
			lhs:        "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8",
			rhs:        "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8,8bc65cca-3fe4-11ed-bbfb-091034d48b3e:1",
			difference: "",
		}, {
			name:       "Empty left side",
			lhs:        "",
			rhs:        "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8",
			difference: "",
		}, {
			name:       "Empty right side",
			lhs:        "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8,8bc65cca-3fe4-11ed-bbfb-091034d48b3e:1",
			rhs:        "",
			difference: "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8,8bc65cca-3fe4-11ed-bbfb-091034d48b3e:1",
		}, {
			name:       "Equal sets",
			lhs:        "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8,8bc65cca-3fe4-11ed-bbfb-091034d48b3e:1",
			rhs:        "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8,8bc65cca-3fe4-11ed-bbfb-091034d48b3e:1",
			difference: "",
		}, {
			name:       "subtract prefix",
			lhs:        "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8",
			rhs:        "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-3",
			difference: "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:4-8",
		}, {
			name:       "subtract mid",
			lhs:        "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8",
			rhs:        "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:2-3",
			difference: "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1:4-8",
		}, {
			name:       "subtract suffix",
			lhs:        "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8",
			rhs:        "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:7-8",
			difference: "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-6",
		}, {
			name:       "subtract complex range 1",
			lhs:        "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8:12-17",
			rhs:        "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:7-8",
			difference: "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-6:12-17",
		}, {
			name:       "subtract complex range 2",
			lhs:        "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8:12-17",
			rhs:        "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:12-13",
			difference: "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8:14-17",
		}, {
			name:       "subtract complex range 3",
			lhs:        "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8:12-17",
			rhs:        "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:7-13",
			difference: "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-6:14-17",
		}, {
			name:       "subtract repeating uuid",
			lhs:        "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8,8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:12-17",
			rhs:        "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:7-13",
			difference: "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-6:14-17",
		}, {
			name:       "subtract repeating uuid in descending order",
			lhs:        "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:12-17,8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8",
			rhs:        "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:7-13",
			difference: "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-6:14-17",
		}, {
			name:    "parsing error in left set",
			lhs:     "incorrect set",
			rhs:     "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8",
			wantErr: `invalid MySQL 5.6 GTID set ("incorrect set"): expected uuid:interval`,
		}, {
			name:    "parsing error in right set",
			lhs:     "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8",
			rhs:     "incorrect set",
			wantErr: `invalid MySQL 5.6 GTID set ("incorrect set"): expected uuid:interval`,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s: %s-%s", tt.name, tt.lhs, tt.rhs), func(t *testing.T) {
			got, err := Subtract(tt.lhs, tt.rhs)
			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.difference, got)
			}
		})
	}
}

func BenchmarkMySQL56GTIDParsing(b *testing.B) {
	var Inputs = []string{
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-5",
		"00010203-0405-0607-0809-0a0b0c0d0e0f:12",
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20",
		"00010203-0405-0607-0809-0a0b0c0d0e0f:10-20:1-5",
		"00010203-0405-0607-0809-0a0b0c0d0e0f:8-7",
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:8-7:10-20",
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20,00010203-0405-0607-0809-0a0b0c0d0eff:1-5:50",
		"8aabbf4f-5074-11ed-b225-aa23ce7e3ba2:1-20443,a6f1bf40-5073-11ed-9c0f-12a3889dc912:1-343402",
	}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		for _, input := range Inputs {
			_, _ = ParseMysql56GTIDSet(input)
		}
	}
}
