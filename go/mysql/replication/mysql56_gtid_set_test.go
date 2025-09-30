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
	"maps"
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
	tcases := []struct {
		name   string
		add    GTID
		expect Mysql56GTIDSet
	}{
		{
			"Adding wrong flavor is a no-op.",
			fakeGTID{},
			Mysql56GTIDSet{
				sid1: []interval{{20, 30}, {35, 40}, {42, 45}},
				sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
			},
		},
		{
			"Adding GTIDs that are already in the set",
			Mysql56GTID{Server: sid1, Sequence: 20},
			Mysql56GTIDSet{
				sid1: []interval{{20, 30}, {35, 40}, {42, 45}},
				sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
			},
		},
		{
			"Adding GTIDs that are already in the set 2",
			Mysql56GTID{Server: sid1, Sequence: 30},
			Mysql56GTIDSet{
				sid1: []interval{{20, 30}, {35, 40}, {42, 45}},
				sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
			},
		},
		{
			"Adding GTIDs that are already in the set 3",
			Mysql56GTID{Server: sid1, Sequence: 25},
			Mysql56GTIDSet{
				sid1: []interval{{20, 30}, {35, 40}, {42, 45}},
				sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
			},
		},
		{
			"New interval beginning",
			Mysql56GTID{Server: sid1, Sequence: 1},
			Mysql56GTIDSet{
				sid1: []interval{{1, 1}, {20, 30}, {35, 40}, {42, 45}},
				sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
			},
		},
		{
			"New interval middle",
			Mysql56GTID{Server: sid1, Sequence: 32},
			Mysql56GTIDSet{
				sid1: []interval{{20, 30}, {32, 32}, {35, 40}, {42, 45}},
				sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
			},
		},
		{
			"New interval end",
			Mysql56GTID{Server: sid1, Sequence: 50},
			Mysql56GTIDSet{
				sid1: []interval{{20, 30}, {35, 40}, {42, 45}, {50, 50}},
				sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
			},
		},
		{
			"Extend interval start",
			Mysql56GTID{Server: sid2, Sequence: 49},
			Mysql56GTIDSet{
				sid1: []interval{{20, 30}, {35, 40}, {42, 45}},
				sid2: []interval{{1, 5}, {49, 50}, {60, 70}},
			},
		},
		{
			"Extend interval end",
			Mysql56GTID{Server: sid2, Sequence: 51},
			Mysql56GTIDSet{
				sid1: []interval{{20, 30}, {35, 40}, {42, 45}},
				sid2: []interval{{1, 5}, {50, 51}, {60, 70}},
			},
		},
		{
			"Merge intervals",
			Mysql56GTID{Server: sid1, Sequence: 41},
			Mysql56GTIDSet{
				sid1: []interval{{20, 30}, {35, 45}},
				sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
			},
		},
		{
			"Different SID",
			Mysql56GTID{Server: sid3, Sequence: 1},
			Mysql56GTIDSet{
				sid1: []interval{{20, 30}, {35, 40}, {42, 45}},
				sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
				sid3: []interval{{1, 1}},
			},
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			set := Mysql56GTIDSet{
				sid1: []interval{{20, 30}, {35, 40}, {42, 45}},
				sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
			}

			originalSet := maps.Clone(set)
			got := set.AddGTID(tcase.add)
			assert.Equal(t, originalSet, set) // ensure immutable
			assert.Equal(t, tcase.expect, got)

			gotInplace := set.AddGTIDInPlace(tcase.add)
			assert.Equal(t, gotInplace, set) // ensure mutable
			assert.Equal(t, tcase.expect, gotInplace)
		})
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

func TestMysql56GTIDSetInPlaceUnion(t *testing.T) {
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

	got := set1.UnionInPlace(set2)

	want := Mysql56GTIDSet{
		sid1: []interval{{20, 31}, {35, 46}},
		sid2: []interval{{1, 6}, {20, 50}, {60, 72}},
		sid3: []interval{{1, 45}},
	}
	assert.Equal(t, set1, got) // Because this is in-place
	assert.Equal(t, want, got)
	assert.True(t, got.Equal(want), "set1: %#v, set1.Union(%#v) = %#v, want %#v", set1, set2, got, want)

}

func BenchmarkMysql56GTIDSetAdd(b *testing.B) {
	base := "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5"
	gtidSet, err := ParseMysql56GTIDSet(base)
	require.NoError(b, err)
	pos := Position{GTIDSet: gtidSet}

	var Inputs = []string{
		"00010203-0405-0607-0809-0a0b0c0d0e0f:6",
		"00010203-0405-0607-0809-0a0b0c0d0e0f:12",
		"00010203-0405-0607-0809-0a0b0c0d0e0f:13",
		"00010203-0405-0607-0809-0a0b0c0d0e0f:14",
		"00010203-0405-0607-0809-0a0b0c0d0e0f:18",
		"00010203-0405-0607-0809-0a0b0c0d0e0f:21",
		"00010203-0405-0607-0809-0a0b0c0d0e0f:22",
	}
	gtids := make([]GTID, len(Inputs))
	for i, input := range Inputs {
		gtid, err := parseMysql56GTID(input)
		require.NoError(b, err)
		gtids[i] = gtid
	}
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		for _, gtid := range gtids {
			pos.GTIDSet = pos.GTIDSet.AddGTID(gtid)
		}
	}
}

func BenchmarkMysql56GTIDSetUnion(b *testing.B) {
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
	positions := make([]Position, len(Inputs))
	for i, input := range Inputs {
		gtid, err := ParseMysql56GTIDSet(input)
		require.NoError(b, err)
		positions[i] = Position{GTIDSet: gtid}
	}
	var pos Position
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		for _, p := range positions {
			pos.GTIDSet = p.GTIDSet.UnionInPlace(pos.GTIDSet)
		}
	}
}

func BenchmarkMysql56GTIDSetUnionHappyPath(b *testing.B) {
	var Inputs = []string{
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-5",
		"00010203-0405-0607-0809-0a0b0c0d0e0f:12",
		"00010203-0405-0607-0809-0a0b0c0d0e0f:12-15:17-20",
		"00010203-0405-0607-0809-0a0b0c0d0e0f:21-30:41-45",
		"00010203-0405-0607-0809-0a0b0c0d0e0f:48",
		"00010203-0405-0607-0809-0a0b0c0d0e0f:49-50:52-53",
	}
	positions := make([]Position, len(Inputs))
	for i, input := range Inputs {
		gtid, err := ParseMysql56GTIDSet(input)
		require.NoError(b, err)
		positions[i] = Position{GTIDSet: gtid}
	}
	var pos = Position{GTIDSet: Mysql56GTIDSet{}}
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		for _, p := range positions {
			pos.GTIDSet = pos.GTIDSet.UnionInPlace(p.GTIDSet)
		}
	}
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

func TestMySQL56GTIDSetCount(t *testing.T) {
	tests := []struct {
		name      string
		gtidStr   string
		wantCount int64
	}{
		{
			name:      "Empty GTID String",
			gtidStr:   "",
			wantCount: 0,
		}, {
			name:      "Single GTID",
			gtidStr:   "00010203-0405-0607-0809-0a0b0c0d0e0f:12",
			wantCount: 1,
		}, {
			name:      "Single GTID Interval",
			gtidStr:   "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5",
			wantCount: 5,
		}, {
			name:      "Single UUID",
			gtidStr:   "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:11-20",
			wantCount: 15,
		}, {
			name:      "Multiple UUIDs",
			gtidStr:   "00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20,00010203-0405-0607-0809-0a0b0c0d0eff:1-5:50",
			wantCount: 22,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gtidSet, _ := ParseMysql56GTIDSet(tt.gtidStr)
			require.EqualValues(t, tt.wantCount, gtidSet.Count())
		})
	}
}

func TestErrantGTIDsOnReplica(t *testing.T) {
	tests := []struct {
		name             string
		replicaPosition  string
		primaryPosition  string
		primarySID       string
		errantGtidWanted string
		wantErr          string
	}{
		{
			name:             "Empty replica position",
			replicaPosition:  "MySQL56/",
			primaryPosition:  "MySQL56/8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8",
			primarySID:       "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9",
			errantGtidWanted: "",
		}, {
			name:             "Empty primary position",
			replicaPosition:  "MySQL56/8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8",
			primaryPosition:  "MySQL56/",
			primarySID:       "8bc65cca-3fe4-11ed-bbfb-091034d48b3e",
			errantGtidWanted: "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8",
		}, {
			name:            "Primary seen as lagging for its own writes",
			replicaPosition: "MySQL56/8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-33",
			primaryPosition: "MySQL56/8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-29",
			primarySID:      "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9",
		}, {
			name:             "Empty primary position - with multiple errant gtids",
			replicaPosition:  "MySQL56/8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8,8bc65cca-3fe4-11ed-bbfb-091034d48b3e:1",
			primaryPosition:  "MySQL56/",
			primarySID:       "8bc65cca-3fe4-11ed-bbfb-091034d49c4f",
			errantGtidWanted: "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8,8bc65cca-3fe4-11ed-bbfb-091034d48b3e:1",
		}, {
			name:             "Single errant GTID",
			replicaPosition:  "MySQL56/8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8,8bc65cca-3fe4-11ed-bbfb-091034d48b3e:1,8bc65cca-3fe4-11ed-bbfb-091034d48bd3:34",
			primaryPosition:  "MySQL56/8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-50,8bc65cca-3fe4-11ed-bbfb-091034d48b3e:1-30",
			primarySID:       "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9",
			errantGtidWanted: "8bc65cca-3fe4-11ed-bbfb-091034d48bd3:34",
		}, {
			name:             "Multiple errant GTID",
			replicaPosition:  "MySQL56/8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8,8bc65cca-3fe4-11ed-bbfb-091034d48b3e:1-32,8bc65cca-3fe4-11ed-bbfb-091034d48bd3:3-35",
			primaryPosition:  "MySQL56/8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-50,8bc65cca-3fe4-11ed-bbfb-091034d48b3e:1-30,8bc65cca-3fe4-11ed-bbfb-091034d48bd3:34",
			primarySID:       "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9",
			errantGtidWanted: "8bc65cca-3fe4-11ed-bbfb-091034d48b3e:31-32,8bc65cca-3fe4-11ed-bbfb-091034d48bd3:3-33:35",
		}, {
			name:             "Multiple errant GTID after discounting primary writes",
			replicaPosition:  "MySQL56/8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-10,8bc65cca-3fe4-11ed-bbfb-091034d48b3e:1-32,8bc65cca-3fe4-11ed-bbfb-091034d48bd3:3-35",
			primaryPosition:  "MySQL56/8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8,8bc65cca-3fe4-11ed-bbfb-091034d48b3e:1-30,8bc65cca-3fe4-11ed-bbfb-091034d48bd3:34",
			primarySID:       "8bc65cca-3fe4-11ed-bbfb-091034d48b3e",
			errantGtidWanted: "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:9-10,8bc65cca-3fe4-11ed-bbfb-091034d48bd3:3-33:35",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			replPos, err := DecodePosition(tt.replicaPosition)
			require.NoError(t, err)
			primaryPos, err := DecodePosition(tt.primaryPosition)
			require.NoError(t, err)
			primarySID, err := ParseSID(tt.primarySID)
			require.NoError(t, err)
			errantGTIDs, err := ErrantGTIDsOnReplica(replPos, primaryPos, primarySID)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				require.EqualValues(t, tt.errantGtidWanted, errantGTIDs)
			}

		})
	}
}

func TestMysql56GTIDSet_RemoveUUID(t *testing.T) {
	tests := []struct {
		name       string
		initialSet string
		uuid       string
		wantSet    string
	}{
		{
			name:       "Remove unknown UUID",
			initialSet: "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8,8bc65cca-3fe4-11ed-bbfb-091034d48b3e:1:4-24",
			uuid:       "8bc65c84-3fe4-11ed-a912-257f0fcde6c9",
			wantSet:    "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8,8bc65cca-3fe4-11ed-bbfb-091034d48b3e:1:4-24",
		},
		{
			name:       "Remove a single UUID",
			initialSet: "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8,8bc65cca-3fe4-11ed-bbfb-091034d48b3e:1:4-24",
			uuid:       "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9",
			wantSet:    "8bc65cca-3fe4-11ed-bbfb-091034d48b3e:1:4-24",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gtidSet, err := ParseMysql56GTIDSet(tt.initialSet)
			require.NoError(t, err)
			sid, err := ParseSID(tt.uuid)
			require.NoError(t, err)
			gtidSet = gtidSet.RemoveUUID(sid)
			require.EqualValues(t, tt.wantSet, gtidSet.String())
		})
	}
}

func TestSIDs(t *testing.T) {
	var set Mysql56GTIDSet // nil
	sids := set.SIDs()
	assert.NotNil(t, sids)
	assert.Empty(t, sids)

	gtid := "8bc65cca-3fe4-11ed-bbfb-091034d48b3e:1:4-24"
	gtidSet, err := ParseMysql56GTIDSet(gtid)
	require.NoError(t, err)
	sids = gtidSet.SIDs()
	assert.NotNil(t, sids)
	require.Len(t, sids, 1)
	assert.Equal(t, "8bc65cca-3fe4-11ed-bbfb-091034d48b3e", sids[0].String())
}
