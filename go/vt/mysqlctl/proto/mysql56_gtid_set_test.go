// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"reflect"
	"strings"
	"testing"
)

func TestMysql56GTIDSetString(t *testing.T) {
	sid1 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	sid2 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 255}

	table := map[string]Mysql56GTIDSet{
		// Simple case
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-5": Mysql56GTIDSet{
			sid1: []interval{{1, 5}},
		},
		// Interval with same start and end
		"00010203-0405-0607-0809-0a0b0c0d0e0f:12": Mysql56GTIDSet{
			sid1: []interval{{12, 12}},
		},
		// Multiple intervals
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20": Mysql56GTIDSet{
			sid1: []interval{{1, 5}, {10, 20}},
		},
		// Multiple SIDs
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20,00010203-0405-0607-0809-0a0b0c0d0eff:1-5:50": Mysql56GTIDSet{
			sid1: []interval{{1, 5}, {10, 20}},
			sid2: []interval{{1, 5}, {50, 50}},
		},
	}

	for want, input := range table {
		got := strings.ToLower(input.String())
		if got != want {
			t.Errorf("%#v.String() = %#v, want %#v", input, got, want)
		}
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
		Mysql56GTIDSet{},

		// Simple case
		Mysql56GTIDSet{sid1: []interval{{25, 30}}},
		// Multiple intervals
		Mysql56GTIDSet{sid2: []interval{{1, 2}, {4, 5}, {60, 70}}},
		// Multiple SIDs
		Mysql56GTIDSet{
			sid1: []interval{{25, 30}, {35, 37}},
			sid2: []interval{{1, 5}},
		},
	}

	for _, other := range contained {
		if !set.Contains(other) {
			t.Errorf("Contains(%#v) = false, want true", other)
		}
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
		Mysql56GTIDSet{
			sid1: []interval{{20, 30}, {35, 40}},
			sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
		},
	}

	for _, other := range equal {
		if !set.Equal(other) {
			t.Errorf("Equal(%#v) = false, want true", other)
		}
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
			t.Errorf("Equal(%#v) = true, want false", other)
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

		// New interval beginning
		Mysql56GTID{Server: sid1, Sequence: 1}: Mysql56GTIDSet{
			sid1: []interval{{1, 1}, {20, 30}, {35, 40}, {42, 45}},
			sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
		},
		// New interval middle
		Mysql56GTID{Server: sid1, Sequence: 32}: Mysql56GTIDSet{
			sid1: []interval{{20, 30}, {32, 32}, {35, 40}, {42, 45}},
			sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
		},
		// New interval end
		Mysql56GTID{Server: sid1, Sequence: 50}: Mysql56GTIDSet{
			sid1: []interval{{20, 30}, {35, 40}, {42, 45}, {50, 50}},
			sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
		},
		// Extend interval start
		Mysql56GTID{Server: sid2, Sequence: 49}: Mysql56GTIDSet{
			sid1: []interval{{20, 30}, {35, 40}, {42, 45}},
			sid2: []interval{{1, 5}, {49, 50}, {60, 70}},
		},
		// Extend interval end
		Mysql56GTID{Server: sid2, Sequence: 51}: Mysql56GTIDSet{
			sid1: []interval{{20, 30}, {35, 40}, {42, 45}},
			sid2: []interval{{1, 5}, {50, 51}, {60, 70}},
		},
		// Merge intervals
		Mysql56GTID{Server: sid1, Sequence: 41}: Mysql56GTIDSet{
			sid1: []interval{{20, 30}, {35, 45}},
			sid2: []interval{{1, 5}, {50, 50}, {60, 70}},
		},
		// Different SID
		Mysql56GTID{Server: sid3, Sequence: 1}: Mysql56GTIDSet{
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
		30, 0, 0, 0, 0, 0, 0, 0,
		// sid1: interval 2 start
		35, 0, 0, 0, 0, 0, 0, 0,
		// sid1: interval 2 end
		40, 0, 0, 0, 0, 0, 0, 0,
		// sid2
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16,
		// sid2: n_intervals
		1, 0, 0, 0, 0, 0, 0, 0,
		// sid2: interval 1 start
		1, 0, 0, 0, 0, 0, 0, 0,
		// sid2: interval 1 end
		5, 0, 0, 0, 0, 0, 0, 0,
	}
	if got := input.SIDBlock(); !reflect.DeepEqual(got, want) {
		t.Errorf("%#v.SIDBlock() = %#v, want %#v", input, got, want)
	}
}
