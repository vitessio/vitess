// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package replication

import (
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestSortPSIDList(t *testing.T) {
	input := []PSID{
		{1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16},
		{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
	}
	want := []PSID{
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16},
		{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		{1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
	}
	sort.Sort(psidList(input))
	if !reflect.DeepEqual(input, want) {
		t.Errorf("got %#v, want %#v", input, want)
	}
}

func TestParsePerconaGTIDSet(t *testing.T) {
	sid1 := PSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	sid2 := PSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 255}

	table := map[string]PerconaGTIDSet{
		// Empty
		"": {},
		// Simple case
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-5": {
			sid1: []pinterval{{1, 5}},
		},
		// Capital hex chars
		"00010203-0405-0607-0809-0A0B0C0D0E0F:1-5": {
			sid1: []pinterval{{1, 5}},
		},
		// Interval with same start and end
		"00010203-0405-0607-0809-0a0b0c0d0e0f:12": {
			sid1: []pinterval{{12, 12}},
		},
		// Multiple pintervals
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20": {
			sid1: []pinterval{{1, 5}, {10, 20}},
		},
		// Multiple pintervals, out of oder
		"00010203-0405-0607-0809-0a0b0c0d0e0f:10-20:1-5": {
			sid1: []pinterval{{1, 5}, {10, 20}},
		},
		// Intervals with end < start are discarded by MySQL 5.6
		"00010203-0405-0607-0809-0a0b0c0d0e0f:8-7": {},
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:8-7:10-20": {
			sid1: []pinterval{{1, 5}, {10, 20}},
		},
		// Multiple PSIDs
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20,00010203-0405-0607-0809-0a0b0c0d0eff:1-5:50": {
			sid1: []pinterval{{1, 5}, {10, 20}},
			sid2: []pinterval{{1, 5}, {50, 50}},
		},
		// Multiple PSIDs with space around the comma
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20, 00010203-0405-0607-0809-0a0b0c0d0eff:1-5:50": {
			sid1: []pinterval{{1, 5}, {10, 20}},
			sid2: []pinterval{{1, 5}, {50, 50}},
		},
	}

	for input, want := range table {
		got, err := parsePerconaGTIDSet(input)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			continue
		}
		if !got.Equal(want) {
			t.Errorf("parsePerconaGTIDSet(%#v) = %#v, want %#v", input, got, want)
		}
	}
}

func TestParsePerconaGTIDSetInvalid(t *testing.T) {
	table := []string{
		// No intervals
		"00010203-0405-0607-0809-0a0b0c0d0e0f",
		// Invalid PSID
		"00010203-0405-060X-0809-0a0b0c0d0e0f:1-5",
		// Invalid intervals
		"00010203-0405-0607-0809-0a0b0c0d0e0f:0-5",
		"00010203-0405-0607-0809-0a0b0c0d0e0f:-5",
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-2-3",
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-",
	}

	for _, input := range table {
		_, err := parsePerconaGTIDSet(input)
		if err == nil {
			t.Errorf("parsePerconaGTIDSet(%#v) expected error, got none", err)
		}
	}
}

func TestPerconaGTIDSetString(t *testing.T) {
	sid1 := PSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	sid2 := PSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 255}

	table := map[string]PerconaGTIDSet{
		// Simple case
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-5": {
			sid1: []pinterval{{1, 5}},
		},
		// Interval with same start and end
		"00010203-0405-0607-0809-0a0b0c0d0e0f:12": {
			sid1: []pinterval{{12, 12}},
		},
		// Multiple intervals
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20": {
			sid1: []pinterval{{1, 5}, {10, 20}},
		},
		// Multiple PSIDs
		"00010203-0405-0607-0809-0a0b0c0d0e0f:1-5:10-20,00010203-0405-0607-0809-0a0b0c0d0eff:1-5:50": {
			sid1: []pinterval{{1, 5}, {10, 20}},
			sid2: []pinterval{{1, 5}, {50, 50}},
		},
	}

	for want, input := range table {
		got := strings.ToLower(input.String())
		if got != want {
			t.Errorf("%#v.String() = %#v, want %#v", input, got, want)
		}
	}
}

func TestPerconaGTIDSetFlavor(t *testing.T) {
	input := PerconaGTIDSet{}
	if got, want := input.Flavor(), "Percona"; got != want {
		t.Errorf("%#v.Flavor() = %#v, want %#v", input, got, want)
	}
}

func TestPerconaGTIDSetContainsGTID(t *testing.T) {
	sid1 := PSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	sid2 := PSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16}
	sid3 := PSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 17}

	set := PerconaGTIDSet{
		sid1: []pinterval{{20, 30}, {35, 40}},
		sid2: []pinterval{{1, 5}, {50, 50}},
	}

	table := map[GTID]bool{
		fakeGTID{}: false,

		PerconaGTID{sid1, 1}:  false,
		PerconaGTID{sid1, 19}: false,
		PerconaGTID{sid1, 20}: true,
		PerconaGTID{sid1, 23}: true,
		PerconaGTID{sid1, 30}: true,
		PerconaGTID{sid1, 31}: false,

		PerconaGTID{sid2, 1}:  true,
		PerconaGTID{sid2, 10}: false,
		PerconaGTID{sid2, 50}: true,
		PerconaGTID{sid2, 51}: false,

		PerconaGTID{sid3, 1}: false,
	}

	for input, want := range table {
		if got := set.ContainsGTID(input); got != want {
			t.Errorf("ContainsGTID(%#v) = %#v, want %#v", input, got, want)
		}
	}
}

func TestPerconaGTIDSetContains(t *testing.T) {
	sid1 := PSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	sid2 := PSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16}
	sid3 := PSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 17}

	// The set to test against.
	set := PerconaGTIDSet{
		sid1: []pinterval{{20, 30}, {35, 40}},
		sid2: []pinterval{{1, 5}, {50, 50}, {60, 70}},
	}

	// Test cases that should return Contains() = true.
	contained := []PerconaGTIDSet{
		// The set should contain itself.
		set,
		// Every set contains the empty set.
		{},

		// Simple case
		{sid1: []pinterval{{25, 30}}},
		// Multiple pintervals
		{sid2: []pinterval{{1, 2}, {4, 5}, {60, 70}}},
		// Multiple PSIDs
		{
			sid1: []pinterval{{25, 30}, {35, 37}},
			sid2: []pinterval{{1, 5}},
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
		PerconaGTIDSet{sid1: []pinterval{{1, 5}}},
		PerconaGTIDSet{sid1: []pinterval{{10, 19}}},
		// Overlapping pintervals
		PerconaGTIDSet{sid1: []pinterval{{10, 20}}},
		PerconaGTIDSet{sid1: []pinterval{{10, 25}}},
		PerconaGTIDSet{sid1: []pinterval{{25, 31}}},
		PerconaGTIDSet{sid1: []pinterval{{30, 31}}},
		// Multiple intervals
		PerconaGTIDSet{sid1: []pinterval{{20, 30}, {34, 34}}},
		// Multiple PSIDs
		PerconaGTIDSet{
			sid1: []pinterval{{20, 30}, {36, 36}},
			sid2: []pinterval{{3, 5}, {55, 60}},
		},
		// PSID is missing entirely
		PerconaGTIDSet{sid3: []pinterval{{1, 5}}},
	}

	for _, other := range notContained {
		if set.Contains(other) {
			t.Errorf("Contains(%#v) = true, want false", other)
		}
	}
}

func TestPerconaGTIDSetEqual(t *testing.T) {
	sid1 := PSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	sid2 := PSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16}
	sid3 := PSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 17}

	// The set to test against.
	set := PerconaGTIDSet{
		sid1: []pinterval{{20, 30}, {35, 40}},
		sid2: []pinterval{{1, 5}, {50, 50}, {60, 70}},
	}

	// Test cases that should return Equal() = true.
	equal := []PerconaGTIDSet{
		// Same underlying map instance
		set,
		// Different instance, same data
		{
			sid1: []pinterval{{20, 30}, {35, 40}},
			sid2: []pinterval{{1, 5}, {50, 50}, {60, 70}},
		},
	}

	for _, other := range equal {
		if !set.Equal(other) {
			t.Errorf("%#v.Equal(%#v) = false, want true", set, other)
		}
		// Equality should be transitive.
		if !other.Equal(set) {
			t.Errorf("%#v.Equal(%#v) = false, want true", other, set)
		}
	}

	// Test cases that should return Equal() = false.
	notEqual := []GTIDSet{
		// Wrong flavor is not equal.
		fakeGTID{},
		// Empty set
		PerconaGTIDSet{},
		// Interval changed
		PerconaGTIDSet{
			sid1: []pinterval{{20, 31}, {35, 40}},
			sid2: []pinterval{{1, 5}, {50, 50}, {60, 70}},
		},
		// Interval added
		PerconaGTIDSet{
			sid1: []pinterval{{20, 30}, {32, 33}, {35, 40}},
			sid2: []pinterval{{1, 5}, {50, 50}, {60, 70}},
		},
		// Interval removed
		PerconaGTIDSet{
			sid1: []pinterval{{20, 30}, {35, 40}},
			sid2: []pinterval{{1, 5}, {60, 70}},
		},
		// Different PSID, same intervals
		PerconaGTIDSet{
			sid1: []pinterval{{20, 30}, {35, 40}},
			sid3: []pinterval{{1, 5}, {50, 50}, {60, 70}},
		},
		// PSID added
		PerconaGTIDSet{
			sid1: []pinterval{{20, 30}, {35, 40}},
			sid2: []pinterval{{1, 5}, {50, 50}, {60, 70}},
			sid3: []pinterval{{1, 5}},
		},
		// PSID removed
		PerconaGTIDSet{
			sid1: []pinterval{{20, 30}, {35, 40}},
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

func TestPerconaGTIDSetAddGTID(t *testing.T) {
	sid1 := PSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	sid2 := PSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16}
	sid3 := PSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 17}

	// The set to test against.
	set := PerconaGTIDSet{
		sid1: []pinterval{{20, 30}, {35, 40}, {42, 45}},
		sid2: []pinterval{{1, 5}, {50, 50}, {60, 70}},
	}

	table := map[GTID]PerconaGTIDSet{
		// Adding wrong flavor is a no-op.
		fakeGTID{}: set,

		// Adding GTIDs that are already in the set
		PerconaGTID{Server: sid1, Sequence: 20}: {
			sid1: []pinterval{{20, 30}, {35, 40}, {42, 45}},
			sid2: []pinterval{{1, 5}, {50, 50}, {60, 70}},
		},
		PerconaGTID{Server: sid1, Sequence: 30}: {
			sid1: []pinterval{{20, 30}, {35, 40}, {42, 45}},
			sid2: []pinterval{{1, 5}, {50, 50}, {60, 70}},
		},
		PerconaGTID{Server: sid1, Sequence: 25}: {
			sid1: []pinterval{{20, 30}, {35, 40}, {42, 45}},
			sid2: []pinterval{{1, 5}, {50, 50}, {60, 70}},
		},
		// New interval beginning
		PerconaGTID{Server: sid1, Sequence: 1}: {
			sid1: []pinterval{{1, 1}, {20, 30}, {35, 40}, {42, 45}},
			sid2: []pinterval{{1, 5}, {50, 50}, {60, 70}},
		},
		// New interval middle
		PerconaGTID{Server: sid1, Sequence: 32}: {
			sid1: []pinterval{{20, 30}, {32, 32}, {35, 40}, {42, 45}},
			sid2: []pinterval{{1, 5}, {50, 50}, {60, 70}},
		},
		// New interval end
		PerconaGTID{Server: sid1, Sequence: 50}: {
			sid1: []pinterval{{20, 30}, {35, 40}, {42, 45}, {50, 50}},
			sid2: []pinterval{{1, 5}, {50, 50}, {60, 70}},
		},
		// Extend interval start
		PerconaGTID{Server: sid2, Sequence: 49}: {
			sid1: []pinterval{{20, 30}, {35, 40}, {42, 45}},
			sid2: []pinterval{{1, 5}, {49, 50}, {60, 70}},
		},
		// Extend interval end
		PerconaGTID{Server: sid2, Sequence: 51}: {
			sid1: []pinterval{{20, 30}, {35, 40}, {42, 45}},
			sid2: []pinterval{{1, 5}, {50, 51}, {60, 70}},
		},
		// Merge intervals
		PerconaGTID{Server: sid1, Sequence: 41}: {
			sid1: []pinterval{{20, 30}, {35, 45}},
			sid2: []pinterval{{1, 5}, {50, 50}, {60, 70}},
		},
		// Different PSID
		PerconaGTID{Server: sid3, Sequence: 1}: {
			sid1: []pinterval{{20, 30}, {35, 40}, {42, 45}},
			sid2: []pinterval{{1, 5}, {50, 50}, {60, 70}},
			sid3: []pinterval{{1, 1}},
		},
	}

	for input, want := range table {
		if got := set.AddGTID(input); !got.Equal(want) {
			t.Errorf("AddGTID(%#v) = %#v, want %#v", input, got, want)
		}
	}
}

func TestPerconaGTIDSetPSIDBlock(t *testing.T) {
	sid1 := PSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	sid2 := PSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16}

	input := PerconaGTIDSet{
		sid1: []pinterval{{20, 30}, {35, 40}},
		sid2: []pinterval{{1, 5}},
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
	if got := input.PSIDBlock(); !reflect.DeepEqual(got, want) {
		t.Errorf("%#v.PSIDBlock() = %#v, want %#v", input, got, want)
	}
}
