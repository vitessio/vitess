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

package key

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestKey(t *testing.T) {
	k0 := Uint64Key(0)
	k1 := Uint64Key(1)
	k2 := Uint64Key(0x7FFFFFFFFFFFFFFF)
	k3 := Uint64Key(0x8000000000000000)
	k4 := Uint64Key(0xFFFFFFFFFFFFFFFF)

	f := func(k Uint64Key, x string) {
		hexK := hex.EncodeToString(k.Bytes())
		if x != hexK {
			t.Errorf("byte mismatch %#v != %#v", k, x)
		}
	}

	f(k0, "0000000000000000")
	f(k1, "0000000000000001")
	f(k2, "7fffffffffffffff")
	f(k3, "8000000000000000")
	f(k4, "ffffffffffffffff")
}

func TestEvenShardsKeyRange(t *testing.T) {
	testCases := []struct {
		i, n     int
		wantSpec string
		want     *topodatapb.KeyRange
	}{
		{0, 1,
			"-",
			&topodatapb.KeyRange{},
		},
		{0, 2,
			"-80",
			&topodatapb.KeyRange{
				End: []byte{0x80},
			},
		},
		{1, 2,
			"80-",
			&topodatapb.KeyRange{
				Start: []byte{0x80},
			},
		},
		{1, 4,
			"40-80",
			&topodatapb.KeyRange{
				Start: []byte{0x40},
				End:   []byte{0x80},
			},
		},
		{2, 4,
			"80-c0",
			&topodatapb.KeyRange{
				Start: []byte{0x80},
				End:   []byte{0xc0},
			},
		},
		{1, 256,
			"01-02",
			&topodatapb.KeyRange{
				Start: []byte{0x01},
				End:   []byte{0x02},
			},
		},
		{256, 512,
			"8000-8080",
			&topodatapb.KeyRange{
				Start: []byte{0x80, 0x00},
				End:   []byte{0x80, 0x80},
			},
		},
		// Second to last shard out of 512.
		{510, 512,
			"ff00-ff80",
			&topodatapb.KeyRange{
				Start: []byte{0xff, 0x00},
				End:   []byte{0xff, 0x80},
			},
		},
		// Last out of 512 shards.
		{511, 512,
			"ff80-",
			&topodatapb.KeyRange{
				Start: []byte{0xff, 0x80},
			},
		},
	}

	for _, tc := range testCases {
		got, err := EvenShardsKeyRange(tc.i, tc.n)
		if err != nil {
			t.Fatalf("EvenShardsKeyRange(%v, %v) returned unexpected error: %v", tc.i, tc.n, err)
		}
		if !proto.Equal(got, tc.want) {
			t.Errorf("EvenShardsKeyRange(%v, %v) = (%x, %x), want = (%x, %x)", tc.i, tc.n, got.Start, got.End, tc.want.Start, tc.want.End)
		}

		// Check if the string representation is equal as well.
		if gotStr, want := KeyRangeString(got), tc.wantSpec; gotStr != want {
			t.Errorf("EvenShardsKeyRange(%v) = %v, want = %v", got, gotStr, want)
		}

		// Now verify that ParseKeyRangeParts() produces the same KeyRange object as
		// we do.
		parts := strings.Split(tc.wantSpec, "-")
		kr, _ := ParseKeyRangeParts(parts[0], parts[1])
		if !proto.Equal(got, kr) {
			t.Errorf("EvenShardsKeyRange(%v, %v) != ParseKeyRangeParts(%v, %v): (%x, %x) != (%x, %x)", tc.i, tc.n, parts[0], parts[1], got.Start, got.End, kr.Start, kr.End)
		}
	}
}

func TestKeyRangeAdd(t *testing.T) {
	testcases := []struct {
		first  string
		second string
		out    string
		ok     bool
	}{{
		first:  "",
		second: "",
		out:    "",
		ok:     false,
	}, {
		first:  "",
		second: "-80",
		out:    "",
		ok:     false,
	}, {
		first:  "-80",
		second: "",
		out:    "",
		ok:     false,
	}, {
		first:  "",
		second: "80-",
		out:    "",
		ok:     false,
	}, {
		first:  "80-",
		second: "",
		out:    "",
		ok:     false,
	}, {
		first:  "80-",
		second: "-40",
		out:    "",
		ok:     false,
	}, {
		first:  "-40",
		second: "80-",
		out:    "",
		ok:     false,
	}, {
		first:  "-80",
		second: "80-",
		out:    "-",
		ok:     true,
	}, {
		first:  "80-",
		second: "-80",
		out:    "-",
		ok:     true,
	}, {
		first:  "-40",
		second: "40-80",
		out:    "-80",
		ok:     true,
	}, {
		first:  "40-80",
		second: "-40",
		out:    "-80",
		ok:     true,
	}, {
		first:  "40-80",
		second: "80-c0",
		out:    "40-c0",
		ok:     true,
	}, {
		first:  "80-c0",
		second: "40-80",
		out:    "40-c0",
		ok:     true,
	}}
	stringToKeyRange := func(spec string) *topodatapb.KeyRange {
		if spec == "" {
			return nil
		}
		parts := strings.Split(spec, "-")
		if len(parts) != 2 {
			panic("invalid spec")
		}
		kr, err := ParseKeyRangeParts(parts[0], parts[1])
		if err != nil {
			panic(err)
		}
		return kr
	}
	keyRangeToString := func(kr *topodatapb.KeyRange) string {
		if kr == nil {
			return ""
		}
		return KeyRangeString(kr)
	}
	for _, tcase := range testcases {
		first := stringToKeyRange(tcase.first)
		second := stringToKeyRange(tcase.second)
		out, ok := KeyRangeAdd(first, second)
		assert.Equal(t, tcase.out, keyRangeToString(out))
		assert.Equal(t, tcase.ok, ok)
	}
}

func TestEvenShardsKeyRange_Error(t *testing.T) {
	testCases := []struct {
		i, n      int
		wantError string
	}{
		{
			-1, 0,
			"the shard count must be > 0",
		},
		{
			32, 8,
			"must be less than",
		},
		{
			1, 6,
			"must be a power of two",
		},
	}

	for _, tc := range testCases {
		kr, err := EvenShardsKeyRange(tc.i, tc.n)
		if err == nil || !strings.Contains(err.Error(), tc.wantError) {
			t.Fatalf("EvenShardsKeyRange(%v, %v) = (%v, %v) want error = %v", tc.i, tc.n, kr, err, tc.wantError)
		}
	}
}

func TestParseShardingSpec(t *testing.T) {
	x40 := []byte{0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	x80 := []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	goodTable := map[string][]*topodatapb.KeyRange{
		"-": {{}},
		"0": {{}},
		"-4000000000000000-8000000000000000-": {
			{End: x40},
			{Start: x40, End: x80},
			{Start: x80},
		},
	}
	badTable := []string{
		"4000000000000000",
		"---",
		"4000000000000000--8000000000000000",
		"4000000000000000-3000000000000000", // not in order
	}
	for key, wanted := range goodTable {
		r, err := ParseShardingSpec(key)
		if err != nil {
			t.Errorf("Unexpected error: %v.", err)
		}
		if len(r) != len(wanted) {
			t.Errorf("Wrong result: wanted %v, got %v", wanted, r)
			continue
		}
		for i, w := range wanted {
			if !proto.Equal(r[i], w) {
				t.Errorf("Wrong result: wanted %v, got %v", w, r[i])
				break
			}
		}
	}
	for _, bad := range badTable {
		_, err := ParseShardingSpec(bad)
		if err == nil {
			t.Errorf("Didn't get expected error for %v.", bad)
		}
	}
}

func TestContains(t *testing.T) {
	var table = []struct {
		kid       string
		start     string
		end       string
		contained bool
	}{
		{kid: "3000000000000000", start: "3000000000000000", end: "", contained: true},
		{kid: "3000000000000000", start: "", end: "3000000000000000", contained: false},
		{kid: "4000000000000000", start: "3000000000000000", end: "", contained: true},
		{kid: "2000000000000000", start: "3000000000000000", end: "", contained: false},
	}

	for _, el := range table {
		s, err := hex.DecodeString(el.start)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		e, err := hex.DecodeString(el.end)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		kr := &topodatapb.KeyRange{
			Start: s,
			End:   e,
		}
		k, err := hex.DecodeString(el.kid)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if c := KeyRangeContains(kr, k); c != el.contained {
			t.Errorf("Unexpected result: contains for %v and (%v-%v) yields %v.", el.kid, el.start, el.end, c)
		}
		if !KeyRangeContains(nil, k) {
			t.Errorf("KeyRangeContains(nil, x) should always be true")
		}
	}
}

func TestIntersectOverlap(t *testing.T) {
	var table = []struct {
		a          string
		b          string
		c          string
		d          string
		intersects bool
		overlap    string
	}{
		{a: "40", b: "80", c: "c0", d: "d0", intersects: false},
		{a: "", b: "80", c: "80", d: "", intersects: false},
		{a: "", b: "80", c: "", d: "40", intersects: true, overlap: "-40"},
		{a: "80", b: "", c: "c0", d: "", intersects: true, overlap: "c0-"},
		{a: "", b: "80", c: "40", d: "80", intersects: true, overlap: "40-80"},
		{a: "40", b: "80", c: "60", d: "a0", intersects: true, overlap: "60-80"},
		{a: "40", b: "80", c: "50", d: "60", intersects: true, overlap: "50-60"},
		{a: "40", b: "80", c: "10", d: "50", intersects: true, overlap: "40-50"},
		{a: "40", b: "80", c: "40", d: "80", intersects: true, overlap: "40-80"},
		{a: "", b: "80", c: "", d: "80", intersects: true, overlap: "-80"},
		{a: "40", b: "", c: "40", d: "", intersects: true, overlap: "40-"},
		{a: "40", b: "80", c: "20", d: "40", intersects: false},
		{a: "80", b: "", c: "80", d: "c0", intersects: true, overlap: "80-c0"},
		{a: "", b: "", c: "c0", d: "d0", intersects: true, overlap: "c0-d0"},
	}

	for _, el := range table {
		a, err := hex.DecodeString(el.a)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		b, err := hex.DecodeString(el.b)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		left := &topodatapb.KeyRange{Start: a, End: b}
		c, err := hex.DecodeString(el.c)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		d, err := hex.DecodeString(el.d)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		right := &topodatapb.KeyRange{Start: c, End: d}
		if c := KeyRangesIntersect(left, right); c != el.intersects {
			t.Errorf("Unexpected result: KeyRangesIntersect for %v and %v yields %v.", left, right, c)
		}
		overlap, err := KeyRangesOverlap(left, right)
		if el.intersects {
			if err != nil {
				t.Errorf("Unexpected result: KeyRangesOverlap for overlapping %v and %v returned an error: %v", left, right, err)
			} else {
				got := hex.EncodeToString(overlap.Start) + "-" + hex.EncodeToString(overlap.End)
				if got != el.overlap {
					t.Errorf("Unexpected result: KeyRangesOverlap for overlapping %v and %v should have returned: %v but got: %v", left, right, el.overlap, got)
				}
			}
		} else {
			if err == nil {
				t.Errorf("Unexpected result: KeyRangesOverlap for non-overlapping %v and %v should have returned an error", left, right)
			}
		}
	}
}

func TestKeyRangeIncludes(t *testing.T) {
	var table = []struct {
		name     string
		big      string
		small    string
		expected bool
	}{
		{"big nil, small nil", "nil", "nil", true},
		{"big nil, small non nil, fully partial", "nil", "80-c0", true},
		{"big nil, small non nil, full start", "nil", "-c0", true},
		{"big nil, small non nil, full end", "nil", "80-", true},
		{"big non-nil, fully partial, small nil", "80-c0", "nil", false},
		{"big non-nil, full start, small nil", "-c0", "nil", false},
		{"big non-nil, full end, small nil", "80-", "nil", false},
		{"big full, small full", "-", "-", true},
		{"big full, small partial", "-", "40-60", true},
		{"big partial, small full", "40-60", "-", false},

		{"big partial, small to the end", "40-60", "40-", false},
		{"big partial, small bigger to the right", "40-60", "40-80", false},
		{"big partial, small equal", "40-60", "40-60", true},
		{"big partial, small smaller right", "40-60", "40-50", true},

		{"big partial, small to the beginning", "40-60", "-60", false},
		{"big partial, small smaller to the left", "40-60", "20-60", false},
		{"big partial, small bigger left", "40-60", "50-60", true},
	}

	var err error
	for _, tc := range table {
		var big, small *topodatapb.KeyRange
		if tc.big != "nil" {
			parts := strings.Split(tc.big, "-")
			big, err = ParseKeyRangeParts(parts[0], parts[1])
			if err != nil {
				t.Fatalf("test data error in %v: %v", tc.big, err)
			}
		}
		if tc.small != "nil" {
			parts := strings.Split(tc.small, "-")
			small, err = ParseKeyRangeParts(parts[0], parts[1])
			if err != nil {
				t.Fatalf("test data error in %v: %v", tc.small, err)
			}
		}
		got := KeyRangeIncludes(big, small)
		if got != tc.expected {
			t.Errorf("KeyRangeIncludes for test case '%v' returned %v but expected %v", tc.name, got, tc.expected)
		}
	}
}

func BenchmarkUint64KeyBytes(b *testing.B) {
	keys := []Uint64Key{
		0, 1, 0x7FFFFFFFFFFFFFFF, 0x8000000000000000, 0xFFFFFFFFFFFFFFFF,
	}

	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			key.Bytes()
		}
	}
}

func BenchmarkUint64KeyString(b *testing.B) {
	keys := []Uint64Key{
		0, 1, 0x7FFFFFFFFFFFFFFF, 0x8000000000000000, 0xFFFFFFFFFFFFFFFF,
	}

	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			_ = key.String()
		}
	}
}

func BenchmarkKeyRangeContains(b *testing.B) {
	kr := &topodatapb.KeyRange{
		Start: []byte{0x40, 0, 0, 0, 0, 0, 0, 0},
		End:   []byte{0x80, 0, 0, 0, 0, 0, 0, 0},
	}
	keys := [][]byte{
		{0x30, 0, 0, 0, 0, 0, 0, 0},
		{0x40, 0, 0, 0, 0, 0, 0, 0},
		{0x50, 0, 0, 0, 0, 0, 0, 0},
		{0x80, 0, 0, 0, 0, 0, 0, 0},
		{0x90, 0, 0, 0, 0, 0, 0, 0},
	}

	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			KeyRangeContains(kr, key)
		}
	}
}

func BenchmarkKeyRangesIntersect(b *testing.B) {
	kr1 := &topodatapb.KeyRange{
		Start: []byte{0x40, 0, 0, 0, 0, 0, 0, 0},
		End:   []byte{0x80, 0, 0, 0, 0, 0, 0, 0},
	}
	kr2 := &topodatapb.KeyRange{
		Start: []byte{0x30, 0, 0, 0, 0, 0, 0, 0},
		End:   []byte{0x50, 0, 0, 0, 0, 0, 0, 0},
	}

	for i := 0; i < b.N; i++ {
		KeyRangesIntersect(kr1, kr2)
	}
}

func BenchmarkKeyRangesOverlap(b *testing.B) {
	kr1 := &topodatapb.KeyRange{
		Start: []byte{0x40, 0, 0, 0, 0, 0, 0, 0},
		End:   []byte{0x80, 0, 0, 0, 0, 0, 0, 0},
	}
	kr2 := &topodatapb.KeyRange{
		Start: []byte{0x30, 0, 0, 0, 0, 0, 0, 0},
		End:   []byte{0x50, 0, 0, 0, 0, 0, 0, 0},
	}

	for i := 0; i < b.N; i++ {
		KeyRangesOverlap(kr1, kr2)
	}
}

func TestIsKeyRange(t *testing.T) {
	testcases := []struct {
		in  string
		out bool
	}{{
		in:  "-",
		out: true,
	}, {
		in:  "-80",
		out: true,
	}, {
		in:  "40-80",
		out: true,
	}, {
		in:  "80-",
		out: true,
	}, {
		in:  "a0-",
		out: true,
	}, {
		in:  "-A0",
		out: true,
	}, {
		in:  "",
		out: false,
	}, {
		in:  "x-80",
		out: false,
	}, {
		in:  "-80x",
		out: false,
	}, {
		in:  "select",
		out: false,
	}}

	for _, tcase := range testcases {
		assert.Equal(t, IsKeyRange(tcase.in), tcase.out, tcase.in)
	}
}

func TestGenerateShardRanges(t *testing.T) {
	type args struct {
		shards int
	}

	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		{
			"errors for shards less than 0",
			args{0},
			nil,
			true,
		},
		{
			"errors for shards more than 65536",
			args{65537},
			nil,
			true,
		},
		{
			"works for a single shard",
			args{1},
			[]string{"-"},
			false,
		},
		{
			"works for more than one shard",
			args{2},
			[]string{"-80", "80-"},
			false,
		},
		{
			"works for an odd number of shards",
			args{7},
			[]string{"-24", "24-49", "49-6d", "6d-92", "92-b6", "b6-db", "db-"},
			false,
		},
		{
			"works for large number of shards",
			args{256},
			[]string{"-01", "01-02", "02-03", "03-04", "04-05", "05-06", "06-07", "07-08", "08-09", "09-0a", "0a-0b", "0b-0c", "0c-0d", "0d-0e", "0e-0f", "0f-10", "10-11", "11-12", "12-13", "13-14", "14-15", "15-16", "16-17", "17-18", "18-19", "19-1a", "1a-1b", "1b-1c", "1c-1d", "1d-1e", "1e-1f", "1f-20", "20-21", "21-22", "22-23", "23-24", "24-25", "25-26", "26-27", "27-28", "28-29", "29-2a", "2a-2b", "2b-2c", "2c-2d", "2d-2e", "2e-2f", "2f-30", "30-31", "31-32", "32-33", "33-34", "34-35", "35-36", "36-37", "37-38", "38-39", "39-3a", "3a-3b", "3b-3c", "3c-3d", "3d-3e", "3e-3f", "3f-40", "40-41", "41-42", "42-43", "43-44", "44-45", "45-46", "46-47", "47-48", "48-49", "49-4a", "4a-4b", "4b-4c", "4c-4d", "4d-4e", "4e-4f", "4f-50", "50-51", "51-52", "52-53", "53-54", "54-55", "55-56", "56-57", "57-58", "58-59", "59-5a", "5a-5b", "5b-5c", "5c-5d", "5d-5e", "5e-5f", "5f-60", "60-61", "61-62", "62-63", "63-64", "64-65", "65-66", "66-67", "67-68", "68-69", "69-6a", "6a-6b", "6b-6c", "6c-6d", "6d-6e", "6e-6f", "6f-70", "70-71", "71-72", "72-73", "73-74", "74-75", "75-76", "76-77", "77-78", "78-79", "79-7a", "7a-7b", "7b-7c", "7c-7d", "7d-7e", "7e-7f", "7f-80", "80-81", "81-82", "82-83", "83-84", "84-85", "85-86", "86-87", "87-88", "88-89", "89-8a", "8a-8b", "8b-8c", "8c-8d", "8d-8e", "8e-8f", "8f-90", "90-91", "91-92", "92-93", "93-94", "94-95", "95-96", "96-97", "97-98", "98-99", "99-9a", "9a-9b", "9b-9c", "9c-9d", "9d-9e", "9e-9f", "9f-a0", "a0-a1", "a1-a2", "a2-a3", "a3-a4", "a4-a5", "a5-a6", "a6-a7", "a7-a8", "a8-a9", "a9-aa", "aa-ab", "ab-ac", "ac-ad", "ad-ae", "ae-af", "af-b0", "b0-b1", "b1-b2", "b2-b3", "b3-b4", "b4-b5", "b5-b6", "b6-b7", "b7-b8", "b8-b9", "b9-ba", "ba-bb", "bb-bc", "bc-bd", "bd-be", "be-bf", "bf-c0", "c0-c1", "c1-c2", "c2-c3", "c3-c4", "c4-c5", "c5-c6", "c6-c7", "c7-c8", "c8-c9", "c9-ca", "ca-cb", "cb-cc", "cc-cd", "cd-ce", "ce-cf", "cf-d0", "d0-d1", "d1-d2", "d2-d3", "d3-d4", "d4-d5", "d5-d6", "d6-d7", "d7-d8", "d8-d9", "d9-da", "da-db", "db-dc", "dc-dd", "dd-de", "de-df", "df-e0", "e0-e1", "e1-e2", "e2-e3", "e3-e4", "e4-e5", "e5-e6", "e6-e7", "e7-e8", "e8-e9", "e9-ea", "ea-eb", "eb-ec", "ec-ed", "ed-ee", "ee-ef", "ef-f0", "f0-f1", "f1-f2", "f2-f3", "f3-f4", "f4-f5", "f5-f6", "f6-f7", "f7-f8", "f8-f9", "f9-fa", "fa-fb", "fb-fc", "fc-fd", "fd-fe", "fe-ff", "ff-"},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GenerateShardRanges(tt.args.shards)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, got, tt.want)
		})
	}
}

func TestShardCalculatorForShardsGreaterThan512(t *testing.T) {
	got, err := GenerateShardRanges(512)
	assert.NoError(t, err)

	want := "ff80-"

	assert.Equal(t, want, got[511], "Invalid mapping for a 512-shard keyspace. Expected %v, got %v", want, got[511])
}
