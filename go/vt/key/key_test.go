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

func TestNormalize(t *testing.T) {
	type args struct {
		id []byte
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			"empty should empty",
			args{[]byte{}},
			[]byte{},
		},
		{
			"one zero should be empty",
			args{[]byte{0x00}},
			[]byte{},
		},
		{
			"any number of zeroes should be empty",
			args{[]byte{0x00, 0x00, 0x00}},
			[]byte{},
		},
		{
			"one non-zero byte should be left alone",
			args{[]byte{0x11}},
			[]byte{0x11},
		},
		{
			"multiple non-zero bytes should be left alone",
			args{[]byte{0x11, 0x22, 0x33}},
			[]byte{0x11, 0x22, 0x33},
		},
		{
			"zeroes that aren't trailing should be left alone",
			args{[]byte{0x11, 0x00, 0x22, 0x00, 0x33, 0x00}},
			[]byte{0x11, 0x00, 0x22, 0x00, 0x33},
		},
		{
			"excess zero bytes should be removed after a non-zero byte",
			args{[]byte{0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
			[]byte{0x11},
		},
		{
			"excess zero bytes should be removed after multiple non-zero bytes",
			args{[]byte{0x11, 0x22, 0x00, 0x00, 0x00}},
			[]byte{0x11, 0x22},
		},
		{
			"values longer than eight bytes should be supported",
			args{[]byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0x00}},
			[]byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, Normalize(tt.args.id), "Normalize(%v)", tt.args.id)
		})
	}
}

func TestCompare(t *testing.T) {
	type args struct {
		a []byte
		b []byte
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			"empty ids are equal",
			args{[]byte{}, []byte{}},
			0,
		},
		{
			"equal full id values are equal",
			args{
				[]byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88},
				[]byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88},
			},
			0,
		},
		{
			"equal partial id values are equal",
			args{
				[]byte{0x11, 0x22},
				[]byte{0x11, 0x22},
			},
			0,
		},
		{
			"equal full and partial id values are equal",
			args{[]byte{0x11, 0x22, 0x33, 0x44}, []byte{0x11, 0x22, 0x33, 0x44, 0x00, 0x00, 0x00, 0x00}},
			0,
		},
		{
			"equal partial and full id values are equal",
			args{[]byte{0x11, 0x22, 0x33, 0x44, 0x00, 0x00, 0x00, 0x00}, []byte{0x11, 0x22, 0x33, 0x44}},
			0,
		},
		{"a less than b", args{[]byte{0x01}, []byte{0x02}}, -1},
		{"a greater than b", args{[]byte{0x02}, []byte{0x01}}, +1},
		{
			"equal partial a and b with different lengths",
			args{[]byte{0x30, 0x00}, []byte{0x20}},
			1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, Compare(tt.args.a, tt.args.b), "Compare(%v, %v)", tt.args.a, tt.args.b)
		})
	}
}

func TestLess(t *testing.T) {
	type args struct {
		a []byte
		b []byte
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		// Less uses Compare which is already robustly tested, so we're just aiming to ensure that the result
		// of the Compare is used correctly in context and not e.g. reversed, so test a few obvious cases.
		{
			"a is less than b",
			args{[]byte{0x01}, []byte{0x02}},
			true,
		},
		{
			"a is equal to b",
			args{[]byte{0x01}, []byte{0x01}},
			false,
		},
		{
			"a is greater than b",
			args{[]byte{0x02}, []byte{0x01}},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, Less(tt.args.a, tt.args.b), "Less(%v, %v)", tt.args.a, tt.args.b)
		})
	}
}

func TestEqual(t *testing.T) {
	type args struct {
		a []byte
		b []byte
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		// Equal uses Compare which is already robustly tested, so we're just aiming to ensure that the result
		// of the Compare is used correctly in context and not e.g. reversed, so test a few obvious cases.
		{
			"a is less than b",
			args{[]byte{0x01}, []byte{0x02}},
			false,
		},
		{
			"a is equal to b",
			args{[]byte{0x01}, []byte{0x01}},
			true,
		},
		{
			"a is greater than b",
			args{[]byte{0x02}, []byte{0x01}},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, Equal(tt.args.a, tt.args.b), "Equal(%v, %v)", tt.args.a, tt.args.b)
		})
	}
}

func TestEmpty(t *testing.T) {
	type args struct {
		id []byte
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			"empty",
			args{[]byte{}},
			true,
		},
		{
			"not empty",
			args{[]byte{0x11, 0x22, 0x33}},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, Empty(tt.args.id), "Empty(%v)", tt.args.id)
		})
	}
}

func TestUint64Key(t *testing.T) {
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

func TestKeyRangeEndEqual(t *testing.T) {
	testcases := []struct {
		first  string
		second string
		out    bool
	}{{
		first:  "",
		second: "",
		out:    true,
	}, {
		first:  "",
		second: "-80",
		out:    false,
	}, {
		first:  "40-",
		second: "10-",
		out:    true,
	}, {
		first:  "-8000",
		second: "-80",
		out:    true,
	}, {
		first:  "-8000",
		second: "-8000000000000000",
		out:    true,
	}, {
		first:  "-80",
		second: "-8000",
		out:    true,
	}}

	for _, tcase := range testcases {
		first := stringToKeyRange(tcase.first)
		second := stringToKeyRange(tcase.second)
		out := KeyRangeEndEqual(first, second)
		if out != tcase.out {
			t.Fatalf("KeyRangeEndEqual(%q, %q) expected %t, got %t", tcase.first, tcase.second, tcase.out, out)
		}
	}
}

func TestKeyRangeStartEqual(t *testing.T) {
	testcases := []struct {
		first  string
		second string
		out    bool
	}{{
		first:  "",
		second: "",
		out:    true,
	}, {
		first:  "",
		second: "-80",
		out:    true,
	}, {
		first:  "40-",
		second: "20-",
		out:    false,
	}, {
		first:  "-8000",
		second: "-80",
		out:    true,
	}, {
		first:  "-8000",
		second: "-8000000000000000",
		out:    true,
	}, {
		first:  "-80",
		second: "-8000",
		out:    true,
	}}

	for _, tcase := range testcases {
		first := stringToKeyRange(tcase.first)
		second := stringToKeyRange(tcase.second)
		out := KeyRangeStartEqual(first, second)
		if out != tcase.out {
			t.Fatalf("KeyRangeStartEqual(%q, %q) expected %t, got %t", tcase.first, tcase.second, tcase.out, out)
		}
	}
}

func TestKeyRangeEqual(t *testing.T) {
	testcases := []struct {
		first  string
		second string
		out    bool
	}{{
		first:  "",
		second: "",
		out:    true,
	}, {
		first:  "",
		second: "-80",
		out:    false,
	}, {
		first:  "-8000",
		second: "-80",
		out:    true,
	}, {
		first:  "-8000",
		second: "-8000000000000000",
		out:    true,
	}, {
		first:  "-80",
		second: "-8000",
		out:    true,
	}}

	for _, tcase := range testcases {
		first := stringToKeyRange(tcase.first)
		second := stringToKeyRange(tcase.second)
		out := KeyRangeEqual(first, second)
		if out != tcase.out {
			t.Fatalf("KeyRangeEqual(%q, %q) expected %t, got %t", tcase.first, tcase.second, tcase.out, out)
		}
	}
}

func TestKeyRangeContiguous(t *testing.T) {
	testcases := []struct {
		first  string
		second string
		out    bool
	}{{
		first:  "-40",
		second: "40-80",
		out:    true,
	}, {
		first:  "40-80",
		second: "-40",
		out:    false,
	}, {
		first:  "-",
		second: "-40",
		out:    false,
	}, {
		first:  "40-80",
		second: "c0-",
		out:    false,
	}, {
		first:  "40-80",
		second: "80-c0",
		out:    true,
	}, {
		first:  "40-80",
		second: "8000000000000000-c000000000000000",
		out:    true,
	}, {
		first:  "4000000000000000-8000000000000000",
		second: "80-c0",
		out:    true,
	}}

	for _, tcase := range testcases {
		first := stringToKeyRange(tcase.first)
		second := stringToKeyRange(tcase.second)
		out := KeyRangeContiguous(first, second)
		if out != tcase.out {
			t.Fatalf("KeyRangeContiguous(%q, %q) expected %t, got %t", tcase.first, tcase.second, tcase.out, out)
		}
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

func TestKeyRangeComparisons(t *testing.T) {
	type args struct {
		a *topodatapb.KeyRange
		b *topodatapb.KeyRange
	}
	type wants struct {
		wantStartCompare int
		wantStartEqual   bool
		wantEndCompare   int
		wantEndEqual     bool
		wantCompare      int
		wantEqual        bool
	}
	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "a and b are both full range",
			args: args{
				a: stringToKeyRange("-"),
				b: stringToKeyRange("-"),
			},
			wants: wants{
				wantStartCompare: 0,
				wantStartEqual:   true,
				wantEndCompare:   0,
				wantEndEqual:     true,
				wantCompare:      0,
				wantEqual:        true,
			},
		},
		{
			name: "a is equal to b",
			args: args{
				a: stringToKeyRange("10-30"),
				b: stringToKeyRange("10-30"),
			},
			wants: wants{
				wantStartCompare: 0,
				wantStartEqual:   true,
				wantEndCompare:   0,
				wantEndEqual:     true,
				wantCompare:      0,
				wantEqual:        true,
			},
		},
		{
			name: "a (2 digit, end only) but equal to b (2 digits, end only)",
			args: args{
				a: stringToKeyRange("-80"),
				b: stringToKeyRange("-80"),
			},
			wants: wants{
				wantStartCompare: 0,
				wantStartEqual:   true,
				wantEndCompare:   0,
				wantEndEqual:     true,
				wantCompare:      0,
				wantEqual:        true,
			},
		},
		{
			name: "a (2 digit, end only) but equal to b (4 digits, end only)",
			args: args{
				a: stringToKeyRange("-80"),
				b: stringToKeyRange("-8000"),
			},
			wants: wants{
				wantStartCompare: 0,
				wantStartEqual:   true,
				wantEndCompare:   0,
				wantEndEqual:     true,
				wantCompare:      0,
				wantEqual:        true,
			},
		},
		{
			name: "a (2 digit, end only) but equal to b (6 digits, end only)",
			args: args{
				a: stringToKeyRange("-80"),
				b: stringToKeyRange("-800000"),
			},
			wants: wants{
				wantStartCompare: 0,
				wantStartEqual:   true,
				wantEndCompare:   0,
				wantEndEqual:     true,
				wantCompare:      0,
				wantEqual:        true,
			},
		},
		{
			name: "a (2 digit, end only) but equal to b (8 digits, end only)",
			args: args{
				a: stringToKeyRange("-80"),
				b: stringToKeyRange("-80000000"),
			},
			wants: wants{
				wantStartCompare: 0,
				wantStartEqual:   true,
				wantEndCompare:   0,
				wantEndEqual:     true,
				wantCompare:      0,
				wantEqual:        true,
			},
		},
		{
			name: "a (2 digit, start only) but equal to b (2 digits, start only)",
			args: args{
				stringToKeyRange("80-"),
				stringToKeyRange("80-"),
			},
			wants: wants{
				wantStartCompare: 0,
				wantStartEqual:   true,
				wantEndCompare:   0,
				wantEndEqual:     true,
				wantCompare:      0,
				wantEqual:        true,
			},
		},
		{
			name: "a (2 digit, start only) but equal to b (4 digits, start only)",
			args: args{
				a: stringToKeyRange("80-"),
				b: stringToKeyRange("8000-"),
			},
			wants: wants{
				wantStartCompare: 0,
				wantStartEqual:   true,
				wantEndCompare:   0,
				wantEndEqual:     true,
				wantCompare:      0,
				wantEqual:        true,
			},
		},
		{
			name: "a (2 digit, start only) but equal to b (6 digits, start only)",
			args: args{
				a: stringToKeyRange("80-"),
				b: stringToKeyRange("800000-"),
			},
			wants: wants{
				wantStartCompare: 0,
				wantStartEqual:   true,
				wantEndCompare:   0,
				wantEndEqual:     true,
				wantCompare:      0,
				wantEqual:        true,
			},
		},
		{
			name: "a (2 digit, start only) but equal to b (8 digits, start only)",
			args: args{
				a: stringToKeyRange("80-"),
				b: stringToKeyRange("80000000-"),
			},
			wants: wants{
				wantStartCompare: 0,
				wantStartEqual:   true,
				wantEndCompare:   0,
				wantEndEqual:     true,
				wantCompare:      0,
				wantEqual:        true,
			},
		},
		{
			name: "a (4 digits) but equal to b (2 digits)",
			args: args{
				a: stringToKeyRange("1000-3000"),
				b: stringToKeyRange("10-30"),
			},
			wants: wants{
				wantStartCompare: 0,
				wantStartEqual:   true,
				wantEndCompare:   0,
				wantEndEqual:     true,
				wantCompare:      0,
				wantEqual:        true,
			},
		},
		{
			name: "a (8 digits) but equal to b (4 digits)",
			args: args{
				a: stringToKeyRange("10000000-30000000"),
				b: stringToKeyRange("1000-3000"),
			},
			wants: wants{
				wantStartCompare: 0,
				wantStartEqual:   true,
				wantEndCompare:   0,
				wantEndEqual:     true,
				wantCompare:      0,
				wantEqual:        true,
			},
		},
		{
			name: "b (4 digits) but equal to a (2 digits)",
			args: args{
				a: stringToKeyRange("10-30"),
				b: stringToKeyRange("1000-3000"),
			},
			wants: wants{
				wantStartCompare: 0,
				wantStartEqual:   true,
				wantEndCompare:   0,
				wantEndEqual:     true,
				wantCompare:      0,
				wantEqual:        true,
			},
		},
		{
			name: "b (8 digits) but equal to a (4 digits)",
			args: args{
				a: stringToKeyRange("10-30"),
				b: stringToKeyRange("10000000-30000000"),
			},
			wants: wants{
				wantStartCompare: 0,
				wantStartEqual:   true,
				wantEndCompare:   0,
				wantEndEqual:     true,
				wantCompare:      0,
				wantEqual:        true,
			},
		},
		{
			name: "a is full range, b is not",
			args: args{
				a: stringToKeyRange("-"),
				b: stringToKeyRange("20-30"),
			},
			wants: wants{
				wantStartCompare: -1,
				wantStartEqual:   false,
				wantEndCompare:   1,
				wantEndEqual:     false,
				wantCompare:      -1,
				wantEqual:        false,
			},
		},
		{
			name: "b is full range, a is not",
			args: args{
				a: stringToKeyRange("10-30"),
				b: stringToKeyRange("-"),
			},
			wants: wants{
				wantStartCompare: 1,
				wantStartEqual:   false,
				wantEndCompare:   -1,
				wantEndEqual:     false,
				wantCompare:      1,
				wantEqual:        false,
			},
		},
		{
			name: "a start is greater than b start",
			args: args{
				a: stringToKeyRange("10-30"),
				b: stringToKeyRange("20-30"),
			},
			wants: wants{
				wantStartCompare: -1,
				wantStartEqual:   false,
				wantEndCompare:   0,
				wantEndEqual:     true,
				wantCompare:      -1,
				wantEqual:        false,
			},
		},
		{
			name: "b start is greater than a start",
			args: args{
				a: stringToKeyRange("20-30"),
				b: stringToKeyRange("10-30"),
			},
			wants: wants{
				wantStartCompare: 1,
				wantStartEqual:   false,
				wantEndCompare:   0,
				wantEndEqual:     true,
				wantCompare:      1,
				wantEqual:        false,
			},
		},
		{
			name: "a start is empty, b start is not",
			args: args{
				a: stringToKeyRange("-30"),
				b: stringToKeyRange("10-30"),
			},
			wants: wants{
				wantStartCompare: -1,
				wantStartEqual:   false,
				wantEndCompare:   0,
				wantEndEqual:     true,
				wantCompare:      -1,
				wantEqual:        false,
			},
		},
		{
			name: "b start is empty, a start is not",
			args: args{
				a: stringToKeyRange("10-30"),
				b: stringToKeyRange("-30"),
			},
			wants: wants{
				wantStartCompare: 1,
				wantStartEqual:   false,
				wantEndCompare:   0,
				wantEndEqual:     true,
				wantCompare:      1,
				wantEqual:        false,
			},
		},
		{
			name: "a end is greater than b end",
			args: args{
				a: stringToKeyRange("10-30"),
				b: stringToKeyRange("10-20"),
			},
			wants: wants{
				wantStartCompare: 0,
				wantStartEqual:   true,
				wantEndCompare:   1,
				wantEndEqual:     false,
				wantCompare:      1,
				wantEqual:        false,
			},
		},
		{
			name: "b end is greater than a end",
			args: args{
				a: stringToKeyRange("10-20"),
				b: stringToKeyRange("10-30"),
			},
			wants: wants{
				wantStartCompare: 0,
				wantStartEqual:   true,
				wantEndCompare:   -1,
				wantEndEqual:     false,
				wantCompare:      -1,
				wantEqual:        false,
			},
		},
		{
			name: "a end is empty, b end is not",
			args: args{
				a: stringToKeyRange("10-"),
				b: stringToKeyRange("10-30"),
			},
			wants: wants{
				wantStartCompare: 0,
				wantStartEqual:   true,
				wantEndCompare:   1,
				wantEndEqual:     false,
				wantCompare:      1,
				wantEqual:        false,
			},
		},
		{
			name: "b end is empty, a end is not",
			args: args{
				a: stringToKeyRange("10-30"),
				b: stringToKeyRange("10-"),
			},
			wants: wants{
				wantStartCompare: 0,
				wantStartEqual:   true,
				wantEndCompare:   -1,
				wantEndEqual:     false,
				wantCompare:      -1,
				wantEqual:        false,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.wants.wantStartCompare, KeyRangeStartCompare(tt.args.a, tt.args.b), "KeyRangeStartCompare(%v, %v)", tt.args.a, tt.args.b)
			assert.Equalf(t, tt.wants.wantStartEqual, KeyRangeStartEqual(tt.args.a, tt.args.b), "KeyRangeStartEqual(%v, %v)", tt.args.a, tt.args.b)
			assert.Equalf(t, tt.wants.wantEndCompare, KeyRangeEndCompare(tt.args.a, tt.args.b), "KeyRangeEndCompare(%v, %v)", tt.args.a, tt.args.b)
			assert.Equalf(t, tt.wants.wantEndEqual, KeyRangeEndEqual(tt.args.a, tt.args.b), "KeyRangeEndEqual(%v, %v)", tt.args.a, tt.args.b)
			assert.Equalf(t, tt.wants.wantCompare, KeyRangeCompare(tt.args.a, tt.args.b), "KeyRangeCompare(%v, %v)", tt.args.a, tt.args.b)
			assert.Equalf(t, tt.wants.wantEqual, KeyRangeEqual(tt.args.a, tt.args.b), "KeyRangeEqual(%v, %v)", tt.args.a, tt.args.b)
		})
	}
}

func TestKeyRangeContains(t *testing.T) {
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

func TestKeyRangeIntersect(t *testing.T) {
	type args struct {
		a *topodatapb.KeyRange
		b *topodatapb.KeyRange
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		// non-intersecting cases
		{
			name: "typical half-range split, ascending order",
			args: args{a: stringToKeyRange("-80"), b: stringToKeyRange("80-")},
			want: false,
		},
		{
			name: "typical half-range split, descending order",
			args: args{a: stringToKeyRange("80-"), b: stringToKeyRange("-80")},
			want: false,
		},
		{
			name: "partial ranges, ascending order",
			args: args{a: stringToKeyRange("40-80"), b: stringToKeyRange("c0-d0")},
			want: false,
		},
		{
			name: "partial ranges, descending order",
			args: args{a: stringToKeyRange("40-80"), b: stringToKeyRange("20-40")},
			want: false,
		},
		{
			name: "partial ranges, different key lengths",
			args: args{a: stringToKeyRange("4000-8000"), b: stringToKeyRange("20-40")},
			want: false,
		},

		// intersecting cases with a full range
		{
			name: "full range with full range",
			args: args{a: stringToKeyRange("-"), b: stringToKeyRange("-")},
			want: true,
		},
		{
			name: "full range with maximum key partial range",
			args: args{a: stringToKeyRange("-"), b: stringToKeyRange("80-")},
			want: true,
		},
		{
			name: "full range with partial range",
			args: args{a: stringToKeyRange("-"), b: stringToKeyRange("c0-d0")},
			want: true,
		},
		{
			name: "minimum key partial range with full range",
			args: args{a: stringToKeyRange("-80"), b: stringToKeyRange("-")},
			want: true,
		},
		{
			name: "partial range with full range",
			args: args{a: stringToKeyRange("a0-b0"), b: stringToKeyRange("-")},
			want: true,
		},

		// intersecting cases with only partial ranges
		{
			name: "the same range, both from minimum key",
			args: args{a: stringToKeyRange("-80"), b: stringToKeyRange("-80")},
			want: true,
		},
		{
			name: "the same range, both from minimum key, different key lengths",
			args: args{a: stringToKeyRange("-8000"), b: stringToKeyRange("-80")},
			want: true,
		},
		{
			name: "the same range, both to maximum key",
			args: args{a: stringToKeyRange("40-"), b: stringToKeyRange("40-")},
			want: true,
		},
		{
			name: "the same range, both to maximum key, different key lengths",
			args: args{a: stringToKeyRange("4000-"), b: stringToKeyRange("40-")},
			want: true,
		},
		{
			name: "the same range, both with mid-range keys",
			args: args{a: stringToKeyRange("40-80"), b: stringToKeyRange("40-80")},
			want: true,
		},
		{
			name: "the same range, both with mid-range keys, different key lengths",
			args: args{a: stringToKeyRange("40-80"), b: stringToKeyRange("4000-8000")},
			want: true,
		},
		{
			name: "different-sized partial ranges, both from minimum key",
			args: args{a: stringToKeyRange("-80"), b: stringToKeyRange("-40")},
			want: true,
		},
		{
			name: "different-sized partial ranges, both to maximum key",
			args: args{a: stringToKeyRange("80-"), b: stringToKeyRange("c0-")},
			want: true,
		},
		{
			name: "different-sized partial ranges, from minimum key with mid-range key",
			args: args{a: stringToKeyRange("-80"), b: stringToKeyRange("40-80")},
			want: true,
		},
		{
			name: "different-sized partial ranges, from minimum key with mid-range key, different key lengths",
			args: args{a: stringToKeyRange("-80"), b: stringToKeyRange("4000-8000")},
			want: true,
		},
		{
			name: "different-sized partial ranges, to maximum key with mid-range key",
			args: args{a: stringToKeyRange("80-"), b: stringToKeyRange("80-c0")},
			want: true,
		},
		{
			name: "different-sized partial ranges, to maximum key with mid-range key, different key lengths",
			args: args{a: stringToKeyRange("80-"), b: stringToKeyRange("8000-c000")},
			want: true,
		},
		{
			name: "partially overlapping ranges, in ascending order",
			args: args{a: stringToKeyRange("40-80"), b: stringToKeyRange("60-a0")},
			want: true,
		},
		{
			name: "partially overlapping ranges, in descending order",
			args: args{a: stringToKeyRange("40-80"), b: stringToKeyRange("10-50")},
			want: true,
		},
		{
			name: "partially overlapping ranges, one fully containing the other, in ascending order",
			args: args{a: stringToKeyRange("40-80"), b: stringToKeyRange("50-60")},
			want: true,
		},
		{
			name: "partially overlapping ranges, one fully containing the other, in descending order",
			args: args{a: stringToKeyRange("40-80"), b: stringToKeyRange("30-90")},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, KeyRangeIntersect(tt.args.a, tt.args.b), "KeyRangeIntersect(%v, %v)", tt.args.a, tt.args.b)
		})
	}
}

func TestKeyRangeContainsKeyRange(t *testing.T) {
	type args struct {
		a *topodatapb.KeyRange
		b *topodatapb.KeyRange
	}
	var tests = []struct {
		name string
		args args
		want bool
	}{
		// full range contains itself
		{
			name: "both full range",
			args: args{a: stringToKeyRange("-"), b: stringToKeyRange("-")},
			want: true,
		},

		// full range always contains a partial range
		{
			name: "full range, partial range from minimum key",
			args: args{a: stringToKeyRange("-"), b: stringToKeyRange("-c0")},
			want: true,
		},
		{
			name: "full range, partial range to maximum key",
			args: args{a: stringToKeyRange("-"), b: stringToKeyRange("80-")},
			want: true,
		},
		{
			name: "full range, partial mid-key range",
			args: args{a: stringToKeyRange("-"), b: stringToKeyRange("80-c0")},
			want: true,
		},

		// equal partial ranges contain each other
		{
			name: "equal partial ranges",
			args: args{a: stringToKeyRange("40-60"), b: stringToKeyRange("40-60")},
			want: true,
		},
		{
			name: "equal partial ranges, different size keys",
			args: args{a: stringToKeyRange("40-60"), b: stringToKeyRange("4000-6000")},
			want: true,
		},
		{
			name: "equal partial ranges, different size keys",
			args: args{a: stringToKeyRange("4000-6000"), b: stringToKeyRange("40-60")},
			want: true,
		},

		// partial ranges may contain smaller partial ranges
		{
			name: "partial range, partial touching start",
			args: args{a: stringToKeyRange("40-80"), b: stringToKeyRange("40-50")},
			want: true,
		},
		{
			name: "partial range, partial touching start, different size keys",
			args: args{a: stringToKeyRange("40-80"), b: stringToKeyRange("4000-5000")},
			want: true,
		},
		{
			name: "partial range, partial touching start, different size keys",
			args: args{a: stringToKeyRange("4000-8000"), b: stringToKeyRange("40-50")},
			want: true,
		},
		{
			name: "partial range, partial touching end",
			args: args{a: stringToKeyRange("40-80"), b: stringToKeyRange("70-80")},
			want: true,
		},
		{
			name: "partial range, partial touching end, different size keys",
			args: args{a: stringToKeyRange("40-80"), b: stringToKeyRange("7000-8000")},
			want: true,
		},
		{
			name: "partial range, partial touching end, different size keys",
			args: args{a: stringToKeyRange("4000-8000"), b: stringToKeyRange("70-80")},
			want: true,
		},
		{
			name: "partial range, partial in the middle",
			args: args{a: stringToKeyRange("40-80"), b: stringToKeyRange("50-70")},
			want: true,
		},
		{
			name: "partial range, partial in the middle, different size keys",
			args: args{a: stringToKeyRange("40-80"), b: stringToKeyRange("5000-7000")},
			want: true,
		},
		{
			name: "partial range, partial in the middle, different size keys",
			args: args{a: stringToKeyRange("4000-8000"), b: stringToKeyRange("50-70")},
			want: true,
		},

		// partial ranges do not contain the full range
		{
			name: "partial range from minimum key, full range",
			args: args{a: stringToKeyRange("-c0"), b: stringToKeyRange("-")},
			want: false,
		},
		{
			name: "partial range to maximum key, full range",
			args: args{a: stringToKeyRange("80-"), b: stringToKeyRange("-")},
			want: false,
		},
		{
			name: "partial mid-key range, full range",
			args: args{a: stringToKeyRange("80-c0"), b: stringToKeyRange("-")},
			want: false,
		},

		// partial ranges do not contain overlapping but boundary-crossing partial ranges
		{
			name: "partial range mid-key range, overlapping partial range to maximum key",
			args: args{a: stringToKeyRange("40-60"), b: stringToKeyRange("50-")},
			want: false,
		},
		{
			name: "partial range mid-key range, overlapping partial range to maximum key",
			args: args{a: stringToKeyRange("40-60"), b: stringToKeyRange("5000-")},
			want: false,
		},
		{
			name: "partial range mid-key range, overlapping partial range to maximum key, different size keys",
			args: args{a: stringToKeyRange("4000-6000"), b: stringToKeyRange("50-")},
			want: false,
		},
		{
			name: "partial range mid-key range, overlapping partial range to maximum key, different size keys",
			args: args{a: stringToKeyRange("40-60"), b: stringToKeyRange("5000-")},
			want: false,
		},
		{
			name: "partial range mid-key range, overlapping partial range from minimum key",
			args: args{a: stringToKeyRange("40-60"), b: stringToKeyRange("-50")},
			want: false,
		},
		{
			name: "partial range mid-key range, overlapping partial range from minimum key",
			args: args{a: stringToKeyRange("40-60"), b: stringToKeyRange("-5000")},
			want: false,
		},
		{
			name: "partial range mid-key range, overlapping partial range from minimum key, different size keys",
			args: args{a: stringToKeyRange("4000-6000"), b: stringToKeyRange("-50")},
			want: false,
		},
		{
			name: "partial range mid-key range, overlapping partial range from minimum key, different size keys",
			args: args{a: stringToKeyRange("40-60"), b: stringToKeyRange("-5000")},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, KeyRangeContainsKeyRange(tt.args.a, tt.args.b), "KeyRangeContainsKeyRange(%v, %v)", tt.args.a, tt.args.b)
		})
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
		KeyRangeIntersect(kr1, kr2)
	}
}

func TestIsValidKeyRange(t *testing.T) {
	tests := []struct {
		arg  string
		want bool
	}{
		// normal cases
		{"-", true},
		{"00-", true},
		{"-80", true},
		{"40-80", true},
		{"80-", true},
		{"a0-", true},
		{"-A0", true},

		// special cases
		{"0", true}, // equal to "-"

		// invalid cases
		{"", false},       // empty is not allowed
		{"11", false},     // no hyphen
		{"-1", false},     // odd number of digits
		{"-111", false},   // odd number of digits
		{"1-2", false},    // odd number of digits
		{"x-80", false},   // invalid character
		{"-80x", false},   // invalid character
		{"select", false}, // nonsense
		{"+", false},      // nonsense
	}
	for _, tt := range tests {
		assert.Equalf(t, tt.want, IsValidKeyRange(tt.arg), "IsValidKeyRange(%v)", tt.arg)
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

func stringToKeyRange(spec string) *topodatapb.KeyRange {
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
