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
	"fmt"
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
		require.NoError(t, err)
		assert.True(t, proto.Equal(got, tc.want), "got=(%x, %x), want=(%x, %x)", got.Start, got.End, tc.want.Start, tc.want.End)

		// Check if the string representation is equal as well.
		gotStr := KeyRangeString(got)
		assert.Equal(t, tc.wantSpec, gotStr)

		// Now verify that ParseKeyRangeParts() produces the same KeyRange object as
		// we do.
		parts := strings.Split(tc.wantSpec, "-")
		kr, _ := ParseKeyRangeParts(parts[0], parts[1])
		assert.True(t, proto.Equal(got, kr), "EvenShardsKeyRange(%v, %v) != ParseKeyRangeParts(%v, %v): (%x, %x) != (%x, %x)", tc.i, tc.n, parts[0], parts[1], got.Start, got.End, kr.Start, kr.End)
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
		require.Equal(t, tcase.out, out)
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
		require.Equal(t, tcase.out, out)
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
		require.Equal(t, tcase.out, out)
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
		require.Equal(t, tcase.out, out)
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
		_, err := EvenShardsKeyRange(tc.i, tc.n)
		require.ErrorContains(t, err, tc.wantError)
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
		assert.NoError(t, err)
		if !assert.Len(t, r, len(wanted)) {
			continue
		}
		for i, w := range wanted {
			require.Truef(t, proto.Equal(r[i], w), "wanted %v, got %v", w, r[i])
		}
	}
	for _, bad := range badTable {
		_, err := ParseShardingSpec(bad)
		assert.Error(t, err)
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
		assert.NoError(t, err)
		e, err := hex.DecodeString(el.end)
		assert.NoError(t, err)
		kr := &topodatapb.KeyRange{
			Start: s,
			End:   e,
		}
		k, err := hex.DecodeString(el.kid)
		assert.NoError(t, err)
		c := KeyRangeContains(kr, k)
		assert.Equal(t, el.contained, c)

		assert.True(t, KeyRangeContains(nil, k), "KeyRangeContains(nil, x) should always be true")
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
			"works for 49 shards",
			args{49},
			[]string{"-05", "05-0a", "0a-0f", "0f-14", "14-1a", "1a-1f", "1f-24", "24-29", "29-2f", "2f-34", "34-39", "39-3e", "3e-43", "43-49", "49-4e", "4e-53", "53-58", "58-5e", "5e-63", "63-68", "68-6d", "6d-72", "72-78", "78-7d", "7d-82", "82-87", "87-8d", "8d-92", "92-97", "97-9c", "9c-a1", "a1-a7", "a7-ac", "ac-b1", "b1-b6", "b6-bc", "bc-c1", "c1-c6", "c6-cb", "cb-d0", "d0-d6", "d6-db", "db-e0", "e0-e5", "e5-eb", "eb-f0", "f0-f5", "f5-fa", "fa-"},
			false,
		},
		{
			"works for 103 shards",
			args{103},
			[]string{"-02", "02-04", "04-07", "07-09", "09-0c", "0c-0e", "0e-11", "11-13", "13-16", "16-18", "18-1b", "1b-1d", "1d-20", "20-22", "22-25", "25-27", "27-2a", "2a-2c", "2c-2f", "2f-31", "31-34", "34-36", "36-39", "39-3b", "3b-3e", "3e-40", "40-43", "43-45", "45-48", "48-4a", "4a-4d", "4d-4f", "4f-52", "52-54", "54-56", "56-59", "59-5b", "5b-5e", "5e-60", "60-63", "63-65", "65-68", "68-6a", "6a-6d", "6d-6f", "6f-72", "72-74", "74-77", "77-79", "79-7c", "7c-7e", "7e-81", "81-83", "83-86", "86-88", "88-8b", "8b-8d", "8d-90", "90-92", "92-95", "95-97", "97-9a", "9a-9c", "9c-9f", "9f-a1", "a1-a4", "a4-a6", "a6-a9", "a9-ab", "ab-ad", "ad-b0", "b0-b2", "b2-b5", "b5-b7", "b7-ba", "ba-bc", "bc-bf", "bf-c1", "c1-c4", "c4-c6", "c6-c9", "c9-cb", "cb-ce", "ce-d0", "d0-d3", "d3-d5", "d5-d8", "d8-da", "da-dd", "dd-df", "df-e2", "e2-e4", "e4-e7", "e7-e9", "e9-ec", "ec-ee", "ee-f1", "f1-f3", "f3-f6", "f6-f8", "f8-fb", "fb-fd", "fd-"},
			false,
		},
		{
			"works for 107 shards",
			args{107},
			[]string{"-02", "02-04", "04-07", "07-09", "09-0b", "0b-0e", "0e-10", "10-13", "13-15", "15-17", "17-1a", "1a-1c", "1c-1f", "1f-21", "21-23", "23-26", "26-28", "28-2b", "2b-2d", "2d-2f", "2f-32", "32-34", "34-37", "37-39", "39-3b", "3b-3e", "3e-40", "40-42", "42-45", "45-47", "47-4a", "4a-4c", "4c-4e", "4e-51", "51-53", "53-56", "56-58", "58-5a", "5a-5d", "5d-5f", "5f-62", "62-64", "64-66", "66-69", "69-6b", "6b-6e", "6e-70", "70-72", "72-75", "75-77", "77-7a", "7a-7c", "7c-7e", "7e-81", "81-83", "83-85", "85-88", "88-8a", "8a-8d", "8d-8f", "8f-91", "91-94", "94-96", "96-99", "99-9b", "9b-9d", "9d-a0", "a0-a2", "a2-a5", "a5-a7", "a7-a9", "a9-ac", "ac-ae", "ae-b1", "b1-b3", "b3-b5", "b5-b8", "b8-ba", "ba-bd", "bd-bf", "bf-c1", "c1-c4", "c4-c6", "c6-c8", "c8-cb", "cb-cd", "cd-d0", "d0-d2", "d2-d4", "d4-d7", "d7-d9", "d9-dc", "dc-de", "de-e0", "e0-e3", "e3-e5", "e5-e8", "e8-ea", "ea-ec", "ec-ef", "ef-f1", "f1-f4", "f4-f6", "f6-f8", "f8-fb", "fb-fd", "fd-"},
			false,
		},
		{
			"works for large number of shards",
			args{256},
			[]string{"-01", "01-02", "02-03", "03-04", "04-05", "05-06", "06-07", "07-08", "08-09", "09-0a", "0a-0b", "0b-0c", "0c-0d", "0d-0e", "0e-0f", "0f-10", "10-11", "11-12", "12-13", "13-14", "14-15", "15-16", "16-17", "17-18", "18-19", "19-1a", "1a-1b", "1b-1c", "1c-1d", "1d-1e", "1e-1f", "1f-20", "20-21", "21-22", "22-23", "23-24", "24-25", "25-26", "26-27", "27-28", "28-29", "29-2a", "2a-2b", "2b-2c", "2c-2d", "2d-2e", "2e-2f", "2f-30", "30-31", "31-32", "32-33", "33-34", "34-35", "35-36", "36-37", "37-38", "38-39", "39-3a", "3a-3b", "3b-3c", "3c-3d", "3d-3e", "3e-3f", "3f-40", "40-41", "41-42", "42-43", "43-44", "44-45", "45-46", "46-47", "47-48", "48-49", "49-4a", "4a-4b", "4b-4c", "4c-4d", "4d-4e", "4e-4f", "4f-50", "50-51", "51-52", "52-53", "53-54", "54-55", "55-56", "56-57", "57-58", "58-59", "59-5a", "5a-5b", "5b-5c", "5c-5d", "5d-5e", "5e-5f", "5f-60", "60-61", "61-62", "62-63", "63-64", "64-65", "65-66", "66-67", "67-68", "68-69", "69-6a", "6a-6b", "6b-6c", "6c-6d", "6d-6e", "6e-6f", "6f-70", "70-71", "71-72", "72-73", "73-74", "74-75", "75-76", "76-77", "77-78", "78-79", "79-7a", "7a-7b", "7b-7c", "7c-7d", "7d-7e", "7e-7f", "7f-80", "80-81", "81-82", "82-83", "83-84", "84-85", "85-86", "86-87", "87-88", "88-89", "89-8a", "8a-8b", "8b-8c", "8c-8d", "8d-8e", "8e-8f", "8f-90", "90-91", "91-92", "92-93", "93-94", "94-95", "95-96", "96-97", "97-98", "98-99", "99-9a", "9a-9b", "9b-9c", "9c-9d", "9d-9e", "9e-9f", "9f-a0", "a0-a1", "a1-a2", "a2-a3", "a3-a4", "a4-a5", "a5-a6", "a6-a7", "a7-a8", "a8-a9", "a9-aa", "aa-ab", "ab-ac", "ac-ad", "ad-ae", "ae-af", "af-b0", "b0-b1", "b1-b2", "b2-b3", "b3-b4", "b4-b5", "b5-b6", "b6-b7", "b7-b8", "b8-b9", "b9-ba", "ba-bb", "bb-bc", "bc-bd", "bd-be", "be-bf", "bf-c0", "c0-c1", "c1-c2", "c2-c3", "c3-c4", "c4-c5", "c5-c6", "c6-c7", "c7-c8", "c8-c9", "c9-ca", "ca-cb", "cb-cc", "cc-cd", "cd-ce", "ce-cf", "cf-d0", "d0-d1", "d1-d2", "d2-d3", "d3-d4", "d4-d5", "d5-d6", "d6-d7", "d7-d8", "d8-d9", "d9-da", "da-db", "db-dc", "dc-dd", "dd-de", "de-df", "df-e0", "e0-e1", "e1-e2", "e2-e3", "e3-e4", "e4-e5", "e5-e6", "e6-e7", "e7-e8", "e8-e9", "e9-ea", "ea-eb", "eb-ec", "ec-ed", "ed-ee", "ee-ef", "ef-f0", "f0-f1", "f1-f2", "f2-f3", "f3-f4", "f4-f5", "f5-f6", "f6-f7", "f7-f8", "f8-f9", "f9-fa", "fa-fb", "fb-fc", "fc-fd", "fd-fe", "fe-ff", "ff-"},
			false,
		},
		{
			"works for very large number of shards",
			args{512},
			[]string{"-0080", "0080-0100", "0100-0180", "0180-0200", "0200-0280", "0280-0300", "0300-0380", "0380-0400", "0400-0480", "0480-0500", "0500-0580", "0580-0600", "0600-0680", "0680-0700", "0700-0780", "0780-0800", "0800-0880", "0880-0900", "0900-0980", "0980-0a00", "0a00-0a80", "0a80-0b00", "0b00-0b80", "0b80-0c00", "0c00-0c80", "0c80-0d00", "0d00-0d80", "0d80-0e00", "0e00-0e80", "0e80-0f00", "0f00-0f80", "0f80-1000", "1000-1080", "1080-1100", "1100-1180", "1180-1200", "1200-1280", "1280-1300", "1300-1380", "1380-1400", "1400-1480", "1480-1500", "1500-1580", "1580-1600", "1600-1680", "1680-1700", "1700-1780", "1780-1800", "1800-1880", "1880-1900", "1900-1980", "1980-1a00", "1a00-1a80", "1a80-1b00", "1b00-1b80", "1b80-1c00", "1c00-1c80", "1c80-1d00", "1d00-1d80", "1d80-1e00", "1e00-1e80", "1e80-1f00", "1f00-1f80", "1f80-2000", "2000-2080", "2080-2100", "2100-2180", "2180-2200", "2200-2280", "2280-2300", "2300-2380", "2380-2400", "2400-2480", "2480-2500", "2500-2580", "2580-2600", "2600-2680", "2680-2700", "2700-2780", "2780-2800", "2800-2880", "2880-2900", "2900-2980", "2980-2a00", "2a00-2a80", "2a80-2b00", "2b00-2b80", "2b80-2c00", "2c00-2c80", "2c80-2d00", "2d00-2d80", "2d80-2e00", "2e00-2e80", "2e80-2f00", "2f00-2f80", "2f80-3000", "3000-3080", "3080-3100", "3100-3180", "3180-3200", "3200-3280", "3280-3300", "3300-3380", "3380-3400", "3400-3480", "3480-3500", "3500-3580", "3580-3600", "3600-3680", "3680-3700", "3700-3780", "3780-3800", "3800-3880", "3880-3900", "3900-3980", "3980-3a00", "3a00-3a80", "3a80-3b00", "3b00-3b80", "3b80-3c00", "3c00-3c80", "3c80-3d00", "3d00-3d80", "3d80-3e00", "3e00-3e80", "3e80-3f00", "3f00-3f80", "3f80-4000", "4000-4080", "4080-4100", "4100-4180", "4180-4200", "4200-4280", "4280-4300", "4300-4380", "4380-4400", "4400-4480", "4480-4500", "4500-4580", "4580-4600", "4600-4680", "4680-4700", "4700-4780", "4780-4800", "4800-4880", "4880-4900", "4900-4980", "4980-4a00", "4a00-4a80", "4a80-4b00", "4b00-4b80", "4b80-4c00", "4c00-4c80", "4c80-4d00", "4d00-4d80", "4d80-4e00", "4e00-4e80", "4e80-4f00", "4f00-4f80", "4f80-5000", "5000-5080", "5080-5100", "5100-5180", "5180-5200", "5200-5280", "5280-5300", "5300-5380", "5380-5400", "5400-5480", "5480-5500", "5500-5580", "5580-5600", "5600-5680", "5680-5700", "5700-5780", "5780-5800", "5800-5880", "5880-5900", "5900-5980", "5980-5a00", "5a00-5a80", "5a80-5b00", "5b00-5b80", "5b80-5c00", "5c00-5c80", "5c80-5d00", "5d00-5d80", "5d80-5e00", "5e00-5e80", "5e80-5f00", "5f00-5f80", "5f80-6000", "6000-6080", "6080-6100", "6100-6180", "6180-6200", "6200-6280", "6280-6300", "6300-6380", "6380-6400", "6400-6480", "6480-6500", "6500-6580", "6580-6600", "6600-6680", "6680-6700", "6700-6780", "6780-6800", "6800-6880", "6880-6900", "6900-6980", "6980-6a00", "6a00-6a80", "6a80-6b00", "6b00-6b80", "6b80-6c00", "6c00-6c80", "6c80-6d00", "6d00-6d80", "6d80-6e00", "6e00-6e80", "6e80-6f00", "6f00-6f80", "6f80-7000", "7000-7080", "7080-7100", "7100-7180", "7180-7200", "7200-7280", "7280-7300", "7300-7380", "7380-7400", "7400-7480", "7480-7500", "7500-7580", "7580-7600", "7600-7680", "7680-7700", "7700-7780", "7780-7800", "7800-7880", "7880-7900", "7900-7980", "7980-7a00", "7a00-7a80", "7a80-7b00", "7b00-7b80", "7b80-7c00", "7c00-7c80", "7c80-7d00", "7d00-7d80", "7d80-7e00", "7e00-7e80", "7e80-7f00", "7f00-7f80", "7f80-8000", "8000-8080", "8080-8100", "8100-8180", "8180-8200", "8200-8280", "8280-8300", "8300-8380", "8380-8400", "8400-8480", "8480-8500", "8500-8580", "8580-8600", "8600-8680", "8680-8700", "8700-8780", "8780-8800", "8800-8880", "8880-8900", "8900-8980", "8980-8a00", "8a00-8a80", "8a80-8b00", "8b00-8b80", "8b80-8c00", "8c00-8c80", "8c80-8d00", "8d00-8d80", "8d80-8e00", "8e00-8e80", "8e80-8f00", "8f00-8f80", "8f80-9000", "9000-9080", "9080-9100", "9100-9180", "9180-9200", "9200-9280", "9280-9300", "9300-9380", "9380-9400", "9400-9480", "9480-9500", "9500-9580", "9580-9600", "9600-9680", "9680-9700", "9700-9780", "9780-9800", "9800-9880", "9880-9900", "9900-9980", "9980-9a00", "9a00-9a80", "9a80-9b00", "9b00-9b80", "9b80-9c00", "9c00-9c80", "9c80-9d00", "9d00-9d80", "9d80-9e00", "9e00-9e80", "9e80-9f00", "9f00-9f80", "9f80-a000", "a000-a080", "a080-a100", "a100-a180", "a180-a200", "a200-a280", "a280-a300", "a300-a380", "a380-a400", "a400-a480", "a480-a500", "a500-a580", "a580-a600", "a600-a680", "a680-a700", "a700-a780", "a780-a800", "a800-a880", "a880-a900", "a900-a980", "a980-aa00", "aa00-aa80", "aa80-ab00", "ab00-ab80", "ab80-ac00", "ac00-ac80", "ac80-ad00", "ad00-ad80", "ad80-ae00", "ae00-ae80", "ae80-af00", "af00-af80", "af80-b000", "b000-b080", "b080-b100", "b100-b180", "b180-b200", "b200-b280", "b280-b300", "b300-b380", "b380-b400", "b400-b480", "b480-b500", "b500-b580", "b580-b600", "b600-b680", "b680-b700", "b700-b780", "b780-b800", "b800-b880", "b880-b900", "b900-b980", "b980-ba00", "ba00-ba80", "ba80-bb00", "bb00-bb80", "bb80-bc00", "bc00-bc80", "bc80-bd00", "bd00-bd80", "bd80-be00", "be00-be80", "be80-bf00", "bf00-bf80", "bf80-c000", "c000-c080", "c080-c100", "c100-c180", "c180-c200", "c200-c280", "c280-c300", "c300-c380", "c380-c400", "c400-c480", "c480-c500", "c500-c580", "c580-c600", "c600-c680", "c680-c700", "c700-c780", "c780-c800", "c800-c880", "c880-c900", "c900-c980", "c980-ca00", "ca00-ca80", "ca80-cb00", "cb00-cb80", "cb80-cc00", "cc00-cc80", "cc80-cd00", "cd00-cd80", "cd80-ce00", "ce00-ce80", "ce80-cf00", "cf00-cf80", "cf80-d000", "d000-d080", "d080-d100", "d100-d180", "d180-d200", "d200-d280", "d280-d300", "d300-d380", "d380-d400", "d400-d480", "d480-d500", "d500-d580", "d580-d600", "d600-d680", "d680-d700", "d700-d780", "d780-d800", "d800-d880", "d880-d900", "d900-d980", "d980-da00", "da00-da80", "da80-db00", "db00-db80", "db80-dc00", "dc00-dc80", "dc80-dd00", "dd00-dd80", "dd80-de00", "de00-de80", "de80-df00", "df00-df80", "df80-e000", "e000-e080", "e080-e100", "e100-e180", "e180-e200", "e200-e280", "e280-e300", "e300-e380", "e380-e400", "e400-e480", "e480-e500", "e500-e580", "e580-e600", "e600-e680", "e680-e700", "e700-e780", "e780-e800", "e800-e880", "e880-e900", "e900-e980", "e980-ea00", "ea00-ea80", "ea80-eb00", "eb00-eb80", "eb80-ec00", "ec00-ec80", "ec80-ed00", "ed00-ed80", "ed80-ee00", "ee00-ee80", "ee80-ef00", "ef00-ef80", "ef80-f000", "f000-f080", "f080-f100", "f100-f180", "f180-f200", "f200-f280", "f280-f300", "f300-f380", "f380-f400", "f400-f480", "f480-f500", "f500-f580", "f580-f600", "f600-f680", "f680-f700", "f700-f780", "f780-f800", "f800-f880", "f880-f900", "f900-f980", "f980-fa00", "fa00-fa80", "fa80-fb00", "fb00-fb80", "fb80-fc00", "fc00-fc80", "fc80-fd00", "fd00-fd80", "fd80-fe00", "fe00-fe80", "fe80-ff00", "ff00-ff80", "ff80-"},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GenerateShardRanges(tt.args.shards, 0)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, got, tt.want)
		})
	}
}

func TestGenerateShardRangesForManyShards(t *testing.T) {
	for i := 1; i <= 1024; i++ {
		t.Run(fmt.Sprintf("shards=%d", i), func(t *testing.T) {
			ranges, err := GenerateShardRanges(i, 0)

			require.NoError(t, err)
			require.Len(t, ranges, i)

			// verify that the shards are contiguous and non-overlapping
			for j := 1; j < len(ranges); j++ {
				prevEnd := ranges[j-1][strings.Index(ranges[j-1], "-")+1:]
				currStart := ranges[j][:strings.Index(ranges[j], "-")]

				require.Equal(t, prevEnd, currStart, "Shards %d and %d are not contiguous: %s != %s", j-1, j, prevEnd, currStart)
			}

			// verify that the shards cover the full keyspace
			require.True(t, strings.HasPrefix(ranges[0], "-"), "First shard does not start with a hyphen: %s", ranges[0])
			require.True(t, strings.HasSuffix(ranges[len(ranges)-1], "-"), "Last shard does not end with a hyphen: %s", ranges[len(ranges)-1])
		})
	}
}

func TestGenerateShardRangesWithHexCharacterCount(t *testing.T) {
	{
		ranges, err := GenerateShardRanges(7, 1)

		require.NoError(t, err)

		require.EqualValues(t, 7, len(ranges))
		require.EqualValues(t, []string{"-2", "2-4", "4-6", "6-9", "9-b", "b-d", "d-"}, ranges)
	}

	{
		ranges, err := GenerateShardRanges(7, 2)

		require.NoError(t, err)

		require.EqualValues(t, 7, len(ranges))
		require.EqualValues(t, []string{"-24", "24-49", "49-6d", "6d-92", "92-b6", "b6-db", "db-"}, ranges)
	}

	{
		ranges, err := GenerateShardRanges(7, 3)

		require.NoError(t, err)

		require.EqualValues(t, 7, len(ranges))
		require.EqualValues(t, []string{"-249", "249-492", "492-6db", "6db-924", "924-b6d", "b6d-db6", "db6-"}, ranges)
	}

	{
		ranges, err := GenerateShardRanges(7, 4)

		require.NoError(t, err)

		require.EqualValues(t, 7, len(ranges))
		require.EqualValues(t, []string{"-2492", "2492-4924", "4924-6db6", "6db6-9249", "9249-b6db", "b6db-db6d", "db6d-"}, ranges)
	}

	{
		ranges, err := GenerateShardRanges(8, 4)

		require.NoError(t, err)

		require.EqualValues(t, 8, len(ranges))
		require.EqualValues(t, []string{"-2000", "2000-4000", "4000-6000", "6000-8000", "8000-a000", "a000-c000", "c000-e000", "e000-"}, ranges)
	}

	{
		_, err := GenerateShardRanges(32, 1)

		require.Error(t, err)
		require.ErrorContains(t, err, "the given number of shards (32) is too high for the given number of characters to use (1)")
	}
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

func TestKeyRangeIsPartial(t *testing.T) {
	testCases := []struct {
		name     string
		keyRange *topodatapb.KeyRange
		want     bool
	}{
		{"nil key range", nil, false},
		{"empty start and end", &topodatapb.KeyRange{}, false},
		{"empty end", &topodatapb.KeyRange{Start: []byte("12")}, true},
		{"empty start", &topodatapb.KeyRange{End: []byte("13")}, true},
		{"non-empty start and end", &topodatapb.KeyRange{Start: []byte("12"), End: []byte("13")}, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			isPartial := KeyRangeIsPartial(tc.keyRange)
			assert.Equal(t, tc.want, isPartial)
		})
	}
}
