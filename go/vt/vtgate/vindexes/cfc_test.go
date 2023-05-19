/*
Copyright 2021 The Vitess Authors.

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

package vindexes

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func cfcCreateVindexTestCase(
	testName string,
	vindexParams map[string]string,
	expectErr error,
	expectUnknownParams []string,
) createVindexTestCase {
	return createVindexTestCase{
		testName: testName,

		vindexType:   "cfc",
		vindexName:   "cfc",
		vindexParams: vindexParams,

		expectCost:          1,
		expectErr:           expectErr,
		expectIsUnique:      true,
		expectNeedsVCursor:  false,
		expectString:        "cfc",
		expectUnknownParams: expectUnknownParams,
	}
}

func TestCFCCreateVindex(t *testing.T) {
	cases := []createVindexTestCase{
		cfcCreateVindexTestCase(
			"no params",
			nil,
			nil,
			nil,
		),
		cfcCreateVindexTestCase(
			"no hash",
			map[string]string{},
			nil,
			nil,
		),
		cfcCreateVindexTestCase(
			"no hash with offsets",
			map[string]string{"offsets": "[1,2]"},
			nil,
			nil,
		),
		cfcCreateVindexTestCase(
			"hash with no offsets",
			map[string]string{"hash": "md5"},
			vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "CFC vindex requires offsets when hash is defined"),
			nil,
		),
		cfcCreateVindexTestCase(
			"invalid offsets 10,12",
			map[string]string{"hash": "md5", "offsets": "10,12"},
			vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "invalid offsets 10,12 to CFC vindex cfc. expected sorted positive ints in brackets"),
			nil,
		),
		cfcCreateVindexTestCase(
			"invalid offsets xxx",
			map[string]string{"hash": "md5", "offsets": "xxx"},
			vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "invalid offsets xxx to CFC vindex cfc. expected sorted positive ints in brackets"),
			nil,
		),
		cfcCreateVindexTestCase(
			"empty offsets",
			map[string]string{"hash": "md5", "offsets": "[]"},
			vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "invalid offsets [] to CFC vindex cfc. expected sorted positive ints in brackets"),
			nil,
		),
		cfcCreateVindexTestCase(
			"unsorted offsets",
			map[string]string{"hash": "md5", "offsets": "[10,3]"},
			vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "invalid offsets [10,3] to CFC vindex cfc. expected sorted positive ints in brackets"),
			nil,
		),
		cfcCreateVindexTestCase(
			"negative offsets",
			map[string]string{"hash": "md5", "offsets": "[-1,3]"},
			vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "invalid offsets [-1,3] to CFC vindex cfc. expected sorted positive ints in brackets"),
			nil,
		),
		cfcCreateVindexTestCase(
			"duplicated offsets",
			map[string]string{"hash": "md5", "offsets": "[4,4,6]"},
			vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "invalid offsets [4,4,6] to CFC vindex cfc. expected sorted positive ints in brackets"),
			nil,
		),
		cfcCreateVindexTestCase(
			"unknown params",
			map[string]string{"hash": "md5", "offsets": "[3, 7]", "hello": "world"},
			nil,
			[]string{"hello"},
		),
	}

	testCreateVindexes(t, cases)
}

func TestCFCCreateVindexOptions(t *testing.T) {
	vdx, err := CreateVindex(
		"cfc",
		"normal",
		map[string]string{
			"hash":    "md5",
			"offsets": "[3, 7]",
		},
	)
	require.NotNil(t, vdx)
	require.Nil(t, err)
	unknownParams := vdx.(ParamValidating).UnknownParams()
	require.Empty(t, unknownParams)
	require.EqualValues(t, vdx.(*CFC).offsets, []int{3, 7})
}

func makeCFC(t *testing.T, params map[string]string) *CFC {
	vind, err := newCFC("cfc", params)
	require.NoError(t, err)
	cfc, ok := vind.(*CFC)
	require.True(t, ok)
	return cfc
}

func expectedHash(id [][]byte) (res []byte) {
	for _, c := range id {
		res = append(res, md5hash(c)...)
	}
	return res
}

func flattenKey(id [][]byte) (res []byte) {
	for _, c := range id {
		res = append(res, c...)
	}
	return res
}

func TestCFCComputeKsidNoHash(t *testing.T) {
	cfc := makeCFC(t, nil)
	id := []byte{3, 6, 20, 7, 60, 1}
	ksid, err := cfc.computeKsid(id, true)
	assert.NoError(t, err)
	assert.EqualValues(t, id, ksid)
}

func TestCFCComputeKsid(t *testing.T) {
	cfc := makeCFC(t, map[string]string{"hash": "md5", "offsets": "[3,5]"})

	cases := []struct {
		testName string
		id       [][]byte
		prefix   bool
		expected []byte
		err      error
	}{
		{
			testName: "nonprefix too short",
			id:       [][]byte{{3, 4, 5}},
			prefix:   false,
			expected: nil,
			err:      vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "insufficient size for cfc vindex cfc. need 5, got 3"),
		},
		{
			testName: "prefix ok",
			id:       [][]byte{{3, 4, 5}},
			prefix:   true,
			expected: expectedHash([][]byte{{3, 4, 5}}),
			err:      nil,
		},
		{
			testName: "misaligned prefix",
			id:       [][]byte{{3, 4, 5}, {1}},
			prefix:   true,
			// use the first component that's availabe
			expected: expectedHash([][]byte{{3, 4, 5}}),
			err:      nil,
		},
		{
			testName: "misaligned prefix",
			id:       [][]byte{{3, 4}},
			prefix:   true,
			// use the first component that's availabe
			expected: nil,
			err:      nil,
		},
		{
			testName: "normal",
			id:       [][]byte{{12, 234, 2}, {7, 1}, {6}},
			prefix:   false,
			expected: expectedHash([][]byte{{12, 234, 2}, {7, 1}, {6}}),
			err:      nil,
		},
		{
			testName: "normal",
			id:       [][]byte{{5, 21, 124}, {75, 5}},
			prefix:   true,
			expected: expectedHash([][]byte{{5, 21, 124}, {75, 5}}),
			err:      nil,
		},
		{
			testName: "long key",
			id:       [][]byte{{5, 21, 124}, {75, 5}, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}},
			prefix:   true,
			expected: expectedHash([][]byte{{5, 21, 124}, {75, 5}, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}}),
			err:      nil,
		},
		{
			testName: "empty key",
			id:       [][]byte{},
			prefix:   true,
			expected: nil,
			err:      nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.testName, func(t *testing.T) {
			fid := flattenKey(tc.id)
			ksid, err := cfc.computeKsid(fid, tc.prefix)
			assertEqualVtError(t, tc.err, err)
			if err == nil {
				assert.EqualValues(t, tc.expected, ksid)
			}
		})
	}
}

func TestCFCComputeKsidXxhash(t *testing.T) {
	cfc := makeCFC(t, map[string]string{"hash": "xxhash64", "offsets": "[3,5]"})

	expectedHashXX := func(ids [][]byte) (res []byte) {
		for _, c := range ids {
			res = append(res, xxhash64(c)...)
		}
		return res
	}
	cases := []struct {
		testName string
		id       [][]byte
		prefix   bool
		expected []byte
		err      error
	}{
		{
			testName: "nonprefix too short",
			id:       [][]byte{{3, 4, 5}},
			prefix:   false,
			expected: nil,
			err:      vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "insufficient size for cfc vindex cfc. need 5, got 3"),
		},
		{
			testName: "prefix ok",
			id:       [][]byte{{3, 4, 5}},
			prefix:   true,
			expected: expectedHashXX([][]byte{{3, 4, 5}}),
			err:      nil,
		},
		{
			testName: "misaligned prefix",
			id:       [][]byte{{3, 4, 5}, {1}},
			prefix:   true,
			// use the first component that's availabe
			expected: expectedHashXX([][]byte{{3, 4, 5}}),
			err:      nil,
		},
		{
			testName: "misaligned prefix",
			id:       [][]byte{{3, 4}},
			prefix:   true,
			// use the first component that's availabe
			expected: nil,
			err:      nil,
		},
		{
			testName: "normal",
			id:       [][]byte{{12, 234, 2}, {7, 1}, {6}},
			prefix:   false,
			expected: expectedHashXX([][]byte{{12, 234, 2}, {7, 1}, {6}}),
			err:      nil,
		},
		{
			testName: "long key",
			id:       [][]byte{{5, 21, 124}, {75, 5}, {1, 2, 3, 4, 5, 6}},
			prefix:   true,
			expected: expectedHashXX([][]byte{{5, 21, 124}, {75, 5}, {1, 2, 3, 4, 5, 6}}),
			err:      nil,
		},
		{
			testName: "long key",
			id:       [][]byte{},
			prefix:   true,
			expected: nil,
			err:      nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.testName, func(t *testing.T) {
			fid := flattenKey(tc.id)
			ksid, err := cfc.computeKsid(fid, tc.prefix)
			assertEqualVtError(t, tc.err, err)
			if err == nil {
				assert.EqualValues(t, tc.expected, ksid)
			}
		})
	}

}

func TestCFCVerifyNoHash(t *testing.T) {
	cfc := makeCFC(t, nil)
	id := []byte{3, 10, 7, 200}
	out, err := cfc.Verify(context.Background(), nil, []sqltypes.Value{sqltypes.NewVarBinary(string(id))}, [][]byte{id})
	assert.NoError(t, err)
	assert.EqualValues(t, []bool{true}, out)

	out, err = cfc.Verify(context.Background(), nil, []sqltypes.Value{sqltypes.NewVarBinary("foobar")}, [][]byte{id})
	assert.NoError(t, err)
	assert.EqualValues(t, []bool{false}, out)
	pcfc := cfc.PrefixVindex()
	out, err = pcfc.Verify(context.Background(), nil, []sqltypes.Value{sqltypes.NewVarBinary("foobar")}, [][]byte{id})
	assert.NoError(t, err)
	assert.EqualValues(t, []bool{false}, out)
}

func TestCFCVerifyWithHash(t *testing.T) {
	cfc := makeCFC(t, map[string]string{"hash": "md5", "offsets": "[3,5]"})
	id := [][]byte{
		{1, 234, 3}, {12, 32}, {7, 9},
	}
	out, err := cfc.Verify(context.Background(), nil, []sqltypes.Value{sqltypes.NewVarBinary(string(flattenKey(id)))}, [][]byte{expectedHash(id)})
	assert.NoError(t, err)
	assert.EqualValues(t, []bool{true}, out)

	_, err = cfc.Verify(context.Background(), nil, []sqltypes.Value{sqltypes.NewVarBinary("foo")}, [][]byte{expectedHash(id)})
	assert.Error(t, err)

	pcfc := cfc.PrefixVindex()
	out, err = pcfc.Verify(context.Background(), nil, []sqltypes.Value{sqltypes.NewVarBinary(string(flattenKey(id)))}, [][]byte{expectedHash(id)})
	assert.NoError(t, err)
	assert.EqualValues(t, []bool{true}, out)

}

func TestCFCMap(t *testing.T) {
	cfc := makeCFC(t, map[string]string{"hash": "md5", "offsets": "[3,5]"})
	_, err := cfc.Map(context.Background(), nil, []sqltypes.Value{sqltypes.NewVarBinary("abc")})
	assert.EqualError(t, err, "insufficient size for cfc vindex cfc. need 5, got 3")

	dests, err := cfc.Map(context.Background(), nil, []sqltypes.Value{sqltypes.NewVarBinary("12345567")})
	require.NoError(t, err)
	ksid, ok := dests[0].(key.DestinationKeyspaceID)
	require.True(t, ok)
	out, err := cfc.Verify(context.Background(), nil, []sqltypes.Value{sqltypes.NewVarBinary("12345567")}, [][]byte{ksid})
	assert.NoError(t, err)
	assert.EqualValues(t, []bool{true}, out)
}

func TestCFCBuildPrefix(t *testing.T) {
	cfc := makeCFC(t, nil)
	prefixcfc := cfc.PrefixVindex()
	assert.Equal(t, "cfc", prefixcfc.String())
	assert.False(t, prefixcfc.IsUnique())
	assert.False(t, prefixcfc.NeedsVCursor())
	assert.Equal(t, 2, prefixcfc.Cost())

	cfc = makeCFC(t, map[string]string{"offsets": "[1,2,3]", "hash": "xxhash64"})
	prefixcfc = cfc.PrefixVindex()
	assert.Equal(t, "cfc", prefixcfc.String())
	assert.False(t, prefixcfc.IsUnique())
	assert.False(t, prefixcfc.NeedsVCursor())
	assert.Equal(t, 3, prefixcfc.Cost())
}

func TestCFCPrefixMap(t *testing.T) {
	cfc := makeCFC(t, map[string]string{"hash": "md5", "offsets": "[3,5]"})
	prefixcfc := cfc.PrefixVindex()

	cases := []struct {
		testName string
		id       string
		dest     key.Destination
	}{
		{
			testName: "literal regular",
			id:       "abcdef",
			dest:     NewKeyRangeFromPrefix(expectedHash([][]byte{{'a', 'b', 'c'}, {'d', 'e'}, {'f'}})),
		},
		{
			testName: "literal use first component",
			id:       "abcd",
			dest:     NewKeyRangeFromPrefix(expectedHash([][]byte{{'a', 'b', 'c'}})),
		},
		{
			testName: "literal prefix too short",
			id:       "ab",
			dest:     key.DestinationAllShards{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.testName, func(t *testing.T) {
			dests, err := prefixcfc.Map(context.Background(), nil, []sqltypes.Value{sqltypes.NewVarBinary(tc.id)})
			require.NoError(t, err)
			assert.EqualValues(t, tc.dest, dests[0])
		})
	}
}

func TestCFCPrefixQueryMapNoHash(t *testing.T) {
	cfc := makeCFC(t, nil)
	prefixcfc := cfc.PrefixVindex()

	expected := []struct {
		start, end []byte
	}{
		{[]byte{3, 123, 255}, []byte{3, 124, 0}},
		{[]byte{3, 123, 255, 6, 7}, []byte{3, 123, 255, 6, 8}},
		{[]byte{255, 255, 255}, nil},
	}
	var ids []sqltypes.Value
	for _, exp := range expected {
		ids = append(ids, sqltypes.NewVarBinary(string(exp.start)+"%"))
	}
	dests, err := prefixcfc.Map(context.Background(), nil, ids)
	require.NoError(t, err)

	for i, dest := range dests {
		kr, ok := dest.(key.DestinationKeyRange)
		require.True(t, ok)
		assert.EqualValues(t, expected[i].start, kr.KeyRange.Start)
		assert.EqualValues(t, expected[i].end, kr.KeyRange.End)
	}
}

func TestCFCFindPrefixEscape(t *testing.T) {
	cases := []struct {
		str, prefix string
	}{
		{
			str:    "ab%",
			prefix: "ab",
		},
		{
			str:    "abc",
			prefix: "abc",
		},
		{
			str:    "a%%",
			prefix: "a",
		},
		{
			str:    "%ab",
			prefix: "",
		},
		{
			str:    `\%a%`,
			prefix: `%a`,
		},
		{
			str:    `\%%`,
			prefix: `%`,
		},
		{
			str:    `\%`,
			prefix: `%`,
		},
		{
			str:    `\\%a`,
			prefix: `\`,
		},
		{
			str:    `a\\%a`,
			prefix: `a\`,
		},
		{
			str:    `\\\%`,
			prefix: `\%`,
		},
		{
			str:    `\\\%a%`,
			prefix: `\%a`,
		},
		{
			str:    `\_\\\%a_`,
			prefix: `_\%a`,
		},
		{
			str:    `_%a%`,
			prefix: ``,
		},
		{
			str:    `\i\j\k`,
			prefix: `ijk`,
		},
		{
			str:    `\0\'\"\b\n\r\t\Z\\`,
			prefix: "\x00'\"\b\n\r\t\x1A\\",
		},
		{
			str:    `\\0\\'\\"\\b\\n\\r\\t\\Z`,
			prefix: `\0\'\"\b\n\r\t\Z`,
		},
		{
			str:    `\`,
			prefix: `\`,
		},
	}

	for _, tc := range cases {
		assert.EqualValues(t, tc.prefix, string(findPrefix([]byte(tc.str))))
	}
}

func TestDestinationKeyRangeFromPrefix(t *testing.T) {
	testCases := []struct {
		start []byte
		dest  key.Destination
	}{
		{
			start: []byte{3, 123, 255},
			dest: key.DestinationKeyRange{
				KeyRange: &topodatapb.KeyRange{
					Start: []byte{3, 123, 255},
					End:   []byte{3, 124, 0},
				},
			},
		},
		{
			start: []byte{3, 123, 255, 6, 7},
			dest: key.DestinationKeyRange{
				KeyRange: &topodatapb.KeyRange{
					Start: []byte{3, 123, 255, 6, 7},
					End:   []byte{3, 123, 255, 6, 8},
				},
			},
		},
		{
			start: []byte{255, 255, 255},
			dest: key.DestinationKeyRange{
				KeyRange: &topodatapb.KeyRange{
					Start: []byte{255, 255, 255},
					End:   nil,
				},
			},
		},
		{
			start: nil,
			dest:  key.DestinationAllShards{},
		},
		{
			start: []byte{},
			dest:  key.DestinationAllShards{},
		},
	}

	for _, tc := range testCases {
		dest := NewKeyRangeFromPrefix(tc.start)
		assert.EqualValues(t, tc.dest, dest)
	}

	t.Run("add one to bytes", func(t *testing.T) {
		cases := []struct {
			testName        string
			input, expected []byte
		}{
			{
				testName: "regular one byte",
				input:    []byte{213},
				expected: []byte{214},
			},
			{
				testName: "regular two byte",
				input:    []byte{213, 7},
				expected: []byte{213, 8},
			},
			{
				testName: "carry one",
				input:    []byte{213, 255},
				expected: []byte{214, 0},
			},
			{
				testName: "carry multi",
				input:    []byte{1, 255, 255},
				expected: []byte{2, 0, 0},
			},
			{
				testName: "overflow one byte",
				input:    []byte{255},
				expected: nil,
			},
			{
				testName: "overflow multi",
				input:    []byte{255, 255},
				expected: nil,
			},
		}

		for _, tc := range cases {
			t.Run(tc.testName, func(t *testing.T) {
				assert.EqualValues(t, tc.expected, addOne(tc.input))
			})
		}
	})
}

func TestCFCHashFunction(t *testing.T) {
	cases := []struct {
		src               string
		outMD5, outXXHash int
	}{
		{"asdf", 4, 4},
		{"abcdefgh", 8, 8},
		{"abcdefghijkl", 12, 8},
		{"abcdefghijklmnop", 16, 8},
		{"abcdefghijklmnopqrst", 16, 8},
	}
	for _, c := range cases {
		assert.Equal(t, c.outMD5, len(md5hash([]byte(c.src))))
		assert.Equal(t, c.outXXHash, len(xxhash64([]byte(c.src))))
	}
}

// TestCFCCache tests for CFC vindex, cache size can be calculated.
func TestCFCCache(t *testing.T) {
	cfc := makeCFC(t, map[string]string{"hash": "md5", "offsets": "[3,5]"})
	// should be able to return.
	_ = cfc.CachedSize(true)
}
