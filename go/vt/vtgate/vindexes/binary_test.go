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

package vindexes

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

var binOnlyVindex SingleColumn

func init() {
	vindex, err := CreateVindex("binary", "binary_varchar", nil)
	if err != nil {
		panic(err)
	}
	binOnlyVindex = vindex.(SingleColumn)
}

func binaryCreateVindexTestCase(
	testName string,
	vindexParams map[string]string,
	expectErr error,
	expectUnknownParams []string,
) createVindexTestCase {
	return createVindexTestCase{
		testName: testName,

		vindexType:   "binary",
		vindexName:   "binary",
		vindexParams: vindexParams,

		expectCost:          0,
		expectErr:           expectErr,
		expectIsUnique:      true,
		expectNeedsVCursor:  false,
		expectString:        "binary",
		expectUnknownParams: expectUnknownParams,
	}
}

func TestBinaryCreateVindex(t *testing.T) {
	cases := []createVindexTestCase{
		binaryCreateVindexTestCase(
			"no params",
			nil,
			nil,
			nil,
		),
		binaryCreateVindexTestCase(
			"empty params",
			map[string]string{},
			nil,
			nil,
		),
		binaryCreateVindexTestCase(
			"unknown params",
			map[string]string{"hello": "world"},
			nil,
			[]string{"hello"},
		),
	}

	testCreateVindexes(t, cases)
}

func TestBinaryMap(t *testing.T) {
	tcases := []struct {
		in  sqltypes.Value
		out []byte
	}{{
		in:  sqltypes.NewVarChar("test1"),
		out: []byte("test1"),
	}, {
		in:  sqltypes.NULL,
		out: []byte(nil),
	}, {
		in:  sqltypes.NewVarChar("test2"),
		out: []byte("test2"),
	}}
	for _, tcase := range tcases {
		got, err := binOnlyVindex.Map(t.Context(), nil, []sqltypes.Value{tcase.in})
		assert.NoError(t, err)
		out := []byte(got[0].(key.DestinationKeyspaceID))
		assert.Equalf(t, tcase.out, out, "Map(%#v)", tcase.in)
	}
}

func TestBinaryVerify(t *testing.T) {
	hexValStr := "8a1e"
	hexValStrSQL := fmt.Sprintf("x'%s'", hexValStr)
	hexNumStrSQL := "0x" + hexValStr
	hexBytes, _ := hex.DecodeString(hexValStr)
	ids := []sqltypes.Value{sqltypes.NewVarBinary("1"), sqltypes.NewVarBinary("2"), sqltypes.NewHexVal([]byte(hexValStrSQL)), sqltypes.NewHexNum([]byte(hexNumStrSQL))}
	ksids := [][]byte{[]byte("1"), []byte("1"), hexBytes, hexBytes}
	got, err := binOnlyVindex.Verify(t.Context(), nil, ids, ksids)
	require.NoError(t, err)
	assert.Equal(t, []bool{true, false, true, true}, got, "binary.Verify")
}

func TestBinaryReverseMap(t *testing.T) {
	got, err := binOnlyVindex.(Reversible).ReverseMap(nil, [][]byte{[]byte("\x00\x00\x00\x00\x00\x00\x00\x01")})
	require.NoError(t, err)
	assert.Equal(t, []sqltypes.Value{sqltypes.NewVarBinary("\x00\x00\x00\x00\x00\x00\x00\x01")}, got, "ReverseMap()")

	// Negative Test
	_, err = binOnlyVindex.(Reversible).ReverseMap(nil, [][]byte{[]byte(nil)})
	assert.EqualError(t, err, "Binary.ReverseMap: keyspaceId is nil", "ReverseMap()")
}

// TestBinaryRangeMap takes start and env values,
// and checks against a destination keyrange.
func TestBinaryRangeMap(t *testing.T) {
	startInterval := "0x01"
	endInterval := "0x10"

	got, err := binOnlyVindex.(Sequential).RangeMap(t.Context(), nil, sqltypes.NewHexNum([]byte(startInterval)),
		sqltypes.NewHexNum([]byte(endInterval)))
	require.NoError(t, err)
	want := "DestinationKeyRange(01-10)"
	assert.Equal(t, want, got[0].String())
}
