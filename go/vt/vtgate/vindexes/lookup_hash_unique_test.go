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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func lookupHashUniqueCreateVindexTestCase(
	testName string,
	vindexParams map[string]string,
	expectErr error,
	expectUnknownParams []string,
) createVindexTestCase {
	return createVindexTestCase{
		testName: testName,

		vindexType:   "lookup_hash_unique",
		vindexName:   "lookup_hash_unique",
		vindexParams: vindexParams,

		expectCost:          10,
		expectErr:           expectErr,
		expectIsUnique:      true,
		expectNeedsVCursor:  true,
		expectString:        "lookup_hash_unique",
		expectUnknownParams: expectUnknownParams,
	}
}

func TestLookupHashUniqueCreateVindex(t *testing.T) {
	testLookupCreateVindexCommonCases(t, lookupHashUniqueCreateVindexTestCase)
}

func TestLookupHashUniqueNew(t *testing.T) {
	l := createLookup(t, "lookup_hash_unique", false /* writeOnly */)
	assert.False(t, l.(*LookupHashUnique).writeOnly, "Create(lookup, false)")

	vindex, err := CreateVindex("lookup_hash_unique", "lookup_hash_unique", map[string]string{
		"table":      "t",
		"from":       "fromc",
		"to":         "toc",
		"write_only": "true",
	})
	unknownParams := vindex.(ParamValidating).UnknownParams()
	require.Empty(t, unknownParams)
	require.NoError(t, err)

	l = vindex.(SingleColumn)
	assert.True(t, l.(*LookupHashUnique).writeOnly, "Create(lookup, true)")

	vdx, err := CreateVindex("lookup_hash_unique", "lookup_hash_unique", map[string]string{
		"table":      "t",
		"from":       "fromc",
		"to":         "toc",
		"write_only": "invalid",
	})
	assert.EqualError(t, err, "write_only value must be 'true' or 'false': 'invalid'", "Create(bad_scatter)")
	if err == nil {
		unknownParams = vdx.(ParamValidating).UnknownParams()
		require.Empty(t, unknownParams)
	}
}

func TestLookupHashUniqueMap(t *testing.T) {
	lhu := createLookup(t, "lookup_hash_unique", false /* writeOnly */)
	vc := &vcursor{numRows: 1}

	got, err := lhu.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	require.NoError(t, err)
	want := []key.ShardDestination{
		key.DestinationKeyspaceID([]byte("\x16k@\xb4J\xbaK\xd6")),
		key.DestinationNone{},
	}
	assert.Equal(t, want, got, "Map()")

	vc.numRows = 0
	got, err = lhu.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	require.NoError(t, err)
	want = []key.ShardDestination{
		key.DestinationNone{},
		key.DestinationNone{},
	}
	assert.Equal(t, want, got, "Map()")

	vc.numRows = 2
	_, err = lhu.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	assert.EqualError(t, err, "LookupHash.Map: unexpected multiple results from vindex t: INT64(1)", "lhu(query fail)")

	// Test conversion fail.
	vc.result = sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("b|a", "bigint|varbinary"),
		"1|notint",
	)
	got, err = lhu.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1)})
	require.NoError(t, err)
	want = []key.ShardDestination{
		key.DestinationNone{},
	}
	assert.Equal(t, want, got, "Map()")

	// Test query fail.
	vc.mustFail = true
	_, err = lhu.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1)})
	assert.EqualError(t, err, "lookup.Map: execute failed", "lhu(query fail)")
	vc.mustFail = false
}

func TestLookupHashUniqueMapWriteOnly(t *testing.T) {
	lhu := createLookup(t, "lookup_hash_unique", true)
	vc := &vcursor{numRows: 0}

	got, err := lhu.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	require.NoError(t, err)
	want := []key.ShardDestination{
		key.DestinationKeyRange{
			KeyRange: &topodatapb.KeyRange{},
		},
		key.DestinationKeyRange{
			KeyRange: &topodatapb.KeyRange{},
		},
	}
	assert.Equal(t, want, got, "Map()")
}

func TestLookupHashUniqueVerify(t *testing.T) {
	lhu := createLookup(t, "lookup_hash_unique", false /* writeOnly */)
	vc := &vcursor{numRows: 1}

	// The check doesn't actually happen. But we give correct values
	// to avoid confusion.
	got, err := lhu.Verify(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6"), []byte("\x06\xe7\xea\"Βp\x8f")})
	require.NoError(t, err)
	assert.Equal(t, []bool{true, true}, got, "lhu.Verify(match)")

	vc.numRows = 0
	got, err = lhu.Verify(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")})
	require.NoError(t, err)
	assert.Equal(t, []bool{false}, got, "lhu.Verify(mismatch)")

	_, err = lhu.Verify(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("bogus")})
	assert.EqualError(t, err, "lookup.Verify.vunhash: invalid keyspace id: 626f677573", "lhu.Verify(bogus)")
}

func TestLookupHashUniqueVerifyWriteOnly(t *testing.T) {
	lhu := createLookup(t, "lookup_hash_unique", true)
	vc := &vcursor{numRows: 0}

	got, err := lhu.Verify(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("test")})
	require.NoError(t, err)
	assert.Equal(t, []bool{true}, got, "lhu.Verify")
	assert.Empty(t, vc.queries, "vc.queries length")
}

func TestLookupHashUniqueCreate(t *testing.T) {
	lhu := createLookup(t, "lookup_hash_unique", false /* writeOnly */)
	vc := &vcursor{}

	err := lhu.(Lookup).Create(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")}, false /* ignoreMode */)
	require.NoError(t, err)
	assert.Len(t, vc.queries, 1, "vc.queries length")

	err = lhu.(Lookup).Create(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}}, [][]byte{[]byte("bogus")}, false /* ignoreMode */)
	assert.EqualError(t, err, "lookup.Create.vunhash: invalid keyspace id: 626f677573", "lhu.Create(bogus)")
}

func TestLookupHashUniqueDelete(t *testing.T) {
	lhu := createLookup(t, "lookup_hash_unique", false /* writeOnly */)
	vc := &vcursor{}

	err := lhu.(Lookup).Delete(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}}, []byte("\x16k@\xb4J\xbaK\xd6"))
	require.NoError(t, err)
	assert.Len(t, vc.queries, 1, "vc.queries length")

	err = lhu.(Lookup).Delete(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}}, []byte("bogus"))
	assert.EqualError(t, err, "lookup.Delete.vunhash: invalid keyspace id: 626f677573", "lhu.Delete(bogus)")
}

func TestLookupHashUniqueUpdate(t *testing.T) {
	lhu := createLookup(t, "lookup_hash_unique", false /* writeOnly */)
	vc := &vcursor{}

	err := lhu.(Lookup).Update(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1)}, []byte("\x16k@\xb4J\xbaK\xd6"), []sqltypes.Value{sqltypes.NewInt64(2)})
	require.NoError(t, err)
	assert.Len(t, vc.queries, 2, "vc.queries length")
}
