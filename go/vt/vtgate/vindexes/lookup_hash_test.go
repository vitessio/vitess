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

func lookupHashCreateVindexTestCase(
	testName string,
	vindexParams map[string]string,
	expectErr error,
	expectUnknownParams []string,
) createVindexTestCase {
	return createVindexTestCase{
		testName: testName,

		vindexType:   "lookup_hash",
		vindexName:   "lookup_hash",
		vindexParams: vindexParams,

		expectCost:          20,
		expectErr:           expectErr,
		expectIsUnique:      false,
		expectNeedsVCursor:  true,
		expectString:        "lookup_hash",
		expectUnknownParams: expectUnknownParams,
	}
}

func TestLookupHashCreateVindex(t *testing.T) {
	testLookupCreateVindexCommonCases(t, lookupHashCreateVindexTestCase)
}

func TestLookupHashNew(t *testing.T) {
	l := createLookup(t, "lookup_hash", false /* writeOnly */)
	assert.False(t, l.(*LookupHash).writeOnly, "Create(lookup, false)")

	l = createLookup(t, "lookup_hash", true)
	assert.True(t, l.(*LookupHash).writeOnly, "Create(lookup, true)")

	vdx, err := CreateVindex("lookup_hash", "lookup_hash", map[string]string{
		"table":      "t",
		"from":       "fromc",
		"to":         "toc",
		"write_only": "invalid",
	})
	assert.EqualError(t, err, "write_only value must be 'true' or 'false': 'invalid'", "Create(bad_scatter)")
	if err == nil {
		unknownParams := vdx.(ParamValidating).UnknownParams()
		require.Empty(t, unknownParams)
	}
}

func TestLookupHashMap(t *testing.T) {
	lookuphash := createLookup(t, "lookup_hash", false /* writeOnly */)
	vc := &vcursor{numRows: 2}

	got, err := lookuphash.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	require.NoError(t, err)
	want := []key.ShardDestination{
		key.DestinationKeyspaceIDs([][]byte{
			[]byte("\x16k@\xb4J\xbaK\xd6"),
			[]byte("\x06\xe7\xea\"Βp\x8f"),
		}),
		key.DestinationKeyspaceIDs([][]byte{
			[]byte("\x16k@\xb4J\xbaK\xd6"),
			[]byte("\x06\xe7\xea\"Βp\x8f"),
		}),
	}
	assert.Equal(t, want, got, "Map()")

	// Test conversion fail.
	vc.result = sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("b|a", "int64|varbinary"),
		"1|notint",
	)
	got, err = lookuphash.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1)})
	require.NoError(t, err)
	want = []key.ShardDestination{key.DestinationKeyspaceIDs([][]byte{})}
	assert.Equal(t, want, got, "Map()")

	// Test query fail.
	vc.mustFail = true
	_, err = lookuphash.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1)})
	assert.EqualError(t, err, "lookup.Map: execute failed", "lookuphash(query fail)")
	vc.mustFail = false
}

func TestLookupHashMapAbsent(t *testing.T) {
	lookuphash := createLookup(t, "lookup_hash", false /* writeOnly */)
	vc := &vcursor{numRows: 0}

	got, err := lookuphash.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	require.NoError(t, err)
	want := []key.ShardDestination{
		key.DestinationNone{},
		key.DestinationNone{},
	}
	assert.Equal(t, want, got, "Map()")

	// writeOnly true should return full keyranges.
	lookuphash = createLookup(t, "lookup_hash", true)
	got, err = lookuphash.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	require.NoError(t, err)
	want = []key.ShardDestination{
		key.DestinationKeyRange{
			KeyRange: &topodatapb.KeyRange{},
		},
		key.DestinationKeyRange{
			KeyRange: &topodatapb.KeyRange{},
		},
	}
	assert.Equal(t, want, got, "Map()")
}

func TestLookupHashMapNull(t *testing.T) {
	lookuphash := createLookup(t, "lookup_hash", false /* writeOnly */)
	vc := &vcursor{numRows: 1, keys: []sqltypes.Value{sqltypes.NULL}}

	got, err := lookuphash.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NULL})
	require.NoError(t, err)
	want := []key.ShardDestination{
		key.DestinationKeyspaceIDs([][]byte{
			[]byte("\x16k@\xb4J\xbaK\xd6"),
		}),
	}
	assert.Equal(t, want, got, "Map()")

	// writeOnly true should return full keyranges.
	lookuphash = createLookup(t, "lookup_hash", true)
	got, err = lookuphash.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NULL})
	require.NoError(t, err)
	want = []key.ShardDestination{
		key.DestinationKeyRange{
			KeyRange: &topodatapb.KeyRange{},
		},
	}
	assert.Equal(t, want, got, "Map()")
}

func TestLookupHashVerify(t *testing.T) {
	lookuphash := createLookup(t, "lookup_hash", false /* writeOnly */)
	vc := &vcursor{numRows: 1}

	// The check doesn't actually happen. But we give correct values
	// to avoid confusion.
	got, err := lookuphash.Verify(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6"), []byte("\x06\xe7\xea\"Βp\x8f")})
	require.NoError(t, err)
	assert.Equal(t, []bool{true, true}, got, "lookuphash.Verify(match)")

	vc.numRows = 0
	got, err = lookuphash.Verify(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")})
	require.NoError(t, err)
	assert.Equal(t, []bool{false}, got, "lookuphash.Verify(mismatch)")

	_, err = lookuphash.Verify(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("bogus")})
	assert.EqualError(t, err, "lookup.Verify.vunhash: invalid keyspace id: 626f677573", "lookuphash.Verify(bogus)")

	// writeOnly true should always yield true.
	lookuphash = createLookup(t, "lookup_hash", true)
	vc.queries = nil

	got, err = lookuphash.Verify(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}, [][]byte{[]byte(""), []byte("")})
	require.NoError(t, err)
	assert.Nil(t, vc.queries, "lookuphash.Verify(scatter), queries")
	assert.Equal(t, []bool{true, true}, got, "lookuphash.Verify(scatter)")
}

func TestLookupHashCreate(t *testing.T) {
	lookuphash := createLookup(t, "lookup_hash", false /* writeOnly */)
	vc := &vcursor{}

	err := lookuphash.(Lookup).Create(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")}, false /* ignoreMode */)
	require.NoError(t, err)
	assert.Len(t, vc.queries, 1, "vc.queries length")

	err = lookuphash.(Lookup).Create(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NULL}}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")}, false /* ignoreMode */)
	require.ErrorContains(t, err, "VT03028: Column 'fromc' cannot be null on row 0, col 0")

	vc.queries = nil
	lookuphash.(*LookupHash).lkp.IgnoreNulls = true
	err = lookuphash.(Lookup).Create(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NULL}}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")}, false /* ignoreMode */)
	require.NoError(t, err)
	assert.Empty(t, vc.queries, "vc.queries length")

	err = lookuphash.(Lookup).Create(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}}, [][]byte{[]byte("bogus")}, false /* ignoreMode */)
	require.ErrorContains(t, err, "lookup.Create.vunhash: invalid keyspace id: 626f677573")
}

func TestLookupHashDelete(t *testing.T) {
	lookuphash := createLookup(t, "lookup_hash", false /* writeOnly */)
	vc := &vcursor{}

	err := lookuphash.(Lookup).Delete(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}}, []byte("\x16k@\xb4J\xbaK\xd6"))
	require.NoError(t, err)
	assert.Len(t, vc.queries, 1, "vc.queries length")

	vc.queries = nil
	err = lookuphash.(Lookup).Delete(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NULL}}, []byte("\x16k@\xb4J\xbaK\xd6"))
	require.NoError(t, err)
	assert.Len(t, vc.queries, 1, "vc.queries length")

	err = lookuphash.(Lookup).Delete(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}}, []byte("bogus"))
	assert.EqualError(t, err, "lookup.Delete.vunhash: invalid keyspace id: 626f677573", "lookuphash.Delete(bogus)")
}

func TestLookupHashUpdate(t *testing.T) {
	lookuphash := createLookup(t, "lookup_hash", false /* writeOnly */)
	vc := &vcursor{}

	err := lookuphash.(Lookup).Update(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1)}, []byte("\x16k@\xb4J\xbaK\xd6"), []sqltypes.Value{sqltypes.NewInt64(2)})
	require.NoError(t, err)
	assert.Len(t, vc.queries, 2, "vc.queries length")

	vc.queries = nil
	err = lookuphash.(Lookup).Update(context.Background(), vc, []sqltypes.Value{sqltypes.NULL}, []byte("\x16k@\xb4J\xbaK\xd6"), []sqltypes.Value{sqltypes.NewInt64(2)})
	require.NoError(t, err)
	assert.Len(t, vc.queries, 2, "vc.queries length")
}
