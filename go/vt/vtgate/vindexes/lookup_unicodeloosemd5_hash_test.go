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
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	hashed10 uint64 = 17563797831108199066
	hashed20 uint64 = 8729390916138266389
	hashed30 uint64 = 1472608112194674795
	hashed40 uint64 = 16576388050845489136
)

func lookupUnicodeLooseMD5HashCreateVindexTestCase(
	testName string,
	vindexParams map[string]string,
	expectErr error,
	expectUnknownParams []string,
) createVindexTestCase {
	return createVindexTestCase{
		testName: testName,

		vindexType:   "lookup_unicodeloosemd5_hash",
		vindexName:   "lookup_unicodeloosemd5_hash",
		vindexParams: vindexParams,

		expectCost:          20,
		expectErr:           expectErr,
		expectIsUnique:      false,
		expectNeedsVCursor:  true,
		expectString:        "lookup_unicodeloosemd5_hash",
		expectUnknownParams: expectUnknownParams,
	}
}

func TestLookupUnicodeLooseMD5HashCreateVindex(t *testing.T) {
	testLookupCreateVindexCommonCases(t, lookupUnicodeLooseMD5HashCreateVindexTestCase)
}

func TestLookupUnicodeLooseMD5HashMap(t *testing.T) {
	lookup := createLookup(t, "lookup_unicodeloosemd5_hash", false)
	vc := &vcursor{numRows: 2, keys: []sqltypes.Value{sqltypes.NewUint64(hashed10), sqltypes.NewUint64(hashed20)}}

	got, err := lookup.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(10), sqltypes.NewInt64(20)})
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

	vars, err := sqltypes.BuildBindVariable([]any{sqltypes.NewUint64(hashed10), sqltypes.NewUint64(hashed20)})
	require.NoError(t, err)
	wantqueries := []*querypb.BoundQuery{{
		Sql: "select fromc, toc from t where fromc in ::fromc",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc": vars,
		},
	}}
	assert.Equal(t, wantqueries, vc.queries, "lookup.Map queries")

	// Test query fail.
	vc.mustFail = true
	_, err = lookup.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1)})
	assert.EqualError(t, err, "lookup.Map: execute failed", "lookup(query fail)")
	vc.mustFail = false
}

func TestLookupUnicodeLooseMD5HashMapAutocommit(t *testing.T) {
	vindex, err := CreateVindex("lookup_unicodeloosemd5_hash", "lookup", map[string]string{
		"table":      "t",
		"from":       "fromc",
		"to":         "toc",
		"autocommit": "true",
	})
	require.NoError(t, err)
	unknownParams := vindex.(ParamValidating).UnknownParams()
	require.Empty(t, unknownParams)
	lnu := vindex.(SingleColumn)
	vc := &vcursor{numRows: 2, keys: []sqltypes.Value{sqltypes.NewUint64(hashed10), sqltypes.NewUint64(hashed20)}}

	got, err := lnu.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(10), sqltypes.NewInt64(20)})
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

	vars, err := sqltypes.BuildBindVariable([]any{sqltypes.NewUint64(hashed10), sqltypes.NewUint64(hashed20)})
	require.NoError(t, err)
	wantqueries := []*querypb.BoundQuery{{
		Sql: "select fromc, toc from t where fromc in ::fromc",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc": vars,
		},
	}}
	assert.Equal(t, wantqueries, vc.queries, "lookup.Map queries")
	assert.Equal(t, 1, vc.autocommits, "Create(autocommit) count")
}

func TestLookupUnicodeLooseMD5HashMapWriteOnly(t *testing.T) {
	lnu := createLookup(t, "lookup_unicodeloosemd5_hash", true)
	vc := &vcursor{numRows: 0}

	got, err := lnu.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(10), sqltypes.NewInt64(20)})
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

func TestLookupUnicodeLooseMD5HashMapAbsent(t *testing.T) {
	lnu := createLookup(t, "lookup_unicodeloosemd5_hash", false)
	vc := &vcursor{numRows: 0}

	got, err := lnu.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(10), sqltypes.NewInt64(20)})
	require.NoError(t, err)
	want := []key.ShardDestination{
		key.DestinationNone{},
		key.DestinationNone{},
	}
	assert.Equal(t, want, got, "Map()")
}

func TestLookupUnicodeLooseMD5HashVerify(t *testing.T) {
	lnu := createLookup(t, "lookup_unicodeloosemd5_hash", false)
	vc := &vcursor{numRows: 1}

	got, err := lnu.Verify(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(10), sqltypes.NewInt64(20)}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6"), []byte("\x06\xe7\xea\"Βp\x8f")})
	require.NoError(t, err)
	assert.Equal(t, []bool{true, true}, got, "lookuphash.Verify(match)")

	wantqueries := []*querypb.BoundQuery{{
		Sql: "select fromc from t where fromc = :fromc and toc = :toc",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc": sqltypes.Uint64BindVariable(hashed10),
			"toc":   sqltypes.Uint64BindVariable(1),
		},
	}, {
		Sql: "select fromc from t where fromc = :fromc and toc = :toc",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc": sqltypes.Uint64BindVariable(hashed20),
			"toc":   sqltypes.Uint64BindVariable(2),
		},
	}}
	assert.Equal(t, wantqueries, vc.queries, "lookup.Verify queries")

	// Test query fail.
	vc.mustFail = true
	_, err = lnu.Verify(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")})
	assert.EqualError(t, err, "lookup.Verify: execute failed", "lookupNonUnique(query fail)")
	vc.mustFail = false

	// writeOnly true should always yield true.
	lnu = createLookup(t, "lookup_unicodeloosemd5_hash", true)
	vc.queries = nil

	got, err = lnu.Verify(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(10), sqltypes.NewInt64(20)}, [][]byte{[]byte(""), []byte("")})
	require.NoError(t, err)
	assert.Nil(t, vc.queries, "lookup.Verify(writeOnly), queries")
	assert.Equal(t, []bool{true, true}, got, "lookup.Verify(writeOnly)")
}

func TestLookupUnicodeLooseMD5HashVerifyAutocommit(t *testing.T) {
	vindex, err := CreateVindex("lookup_unicodeloosemd5_hash", "lookup", map[string]string{
		"table":      "t",
		"from":       "fromc",
		"to":         "toc",
		"autocommit": "true",
	})
	require.NoError(t, err)
	unknownParams := vindex.(ParamValidating).UnknownParams()
	require.Empty(t, unknownParams)
	lnu := vindex.(SingleColumn)
	vc := &vcursor{numRows: 1}

	_, err = lnu.Verify(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(10), sqltypes.NewInt64(20)}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6"), []byte("\x06\xe7\xea\"Βp\x8f")})
	require.NoError(t, err)

	wantqueries := []*querypb.BoundQuery{{
		Sql: "select fromc from t where fromc = :fromc and toc = :toc",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc": sqltypes.Uint64BindVariable(hashed10),
			"toc":   sqltypes.Uint64BindVariable(1),
		},
	}, {
		Sql: "select fromc from t where fromc = :fromc and toc = :toc",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc": sqltypes.Uint64BindVariable(hashed20),
			"toc":   sqltypes.Uint64BindVariable(2),
		},
	}}
	assert.Equal(t, wantqueries, vc.queries, "lookup.Verify queries")
	assert.Equal(t, 2, vc.autocommits, "Create(autocommit) count")
}

func TestLookupUnicodeLooseMD5HashCreate(t *testing.T) {
	lnu := createLookup(t, "lookup_unicodeloosemd5_hash", false)
	vc := &vcursor{}

	err := lnu.(Lookup).Create(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(10)}, {sqltypes.NewInt64(20)}}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6"), []byte("\x06\xe7\xea\"Βp\x8f")}, false)
	require.NoError(t, err)

	wantqueries := []*querypb.BoundQuery{{
		Sql: "insert into t(fromc, toc) values(:fromc_0, :toc_0), (:fromc_1, :toc_1)",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc_0": sqltypes.Uint64BindVariable(hashed10),
			"toc_0":   sqltypes.Uint64BindVariable(1),
			"fromc_1": sqltypes.Uint64BindVariable(hashed20),
			"toc_1":   sqltypes.Uint64BindVariable(2),
		},
	}}
	assert.Equal(t, wantqueries, vc.queries, "lookup.Create queries")

	// With ignore.
	vc.queries = nil
	err = lnu.(Lookup).Create(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(10)}, {sqltypes.NewInt64(20)}}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6"), []byte("\x06\xe7\xea\"Βp\x8f")}, true)
	require.NoError(t, err)

	wantqueries[0].Sql = "insert ignore into t(fromc, toc) values(:fromc_0, :toc_0), (:fromc_1, :toc_1)"
	assert.Equal(t, wantqueries, vc.queries, "lookup.Create queries")

	// Test query fail.
	vc.mustFail = true
	err = lnu.(Lookup).Create(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(10)}}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")}, false)
	assert.EqualError(t, err, "lookup.Create: execute failed", "lookupNonUnique(query fail)")
	vc.mustFail = false

	// Test column mismatch.
	err = lnu.(Lookup).Create(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(10), sqltypes.NewInt64(20)}}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")}, false)
	require.ErrorContains(t, err, "VT03030: lookup column count does not match value count with the row (columns, count): ([fromc], 2)")
}

func TestLookupUnicodeLooseMD5HashCreateAutocommit(t *testing.T) {
	lnu, err := CreateVindex("lookup_unicodeloosemd5_hash", "lookup", map[string]string{
		"table":      "t",
		"from":       "from1,from2",
		"to":         "toc",
		"autocommit": "true",
	})
	require.NoError(t, err)
	unknownParams := lnu.(ParamValidating).UnknownParams()
	require.Empty(t, unknownParams)
	vc := &vcursor{}

	err = lnu.(Lookup).Create(context.Background(), vc, [][]sqltypes.Value{{
		sqltypes.NewInt64(10), sqltypes.NewInt64(20),
	}, {
		sqltypes.NewInt64(30), sqltypes.NewInt64(40),
	}}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6"), []byte("\x06\xe7\xea\"Βp\x8f")}, false)
	require.NoError(t, err)

	wantqueries := []*querypb.BoundQuery{{
		Sql: "insert into t(from1, from2, toc) values(:from1_0, :from2_0, :toc_0), (:from1_1, :from2_1, :toc_1) on duplicate key update from1=values(from1), from2=values(from2), toc=values(toc)",
		BindVariables: map[string]*querypb.BindVariable{
			"from1_0": sqltypes.Uint64BindVariable(hashed30),
			"from2_0": sqltypes.Uint64BindVariable(hashed40),
			"toc_0":   sqltypes.Uint64BindVariable(2),
			"from1_1": sqltypes.Uint64BindVariable(hashed10),
			"from2_1": sqltypes.Uint64BindVariable(hashed20),
			"toc_1":   sqltypes.Uint64BindVariable(1),
		},
	}}
	assert.Equal(t, wantqueries, vc.queries, "lookup.Create queries")
	assert.Equal(t, 1, vc.autocommits, "Create(autocommit) count")
}

func TestLookupUnicodeLooseMD5HashCreateMultiShardAutocommit(t *testing.T) {
	lnu, err := CreateVindex("lookup_unicodeloosemd5_hash", "lookup", map[string]string{
		"table":                  "t",
		"from":                   "from1,from2",
		"to":                     "toc",
		"multi_shard_autocommit": "true",
	})
	require.NoError(t, err)
	unknownParams := lnu.(ParamValidating).UnknownParams()
	require.Empty(t, unknownParams)
	vc := &vcursor{}

	err = lnu.(Lookup).Create(context.Background(), vc, [][]sqltypes.Value{{
		sqltypes.NewInt64(10), sqltypes.NewInt64(20),
	}, {
		sqltypes.NewInt64(30), sqltypes.NewInt64(40),
	}}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6"), []byte("\x06\xe7\xea\"Βp\x8f")}, false)
	require.NoError(t, err)

	wantqueries := []*querypb.BoundQuery{{
		Sql: "insert /*vt+ MULTI_SHARD_AUTOCOMMIT=1 */ into t(from1, from2, toc) values(:from1_0, :from2_0, :toc_0), (:from1_1, :from2_1, :toc_1) on duplicate key update from1=values(from1), from2=values(from2), toc=values(toc)",
		BindVariables: map[string]*querypb.BindVariable{
			"from1_0": sqltypes.Uint64BindVariable(hashed30),
			"from2_0": sqltypes.Uint64BindVariable(hashed40),
			"toc_0":   sqltypes.Uint64BindVariable(2),
			"from1_1": sqltypes.Uint64BindVariable(hashed10),
			"from2_1": sqltypes.Uint64BindVariable(hashed20),
			"toc_1":   sqltypes.Uint64BindVariable(1),
		},
	}}
	assert.Equal(t, wantqueries, vc.queries, "lookup.Create queries")
	assert.Equal(t, 1, vc.autocommits, "Create(autocommit) count")
}

func TestLookupUnicodeLooseMD5HashDelete(t *testing.T) {
	lnu := createLookup(t, "lookup_unicodeloosemd5_hash", false)
	vc := &vcursor{}

	err := lnu.(Lookup).Delete(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(10)}, {sqltypes.NewInt64(20)}}, []byte("\x16k@\xb4J\xbaK\xd6"))
	require.NoError(t, err)

	wantqueries := []*querypb.BoundQuery{{
		Sql: "delete from t where fromc = :fromc and toc = :toc",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc": sqltypes.Uint64BindVariable(hashed10),
			"toc":   sqltypes.Uint64BindVariable(1),
		},
	}, {
		Sql: "delete from t where fromc = :fromc and toc = :toc",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc": sqltypes.Uint64BindVariable(hashed20),
			"toc":   sqltypes.Uint64BindVariable(1),
		},
	}}
	assert.Equal(t, wantqueries, vc.queries, "lookup.Delete queries")

	// Test query fail.
	vc.mustFail = true
	err = lnu.(Lookup).Delete(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}}, []byte("\x16k@\xb4J\xbaK\xd6"))
	assert.EqualError(t, err, "lookup.Delete: execute failed", "lookupNonUnique(query fail)")
	vc.mustFail = false

	// Test column count fail.
	err = lnu.(Lookup).Delete(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}}, []byte("\x16k@\xb4J\xbaK\xd6"))
	require.ErrorContains(t, err, "VT03030: lookup column count does not match value count with the row (columns, count): ([fromc], 2)")
}

func TestLookupUnicodeLooseMD5HashDeleteAutocommit(t *testing.T) {
	lnu, err := CreateVindex("lookup_unicodeloosemd5_hash", "lookup", map[string]string{
		"table":      "t",
		"from":       "fromc",
		"to":         "toc",
		"autocommit": "true",
	})
	unknownParams := lnu.(ParamValidating).UnknownParams()
	require.Empty(t, unknownParams)
	require.NoError(t, err)

	vc := &vcursor{}

	err = lnu.(Lookup).Delete(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(10)}, {sqltypes.NewInt64(20)}}, []byte("\x16k@\xb4J\xbaK\xd6"))
	require.NoError(t, err)

	wantqueries := []*querypb.BoundQuery(nil)
	assert.Equal(t, wantqueries, vc.queries, "lookup.Delete queries")
}

func TestLookupUnicodeLooseMD5HashUpdate(t *testing.T) {
	lnu := createLookup(t, "lookup_unicodeloosemd5_hash", false)
	vc := &vcursor{}

	err := lnu.(Lookup).Update(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(10)}, []byte("\x16k@\xb4J\xbaK\xd6"), []sqltypes.Value{sqltypes.NewInt64(20)})
	require.NoError(t, err)

	wantqueries := []*querypb.BoundQuery{{
		Sql: "delete from t where fromc = :fromc and toc = :toc",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc": sqltypes.Uint64BindVariable(hashed10),
			"toc":   sqltypes.Uint64BindVariable(1),
		},
	}, {
		Sql: "insert into t(fromc, toc) values(:fromc_0, :toc_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc_0": sqltypes.Uint64BindVariable(hashed20),
			"toc_0":   sqltypes.Uint64BindVariable(1),
		},
	}}
	assert.Equal(t, wantqueries, vc.queries, "lookup.Update queries")
}
