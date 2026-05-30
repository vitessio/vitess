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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestLookupUniqueNew(t *testing.T) {
	l := createLookup(t, "lookup_unique", false)
	assert.False(t, l.(*LookupUnique).writeOnly, "Create(lookup, false)")

	vindex, err := CreateVindex("lookup_unique", "lookup_unique", map[string]string{
		"table":      "t",
		"from":       "fromc",
		"to":         "toc",
		"write_only": "true",
	})
	require.NoError(t, err)
	require.Empty(t, vindex.(ParamValidating).UnknownParams())

	l = vindex.(SingleColumn)
	assert.True(t, l.(*LookupUnique).writeOnly, "Create(lookup, true)")

	_, err = CreateVindex("lookup_unique", "lookup_unique", map[string]string{
		"table":      "t",
		"from":       "fromc",
		"to":         "toc",
		"write_only": "invalid",
	})
	assert.EqualError(t, err, "write_only value must be 'true' or 'false': 'invalid'", "Create(bad_scatter)")
}

func TestLookupUniqueMap(t *testing.T) {
	lookupUnique := createLookup(t, "lookup_unique", false)
	vc := &vcursor{numRows: 1}

	got, err := lookupUnique.Map(t.Context(), vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	require.NoError(t, err)
	want := []key.ShardDestination{
		key.DestinationKeyspaceID([]byte("1")),
		key.DestinationNone{},
	}
	assert.Equal(t, want, got, "Map()")

	vc.numRows = 0
	got, err = lookupUnique.Map(t.Context(), vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	require.NoError(t, err)
	want = []key.ShardDestination{
		key.DestinationNone{},
		key.DestinationNone{},
	}
	assert.Equal(t, want, got, "Map()")

	vc.numRows = 2
	_, err = lookupUnique.Map(t.Context(), vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	assert.EqualError(t, err, "Lookup.Map: unexpected multiple results from vindex t: INT64(1)", "lookupUnique(query fail)")

	// Test query fail.
	vc.mustFail = true
	_, err = lookupUnique.Map(t.Context(), vc, []sqltypes.Value{sqltypes.NewInt64(1)})
	assert.EqualError(t, err, "lookup.Map: execute failed", "lookupUnique(query fail)")
	vc.mustFail = false
}

func TestLookupUniqueMapWriteOnly(t *testing.T) {
	lookupUnique := createLookup(t, "lookup_unique", true)
	vc := &vcursor{numRows: 0}

	got, err := lookupUnique.Map(t.Context(), vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
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

func TestLookupUniqueVerify(t *testing.T) {
	lookupUnique := createLookup(t, "lookup_unique", false)
	vc := &vcursor{numRows: 1}

	_, err := lookupUnique.Verify(t.Context(), vc, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("test")})
	require.NoError(t, err)
	assert.Len(t, vc.queries, 1, "vc.queries length")
}

func TestLookupUniqueVerifyWriteOnly(t *testing.T) {
	lookupUnique := createLookup(t, "lookup_unique", true)
	vc := &vcursor{numRows: 0}

	got, err := lookupUnique.Verify(t.Context(), vc, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("test")})
	require.NoError(t, err)
	assert.Equal(t, []bool{true}, got, "lookupUnique.Verify")
	assert.Empty(t, vc.queries, "vc.queries length")
}

func TestLookupUniqueCreate(t *testing.T) {
	lookupUnique, err := CreateVindex("lookup_unique", "lookup_unique", map[string]string{
		"table":      "t",
		"from":       "from",
		"to":         "toc",
		"autocommit": "true",
	})
	require.NoError(t, err)
	require.Empty(t, lookupUnique.(ParamValidating).UnknownParams())
	vc := &vcursor{}

	err = lookupUnique.(Lookup).Create(t.Context(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}}, [][]byte{[]byte("test")}, false /* ignoreMode */)
	require.NoError(t, err)

	wantqueries := []*querypb.BoundQuery{{
		Sql: "insert into t(from, toc) values(:from_0, :toc_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"from_0": sqltypes.Int64BindVariable(1),
			"toc_0":  sqltypes.BytesBindVariable([]byte("test")),
		},
	}}
	assert.Equal(t, wantqueries, vc.queries, "lookup.Create queries")
	assert.Equal(t, 1, vc.autocommits, "Create(autocommit) count")
}

func TestLookupUniqueCreateAutocommit(t *testing.T) {
	lookupUnique := createLookup(t, "lookup_unique", false)
	vc := &vcursor{}

	err := lookupUnique.(Lookup).Create(t.Context(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}}, [][]byte{[]byte("test")}, false /* ignoreMode */)
	require.NoError(t, err)
	assert.Len(t, vc.queries, 1, "vc.queries length")
}

func TestLookupUniqueDelete(t *testing.T) {
	lookupUnique := createLookup(t, "lookup_unique", false)
	vc := &vcursor{}

	err := lookupUnique.(Lookup).Delete(t.Context(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}}, []byte("test"))
	require.NoError(t, err)
	assert.Len(t, vc.queries, 1, "vc.queries length")
}

func TestLookupUniqueUpdate(t *testing.T) {
	lookupUnique := createLookup(t, "lookup_unique", false)
	vc := &vcursor{}

	err := lookupUnique.(Lookup).Update(t.Context(), vc, []sqltypes.Value{sqltypes.NewInt64(1)}, []byte("test"), []sqltypes.Value{sqltypes.NewInt64(2)})
	require.NoError(t, err)
	assert.Len(t, vc.queries, 2, "vc.queries length")
}
