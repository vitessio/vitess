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
	"errors"
	"testing"

	"vitess.io/vitess/go/test/utils"

	"strings"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"

	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

// LookupNonUnique tests are more comprehensive than others.
// They also test lookupInternal functionality.

var _ VCursor = (*vcursor)(nil)

type vcursor struct {
	mustFail    bool
	numRows     int
	result      *sqltypes.Result
	queries     []*querypb.BoundQuery
	autocommits int
	pre, post   int
	keys        []sqltypes.Value
}

func (vc *vcursor) LookupRowLockShardSession() vtgatepb.CommitOrder {
	panic("implement me")
}

func (vc *vcursor) InTransactionAndIsDML() bool {
	return false
}

func (vc *vcursor) Execute(ctx context.Context, method string, query string, bindvars map[string]*querypb.BindVariable, rollbackOnError bool, co vtgatepb.CommitOrder) (*sqltypes.Result, error) {
	switch co {
	case vtgatepb.CommitOrder_PRE:
		vc.pre++
	case vtgatepb.CommitOrder_POST:
		vc.post++
	case vtgatepb.CommitOrder_AUTOCOMMIT:
		vc.autocommits++
	}
	return vc.execute(query, bindvars)
}

func (vc *vcursor) ExecuteKeyspaceID(ctx context.Context, keyspace string, ksid []byte, query string, bindVars map[string]*querypb.BindVariable, rollbackOnError, autocommit bool) (*sqltypes.Result, error) {
	return vc.execute(query, bindVars)
}

func (vc *vcursor) execute(query string, bindvars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	vc.queries = append(vc.queries, &querypb.BoundQuery{
		Sql:           query,
		BindVariables: bindvars,
	})
	if vc.mustFail {
		return nil, errors.New("execute failed")
	}
	switch {
	case strings.HasPrefix(query, "select"):
		if vc.result != nil {
			return vc.result, nil
		}
		result := &sqltypes.Result{
			Fields:       sqltypes.MakeTestFields("key|col", "int64|int32"),
			RowsAffected: uint64(vc.numRows),
		}
		for i := 0; i < vc.numRows; i++ {
			for j := 0; j < vc.numRows; j++ {
				k := sqltypes.NewInt64(int64(j + 1))
				if vc.keys != nil {
					k = vc.keys[j]
				}
				result.Rows = append(result.Rows, []sqltypes.Value{
					k, sqltypes.NewInt64(int64(i + 1)),
				})
			}
		}
		return result, nil
	case strings.HasPrefix(query, "insert"):
		return &sqltypes.Result{InsertID: 1}, nil
	case strings.HasPrefix(query, "delete"):
		return &sqltypes.Result{}, nil
	}
	panic("unexpected")
}

func TestLookupNonUniqueNew(t *testing.T) {
	l := createLookup(t, "lookup", false /* writeOnly */)
	assert.False(t, l.(*LookupNonUnique).writeOnly, "Create(lookup, false)")

	l = createLookup(t, "lookup", true)
	assert.True(t, l.(*LookupNonUnique).writeOnly, "Create(lookup, false)")

	_, err := CreateVindex("lookup", "lookup", map[string]string{
		"table":      "t",
		"from":       "fromc",
		"to":         "toc",
		"write_only": "invalid",
	})
	require.EqualError(t, err, "write_only value must be 'true' or 'false': 'invalid'")
}

func TestLookupNonUniqueInfo(t *testing.T) {
	lookupNonUnique := createLookup(t, "lookup", false /* writeOnly */)
	assert.Equal(t, 20, lookupNonUnique.Cost())
	assert.Equal(t, "lookup", lookupNonUnique.String())
	assert.False(t, lookupNonUnique.IsUnique())
	assert.True(t, lookupNonUnique.NeedsVCursor())
}

func TestLookupNilVCursor(t *testing.T) {
	lookupNonUnique := createLookup(t, "lookup", false /* writeOnly */)
	_, err := lookupNonUnique.Map(context.Background(), nil, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	require.EqualError(t, err, "cannot perform lookup: no vcursor provided")
}

func TestLookupNonUniqueMap(t *testing.T) {
	lookupNonUnique := createLookup(t, "lookup", false /* writeOnly */)
	vc := &vcursor{numRows: 2}

	got, err := lookupNonUnique.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	require.NoError(t, err)
	want := []key.Destination{
		key.DestinationKeyspaceIDs([][]byte{
			[]byte("1"),
			[]byte("2"),
		}),
		key.DestinationKeyspaceIDs([][]byte{
			[]byte("1"),
			[]byte("2"),
		}),
	}
	utils.MustMatch(t, want, got)

	vars, err := sqltypes.BuildBindVariable([]any{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	require.NoError(t, err)
	wantqueries := []*querypb.BoundQuery{{
		Sql: "select fromc, toc from t where fromc in ::fromc",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc": vars,
		},
	}}
	utils.MustMatch(t, wantqueries, vc.queries, "lookup.Map")

	// Test query fail.
	vc.mustFail = true
	_, err = lookupNonUnique.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1)})
	require.EqualError(t, err, "lookup.Map: execute failed")
}

func TestLookupNonUniqueMapAutocommit(t *testing.T) {
	vindex, err := CreateVindex("lookup", "lookup", map[string]string{
		"table":      "t",
		"from":       "fromc",
		"to":         "toc",
		"autocommit": "true",
	})
	require.NoError(t, err)
	lookupNonUnique := vindex.(SingleColumn)
	vc := &vcursor{numRows: 2}

	got, err := lookupNonUnique.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	require.NoError(t, err)
	want := []key.Destination{
		key.DestinationKeyspaceIDs([][]byte{
			[]byte("1"),
			[]byte("2"),
		}),
		key.DestinationKeyspaceIDs([][]byte{
			[]byte("1"),
			[]byte("2"),
		}),
	}
	utils.MustMatch(t, want, got)

	vars, err := sqltypes.BuildBindVariable([]any{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	require.NoError(t, err)
	wantqueries := []*querypb.BoundQuery{{
		Sql: "select fromc, toc from t where fromc in ::fromc",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc": vars,
		},
	}}
	utils.MustMatch(t, wantqueries, vc.queries)
	assert.Equal(t, 1, vc.autocommits, "autocommits")
}

func TestLookupNonUniqueMapWriteOnly(t *testing.T) {
	lookupNonUnique := createLookup(t, "lookup", true)
	vc := &vcursor{numRows: 0}

	got, err := lookupNonUnique.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	require.NoError(t, err)
	want := []key.Destination{
		key.DestinationKeyRange{
			KeyRange: &topodatapb.KeyRange{},
		},
		key.DestinationKeyRange{
			KeyRange: &topodatapb.KeyRange{},
		},
	}
	utils.MustMatch(t, want, got)
}

func TestLookupNonUniqueMapAbsent(t *testing.T) {
	lookupNonUnique := createLookup(t, "lookup", false /* writeOnly */)
	vc := &vcursor{numRows: 0}

	got, err := lookupNonUnique.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	require.NoError(t, err)
	want := []key.Destination{
		key.DestinationNone{},
		key.DestinationNone{},
	}
	utils.MustMatch(t, want, got)
}

func TestLookupNonUniqueVerify(t *testing.T) {
	lookupNonUnique := createLookup(t, "lookup", false /* writeOnly */)
	vc := &vcursor{numRows: 1}

	_, err := lookupNonUnique.Verify(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}, [][]byte{[]byte("test1"), []byte("test2")})
	require.NoError(t, err)

	wantqueries := []*querypb.BoundQuery{{
		Sql: "select fromc from t where fromc = :fromc and toc = :toc",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc": sqltypes.Int64BindVariable(1),
			"toc":   sqltypes.BytesBindVariable([]byte("test1")),
		},
	}, {
		Sql: "select fromc from t where fromc = :fromc and toc = :toc",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc": sqltypes.Int64BindVariable(2),
			"toc":   sqltypes.BytesBindVariable([]byte("test2")),
		},
	}}
	utils.MustMatch(t, wantqueries, vc.queries)

	// Test query fail.
	vc.mustFail = true
	_, err = lookupNonUnique.Verify(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")})
	require.EqualError(t, err, "lookup.Verify: execute failed")
	vc.mustFail = false

	// writeOnly true should always yield true.
	lookupNonUnique = createLookup(t, "lookup", true)
	vc.queries = nil

	got, err := lookupNonUnique.Verify(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}, [][]byte{[]byte(""), []byte("")})
	require.NoError(t, err)
	assert.Empty(t, vc.queries, "lookup verify queries")
	utils.MustMatch(t, []bool{true, true}, got)
}

func TestLookupNonUniqueVerifyAutocommit(t *testing.T) {
	vindex, err := CreateVindex("lookup", "lookup", map[string]string{
		"table":      "t",
		"from":       "fromc",
		"to":         "toc",
		"autocommit": "true",
	})
	require.NoError(t, err)
	lookupNonUnique := vindex.(SingleColumn)
	vc := &vcursor{numRows: 1}

	_, err = lookupNonUnique.Verify(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}, [][]byte{[]byte("test1"), []byte("test2")})
	require.NoError(t, err)

	wantqueries := []*querypb.BoundQuery{{
		Sql: "select fromc from t where fromc = :fromc and toc = :toc",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc": sqltypes.Int64BindVariable(1),
			"toc":   sqltypes.BytesBindVariable([]byte("test1")),
		},
	}, {
		Sql: "select fromc from t where fromc = :fromc and toc = :toc",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc": sqltypes.Int64BindVariable(2),
			"toc":   sqltypes.BytesBindVariable([]byte("test2")),
		},
	}}

	utils.MustMatch(t, wantqueries, vc.queries)
	assert.Equal(t, 2, vc.autocommits, "autocommits")
}

func TestLookupNonUniqueCreate(t *testing.T) {
	lookupNonUnique := createLookup(t, "lookup", false /* writeOnly */)
	vc := &vcursor{}

	err := lookupNonUnique.(Lookup).Create(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}, {sqltypes.NewInt64(2)}}, [][]byte{[]byte("test1"), []byte("test2")}, false /* ignoreMode */)
	require.NoError(t, err)

	wantqueries := []*querypb.BoundQuery{{
		Sql: "insert into t(fromc, toc) values(:fromc_0, :toc_0), (:fromc_1, :toc_1)",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc_0": sqltypes.Int64BindVariable(1),
			"toc_0":   sqltypes.BytesBindVariable([]byte("test1")),
			"fromc_1": sqltypes.Int64BindVariable(2),
			"toc_1":   sqltypes.BytesBindVariable([]byte("test2")),
		},
	}}
	utils.MustMatch(t, wantqueries, vc.queries)

	// With ignore.
	vc.queries = nil
	err = lookupNonUnique.(Lookup).Create(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(2)}, {sqltypes.NewInt64(1)}}, [][]byte{[]byte("test2"), []byte("test1")}, true /* ignoreMode */)
	require.NoError(t, err)
	wantqueries[0].Sql = "insert ignore into t(fromc, toc) values(:fromc_0, :toc_0), (:fromc_1, :toc_1)"
	utils.MustMatch(t, wantqueries, vc.queries)

	// With ignore_nulls off
	err = lookupNonUnique.(Lookup).Create(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(2)}, {sqltypes.NULL}}, [][]byte{[]byte("test2"), []byte("test1")}, true /* ignoreMode */)
	assert.EqualError(t, err, "lookup.Create: input has null values: row: 1, col: 0")

	// With ignore_nulls on
	vc.queries = nil
	lookupNonUnique.(*LookupNonUnique).lkp.IgnoreNulls = true
	err = lookupNonUnique.(Lookup).Create(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(2)}, {sqltypes.NULL}}, [][]byte{[]byte("test2"), []byte("test1")}, true /* ignoreMode */)
	require.NoError(t, err)
	wantqueries = []*querypb.BoundQuery{{
		Sql: "insert ignore into t(fromc, toc) values(:fromc_0, :toc_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc_0": sqltypes.Int64BindVariable(2),
			"toc_0":   sqltypes.BytesBindVariable([]byte("test2")),
		},
	}}
	utils.MustMatch(t, wantqueries, vc.queries)

	// Test query fail.
	vc.mustFail = true
	err = lookupNonUnique.(Lookup).Create(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")}, false /* ignoreMode */)
	assert.EqualError(t, err, "lookup.Create: execute failed")
	vc.mustFail = false

	// Test column mismatch.
	err = lookupNonUnique.(Lookup).Create(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")}, false /* ignoreMode */)
	assert.EqualError(t, err, "lookup.Create: column vindex count does not match the columns in the lookup: 2 vs [fromc]")
}

func TestLookupNonUniqueCreateAutocommit(t *testing.T) {
	lookupNonUnique, err := CreateVindex("lookup", "lookup", map[string]string{
		"table":      "t",
		"from":       "from1,from2",
		"to":         "toc",
		"autocommit": "true",
	})
	require.NoError(t, err)
	vc := &vcursor{}

	err = lookupNonUnique.(Lookup).Create(context.Background(), vc, [][]sqltypes.Value{{
		sqltypes.NewInt64(1), sqltypes.NewInt64(2),
	}, {
		sqltypes.NewInt64(3), sqltypes.NewInt64(4),
	}}, [][]byte{[]byte("test1"), []byte("test2")}, false /* ignoreMode */)
	require.NoError(t, err)

	wantqueries := []*querypb.BoundQuery{{
		Sql: "insert into t(from1, from2, toc) values(:from1_0, :from2_0, :toc_0), (:from1_1, :from2_1, :toc_1) on duplicate key update from1=values(from1), from2=values(from2), toc=values(toc)",
		BindVariables: map[string]*querypb.BindVariable{
			"from1_0": sqltypes.Int64BindVariable(1),
			"from2_0": sqltypes.Int64BindVariable(2),
			"toc_0":   sqltypes.BytesBindVariable([]byte("test1")),
			"from1_1": sqltypes.Int64BindVariable(3),
			"from2_1": sqltypes.Int64BindVariable(4),
			"toc_1":   sqltypes.BytesBindVariable([]byte("test2")),
		},
	}}
	utils.MustMatch(t, wantqueries, vc.queries)
	assert.Equal(t, 1, vc.autocommits, "autocommits")
}

func TestLookupNonUniqueDelete(t *testing.T) {
	lookupNonUnique := createLookup(t, "lookup", false /* writeOnly */)
	vc := &vcursor{}

	err := lookupNonUnique.(Lookup).Delete(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}, {sqltypes.NewInt64(2)}}, []byte("test"))
	require.NoError(t, err)

	wantqueries := []*querypb.BoundQuery{{
		Sql: "delete from t where fromc = :fromc and toc = :toc",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc": sqltypes.Int64BindVariable(1),
			"toc":   sqltypes.BytesBindVariable([]byte("test")),
		},
	}, {
		Sql: "delete from t where fromc = :fromc and toc = :toc",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc": sqltypes.Int64BindVariable(2),
			"toc":   sqltypes.BytesBindVariable([]byte("test")),
		},
	}}
	utils.MustMatch(t, wantqueries, vc.queries)

	// Test query fail.
	vc.mustFail = true
	err = lookupNonUnique.(Lookup).Delete(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}}, []byte("\x16k@\xb4J\xbaK\xd6"))
	assert.EqualError(t, err, "lookup.Delete: execute failed")
	vc.mustFail = false

	// Test column count fail.
	err = lookupNonUnique.(Lookup).Delete(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}}, []byte("\x16k@\xb4J\xbaK\xd6"))
	assert.EqualError(t, err, "lookup.Delete: column vindex count does not match the columns in the lookup: 2 vs [fromc]")
}

func TestLookupNonUniqueDeleteAutocommit(t *testing.T) {
	lookupNonUnique, _ := CreateVindex("lookup", "lookup", map[string]string{
		"table":      "t",
		"from":       "fromc",
		"to":         "toc",
		"autocommit": "true",
	})
	vc := &vcursor{}

	err := lookupNonUnique.(Lookup).Delete(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}, {sqltypes.NewInt64(2)}}, []byte("test"))
	require.NoError(t, err)

	utils.MustMatch(t, []*querypb.BoundQuery(nil), vc.queries)
}

func TestLookupNonUniqueUpdate(t *testing.T) {
	lookupNonUnique := createLookup(t, "lookup", false /* writeOnly */)
	vc := &vcursor{}

	err := lookupNonUnique.(Lookup).Update(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1)}, []byte("test"), []sqltypes.Value{sqltypes.NewInt64(2)})
	require.NoError(t, err)

	wantqueries := []*querypb.BoundQuery{{
		Sql: "delete from t where fromc = :fromc and toc = :toc",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc": sqltypes.Int64BindVariable(1),
			"toc":   sqltypes.BytesBindVariable([]byte("test")),
		},
	}, {
		Sql: "insert into t(fromc, toc) values(:fromc_0, :toc_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc_0": sqltypes.Int64BindVariable(2),
			"toc_0":   sqltypes.BytesBindVariable([]byte("test")),
		},
	}}
	utils.MustMatch(t, wantqueries, vc.queries)
}

func TestLookupMapResult(t *testing.T) {
	lookup := createLookup(t, "lookup", false)

	ids := []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}
	results := []*sqltypes.Result{{
		Fields:       sqltypes.MakeTestFields("key|col", "int64|int32"),
		RowsAffected: 2,
		Rows: []sqltypes.Row{
			{sqltypes.NewInt64(1), sqltypes.NewInt64(3)},
			{sqltypes.NewInt64(3), sqltypes.NewInt64(4)},
			{sqltypes.NewInt64(5), sqltypes.NewInt64(6)},
		},
	}}

	got, err := lookup.(LookupPlanable).MapResult(ids, results)
	require.NoError(t, err)
	want := []key.Destination{
		key.DestinationKeyspaceIDs([][]byte{
			[]byte("1"),
			[]byte("3"),
			[]byte("5"),
		}),
	}
	utils.MustMatch(t, want, got)
}

func TestLookupUniqueMapResult(t *testing.T) {
	lookup := createLookup(t, "lookup_unique", false)

	ids := []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}
	results := []*sqltypes.Result{{
		Fields:       sqltypes.MakeTestFields("key|col", "int64|int32"),
		RowsAffected: 2,
		Rows: []sqltypes.Row{
			{sqltypes.NewInt64(1), sqltypes.NewInt64(3)},
		},
	}}

	got, err := lookup.(LookupPlanable).MapResult(ids, results)
	require.NoError(t, err)
	want := []key.Destination{
		key.DestinationKeyspaceID("1"),
	}
	utils.MustMatch(t, want, got)

	results[0].Rows = append(results[0].Rows, results[0].Rows...)
	_, err = lookup.(LookupPlanable).MapResult(ids, results)
	require.Error(t, err)
}

func TestLookupNonUniqueCreateMultiShardAutocommit(t *testing.T) {
	lookupNonUnique, err := CreateVindex("lookup", "lookup", map[string]string{
		"table":                  "t",
		"from":                   "from1,from2",
		"to":                     "toc",
		"multi_shard_autocommit": "true",
	})
	require.NoError(t, err)

	vc := &vcursor{}
	err = lookupNonUnique.(Lookup).Create(context.Background(), vc, [][]sqltypes.Value{{
		sqltypes.NewInt64(1), sqltypes.NewInt64(2),
	}, {
		sqltypes.NewInt64(3), sqltypes.NewInt64(4),
	}}, [][]byte{[]byte("test1"), []byte("test2")}, false /* ignoreMode */)
	require.NoError(t, err)

	wantqueries := []*querypb.BoundQuery{{
		Sql: "insert /*vt+ MULTI_SHARD_AUTOCOMMIT=1 */ into t(from1, from2, toc) values(:from1_0, :from2_0, :toc_0), (:from1_1, :from2_1, :toc_1) on duplicate key update from1=values(from1), from2=values(from2), toc=values(toc)",
		BindVariables: map[string]*querypb.BindVariable{
			"from1_0": sqltypes.Int64BindVariable(1),
			"from2_0": sqltypes.Int64BindVariable(2),
			"toc_0":   sqltypes.BytesBindVariable([]byte("test1")),
			"from1_1": sqltypes.Int64BindVariable(3),
			"from2_1": sqltypes.Int64BindVariable(4),
			"toc_1":   sqltypes.BytesBindVariable([]byte("test2")),
		},
	}}
	utils.MustMatch(t, vc.queries, wantqueries)
	require.Equal(t, 1, vc.autocommits, "Create(autocommit) count")
}

func createLookup(t *testing.T, name string, writeOnly bool) SingleColumn {
	t.Helper()
	write := "false"
	if writeOnly {
		write = "true"
	}
	l, err := CreateVindex(name, name, map[string]string{
		"table":      "t",
		"from":       "fromc",
		"to":         "toc",
		"write_only": write,
	})
	require.NoError(t, err)
	return l.(SingleColumn)
}
