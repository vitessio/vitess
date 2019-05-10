/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vindexes

import (
	"errors"
	"reflect"
	"testing"

	"strings"

	"vitess.io/vitess/go/sqltypes"

	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

// LookupNonUnique tests are more comprehensive than others.
// They also test lookupInternal functionality.

type vcursor struct {
	mustFail    bool
	numRows     int
	result      *sqltypes.Result
	queries     []*querypb.BoundQuery
	autocommits int
	pre, post   int
}

func (vc *vcursor) Execute(method string, query string, bindvars map[string]*querypb.BindVariable, isDML bool, co vtgatepb.CommitOrder) (*sqltypes.Result, error) {
	switch co {
	case vtgatepb.CommitOrder_PRE:
		vc.pre++
	case vtgatepb.CommitOrder_POST:
		vc.post++
	case vtgatepb.CommitOrder_AUTOCOMMIT:
		vc.autocommits++
	}
	return vc.execute(method, query, bindvars, isDML)
}

func (vc *vcursor) ExecuteKeyspaceID(keyspace string, ksid []byte, query string, bindVars map[string]*querypb.BindVariable, isDML, autocommit bool) (*sqltypes.Result, error) {
	return vc.execute("ExecuteKeyspaceID", query, bindVars, isDML)
}

func (vc *vcursor) execute(method string, query string, bindvars map[string]*querypb.BindVariable, isDML bool) (*sqltypes.Result, error) {
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
			Fields:       sqltypes.MakeTestFields("col", "int32"),
			RowsAffected: uint64(vc.numRows),
		}
		for i := 0; i < vc.numRows; i++ {
			result.Rows = append(result.Rows, []sqltypes.Value{
				sqltypes.NewInt64(int64(i + 1)),
			})
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
	l := createLookup(t, "lookup", false)
	if want, got := l.(*LookupNonUnique).writeOnly, false; got != want {
		t.Errorf("Create(lookup, false): %v, want %v", got, want)
	}

	l = createLookup(t, "lookup", true)
	if want, got := l.(*LookupNonUnique).writeOnly, true; got != want {
		t.Errorf("Create(lookup, false): %v, want %v", got, want)
	}

	_, err := CreateVindex("lookup", "lookup", map[string]string{
		"table":      "t",
		"from":       "fromc",
		"to":         "toc",
		"write_only": "invalid",
	})
	want := "write_only value must be 'true' or 'false': 'invalid'"
	if err == nil || err.Error() != want {
		t.Errorf("Create(bad_scatter): %v, want %s", err, want)
	}
}

func TestLookupNonUniqueCost(t *testing.T) {
	lookupNonUnique := createLookup(t, "lookup", false)
	if lookupNonUnique.Cost() != 20 {
		t.Errorf("Cost(): %d, want 20", lookupNonUnique.Cost())
	}
}

func TestLookupNonUniqueString(t *testing.T) {
	lookupNonUnique := createLookup(t, "lookup", false)
	if strings.Compare("lookup", lookupNonUnique.String()) != 0 {
		t.Errorf("String(): %s, want lookup", lookupNonUnique.String())
	}
}

func TestLookupNonUniqueMap(t *testing.T) {
	lookupNonUnique := createLookup(t, "lookup", false)
	vc := &vcursor{numRows: 2}

	got, err := lookupNonUnique.Map(vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	if err != nil {
		t.Error(err)
	}
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
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}

	wantqueries := []*querypb.BoundQuery{{
		Sql: "select toc from t where fromc = :fromc",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc": sqltypes.Int64BindVariable(1),
		},
	}, {
		Sql: "select toc from t where fromc = :fromc",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc": sqltypes.Int64BindVariable(2),
		},
	}}
	if !reflect.DeepEqual(vc.queries, wantqueries) {
		t.Errorf("lookup.Map queries:\n%v, want\n%v", vc.queries, wantqueries)
	}

	// Test query fail.
	vc.mustFail = true
	_, err = lookupNonUnique.Map(vc, []sqltypes.Value{sqltypes.NewInt64(1)})
	wantErr := "lookup.Map: execute failed"
	if err == nil || err.Error() != wantErr {
		t.Errorf("lookupNonUnique(query fail) err: %v, want %s", err, wantErr)
	}
	vc.mustFail = false
}

func TestLookupNonUniqueMapAutocommit(t *testing.T) {
	lookupNonUnique, err := CreateVindex("lookup", "lookup", map[string]string{
		"table":      "t",
		"from":       "fromc",
		"to":         "toc",
		"autocommit": "true",
	})
	if err != nil {
		t.Fatal(err)
	}
	vc := &vcursor{numRows: 2}

	got, err := lookupNonUnique.Map(vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	if err != nil {
		t.Error(err)
	}
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
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}

	wantqueries := []*querypb.BoundQuery{{
		Sql: "select toc from t where fromc = :fromc",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc": sqltypes.Int64BindVariable(1),
		},
	}, {
		Sql: "select toc from t where fromc = :fromc",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc": sqltypes.Int64BindVariable(2),
		},
	}}
	if !reflect.DeepEqual(vc.queries, wantqueries) {
		t.Errorf("lookup.Map queries:\n%v, want\n%v", vc.queries, wantqueries)
	}

	if got, want := vc.autocommits, 2; got != want {
		t.Errorf("Create(autocommit) count: %d, want %d", got, want)
	}
}

func TestLookupNonUniqueMapWriteOnly(t *testing.T) {
	lookupNonUnique := createLookup(t, "lookup", true)
	vc := &vcursor{numRows: 0}

	got, err := lookupNonUnique.Map(vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	if err != nil {
		t.Error(err)
	}
	want := []key.Destination{
		key.DestinationKeyRange{
			KeyRange: &topodatapb.KeyRange{},
		},
		key.DestinationKeyRange{
			KeyRange: &topodatapb.KeyRange{},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}
}

func TestLookupNonUniqueMapAbsent(t *testing.T) {
	lookupNonUnique := createLookup(t, "lookup", false)
	vc := &vcursor{numRows: 0}

	got, err := lookupNonUnique.Map(vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	if err != nil {
		t.Error(err)
	}
	want := []key.Destination{
		key.DestinationNone{},
		key.DestinationNone{},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}
}

func TestLookupNonUniqueVerify(t *testing.T) {
	lookupNonUnique := createLookup(t, "lookup", false)
	vc := &vcursor{numRows: 1}

	_, err := lookupNonUnique.Verify(vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}, [][]byte{[]byte("test1"), []byte("test2")})
	if err != nil {
		t.Error(err)
	}

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
	if !reflect.DeepEqual(vc.queries, wantqueries) {
		t.Errorf("lookup.Verify queries:\n%v, want\n%v", vc.queries, wantqueries)
	}

	// Test query fail.
	vc.mustFail = true
	_, err = lookupNonUnique.Verify(vc, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")})
	want := "lookup.Verify: execute failed"
	if err == nil || err.Error() != want {
		t.Errorf("lookupNonUnique(query fail) err: %v, want %s", err, want)
	}
	vc.mustFail = false

	// writeOnly true should always yield true.
	lookupNonUnique = createLookup(t, "lookup", true)
	vc.queries = nil

	got, err := lookupNonUnique.Verify(vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}, [][]byte{[]byte(""), []byte("")})
	if err != nil {
		t.Error(err)
	}
	if vc.queries != nil {
		t.Errorf("lookup.Verify(writeOnly), queries: %v, want nil", vc.queries)
	}
	wantBools := []bool{true, true}
	if !reflect.DeepEqual(got, wantBools) {
		t.Errorf("lookup.Verify(writeOnly): %v, want %v", got, wantBools)
	}
}

func TestLookupNonUniqueVerifyAutocommit(t *testing.T) {
	lookupNonUnique, err := CreateVindex("lookup", "lookup", map[string]string{
		"table":      "t",
		"from":       "fromc",
		"to":         "toc",
		"autocommit": "true",
	})
	if err != nil {
		t.Fatal(err)
	}
	vc := &vcursor{numRows: 1}

	_, err = lookupNonUnique.Verify(vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}, [][]byte{[]byte("test1"), []byte("test2")})
	if err != nil {
		t.Error(err)
	}

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
	if !reflect.DeepEqual(vc.queries, wantqueries) {
		t.Errorf("lookup.Verify queries:\n%v, want\n%v", vc.queries, wantqueries)
	}

	if got, want := vc.autocommits, 2; got != want {
		t.Errorf("Create(autocommit) count: %d, want %d", got, want)
	}
}

func TestLookupNonUniqueCreate(t *testing.T) {
	lookupNonUnique := createLookup(t, "lookup", false)
	vc := &vcursor{}

	err := lookupNonUnique.(Lookup).Create(vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}, {sqltypes.NewInt64(2)}}, [][]byte{[]byte("test1"), []byte("test2")}, false /* ignoreMode */)
	if err != nil {
		t.Error(err)
	}

	wantqueries := []*querypb.BoundQuery{{
		Sql: "insert into t(fromc, toc) values(:fromc0, :toc0), (:fromc1, :toc1)",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc0": sqltypes.Int64BindVariable(1),
			"toc0":   sqltypes.BytesBindVariable([]byte("test1")),
			"fromc1": sqltypes.Int64BindVariable(2),
			"toc1":   sqltypes.BytesBindVariable([]byte("test2")),
		},
	}}
	if !reflect.DeepEqual(vc.queries, wantqueries) {
		t.Errorf("lookup.Create queries:\n%v, want\n%v", vc.queries, wantqueries)
	}

	// With ignore.
	vc.queries = nil
	err = lookupNonUnique.(Lookup).Create(vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}, {sqltypes.NewInt64(2)}}, [][]byte{[]byte("test1"), []byte("test2")}, true /* ignoreMode */)
	if err != nil {
		t.Error(err)
	}

	wantqueries[0].Sql = "insert ignore into t(fromc, toc) values(:fromc0, :toc0), (:fromc1, :toc1)"
	if !reflect.DeepEqual(vc.queries, wantqueries) {
		t.Errorf("lookup.Create queries:\n%v, want\n%v", vc.queries, wantqueries)
	}

	// Test query fail.
	vc.mustFail = true
	err = lookupNonUnique.(Lookup).Create(vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")}, false /* ignoreMode */)
	want := "lookup.Create: execute failed"
	if err == nil || err.Error() != want {
		t.Errorf("lookupNonUnique(query fail) err: %v, want %s", err, want)
	}
	vc.mustFail = false

	// Test column mismatch.
	err = lookupNonUnique.(Lookup).Create(vc, [][]sqltypes.Value{{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")}, false /* ignoreMode */)
	want = "lookup.Create: column vindex count does not match the columns in the lookup: 2 vs [fromc]"
	if err == nil || err.Error() != want {
		t.Errorf("lookupNonUnique(query fail) err: %v, want %s", err, want)
	}
}

func TestLookupNonUniqueCreateAutocommit(t *testing.T) {
	lookupNonUnique, err := CreateVindex("lookup", "lookup", map[string]string{
		"table":      "t",
		"from":       "from1,from2",
		"to":         "toc",
		"autocommit": "true",
	})
	if err != nil {
		t.Fatal(err)
	}
	vc := &vcursor{}

	err = lookupNonUnique.(Lookup).Create(
		vc,
		[][]sqltypes.Value{{
			sqltypes.NewInt64(1), sqltypes.NewInt64(2),
		}, {
			sqltypes.NewInt64(3), sqltypes.NewInt64(4),
		}},
		[][]byte{[]byte("test1"), []byte("test2")},
		false /* ignoreMode */)
	if err != nil {
		t.Error(err)
	}

	wantqueries := []*querypb.BoundQuery{{
		Sql: "insert into t(from1, from2, toc) values(:from10, :from20, :toc0), (:from11, :from21, :toc1) on duplicate key update from1=values(from1), from2=values(from2), toc=values(toc)",
		BindVariables: map[string]*querypb.BindVariable{
			"from10": sqltypes.Int64BindVariable(1),
			"from20": sqltypes.Int64BindVariable(2),
			"toc0":   sqltypes.BytesBindVariable([]byte("test1")),
			"from11": sqltypes.Int64BindVariable(3),
			"from21": sqltypes.Int64BindVariable(4),
			"toc1":   sqltypes.BytesBindVariable([]byte("test2")),
		},
	}}
	if !reflect.DeepEqual(vc.queries, wantqueries) {
		t.Errorf("lookup.Create queries:\n%v, want\n%v", vc.queries, wantqueries)
	}

	if got, want := vc.autocommits, 1; got != want {
		t.Errorf("Create(autocommit) count: %d, want %d", got, want)
	}
}

func TestLookupNonUniqueDelete(t *testing.T) {
	lookupNonUnique := createLookup(t, "lookup", false)
	vc := &vcursor{}

	err := lookupNonUnique.(Lookup).Delete(vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}, {sqltypes.NewInt64(2)}}, []byte("test"))
	if err != nil {
		t.Error(err)
	}

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
	if !reflect.DeepEqual(vc.queries, wantqueries) {
		t.Errorf("lookup.Delete queries:\n%v, want\n%v", vc.queries, wantqueries)
	}

	// Test query fail.
	vc.mustFail = true
	err = lookupNonUnique.(Lookup).Delete(vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}}, []byte("\x16k@\xb4J\xbaK\xd6"))
	want := "lookup.Delete: execute failed"
	if err == nil || err.Error() != want {
		t.Errorf("lookupNonUnique(query fail) err: %v, want %s", err, want)
	}
	vc.mustFail = false

	// Test column count fail.
	err = lookupNonUnique.(Lookup).Delete(vc, [][]sqltypes.Value{{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}}, []byte("\x16k@\xb4J\xbaK\xd6"))
	want = "lookup.Delete: column vindex count does not match the columns in the lookup: 2 vs [fromc]"
	if err == nil || err.Error() != want {
		t.Errorf("lookupNonUnique(query fail) err: %v, want %s", err, want)
	}
}

func TestLookupNonUniqueDeleteAutocommit(t *testing.T) {
	lookupNonUnique, _ := CreateVindex("lookup", "lookup", map[string]string{
		"table":      "t",
		"from":       "fromc",
		"to":         "toc",
		"autocommit": "true",
	})
	vc := &vcursor{}

	err := lookupNonUnique.(Lookup).Delete(vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}, {sqltypes.NewInt64(2)}}, []byte("test"))
	if err != nil {
		t.Error(err)
	}

	wantqueries := []*querypb.BoundQuery(nil)
	if !reflect.DeepEqual(vc.queries, wantqueries) {
		t.Errorf("lookup.Delete queries:\n%v, want\n%v", vc.queries, wantqueries)
	}
}

func TestLookupNonUniqueUpdate(t *testing.T) {
	lookupNonUnique := createLookup(t, "lookup", false)
	vc := &vcursor{}

	err := lookupNonUnique.(Lookup).Update(vc, []sqltypes.Value{sqltypes.NewInt64(1)}, []byte("test"), []sqltypes.Value{sqltypes.NewInt64(2)})
	if err != nil {
		t.Error(err)
	}

	wantqueries := []*querypb.BoundQuery{{
		Sql: "delete from t where fromc = :fromc and toc = :toc",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc": sqltypes.Int64BindVariable(1),
			"toc":   sqltypes.BytesBindVariable([]byte("test")),
		},
	}, {
		Sql: "insert into t(fromc, toc) values(:fromc0, :toc0)",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc0": sqltypes.Int64BindVariable(2),
			"toc0":   sqltypes.BytesBindVariable([]byte("test")),
		},
	}}
	if !reflect.DeepEqual(vc.queries, wantqueries) {
		t.Errorf("lookup.Update queries:\n%v, want\n%v", vc.queries, wantqueries)
	}
}

func createLookup(t *testing.T, name string, writeOnly bool) Vindex {
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
	if err != nil {
		t.Fatal(err)
	}
	return l
}
