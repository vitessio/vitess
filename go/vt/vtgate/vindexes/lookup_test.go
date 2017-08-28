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

	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// LookupNonUnique tests are more comprehensive than others.
// They also test lookupInternal functionality.

type vcursor struct {
	mustFail bool
	numRows  int
	result   *sqltypes.Result
	queries  []*querypb.BoundQuery
}

func (vc *vcursor) Execute(query string, bindvars map[string]*querypb.BindVariable, isDML bool) (*sqltypes.Result, error) {
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

var lookupUnique Vindex
var lookupNonUnique Vindex

func init() {
	lkpunique, err := CreateVindex("lookup_unique", "lookupUnique", map[string]string{"table": "t", "from": "fromc", "to": "toc"})
	if err != nil {
		panic(err)
	}
	lkpnonunique, err := CreateVindex("lookup", "lookupNonUnique", map[string]string{"table": "t", "from": "fromc", "to": "toc"})
	if err != nil {
		panic(err)
	}

	lookupUnique = lkpunique
	lookupNonUnique = lkpnonunique
}

func TestLookupNonUniqueCost(t *testing.T) {
	if lookupNonUnique.Cost() != 20 {
		t.Errorf("Cost(): %d, want 20", lookupUnique.Cost())
	}
}

func TestLookupNonUniqueString(t *testing.T) {
	if strings.Compare("lookupNonUnique", lookupNonUnique.String()) != 0 {
		t.Errorf("String(): %s, want lookupNonUnique", lookupNonUnique.String())
	}
}

func TestLookupNonUniqueMap(t *testing.T) {
	vc := &vcursor{numRows: 2}
	got, err := lookupNonUnique.(NonUnique).Map(vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	if err != nil {
		t.Error(err)
	}
	want := [][][]byte{{
		[]byte("1"),
		[]byte("2"),
	}, {
		[]byte("1"),
		[]byte("2"),
	}}
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
	_, err = lookupNonUnique.(NonUnique).Map(vc, []sqltypes.Value{sqltypes.NewInt64(1)})
	wantErr := "lookup.Map: execute failed"
	if err == nil || err.Error() != wantErr {
		t.Errorf("lookupNonUnique(query fail) err: %v, want %s", err, wantErr)
	}
	vc.mustFail = false
}

func TestLookupNonUniqueVerify(t *testing.T) {
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
}

func TestLookupNonUniqueCreate(t *testing.T) {
	vc := &vcursor{}
	err := lookupNonUnique.(Lookup).Create(vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}, [][]byte{[]byte("test1"), []byte("test2")}, false /* ignoreMode */)
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
	err = lookupNonUnique.(Lookup).Create(vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}, [][]byte{[]byte("test1"), []byte("test2")}, true /* ignoreMode */)
	if err != nil {
		t.Error(err)
	}

	wantqueries[0].Sql = "insert ignore into t(fromc, toc) values(:fromc0, :toc0), (:fromc1, :toc1)"
	if !reflect.DeepEqual(vc.queries, wantqueries) {
		t.Errorf("lookup.Create queries:\n%v, want\n%v", vc.queries, wantqueries)
	}

	// Test query fail.
	vc.mustFail = true
	err = lookupNonUnique.(Lookup).Create(vc, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")}, false /* ignoreMode */)
	want := "lookup.Create: execute failed"
	if err == nil || err.Error() != want {
		t.Errorf("lookupNonUnique(query fail) err: %v, want %s", err, want)
	}
	vc.mustFail = false
}

func TestLookupNonUniqueDelete(t *testing.T) {
	vc := &vcursor{}
	err := lookupNonUnique.(Lookup).Delete(vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}, []byte("test"))
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
	err = lookupNonUnique.(Lookup).Delete(vc, []sqltypes.Value{sqltypes.NewInt64(1)}, []byte("\x16k@\xb4J\xbaK\xd6"))
	want := "lookup.Delete: execute failed"
	if err == nil || err.Error() != want {
		t.Errorf("lookupNonUnique(query fail) err: %v, want %s", err, want)
	}
	vc.mustFail = false
}
