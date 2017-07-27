/*
Copyright 2017 Google Inc.

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
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

type vcursor struct {
	mustFail bool
	numRows  int
	result   *sqltypes.Result
	bq       *querypb.BoundQuery
}

func (vc *vcursor) Execute(query string, bindvars map[string]*querypb.BindVariable, isDML bool) (*sqltypes.Result, error) {
	vc.bq = &querypb.BoundQuery{
		Sql:           query,
		BindVariables: bindvars,
	}
	if vc.mustFail {
		return nil, errors.New("execute failed")
	}
	switch {
	case strings.HasPrefix(query, "select"):
		if vc.result != nil {
			return vc.result, nil
		}
		result := &sqltypes.Result{
			Fields: []*querypb.Field{{
				Type: sqltypes.Int32,
			}},
			RowsAffected: uint64(vc.numRows),
		}
		for i := 0; i < vc.numRows; i++ {
			result.Rows = append(result.Rows, []sqltypes.Value{
				sqltypes.MakeTrusted(sqltypes.Int64, []byte(fmt.Sprintf("%d", i+1))),
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

var lookuphash Vindex
var lookuphashunique Vindex

func init() {
	lh, err := CreateVindex("lookup_hash", "lookup_hash", map[string]string{"table": "t", "from": "fromc", "to": "toc"})
	if err != nil {
		panic(err)
	}
	lu, err := CreateVindex("lookup_hash_unique", "lookup_hash_unique", map[string]string{"table": "t", "from": "fromc", "to": "toc"})
	if err != nil {
		panic(err)
	}
	lookuphash = lh
	lookuphashunique = lu
}

func TestLookupHashCost(t *testing.T) {
	if lookuphash.Cost() != 20 {
		t.Errorf("Cost(): %d, want 20", lookuphash.Cost())
	}
	if lookuphashunique.Cost() != 10 {
		t.Errorf("Cost(): %d, want 10", lookuphashunique.Cost())
	}
}

func TestLookupHashString(t *testing.T) {
	if strings.Compare("lookup_hash", lookuphash.String()) != 0 {
		t.Errorf("String(): %s, want lookup_hash", lookuphash.String())
	}
	if strings.Compare("lookup_hash_unique", lookuphashunique.String()) != 0 {
		t.Errorf("String(): %s, want lookup_hash_unique", lookuphashunique.String())
	}
}

func TestLookupHashMap(t *testing.T) {
	vc := &vcursor{numRows: 2}
	got, err := lookuphash.(NonUnique).Map(vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	if err != nil {
		t.Error(err)
	}
	want := [][][]byte{{
		[]byte("\x16k@\xb4J\xbaK\xd6"),
		[]byte("\x06\xe7\xea\"Βp\x8f"),
	}, {
		[]byte("\x16k@\xb4J\xbaK\xd6"),
		[]byte("\x06\xe7\xea\"Βp\x8f"),
	}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}
}

func TestLookupHashVerify(t *testing.T) {
	vc := &vcursor{numRows: 1}
	success, err := lookuphash.Verify(vc, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")})
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Errorf("Verify(): %+v, want true", success)
	}
}

func TestLookupHashCreate(t *testing.T) {
	vc := &vcursor{}
	err := lookuphash.(Lookup).Create(vc, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")})
	if err != nil {
		t.Error(err)
	}
	wantQuery := &querypb.BoundQuery{
		Sql: "insert into t(fromc,toc) values(:fromc0,:toc0)",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc0": sqltypes.Int64BindVariable(1),
			"toc0":   sqltypes.Uint64BindVariable(1),
		},
	}
	if !reflect.DeepEqual(vc.bq, wantQuery) {
		t.Errorf("vc.query = %#v, want %#v", vc.bq, wantQuery)
	}
}

func TestLookupHashReverse(t *testing.T) {
	_, ok := lookuphash.(Reversible)
	if ok {
		t.Errorf("lhm.(Reversible): true, want false")
	}
}

func TestLookupHashDelete(t *testing.T) {
	vc := &vcursor{}
	err := lookuphash.(Lookup).Delete(vc, []sqltypes.Value{sqltypes.NewInt64(1)}, []byte("\x16k@\xb4J\xbaK\xd6"))
	if err != nil {
		t.Error(err)
	}
	wantQuery := &querypb.BoundQuery{
		Sql: "delete from t where fromc = :fromc and toc = :toc",
		BindVariables: map[string]*querypb.BindVariable{
			"fromc": sqltypes.Int64BindVariable(1),
			"toc":   sqltypes.Uint64BindVariable(1),
		},
	}
	if !reflect.DeepEqual(vc.bq, wantQuery) {
		t.Errorf("vc.query = %#v, want %#v", vc.bq, wantQuery)
	}
}
