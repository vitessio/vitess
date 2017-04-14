// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/querytypes"
)

type vcursor struct {
	mustFail bool
	numRows  int
	result   *sqltypes.Result
	bq       *querytypes.BoundQuery
}

func (vc *vcursor) Execute(query string, bindvars map[string]interface{}) (*sqltypes.Result, error) {
	vc.bq = &querytypes.BoundQuery{
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
	got, err := lookuphash.(NonUnique).Map(vc, []interface{}{1, int32(2)})
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
	success, err := lookuphash.Verify(vc, []interface{}{1}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")})
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Errorf("Verify(): %+v, want true", success)
	}
}

func TestLookupHashCreate(t *testing.T) {
	vc := &vcursor{}
	err := lookuphash.(Lookup).Create(vc, []interface{}{1}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")})
	if err != nil {
		t.Error(err)
	}
	wantQuery := &querytypes.BoundQuery{
		Sql: "insert into t(fromc,toc) values(:fromc0,:toc0)",
		BindVariables: map[string]interface{}{
			"fromc0": 1,
			"toc0":   int64(1),
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
	err := lookuphash.(Lookup).Delete(vc, []interface{}{1}, []byte("\x16k@\xb4J\xbaK\xd6"))
	if err != nil {
		t.Error(err)
	}
	wantQuery := &querytypes.BoundQuery{
		Sql: "delete from t where fromc = :fromc and toc = :toc",
		BindVariables: map[string]interface{}{
			"fromc": 1,
			"toc":   int64(1),
		},
	}
	if !reflect.DeepEqual(vc.bq, wantQuery) {
		t.Errorf("vc.query = %#v, want %#v", vc.bq, wantQuery)
	}
}
