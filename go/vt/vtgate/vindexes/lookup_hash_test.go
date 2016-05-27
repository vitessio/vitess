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
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
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

var lhm Vindex

func init() {
	h, err := CreateVindex("lookup_hash", "nn", map[string]string{"table": "t", "from": "fromc", "to": "toc"})
	if err != nil {
		panic(err)
	}
	lhm = h
}

func TestLookupHashCost(t *testing.T) {
	if lhm.Cost() != 20 {
		t.Errorf("Cost(): %d, want 20", lhm.Cost())
	}
}

func TestLookupHashMap(t *testing.T) {
	vc := &vcursor{numRows: 2}
	got, err := lhm.(NonUnique).Map(vc, []interface{}{1, int32(2)})
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
	success, err := lhm.Verify(vc, 1, []byte("\x16k@\xb4J\xbaK\xd6"))
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Errorf("Verify(): %+v, want true", success)
	}
}

func TestLookupHashCreate(t *testing.T) {
	vc := &vcursor{}
	err := lhm.(Lookup).Create(vc, 1, []byte("\x16k@\xb4J\xbaK\xd6"))
	if err != nil {
		t.Error(err)
	}
	wantQuery := &querytypes.BoundQuery{
		Sql: "insert into t(fromc, toc) values(:fromc, :toc)",
		BindVariables: map[string]interface{}{
			"fromc": 1,
			"toc":   int64(1),
		},
	}
	if !reflect.DeepEqual(vc.bq, wantQuery) {
		t.Errorf("vc.query = %#v, want %#v", vc.bq, wantQuery)
	}
}

func TestLookupHashReverse(t *testing.T) {
	_, ok := lhm.(Reversible)
	if ok {
		t.Errorf("lhm.(Reversible): true, want false")
	}
}

func TestLookupHashDelete(t *testing.T) {
	vc := &vcursor{}
	err := lhm.(Lookup).Delete(vc, []interface{}{1}, []byte("\x16k@\xb4J\xbaK\xd6"))
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
