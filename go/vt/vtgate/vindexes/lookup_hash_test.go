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
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
)

type vcursor struct {
	mustFail bool
	numRows  int
	result   *sqltypes.Result
	query    *querytypes.BoundQuery
}

func (vc *vcursor) Execute(query *querytypes.BoundQuery) (*sqltypes.Result, error) {
	vc.query = query
	if vc.mustFail {
		return nil, errors.New("execute failed")
	}
	switch {
	case strings.HasPrefix(query.Sql, "select"):
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
	case strings.HasPrefix(query.Sql, "insert"):
		return &sqltypes.Result{InsertID: 1}, nil
	case strings.HasPrefix(query.Sql, "delete"):
		return &sqltypes.Result{}, nil
	}
	panic("unexpected")
}

var lhm planbuilder.Vindex

func init() {
	h, err := planbuilder.CreateVindex("lookup_hash", "nn", map[string]interface{}{"Table": "t", "From": "fromc", "To": "toc"})
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
	got, err := lhm.(planbuilder.NonUnique).Map(vc, []interface{}{1, int32(2)})
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
	err := lhm.(planbuilder.Lookup).Create(vc, 1, []byte("\x16k@\xb4J\xbaK\xd6"))
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
	if !reflect.DeepEqual(vc.query, wantQuery) {
		t.Errorf("vc.query = %#v, want %#v", vc.query, wantQuery)
	}
}

func TestLookupHashReverse(t *testing.T) {
	_, ok := lhm.(planbuilder.Reversible)
	if ok {
		t.Errorf("lhm.(planbuilder.Reversible): true, want false")
	}
}

func TestLookupHashDelete(t *testing.T) {
	vc := &vcursor{}
	err := lhm.(planbuilder.Lookup).Delete(vc, []interface{}{1}, []byte("\x16k@\xb4J\xbaK\xd6"))
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
	if !reflect.DeepEqual(vc.query, wantQuery) {
		t.Errorf("vc.query = %#v, want %#v", vc.query, wantQuery)
	}
}
