// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"reflect"
	"testing"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/key"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
)

var lhm *LookupHashMulti

func init() {
	h, err := NewLookupHashMulti(map[string]interface{}{"Table": "t", "From": "fromc", "To": "toc"})
	if err != nil {
		panic(err)
	}
	lhm = h.(*LookupHashMulti)
}

func TestLookupHashMultiCost(t *testing.T) {
	if lhm.Cost() != 20 {
		t.Errorf("Cost(): %d, want 20", lhm.Cost())
	}
}

func TestLookupHashMultiMap(t *testing.T) {
	vc := &vcursor{numRows: 2}
	got, err := lhm.Map(vc, []interface{}{1, int32(2)})
	if err != nil {
		t.Error(err)
	}
	want := [][]key.KeyspaceId{{
		"\x16k@\xb4J\xbaK\xd6",
		"\x06\xe7\xea\"Βp\x8f",
	}, {
		"\x16k@\xb4J\xbaK\xd6",
		"\x06\xe7\xea\"Βp\x8f",
	}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}
}

func TestLookupHashMultiMapFail(t *testing.T) {
	vc := &vcursor{mustFail: true}
	_, err := lhm.Map(vc, []interface{}{1, int32(2)})
	want := "LookupHashMulti.Map: Execute failed"
	if err == nil || err.Error() != want {
		t.Errorf("lhm.Map: %v, want %v", err, want)
	}
}

func TestLookupHashMultiMapBadData(t *testing.T) {
	result := &mproto.QueryResult{
		Fields: []mproto.Field{{
			Type: mproto.VT_INT24,
		}},
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{
				sqltypes.MakeFractional([]byte("1.1")),
			},
		},
		RowsAffected: 1,
	}
	vc := &vcursor{result: result}
	_, err := lhm.Map(vc, []interface{}{1, int32(2)})
	want := `LookupHashMulti.Map: strconv.ParseUint: parsing "1.1": invalid syntax`
	if err == nil || err.Error() != want {
		t.Errorf("lhm.Map: %v, want %v", err, want)
	}

	result.Fields = []mproto.Field{{
		Type: mproto.VT_FLOAT,
	}}
	vc = &vcursor{result: result}
	_, err = lhm.Map(vc, []interface{}{1, int32(2)})
	want = `LookupHashMulti.Map: unexpected type for 1.1: float64`
	if err == nil || err.Error() != want {
		t.Errorf("lhm.Map: %v, want %v", err, want)
	}
}

func TestLookupHashMultiVerify(t *testing.T) {
	vc := &vcursor{numRows: 1}
	success, err := lhm.Verify(vc, 1, "\x16k@\xb4J\xbaK\xd6")
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Errorf("Verify(): %+v, want true", success)
	}
}

func TestLookupHashMultiVerifyNomatch(t *testing.T) {
	vc := &vcursor{}
	success, err := lhm.Verify(vc, 1, "\x16k@\xb4J\xbaK\xd6")
	if err != nil {
		t.Error(err)
	}
	if success {
		t.Errorf("Verify(): %+v, want false", success)
	}
}

func TestLookupHashMultiVerifyFail(t *testing.T) {
	vc := &vcursor{mustFail: true}
	_, err := lhm.Verify(vc, 1, "\x16k@\xb4J\xbaK\xd6")
	want := "lookupHash.Verify: Execute failed"
	if err == nil || err.Error() != want {
		t.Errorf("lhm.Verify: %v, want %v", err, want)
	}
}

func TestLookupHashMultiCreate(t *testing.T) {
	vc := &vcursor{}
	err := lhm.Create(vc, 1, "\x16k@\xb4J\xbaK\xd6")
	if err != nil {
		t.Error(err)
	}
	wantQuery := &tproto.BoundQuery{
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

func TestLookupHashMultiCreateFail(t *testing.T) {
	vc := &vcursor{mustFail: true}
	err := lhm.Create(vc, 1, "\x16k@\xb4J\xbaK\xd6")
	want := "lookupHash.Create: Execute failed"
	if err == nil || err.Error() != want {
		t.Errorf("lhm.Create: %v, want %v", err, want)
	}
}

func TestLookupHashMultiDelete(t *testing.T) {
	vc := &vcursor{}
	err := lhm.Delete(vc, []interface{}{1}, "\x16k@\xb4J\xbaK\xd6")
	if err != nil {
		t.Error(err)
	}
	wantQuery := &tproto.BoundQuery{
		Sql: "delete from t where fromc in ::fromc and toc = :toc",
		BindVariables: map[string]interface{}{
			"fromc": []interface{}{1},
			"toc":   int64(1),
		},
	}
	if !reflect.DeepEqual(vc.query, wantQuery) {
		t.Errorf("vc.query = %#v, want %#v", vc.query, wantQuery)
	}
}

func TestLookupHashMultiDeleteFail(t *testing.T) {
	vc := &vcursor{mustFail: true}
	err := lhm.Delete(vc, []interface{}{1}, "\x16k@\xb4J\xbaK\xd6")
	want := "lookupHash.Delete: Execute failed"
	if err == nil || err.Error() != want {
		t.Errorf("lhm.Create: %v, want %v", err, want)
	}
}
