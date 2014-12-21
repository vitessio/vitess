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

var lhu *LookupHashUnique

func init() {
	h, err := NewLookupHashUnique(map[string]interface{}{"Table": "t", "From": "fromc", "To": "toc"})
	if err != nil {
		panic(err)
	}
	lhu = h.(*LookupHashUnique)
}

func TestLookupHashUniqueCost(t *testing.T) {
	if lhu.Cost() != 10 {
		t.Errorf("Cost(): %d, want 10", lhu.Cost())
	}
}

func TestLookupHashUniqueMap(t *testing.T) {
	vc := &vcursor{numRows: 1}
	got, err := lhu.Map(vc, []interface{}{1, int32(2)})
	if err != nil {
		t.Error(err)
	}
	want := []key.KeyspaceId{
		"\x16k@\xb4J\xbaK\xd6",
		"\x16k@\xb4J\xbaK\xd6",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}
}

func TestLookupHashUniqueMapNomatch(t *testing.T) {
	vc := &vcursor{}
	got, err := lhu.Map(vc, []interface{}{1, int32(2)})
	if err != nil {
		t.Error(err)
	}
	want := []key.KeyspaceId{"", ""}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}
}

func TestLookupHashUniqueMapFail(t *testing.T) {
	vc := &vcursor{mustFail: true}
	_, err := lhu.Map(vc, []interface{}{1, int32(2)})
	want := "LookupHashUnique.Map: Execute failed"
	if err == nil || err.Error() != want {
		t.Errorf("lhu.Map: %v, want %v", err, want)
	}
}

func TestLookupHashUniqueMapBadData(t *testing.T) {
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
	_, err := lhu.Map(vc, []interface{}{1, int32(2)})
	want := `LookupHashUnique.Map: strconv.ParseUint: parsing "1.1": invalid syntax`
	if err == nil || err.Error() != want {
		t.Errorf("lhu.Map: %v, want %v", err, want)
	}

	result.Fields = []mproto.Field{{
		Type: mproto.VT_FLOAT,
	}}
	vc = &vcursor{result: result}
	_, err = lhu.Map(vc, []interface{}{1, int32(2)})
	want = `LookupHashUnique.Map: unexpected type for 1.1: float64`
	if err == nil || err.Error() != want {
		t.Errorf("lhu.Map: %v, want %v", err, want)
	}

	vc = &vcursor{numRows: 2}
	_, err = lhu.Map(vc, []interface{}{1, int32(2)})
	want = `LookupHashUnique.Map: unexpected multiple results from vindex t: 1`
	if err == nil || err.Error() != want {
		t.Errorf("lhu.Map: %v, want %v", err, want)
	}
}

func TestLookupHashUniqueVerify(t *testing.T) {
	vc := &vcursor{numRows: 1}
	success, err := lhu.Verify(vc, 1, "\x16k@\xb4J\xbaK\xd6")
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Errorf("Verify(): %+v, want true", success)
	}
}

func TestLookupHashUniqueVerifyNomatch(t *testing.T) {
	vc := &vcursor{}
	success, err := lhu.Verify(vc, 1, "\x16k@\xb4J\xbaK\xd6")
	if err != nil {
		t.Error(err)
	}
	if success {
		t.Errorf("Verify(): %+v, want false", success)
	}
}

func TestLookupHashUniqueVerifyFail(t *testing.T) {
	vc := &vcursor{mustFail: true}
	_, err := lhu.Verify(vc, 1, "\x16k@\xb4J\xbaK\xd6")
	want := "lookupHash.Verify: Execute failed"
	if err == nil || err.Error() != want {
		t.Errorf("lhu.Verify: %v, want %v", err, want)
	}
}

func TestLookupHashUniqueCreate(t *testing.T) {
	vc := &vcursor{}
	err := lhu.Create(vc, 1, "\x16k@\xb4J\xbaK\xd6")
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

func TestLookupHashUniqueCreateFail(t *testing.T) {
	vc := &vcursor{mustFail: true}
	err := lhu.Create(vc, 1, "\x16k@\xb4J\xbaK\xd6")
	want := "lookupHash.Create: Execute failed"
	if err == nil || err.Error() != want {
		t.Errorf("lhu.Create: %v, want %v", err, want)
	}
}

func TestLookupHashUniqueGenerate(t *testing.T) {
	vc := &vcursor{}
	got, err := lhu.Generate(vc, "\x16k@\xb4J\xbaK\xd6")
	if err != nil {
		t.Error(err)
	}
	if got != 1 {
		t.Errorf("Generate(): %+v, want 1", got)
	}
	wantQuery := &tproto.BoundQuery{
		Sql: "insert into t(fromc, toc) values(:fromc, :toc)",
		BindVariables: map[string]interface{}{
			"fromc": nil,
			"toc":   int64(1),
		},
	}
	if !reflect.DeepEqual(vc.query, wantQuery) {
		t.Errorf("vc.query = %#v, want %#v", vc.query, wantQuery)
	}
}

func TestLookupHashUniqueGenerateFail(t *testing.T) {
	vc := &vcursor{mustFail: true}
	_, err := lhu.Generate(vc, "\x16k@\xb4J\xbaK\xd6")
	want := "LookupHashUnique.Generate: Execute failed"
	if err == nil || err.Error() != want {
		t.Errorf("lhu.Create: %v, want %v", err, want)
	}
}

func TestLookupHashUniqueDelete(t *testing.T) {
	vc := &vcursor{}
	err := lhu.Delete(vc, []interface{}{1}, "\x16k@\xb4J\xbaK\xd6")
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

func TestLookupHashUniqueDeleteFail(t *testing.T) {
	vc := &vcursor{mustFail: true}
	err := lhu.Delete(vc, []interface{}{1}, "\x16k@\xb4J\xbaK\xd6")
	want := "lookupHash.Delete: Execute failed"
	if err == nil || err.Error() != want {
		t.Errorf("lhu.Delete: %v, want %v", err, want)
	}
}
