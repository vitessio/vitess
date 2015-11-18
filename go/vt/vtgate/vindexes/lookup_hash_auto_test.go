// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"reflect"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
)

var lha planbuilder.Vindex

func init() {
	h, err := planbuilder.CreateVindex("lookup_hash_autoinc", map[string]interface{}{"Table": "t", "From": "fromc", "To": "toc"})
	if err != nil {
		panic(err)
	}
	lha = h
}

func TestLookupHashAutoCost(t *testing.T) {
	if lha.Cost() != 20 {
		t.Errorf("Cost(): %d, want 20", lha.Cost())
	}
}

func TestLookupHashAutoMap(t *testing.T) {
	vc := &vcursor{numRows: 2}
	got, err := lha.(planbuilder.NonUnique).Map(vc, []interface{}{1, int32(2)})
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

func TestLookupHashAutoMapFail(t *testing.T) {
	vc := &vcursor{mustFail: true}
	_, err := lha.(planbuilder.NonUnique).Map(vc, []interface{}{1, int32(2)})
	want := "lookup.Map: execute failed"
	if err == nil || err.Error() != want {
		t.Errorf("lha.Map: %v, want %v", err, want)
	}
}

func TestLookupHashAutoMapBadData(t *testing.T) {
	result := &sqltypes.Result{
		Fields: []*querypb.Field{{
			Type: sqltypes.Float64,
		}},
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{
				sqltypes.MakeTrusted(sqltypes.Float64, []byte("1.1")),
			},
		},
		RowsAffected: 1,
	}
	vc := &vcursor{result: result}
	_, err := lha.(planbuilder.NonUnique).Map(vc, []interface{}{1, int32(2)})
	want := "unexpected type"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("lha.Map: %v, must contain %v", err, want)
	}

	result.Fields = []*querypb.Field{{
		Type: sqltypes.Float32,
	}}
	vc = &vcursor{result: result}
	_, err = lha.(planbuilder.NonUnique).Map(vc, []interface{}{1, int32(2)})
	want = `lookup.Map: unexpected type for 1.1: float64`
	if err == nil || err.Error() != want {
		t.Errorf("lha.Map: %v, want %v", err, want)
	}
}

func TestLookupHashAutoVerify(t *testing.T) {
	vc := &vcursor{numRows: 1}
	success, err := lha.Verify(vc, 1, []byte("\x16k@\xb4J\xbaK\xd6"))
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Errorf("Verify(): %+v, want true", success)
	}
}

func TestLookupHashAutoVerifyNomatch(t *testing.T) {
	vc := &vcursor{}
	success, err := lha.Verify(vc, 1, []byte("\x16k@\xb4J\xbaK\xd6"))
	if err != nil {
		t.Error(err)
	}
	if success {
		t.Errorf("Verify(): %+v, want false", success)
	}
}

func TestLookupHashAutoCreate(t *testing.T) {
	vc := &vcursor{}
	err := lha.(planbuilder.Lookup).Create(vc, 1, []byte("\x16k@\xb4J\xbaK\xd6"))
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

func TestLookupHashAutoGenerate(t *testing.T) {
	vc := &vcursor{}
	got, err := lha.(planbuilder.LookupGenerator).Generate(vc, []byte("\x16k@\xb4J\xbaK\xd6"))
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

func TestLookupHashAutoReverse(t *testing.T) {
	_, ok := lha.(planbuilder.Reversible)
	if ok {
		t.Errorf("lha.(planbuilder.Reversible): true, want false")
	}
}

func TestLookupHashAutoDelete(t *testing.T) {
	vc := &vcursor{}
	err := lha.(planbuilder.Lookup).Delete(vc, []interface{}{1}, []byte("\x16k@\xb4J\xbaK\xd6"))
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
