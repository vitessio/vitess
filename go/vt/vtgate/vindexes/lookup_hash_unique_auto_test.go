// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"reflect"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

var lhua planbuilder.Vindex

func init() {
	h, err := planbuilder.CreateVindex("lookup_hash_unique_autoinc", map[string]interface{}{"Table": "t", "From": "fromc", "To": "toc"})
	if err != nil {
		panic(err)
	}
	lhua = h
}

func TestLookupHashUniqueAutoCost(t *testing.T) {
	if lhua.Cost() != 10 {
		t.Errorf("Cost(): %d, want 10", lhua.Cost())
	}
}

func TestLookupHashUniqueAutoMap(t *testing.T) {
	vc := &vcursor{numRows: 1}
	got, err := lhua.(planbuilder.Unique).Map(vc, []interface{}{1, int32(2)})
	if err != nil {
		t.Error(err)
	}
	want := [][]byte{
		[]byte("\x16k@\xb4J\xbaK\xd6"),
		[]byte("\x16k@\xb4J\xbaK\xd6"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}
}

func TestLookupHashUniqueAutoMapNomatch(t *testing.T) {
	vc := &vcursor{}
	got, err := lhua.(planbuilder.Unique).Map(vc, []interface{}{1, int32(2)})
	if err != nil {
		t.Error(err)
	}
	want := [][]byte{{}, {}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}
}

func TestLookupHashUniqueAutoMapFail(t *testing.T) {
	vc := &vcursor{mustFail: true}
	_, err := lhua.(planbuilder.Unique).Map(vc, []interface{}{1, int32(2)})
	want := "lookup.Map: execute failed"
	if err == nil || err.Error() != want {
		t.Errorf("lhua.Map: %v, want %v", err, want)
	}
}

func TestLookupHashUniqueAutoMapBadData(t *testing.T) {
	result := &sqltypes.Result{
		Fields: []*querypb.Field{{
			Type: sqltypes.Float64,
		}},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(sqltypes.Float64, []byte("1.1")),
			},
		},
		RowsAffected: 1,
	}
	vc := &vcursor{result: result}
	_, err := lhua.(planbuilder.Unique).Map(vc, []interface{}{1, int32(2)})
	want := "unexpected type"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("lhua.Map: %v, must contain %v", err, want)
	}

	result.Fields = []*querypb.Field{{
		Type: sqltypes.Float32,
	}}
	vc = &vcursor{result: result}
	_, err = lhua.(planbuilder.Unique).Map(vc, []interface{}{1, int32(2)})
	want = `lookup.Map: unexpected type for 1.1: float64`
	if err == nil || err.Error() != want {
		t.Errorf("lhua.Map: %v, want %v", err, want)
	}

	vc = &vcursor{numRows: 2}
	_, err = lhua.(planbuilder.Unique).Map(vc, []interface{}{1, int32(2)})
	want = `lookup.Map: unexpected multiple results from vindex t: 1`
	if err == nil || err.Error() != want {
		t.Errorf("lhua.Map: %v, want %v", err, want)
	}
}

func TestLookupHashUniqueAutoVerify(t *testing.T) {
	vc := &vcursor{numRows: 1}
	success, err := lhua.Verify(vc, 1, []byte("\x16k@\xb4J\xbaK\xd6"))
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Errorf("Verify(): %+v, want true", success)
	}
}

func TestLookupHashUniqueAutoVerifyNomatch(t *testing.T) {
	vc := &vcursor{}
	success, err := lhua.Verify(vc, 1, []byte("\x16k@\xb4J\xbaK\xd6"))
	if err != nil {
		t.Error(err)
	}
	if success {
		t.Errorf("Verify(): %+v, want false", success)
	}
}

func TestLookupHashUniqueAutoVerifyFail(t *testing.T) {
	vc := &vcursor{mustFail: true}

	_, err := lhua.Verify(vc, 1, []byte("\x16k@\xb4J\xbaK\xd6"))
	want := "lookup.Verify: execute failed"
	if err == nil || err.Error() != want {
		t.Errorf("lhua.Verify: %v, want %v", err, want)
	}

	_, err = lhua.Verify(vc, 1, []byte("aa"))
	want = "lookup.Verify: invalid keyspace id: 6161"
	if err == nil || err.Error() != want {
		t.Errorf("lhua.Verify: %v, want %v", err, want)
	}
}

func TestLookupHashUniqueAutoCreate(t *testing.T) {
	vc := &vcursor{}
	err := lhua.(planbuilder.Lookup).Create(vc, 1, []byte("\x16k@\xb4J\xbaK\xd6"))
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

func TestLookupHashUniqueAutoCreateFail(t *testing.T) {
	vc := &vcursor{mustFail: true}

	err := lhua.(planbuilder.Lookup).Create(vc, 1, []byte("\x16k@\xb4J\xbaK\xd6"))
	want := "lookup.Create: execute failed"
	if err == nil || err.Error() != want {
		t.Errorf("lhua.Create: %v, want %v", err, want)
	}

	err = lhua.(planbuilder.Lookup).Create(vc, 1, []byte("aa"))
	want = "lookup.Create: invalid keyspace id: 6161"
	if err == nil || err.Error() != want {
		t.Errorf("lhua.Create: %v, want %v", err, want)
	}
}

func TestLookupHashUniqueAutoGenerate(t *testing.T) {
	vc := &vcursor{}
	got, err := lhua.(planbuilder.LookupGenerator).Generate(vc, []byte("\x16k@\xb4J\xbaK\xd6"))
	if err != nil {
		t.Error(err)
	}
	if got != 1 {
		t.Errorf("Generate(): %+v, want 1", got)
	}
	wantQuery := &querytypes.BoundQuery{
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

func TestLookupHashUniqueAutoGenerateFail(t *testing.T) {
	vc := &vcursor{mustFail: true}

	_, err := lhua.(planbuilder.LookupGenerator).Generate(vc, []byte("\x16k@\xb4J\xbaK\xd6"))
	want := "lookup.Generate: execute failed"
	if err == nil || err.Error() != want {
		t.Errorf("lhua.Generate: %v, want %v", err, want)
	}

	_, err = lhua.(planbuilder.LookupGenerator).Generate(vc, []byte("aa"))
	want = "lookup.Generate: invalid keyspace id: 6161"
	if err == nil || err.Error() != want {
		t.Errorf("lhua.Generate: %v, want %v", err, want)
	}
}

func TestLookupHashUniqueAutoReverse(t *testing.T) {
	_, ok := lhua.(planbuilder.Reversible)
	if ok {
		t.Errorf("lhua.(planbuilder.Reversible): true, want false")
	}
}

func TestLookupHashUniqueAutoDelete(t *testing.T) {
	vc := &vcursor{}
	err := lhua.(planbuilder.Lookup).Delete(vc, []interface{}{1}, []byte("\x16k@\xb4J\xbaK\xd6"))
	if err != nil {
		t.Error(err)
	}
	wantQuery := &querytypes.BoundQuery{
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

func TestLookupHashUniqueAutoDeleteFail(t *testing.T) {
	vc := &vcursor{mustFail: true}

	err := lhua.(planbuilder.Lookup).Delete(vc, []interface{}{1}, []byte("\x16k@\xb4J\xbaK\xd6"))
	want := "lookup.Delete: execute failed"
	if err == nil || err.Error() != want {
		t.Errorf("lhua.Delete: %v, want %v", err, want)
	}

	err = lhua.(planbuilder.Lookup).Delete(vc, []interface{}{1}, []byte("aa"))
	want = "lookup.Delete: invalid keyspace id: 6161"
	if err == nil || err.Error() != want {
		t.Errorf("lhua.Delete: %v, want %v", err, want)
	}
}
