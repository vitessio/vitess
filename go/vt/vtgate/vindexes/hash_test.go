// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
)

var hash planbuilder.Vindex

func init() {
	hv, err := planbuilder.CreateVindex("hash", map[string]interface{}{"Table": "t", "Column": "c"})
	if err != nil {
		panic(err)
	}
	hash = hv
}

func TestHashCost(t *testing.T) {
	if hash.Cost() != 1 {
		t.Errorf("Cost(): %d, want 1", hash.Cost())
	}
}

func TestHashMap(t *testing.T) {
	got, err := hash.(planbuilder.Unique).Map(nil, []interface{}{1, int32(2), int64(3), uint(4), uint32(5), uint64(6)})
	if err != nil {
		t.Error(err)
	}
	want := [][]byte{
		[]byte("\x16k@\xb4J\xbaK\xd6"),
		[]byte("\x06\xe7\xea\"Βp\x8f"),
		[]byte("N\xb1\x90ɢ\xfa\x16\x9c"),
		[]byte("\xd2\xfd\x88g\xd5\r-\xfe"),
		[]byte("p\xbb\x02<\x81\f\xa8z"),
		[]byte("\xf0\x98H\n\xc4ľq"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}
}

func TestHashVerify(t *testing.T) {
	success, err := hash.Verify(nil, 1, []byte("\x16k@\xb4J\xbaK\xd6"))
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Errorf("Verify(): %+v, want true", success)
	}
}

func TestHashReverseMap(t *testing.T) {
	got, err := hash.(planbuilder.Reversible).ReverseMap(nil, []byte("\x16k@\xb4J\xbaK\xd6"))
	if err != nil {
		t.Error(err)
	}
	if got.(int64) != 1 {
		t.Errorf("ReverseMap(): %+v, want 1", got)
	}
}

func TestHashCreate(t *testing.T) {
	vc := &vcursor{}
	err := hash.(planbuilder.Functional).Create(vc, 1)
	if err != nil {
		t.Error(err)
	}
	wantQuery := &querytypes.BoundQuery{
		Sql: "insert into t(c) values(:c)",
		BindVariables: map[string]interface{}{
			"c": 1,
		},
	}
	if !reflect.DeepEqual(vc.query, wantQuery) {
		t.Errorf("vc.query = %#v, want %#v", vc.query, wantQuery)
	}
}

func TestHashGenerate(t *testing.T) {
	_, ok := hash.(planbuilder.FunctionalGenerator)
	if ok {
		t.Errorf("hash.(planbuilder.FunctionalGenerator): true, want false")
	}
}

func TestHashDelete(t *testing.T) {
	vc := &vcursor{}
	err := hash.(planbuilder.Functional).Delete(vc, []interface{}{1}, []byte{})
	if err != nil {
		t.Error(err)
	}
	wantQuery := &querytypes.BoundQuery{
		Sql: "delete from t where c in ::c",
		BindVariables: map[string]interface{}{
			"c": []interface{}{1},
		},
	}
	if !reflect.DeepEqual(vc.query, wantQuery) {
		t.Errorf("vc.query = %#v, want %#v", vc.query, wantQuery)
	}
}
