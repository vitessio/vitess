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

var lhu planbuilder.Vindex

func init() {
	h, err := planbuilder.CreateVindex("lookup_hash_unique", "nn", map[string]interface{}{"Table": "t", "From": "fromc", "To": "toc"})
	if err != nil {
		panic(err)
	}
	lhu = h
}

func TestLookupHashUniqueCost(t *testing.T) {
	if lhu.Cost() != 10 {
		t.Errorf("Cost(): %d, want 10", lhu.Cost())
	}
}

func TestLookupHashUniqueMap(t *testing.T) {
	vc := &vcursor{numRows: 1}
	got, err := lhu.(planbuilder.Unique).Map(vc, []interface{}{1, int32(2)})
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

func TestLookupHashUniqueVerify(t *testing.T) {
	vc := &vcursor{numRows: 1}
	success, err := lhu.Verify(vc, 1, []byte("\x16k@\xb4J\xbaK\xd6"))
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Errorf("Verify(): %+v, want true", success)
	}
}

func TestLookupHashUniqueCreate(t *testing.T) {
	vc := &vcursor{}
	err := lhu.(planbuilder.Lookup).Create(vc, 1, []byte("\x16k@\xb4J\xbaK\xd6"))
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

func TestLookupHashUniqueReverse(t *testing.T) {
	_, ok := lhu.(planbuilder.Reversible)
	if ok {
		t.Errorf("lhu.(planbuilder.Reversible): true, want false")
	}
}

func TestLookupHashUniqueDelete(t *testing.T) {
	vc := &vcursor{}
	err := lhu.(planbuilder.Lookup).Delete(vc, []interface{}{1}, []byte("\x16k@\xb4J\xbaK\xd6"))
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
