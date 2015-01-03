// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/vt/key"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
)

var lhm planbuilder.Vindex

func init() {
	h, err := planbuilder.CreateVindex("lookup_hash", map[string]interface{}{"Table": "t", "From": "fromc", "To": "toc"})
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

func TestLookupHashVerify(t *testing.T) {
	vc := &vcursor{numRows: 1}
	success, err := lhm.Verify(vc, 1, "\x16k@\xb4J\xbaK\xd6")
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Errorf("Verify(): %+v, want true", success)
	}
}

func TestLookupHashCreate(t *testing.T) {
	vc := &vcursor{}
	err := lhm.(planbuilder.Lookup).Create(vc, 1, "\x16k@\xb4J\xbaK\xd6")
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

func TestLookupHashGenerate(t *testing.T) {
	_, ok := lhm.(planbuilder.LookupGenerator)
	if ok {
		t.Errorf("lhm.(planbuilder.LookupGenerator): true, want false")
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
	err := lhm.(planbuilder.Lookup).Delete(vc, []interface{}{1}, "\x16k@\xb4J\xbaK\xd6")
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
