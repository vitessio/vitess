// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"reflect"
	"strings"
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

type vcursormulti struct {
	query *tproto.BoundQuery
}

func (vc *vcursormulti) Execute(query *tproto.BoundQuery) (*mproto.QueryResult, error) {
	vc.query = query
	switch {
	case strings.HasPrefix(query.Sql, "select"):
		return &mproto.QueryResult{
			Fields: []mproto.Field{{
				Type: mproto.VT_LONG,
			}},
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
					sqltypes.MakeNumeric([]byte("1")),
				},
				[]sqltypes.Value{
					sqltypes.MakeNumeric([]byte("2")),
				},
			},
			RowsAffected: 1,
		}, nil
	case strings.HasPrefix(query.Sql, "insert"):
		return &mproto.QueryResult{InsertId: 1}, nil
	case strings.HasPrefix(query.Sql, "delete"):
		return &mproto.QueryResult{}, nil
	}
	panic("unexpected")
}

func TestLookupHashMultiCost(t *testing.T) {
	if lhm.Cost() != 20 {
		t.Errorf("Cost(): %d, want 20", lhm.Cost())
	}
}

func TestLookupHashMultiMap(t *testing.T) {
	vc := &vcursormulti{}
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

func TestLookupHashMultiVerify(t *testing.T) {
	vc := &vcursormulti{}
	success, err := lhm.Verify(vc, 1, "\x16k@\xb4J\xbaK\xd6")
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Errorf("Verify(): %+v, want true", success)
	}
}

func TestLookupHashMultiCreate(t *testing.T) {
	vc := &vcursormulti{}
	err := lhm.Create(vc, 1, "\x16k@\xb4J\xbaK\xd6")
	if err != nil {
		t.Error(err)
	}
	wantQuery := &tproto.BoundQuery{
		Sql: "insert into t(fromc, toc) values(:fromc, :toc)",
		BindVariables: map[string]interface{}{
			"fromc": 1,
			"toc":   uint64(1),
		},
	}
	if !reflect.DeepEqual(vc.query, wantQuery) {
		t.Errorf("vc.query = %#v, want %#v", vc.query, wantQuery)
	}
}

func TestLookupHashMultiDelete(t *testing.T) {
	vc := &vcursormulti{}
	err := lhm.Delete(vc, []interface{}{1}, "\x16k@\xb4J\xbaK\xd6")
	if err != nil {
		t.Error(err)
	}
	wantQuery := &tproto.BoundQuery{
		Sql: "delete from t where fromc in ::fromc and toc = :toc",
		BindVariables: map[string]interface{}{
			"fromc": []interface{}{1},
			"toc":   uint64(1),
		},
	}
	if !reflect.DeepEqual(vc.query, wantQuery) {
		t.Errorf("vc.query = %#v, want %#v", vc.query, wantQuery)
	}
}
