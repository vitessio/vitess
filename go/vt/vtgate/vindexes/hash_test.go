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

var hash *HashVindex

func init() {
	h, err := NewHashVindex(map[string]interface{}{"Table": "t", "Column": "c"})
	if err != nil {
		panic(err)
	}
	hash = h.(*HashVindex)
}

func TestHashConvert(t *testing.T) {
	cases := []struct {
		in  uint64
		out string
	}{
		{1, "\x16k@\xb4J\xbaK\xd6"},
		{0, "\x8c\xa6M\xe9\xc1\xb1#\xa7"},
		{11, "\xae\xfcDI\x1c\xfeGL"},
		{0x100000000000000, "\r\x9f'\x9b\xa5\xd8r`"},
		{0x800000000000000, " \xb9\xe7g\xb2\xfb\x14V"},
		{11, "\xae\xfcDI\x1c\xfeGL"},
		{0, "\x8c\xa6M\xe9\xc1\xb1#\xa7"},
	}
	for _, c := range cases {
		got := string(vhash(c.in))
		want := c.out
		if got != want {
			t.Errorf("vhash(%d): %#v, want %q", c.in, got, want)
		}
		back := vunhash(key.KeyspaceId(got))
		if back != c.in {
			t.Errorf("vunhash(%q): %d, want %d", got, back, c.in)
		}
	}
}

func BenchmarkHashConvert(b *testing.B) {
	for i := 0; i < b.N; i++ {
		vhash(uint64(i))
	}
}

func TestHashCost(t *testing.T) {
	if hash.Cost() != 1 {
		t.Errorf("Cost(): %d, want 1", hash.Cost())
	}
}

func TestHashMap(t *testing.T) {
	got, err := hash.Map(nil, []interface{}{1, int32(2), int64(3), uint(4), uint32(5), uint64(6)})
	if err != nil {
		t.Error(err)
	}
	want := []key.KeyspaceId{
		"\x16k@\xb4J\xbaK\xd6",
		"\x06\xe7\xea\"Βp\x8f",
		"N\xb1\x90ɢ\xfa\x16\x9c",
		"\xd2\xfd\x88g\xd5\r-\xfe",
		"p\xbb\x02<\x81\f\xa8z",
		"\xf0\x98H\n\xc4ľq",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}
}

func TestHashVerify(t *testing.T) {
	success, err := hash.Verify(nil, 1, "\x16k@\xb4J\xbaK\xd6")
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Errorf("Verify(): %+v, want true", success)
	}
}

func TestHashReverseMap(t *testing.T) {
	got, err := hash.ReverseMap(nil, "\x16k@\xb4J\xbaK\xd6")
	if err != nil {
		t.Error(err)
	}
	if got.(uint64) != 1 {
		t.Errorf("ReverseMap(): %+v, want 1", got)
	}
}

type vcursor struct {
	query *tproto.BoundQuery
}

func (vc *vcursor) Execute(query *tproto.BoundQuery) (*mproto.QueryResult, error) {
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

func TestHashCreate(t *testing.T) {
	vc := &vcursor{}
	err := hash.Create(vc, 1)
	if err != nil {
		t.Error(err)
	}
	wantQuery := &tproto.BoundQuery{
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
	vc := &vcursor{}
	got, err := hash.Generate(vc)
	if err != nil {
		t.Error(err)
	}
	if got != 1 {
		t.Errorf("Generate(): %+v, want 1", got)
	}
	wantQuery := &tproto.BoundQuery{
		Sql: "insert into t(c) values(:c)",
		BindVariables: map[string]interface{}{
			"c": nil,
		},
	}
	if !reflect.DeepEqual(vc.query, wantQuery) {
		t.Errorf("vc.query = %#v, want %#v", vc.query, wantQuery)
	}
}

func TestHashDelete(t *testing.T) {
	vc := &vcursor{}
	err := hash.Delete(vc, []interface{}{1}, "")
	if err != nil {
		t.Error(err)
	}
	wantQuery := &tproto.BoundQuery{
		Sql: "delete from t where c in ::c",
		BindVariables: map[string]interface{}{
			"c": []interface{}{1},
		},
	}
	if !reflect.DeepEqual(vc.query, wantQuery) {
		t.Errorf("vc.query = %#v, want %#v", vc.query, wantQuery)
	}
}
