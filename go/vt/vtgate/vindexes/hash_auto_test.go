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

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/key"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
)

var hashAuto planbuilder.Vindex

func init() {
	hv, err := planbuilder.CreateVindex("hash_autoinc", map[string]interface{}{"Table": "t", "Column": "c"})
	if err != nil {
		panic(err)
	}
	hashAuto = hv
}

func TestHashAutoConvert(t *testing.T) {
	cases := []struct {
		in  int64
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
		vhash(int64(i))
	}
}

func TestHashAutoCost(t *testing.T) {
	if hashAuto.Cost() != 1 {
		t.Errorf("Cost(): %d, want 1", hashAuto.Cost())
	}
}

func TestHashAutoMap(t *testing.T) {
	got, err := hashAuto.(planbuilder.Unique).Map(nil, []interface{}{1, int32(2), int64(3), uint(4), uint32(5), uint64(6)})
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

func TestHashAutoMapFail(t *testing.T) {
	_, err := hashAuto.(planbuilder.Unique).Map(nil, []interface{}{1.1})
	want := "hash.Map: unexpected type for 1.1: float64"
	if err == nil || err.Error() != want {
		t.Errorf("hashAuto.Map: %v, want %v", err, want)
	}
}

func TestHashAutoVerify(t *testing.T) {
	success, err := hashAuto.Verify(nil, 1, "\x16k@\xb4J\xbaK\xd6")
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Errorf("Verify(): %+v, want true", success)
	}
}

func TestHashAutoVerifyFail(t *testing.T) {
	_, err := hashAuto.Verify(nil, 1.1, "\x16k@\xb4J\xbaK\xd6")
	want := "hash.Verify: unexpected type for 1.1: float64"
	if err == nil || err.Error() != want {
		t.Errorf("hashAuto.Verify: %v, want %v", err, want)
	}
}

func TestHashAutoReverseMap(t *testing.T) {
	got, err := hashAuto.(planbuilder.Reversible).ReverseMap(nil, "\x16k@\xb4J\xbaK\xd6")
	if err != nil {
		t.Error(err)
	}
	if got.(int64) != 1 {
		t.Errorf("ReverseMap(): %+v, want 1", got)
	}
}

type vcursor struct {
	mustFail bool
	numRows  int
	result   *mproto.QueryResult
	query    *tproto.BoundQuery
}

func (vc *vcursor) Execute(query *tproto.BoundQuery) (*mproto.QueryResult, error) {
	vc.query = query
	if vc.mustFail {
		return nil, errors.New("Execute failed")
	}
	switch {
	case strings.HasPrefix(query.Sql, "select"):
		if vc.result != nil {
			return vc.result, nil
		}
		result := &mproto.QueryResult{
			Fields: []mproto.Field{{
				Type: mproto.VT_LONG,
			}},
			RowsAffected: uint64(vc.numRows),
		}
		for i := 0; i < vc.numRows; i++ {
			result.Rows = append(result.Rows, []sqltypes.Value{
				sqltypes.MakeNumeric([]byte(fmt.Sprintf("%d", i+1))),
			})
		}
		return result, nil
	case strings.HasPrefix(query.Sql, "insert"):
		return &mproto.QueryResult{InsertId: 1}, nil
	case strings.HasPrefix(query.Sql, "delete"):
		return &mproto.QueryResult{}, nil
	}
	panic("unexpected")
}

func TestHashAutoCreate(t *testing.T) {
	vc := &vcursor{}
	err := hashAuto.(planbuilder.Functional).Create(vc, 1)
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

func TestHashAutoCreateFail(t *testing.T) {
	vc := &vcursor{mustFail: true}
	err := hashAuto.(planbuilder.Functional).Create(vc, 1)
	want := "hash.Create: Execute failed"
	if err == nil || err.Error() != want {
		t.Errorf("hashAuto.Create: %v, want %v", err, want)
	}
}

func TestHashAutoGenerate(t *testing.T) {
	vc := &vcursor{}
	got, err := hashAuto.(planbuilder.FunctionalGenerator).Generate(vc)
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

func TestHashAutoGenerateFail(t *testing.T) {
	vc := &vcursor{mustFail: true}
	_, err := hashAuto.(planbuilder.FunctionalGenerator).Generate(vc)
	want := "hash.Generate: Execute failed"
	if err == nil || err.Error() != want {
		t.Errorf("hashAuto.Generate: %v, want %v", err, want)
	}
}

func TestHashAutoDelete(t *testing.T) {
	vc := &vcursor{}
	err := hashAuto.(planbuilder.Functional).Delete(vc, []interface{}{1}, "")
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

func TestHashAutoDeleteFail(t *testing.T) {
	vc := &vcursor{mustFail: true}
	err := hashAuto.(planbuilder.Functional).Delete(vc, []interface{}{1}, "")
	want := "hash.Delete: Execute failed"
	if err == nil || err.Error() != want {
		t.Errorf("hashAuto.Delete: %v, want %v", err, want)
	}
}
