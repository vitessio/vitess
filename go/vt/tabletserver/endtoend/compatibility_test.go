// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package endtoend

import (
	"reflect"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/tabletserver/endtoend/framework"
)

func TestCharaterSet(t *testing.T) {
	qr, err := framework.NewClient().Execute("select * from vitess_test where intval=1", nil)
	if err != nil {
		t.Error(err)
		return
	}
	want := sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "intval",
				Type: sqltypes.Int32,
			}, {
				Name: "floatval",
				Type: sqltypes.Float32,
			}, {
				Name: "charval",
				Type: sqltypes.VarChar,
			}, {
				Name: "binval",
				Type: sqltypes.VarBinary,
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
				sqltypes.MakeTrusted(sqltypes.Float32, []byte("1.12345")),
				sqltypes.MakeTrusted(sqltypes.VarChar, []byte("\xc2\xa2")),
				sqltypes.MakeTrusted(sqltypes.VarBinary, []byte("\x00\xff")),
			},
		},
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", *qr, want)
	}
}

func TestInts(t *testing.T) {
	client := framework.NewClient()
	defer client.Execute("delete from vitess_ints", nil)

	_, err := client.Execute(
		"insert into vitess_ints values(:tiny, :tinyu, :small, "+
			":smallu, :medium, :mediumu, :normal, :normalu, :big, :bigu, :year)",
		map[string]interface{}{
			"tiny":    int32(-128),
			"tinyu":   uint32(255),
			"small":   int32(-32768),
			"smallu":  uint32(65535),
			"medium":  int32(-8388608),
			"mediumu": uint32(16777215),
			"normal":  int64(-2147483648),
			"normalu": uint64(4294967295),
			"big":     int64(-9223372036854775808),
			"bigu":    uint64(18446744073709551615),
			"year":    2012,
		},
	)
	if err != nil {
		t.Error(err)
		return
	}
	qr, err := client.Execute("select * from vitess_ints where tiny = -128", nil)
	if err != nil {
		t.Error(err)
		return
	}
	want := sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "tiny",
				Type: sqltypes.Int8,
			}, {
				Name: "tinyu",
				Type: sqltypes.Uint8,
			}, {
				Name: "small",
				Type: sqltypes.Int16,
			}, {
				Name: "smallu",
				Type: sqltypes.Uint16,
			}, {
				Name: "medium",
				Type: sqltypes.Int24,
			}, {
				Name: "mediumu",
				Type: sqltypes.Uint24,
			}, {
				Name: "normal",
				Type: sqltypes.Int32,
			}, {
				Name: "normalu",
				Type: sqltypes.Uint32,
			}, {
				Name: "big",
				Type: sqltypes.Int64,
			}, {
				Name: "bigu",
				Type: sqltypes.Uint64,
			}, {
				Name: "y",
				Type: sqltypes.Year,
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(sqltypes.Int8, []byte("-128")),
				sqltypes.MakeTrusted(sqltypes.Uint8, []byte("255")),
				sqltypes.MakeTrusted(sqltypes.Int16, []byte("-32768")),
				sqltypes.MakeTrusted(sqltypes.Uint16, []byte("65535")),
				sqltypes.MakeTrusted(sqltypes.Int24, []byte("-8388608")),
				sqltypes.MakeTrusted(sqltypes.Uint24, []byte("16777215")),
				sqltypes.MakeTrusted(sqltypes.Int32, []byte("-2147483648")),
				sqltypes.MakeTrusted(sqltypes.Uint32, []byte("4294967295")),
				sqltypes.MakeTrusted(sqltypes.Int64, []byte("-9223372036854775808")),
				sqltypes.MakeTrusted(sqltypes.Uint64, []byte("18446744073709551615")),
				sqltypes.MakeTrusted(sqltypes.Year, []byte("2012")),
			},
		},
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", *qr, want)
	}
}

func TestFractionals(t *testing.T) {
	client := framework.NewClient()
	defer client.Execute("delete from vitess_fracts", nil)

	_, err := client.Execute(
		"insert into vitess_fracts values(:id, :deci, :num, :f, :d)",
		map[string]interface{}{
			"id":   1,
			"deci": "1.99",
			"num":  "2.99",
			"f":    3.99,
			"d":    4.99,
		},
	)
	if err != nil {
		t.Error(err)
		return
	}
	qr, err := client.Execute("select * from vitess_fracts where id = 1", nil)
	if err != nil {
		t.Error(err)
		return
	}
	want := sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: sqltypes.Int32,
			}, {
				Name: "deci",
				Type: sqltypes.Decimal,
			}, {
				Name: "num",
				Type: sqltypes.Decimal,
			}, {
				Name: "f",
				Type: sqltypes.Float32,
			}, {
				Name: "d",
				Type: sqltypes.Float64,
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
				sqltypes.MakeTrusted(sqltypes.Decimal, []byte("1.99")),
				sqltypes.MakeTrusted(sqltypes.Decimal, []byte("2.99")),
				sqltypes.MakeTrusted(sqltypes.Float32, []byte("3.99")),
				sqltypes.MakeTrusted(sqltypes.Float64, []byte("4.99")),
			},
		},
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", *qr, want)
	}
}

func TestStrings(t *testing.T) {
	client := framework.NewClient()
	defer client.Execute("delete from vitess_strings", nil)

	_, err := client.Execute(
		"insert into vitess_strings values "+
			"(:vb, :c, :vc, :b, :tb, :bl, :ttx, :tx, :en, :s)",
		map[string]interface{}{
			"vb":  "a",
			"c":   "b",
			"vc":  "c",
			"b":   "d",
			"tb":  "e",
			"bl":  "f",
			"ttx": "g",
			"tx":  "h",
			"en":  "a",
			"s":   "a,b",
		},
	)
	if err != nil {
		t.Error(err)
		return
	}
	qr, err := client.Execute("select * from vitess_strings where vb = 'a'", nil)
	if err != nil {
		t.Error(err)
		return
	}
	want := sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "vb",
				Type: sqltypes.VarBinary,
			}, {
				Name: "c",
				Type: sqltypes.Char,
			}, {
				Name: "vc",
				Type: sqltypes.VarChar,
			}, {
				Name: "b",
				Type: sqltypes.Binary,
			}, {
				Name: "tb",
				Type: sqltypes.Blob,
			}, {
				Name: "bl",
				Type: sqltypes.Blob,
			}, {
				Name: "ttx",
				Type: sqltypes.Text,
			}, {
				Name: "tx",
				Type: sqltypes.Text,
			}, {
				Name: "en",
				Type: sqltypes.Enum,
			}, {
				Name: "s",
				Type: sqltypes.Set,
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(sqltypes.VarBinary, []byte("a")),
				sqltypes.MakeTrusted(sqltypes.Char, []byte("b")),
				sqltypes.MakeTrusted(sqltypes.VarChar, []byte("c")),
				sqltypes.MakeTrusted(sqltypes.Binary, []byte("d\x00\x00\x00")),
				sqltypes.MakeTrusted(sqltypes.Blob, []byte("e")),
				sqltypes.MakeTrusted(sqltypes.Blob, []byte("f")),
				sqltypes.MakeTrusted(sqltypes.Text, []byte("g")),
				sqltypes.MakeTrusted(sqltypes.Text, []byte("h")),
				sqltypes.MakeTrusted(sqltypes.Enum, []byte("a")),
				sqltypes.MakeTrusted(sqltypes.Set, []byte("a,b")),
			},
		},
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", *qr, want)
	}
}

func TestMiscTypes(t *testing.T) {
	client := framework.NewClient()
	defer client.Execute("delete from vitess_misc", nil)

	_, err := client.Execute(
		"insert into vitess_misc values(:id, :b, :d, :dt, :t)",
		map[string]interface{}{
			"id": 1,
			"b":  "\x01",
			"d":  "2012-01-01",
			"dt": "2012-01-01 15:45:45",
			"t":  "15:45:45",
		},
	)
	if err != nil {
		t.Error(err)
		return
	}
	qr, err := client.Execute("select * from vitess_misc where id = 1", nil)
	if err != nil {
		t.Error(err)
		return
	}
	want := sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: sqltypes.Int32,
			}, {
				Name: "b",
				Type: sqltypes.Bit,
			}, {
				Name: "d",
				Type: sqltypes.Date,
			}, {
				Name: "dt",
				Type: sqltypes.Datetime,
			}, {
				Name: "t",
				Type: sqltypes.Time,
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
				sqltypes.MakeTrusted(sqltypes.Bit, []byte("\x01")),
				sqltypes.MakeTrusted(sqltypes.Date, []byte("2012-01-01")),
				sqltypes.MakeTrusted(sqltypes.Datetime, []byte("2012-01-01 15:45:45")),
				sqltypes.MakeTrusted(sqltypes.Time, []byte("15:45:45")),
			},
		},
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", *qr, want)
	}
}

func TestNull(t *testing.T) {
	client := framework.NewClient()
	qr, err := client.Execute("select null from dual", nil)
	if err != nil {
		t.Error(err)
		return
	}
	want := sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "NULL",
				Type: sqltypes.Null,
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				{},
			},
		},
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", *qr, want)
	}
}

func TestTypeLimits(t *testing.T) {
	client := framework.NewClient()
	defer func() {
		for _, cleanup := range []string{
			"delete from vitess_ints",
			"delete from vitess_fracts",
			"delete from vitess_strings",
		} {
			_, err := client.Execute(cleanup, nil)
			if err != nil {
				t.Error(err)
			}
		}
	}()

	for _, query := range []string{
		"insert into vitess_ints(tiny, medium) values(1, -129)",
		"insert into vitess_fracts(id, num) values(1, 1)",
		"insert into vitess_strings(vb) values('a')",
	} {
		_, err := client.Execute(query, nil)
		if err != nil {
			t.Error(err)
			return
		}
	}

	want := "error: type mismatch"
	mismatchCases := []struct {
		query string
		bv    map[string]interface{}
	}{{
		query: "insert into vitess_ints(tiny) values('str')",
		bv:    nil,
	}, {
		query: "insert into vitess_ints(tiny) values(:str)",
		bv:    map[string]interface{}{"str": "str"},
	}, {
		query: "insert into vitess_ints(tiny) values(1.2)",
		bv:    nil,
	}, {
		query: "insert into vitess_ints(tiny) values(:fl)",
		bv:    map[string]interface{}{"fl": 1.2},
	}, {
		query: "insert into vitess_strings(vb) values(1)",
		bv:    nil,
	}, {
		query: "insert into vitess_strings(vb) values(:id)",
		bv:    map[string]interface{}{"id": 1},
	}, {
		query: "insert into vitess_strings(vb) select tiny from vitess_ints",
		bv:    nil,
	}, {
		query: "insert into vitess_ints(tiny) select num from vitess_fracts",
		bv:    nil,
	}, {
		query: "insert into vitess_ints(tiny) select vb from vitess_strings",
		bv:    nil,
	}}
	for _, request := range mismatchCases {
		_, err := client.Execute(request.query, request.bv)
		if err == nil || !strings.HasPrefix(err.Error(), want) {
			t.Errorf("Error(%s): %v, want %s", request.query, err, want)
		}
	}

	want = "error: Out of range"
	for _, query := range []string{
		"insert into vitess_ints(tiny) values(-129)",
		"insert into vitess_ints(tiny) select medium from vitess_ints",
	} {
		_, err := client.Execute(query, nil)
		if err == nil || !strings.HasPrefix(err.Error(), want) {
			t.Errorf("Error(%s): %v, want %s", query, err, want)
		}
	}

	want = "error: Data too long"
	_, err := client.Execute("insert into vitess_strings(vb) values('12345678901234567')", nil)
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("Error: %v, want %s", err, want)
	}
}
