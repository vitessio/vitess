// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package endtoend

import (
	"reflect"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/mysql"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/tabletserver/endtoend/framework"
)

func TestCharaterSet(t *testing.T) {
	qr, err := framework.NewClient().Execute("select * from vitess_test where intval=1", nil)
	if err != nil {
		t.Error(err)
		return
	}
	want := mproto.QueryResult{
		Fields: []mproto.Field{
			{
				Name:  "intval",
				Type:  3,
				Flags: 0,
			}, {
				Name:  "floatval",
				Type:  4,
				Flags: 0,
			}, {
				Name:  "charval",
				Type:  253,
				Flags: 0,
			}, {
				Name:  "binval",
				Type:  253,
				Flags: mysql.FlagBinary,
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{
				sqltypes.Value{Inner: sqltypes.Numeric("1")},
				sqltypes.Value{Inner: sqltypes.Fractional("1.12345")},
				sqltypes.Value{Inner: sqltypes.String("\xc2\xa2")},
				sqltypes.Value{Inner: sqltypes.String("\x00\xff")},
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
	want := mproto.QueryResult{
		Fields: []mproto.Field{
			{
				Name:  "tiny",
				Type:  mysql.TypeTiny,
				Flags: 0,
			}, {
				Name:  "tinyu",
				Type:  mysql.TypeTiny,
				Flags: mysql.FlagUnsigned,
			}, {
				Name:  "small",
				Type:  mysql.TypeShort,
				Flags: 0,
			}, {
				Name:  "smallu",
				Type:  mysql.TypeShort,
				Flags: mysql.FlagUnsigned,
			}, {
				Name:  "medium",
				Type:  mysql.TypeInt24,
				Flags: 0,
			}, {
				Name:  "mediumu",
				Type:  mysql.TypeInt24,
				Flags: mysql.FlagUnsigned,
			}, {
				Name:  "normal",
				Type:  mysql.TypeLong,
				Flags: 0,
			}, {
				Name:  "normalu",
				Type:  mysql.TypeLong,
				Flags: mysql.FlagUnsigned,
			}, {
				Name:  "big",
				Type:  mysql.TypeLonglong,
				Flags: 0,
			}, {
				Name:  "bigu",
				Type:  mysql.TypeLonglong,
				Flags: mysql.FlagUnsigned,
			}, {
				Name:  "y",
				Type:  mysql.TypeYear,
				Flags: mysql.FlagUnsigned,
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{
				sqltypes.Value{Inner: sqltypes.Numeric("-128")},
				sqltypes.Value{Inner: sqltypes.Numeric("255")},
				sqltypes.Value{Inner: sqltypes.Numeric("-32768")},
				sqltypes.Value{Inner: sqltypes.Numeric("65535")},
				sqltypes.Value{Inner: sqltypes.Numeric("-8388608")},
				sqltypes.Value{Inner: sqltypes.Numeric("16777215")},
				sqltypes.Value{Inner: sqltypes.Numeric("-2147483648")},
				sqltypes.Value{Inner: sqltypes.Numeric("4294967295")},
				sqltypes.Value{Inner: sqltypes.Numeric("-9223372036854775808")},
				sqltypes.Value{Inner: sqltypes.Numeric("18446744073709551615")},
				sqltypes.Value{Inner: sqltypes.Numeric("2012")},
			},
		},
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", *qr, want)
	}
	wantTypes := []query.Type{
		sqltypes.Int8,
		sqltypes.Uint8,
		sqltypes.Int16,
		sqltypes.Uint16,
		sqltypes.Int24,
		sqltypes.Uint24,
		sqltypes.Int32,
		sqltypes.Uint32,
		sqltypes.Int64,
		sqltypes.Uint64,
		sqltypes.Year,
	}
	for i, field := range qr.Fields {
		got, err := sqltypes.MySQLToType(field.Type, field.Flags)
		if err != nil {
			t.Errorf("col: %d, err: %v", i, err)
			continue
		}
		if got != wantTypes[i] {
			t.Errorf("Unexpected type: col: %d, %d, want %d", i, got, wantTypes[i])
		}
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
	want := mproto.QueryResult{
		Fields: []mproto.Field{
			{
				Name:  "id",
				Type:  mysql.TypeLong,
				Flags: 0,
			}, {
				Name:  "deci",
				Type:  mysql.TypeNewDecimal,
				Flags: 0,
			}, {
				Name:  "num",
				Type:  mysql.TypeNewDecimal,
				Flags: 0,
			}, {
				Name:  "f",
				Type:  mysql.TypeFloat,
				Flags: 0,
			}, {
				Name:  "d",
				Type:  mysql.TypeDouble,
				Flags: 0,
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{
				sqltypes.Value{Inner: sqltypes.Numeric("1")},
				sqltypes.Value{Inner: sqltypes.Fractional("1.99")},
				sqltypes.Value{Inner: sqltypes.Fractional("2.99")},
				sqltypes.Value{Inner: sqltypes.Fractional("3.99")},
				sqltypes.Value{Inner: sqltypes.Fractional("4.99")},
			},
		},
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", *qr, want)
	}
	wantTypes := []query.Type{
		sqltypes.Int32,
		sqltypes.Decimal,
		sqltypes.Decimal,
		sqltypes.Float32,
		sqltypes.Float64,
	}
	for i, field := range qr.Fields {
		got, err := sqltypes.MySQLToType(field.Type, field.Flags)
		if err != nil {
			t.Errorf("col: %d, err: %v", i, err)
			continue
		}
		if got != wantTypes[i] {
			t.Errorf("Unexpected type: col: %d, %d, want %d", i, got, wantTypes[i])
		}
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
	want := mproto.QueryResult{
		Fields: []mproto.Field{
			{
				Name:  "vb",
				Type:  mysql.TypeVarString,
				Flags: mysql.FlagBinary,
			}, {
				Name:  "c",
				Type:  mysql.TypeString,
				Flags: 0,
			}, {
				Name:  "vc",
				Type:  mysql.TypeVarString,
				Flags: 0,
			}, {
				Name:  "b",
				Type:  mysql.TypeString,
				Flags: mysql.FlagBinary,
			}, {
				Name:  "tb",
				Type:  mysql.TypeBlob,
				Flags: mysql.FlagBinary,
			}, {
				Name:  "bl",
				Type:  mysql.TypeBlob,
				Flags: mysql.FlagBinary,
			}, {
				Name:  "ttx",
				Type:  mysql.TypeBlob,
				Flags: 0,
			}, {
				Name:  "tx",
				Type:  mysql.TypeBlob,
				Flags: 0,
			}, {
				Name:  "en",
				Type:  mysql.TypeString,
				Flags: mysql.FlagEnum,
			}, {
				Name:  "s",
				Type:  mysql.TypeString,
				Flags: mysql.FlagSet,
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{
				sqltypes.Value{Inner: sqltypes.String("a")},
				sqltypes.Value{Inner: sqltypes.String("b")},
				sqltypes.Value{Inner: sqltypes.String("c")},
				sqltypes.Value{Inner: sqltypes.String("d\x00\x00\x00")},
				sqltypes.Value{Inner: sqltypes.String("e")},
				sqltypes.Value{Inner: sqltypes.String("f")},
				sqltypes.Value{Inner: sqltypes.String("g")},
				sqltypes.Value{Inner: sqltypes.String("h")},
				sqltypes.Value{Inner: sqltypes.String("a")},
				sqltypes.Value{Inner: sqltypes.String("a,b")},
			},
		},
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", *qr, want)
	}
	wantTypes := []query.Type{
		sqltypes.VarBinary,
		sqltypes.Char,
		sqltypes.VarChar,
		sqltypes.Binary,
		sqltypes.Blob,
		sqltypes.Blob,
		sqltypes.Text,
		sqltypes.Text,
		sqltypes.Enum,
		sqltypes.Set,
	}
	for i, field := range qr.Fields {
		got, err := sqltypes.MySQLToType(field.Type, field.Flags)
		if err != nil {
			t.Errorf("col: %d, err: %v", i, err)
			continue
		}
		if got != wantTypes[i] {
			t.Errorf("Unexpected type: col: %d, %d, want %d", i, got, wantTypes[i])
		}
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
	want := mproto.QueryResult{
		Fields: []mproto.Field{
			{
				Name:  "id",
				Type:  mysql.TypeLong,
				Flags: 0,
			}, {
				Name:  "b",
				Type:  mysql.TypeBit,
				Flags: mysql.FlagUnsigned,
			}, {
				Name:  "d",
				Type:  mysql.TypeDate,
				Flags: mysql.FlagBinary,
			}, {
				Name:  "dt",
				Type:  mysql.TypeDatetime,
				Flags: mysql.FlagBinary,
			}, {
				Name:  "t",
				Type:  mysql.TypeTime,
				Flags: mysql.FlagBinary,
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{
				sqltypes.Value{Inner: sqltypes.Numeric("1")},
				sqltypes.Value{Inner: sqltypes.String("\x01")},
				sqltypes.Value{Inner: sqltypes.String("2012-01-01")},
				sqltypes.Value{Inner: sqltypes.String("2012-01-01 15:45:45")},
				sqltypes.Value{Inner: sqltypes.String("15:45:45")},
			},
		},
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", *qr, want)
	}
	wantTypes := []query.Type{
		sqltypes.Int32,
		sqltypes.Bit,
		sqltypes.Date,
		sqltypes.Datetime,
		sqltypes.Time,
	}
	for i, field := range qr.Fields {
		got, err := sqltypes.MySQLToType(field.Type, field.Flags)
		if err != nil {
			t.Errorf("col: %d, err: %v", i, err)
			continue
		}
		if got != wantTypes[i] {
			t.Errorf("Unexpected type: col: %d, %d, want %d", i, got, wantTypes[i])
		}
	}
}

func TestNull(t *testing.T) {
	client := framework.NewClient()
	qr, err := client.Execute("select null from dual", nil)
	if err != nil {
		t.Error(err)
		return
	}
	want := mproto.QueryResult{
		Fields: []mproto.Field{
			{
				Name:  "NULL",
				Type:  mysql.TypeNull,
				Flags: mysql.FlagBinary,
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{
				sqltypes.Value{},
			},
		},
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", *qr, want)
	}
	wantTypes := []query.Type{
		sqltypes.Null,
	}
	for i, field := range qr.Fields {
		got, err := sqltypes.MySQLToType(field.Type, field.Flags)
		if err != nil {
			t.Errorf("col: %d, err: %v", i, err)
			continue
		}
		if got != wantTypes[i] {
			t.Errorf("Unexpected type: col: %d, %d, want %d", i, got, wantTypes[i])
		}
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
