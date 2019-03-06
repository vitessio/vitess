/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package endtoend

import (
	"reflect"
	"strings"
	"testing"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
)

var point12 = "\x00\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@"

func TestCharaterSet(t *testing.T) {
	qr, err := framework.NewClient().Execute("select * from vitess_test where intval=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	want := sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:         "intval",
				Type:         sqltypes.Int32,
				Table:        "vitess_test",
				OrgTable:     "vitess_test",
				Database:     "vttest",
				OrgName:      "intval",
				ColumnLength: 11,
				Charset:      63,
				Flags:        49155,
			}, {
				Name:         "floatval",
				Type:         sqltypes.Float32,
				Table:        "vitess_test",
				OrgTable:     "vitess_test",
				Database:     "vttest",
				OrgName:      "floatval",
				ColumnLength: 12,
				Charset:      63,
				Decimals:     31,
				Flags:        32768,
			}, {
				Name:         "charval",
				Type:         sqltypes.VarChar,
				Table:        "vitess_test",
				OrgTable:     "vitess_test",
				Database:     "vttest",
				OrgName:      "charval",
				ColumnLength: 30,
				Charset:      33,
			}, {
				Name:         "binval",
				Type:         sqltypes.VarBinary,
				Table:        "vitess_test",
				OrgTable:     "vitess_test",
				Database:     "vttest",
				OrgName:      "binval",
				ColumnLength: 256,
				Charset:      63,
				Flags:        128,
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.TestValue(sqltypes.Int32, "1"),
				sqltypes.TestValue(sqltypes.Float32, "1.12345"),
				sqltypes.TestValue(sqltypes.VarChar, "\xc2\xa2"),
				sqltypes.TestValue(sqltypes.VarBinary, "\x00\xff"),
			},
		},
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", prettyPrint(*qr), prettyPrint(want))
	}
}

func TestInts(t *testing.T) {
	client := framework.NewClient()
	defer client.Execute("delete from vitess_ints", nil)

	_, err := client.Execute(
		"insert into vitess_ints values(:tiny, :tinyu, :small, "+
			":smallu, :medium, :mediumu, :normal, :normalu, :big, :bigu, :year)",
		map[string]*querypb.BindVariable{
			"tiny":    sqltypes.Int64BindVariable(-128),
			"tinyu":   sqltypes.Uint64BindVariable(255),
			"small":   sqltypes.Int64BindVariable(-32768),
			"smallu":  sqltypes.Uint64BindVariable(65535),
			"medium":  sqltypes.Int64BindVariable(-8388608),
			"mediumu": sqltypes.Uint64BindVariable(16777215),
			"normal":  sqltypes.Int64BindVariable(-2147483648),
			"normalu": sqltypes.Uint64BindVariable(4294967295),
			"big":     sqltypes.Int64BindVariable(-9223372036854775808),
			"bigu":    sqltypes.Uint64BindVariable(18446744073709551615),
			"year":    sqltypes.Int64BindVariable(2012),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	qr, err := client.Execute("select * from vitess_ints where tiny = -128", nil)
	if err != nil {
		t.Fatal(err)
	}
	want := sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:         "tiny",
				Type:         sqltypes.Int8,
				Table:        "vitess_ints",
				OrgTable:     "vitess_ints",
				Database:     "vttest",
				OrgName:      "tiny",
				ColumnLength: 4,
				Charset:      63,
				Flags:        49155,
			}, {
				Name:         "tinyu",
				Type:         sqltypes.Uint8,
				Table:        "vitess_ints",
				OrgTable:     "vitess_ints",
				Database:     "vttest",
				OrgName:      "tinyu",
				ColumnLength: 3,
				Charset:      63,
				Flags:        32800,
			}, {
				Name:         "small",
				Type:         sqltypes.Int16,
				Table:        "vitess_ints",
				OrgTable:     "vitess_ints",
				Database:     "vttest",
				OrgName:      "small",
				ColumnLength: 6,
				Charset:      63,
				Flags:        32768,
			}, {
				Name:         "smallu",
				Type:         sqltypes.Uint16,
				Table:        "vitess_ints",
				OrgTable:     "vitess_ints",
				Database:     "vttest",
				OrgName:      "smallu",
				ColumnLength: 5,
				Charset:      63,
				Flags:        32800,
			}, {
				Name:         "medium",
				Type:         sqltypes.Int24,
				Table:        "vitess_ints",
				OrgTable:     "vitess_ints",
				Database:     "vttest",
				OrgName:      "medium",
				ColumnLength: 9,
				Charset:      63,
				Flags:        32768,
			}, {
				Name:         "mediumu",
				Type:         sqltypes.Uint24,
				Table:        "vitess_ints",
				OrgTable:     "vitess_ints",
				Database:     "vttest",
				OrgName:      "mediumu",
				ColumnLength: 8,
				Charset:      63,
				Flags:        32800,
			}, {
				Name:         "normal",
				Type:         sqltypes.Int32,
				Table:        "vitess_ints",
				OrgTable:     "vitess_ints",
				Database:     "vttest",
				OrgName:      "normal",
				ColumnLength: 11,
				Charset:      63,
				Flags:        32768,
			}, {
				Name:         "normalu",
				Type:         sqltypes.Uint32,
				Table:        "vitess_ints",
				OrgTable:     "vitess_ints",
				Database:     "vttest",
				OrgName:      "normalu",
				ColumnLength: 10,
				Charset:      63,
				Flags:        32800,
			}, {
				Name:         "big",
				Type:         sqltypes.Int64,
				Table:        "vitess_ints",
				OrgTable:     "vitess_ints",
				Database:     "vttest",
				OrgName:      "big",
				ColumnLength: 20,
				Charset:      63,
				Flags:        32768,
			}, {
				Name:         "bigu",
				Type:         sqltypes.Uint64,
				Table:        "vitess_ints",
				OrgTable:     "vitess_ints",
				Database:     "vttest",
				OrgName:      "bigu",
				ColumnLength: 20,
				Charset:      63,
				Flags:        32800,
			}, {
				Name:         "y",
				Type:         sqltypes.Year,
				Table:        "vitess_ints",
				OrgTable:     "vitess_ints",
				Database:     "vttest",
				OrgName:      "y",
				ColumnLength: 4,
				Charset:      63,
				Flags:        32864,
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.TestValue(sqltypes.Int8, "-128"),
				sqltypes.TestValue(sqltypes.Uint8, "255"),
				sqltypes.TestValue(sqltypes.Int16, "-32768"),
				sqltypes.TestValue(sqltypes.Uint16, "65535"),
				sqltypes.TestValue(sqltypes.Int24, "-8388608"),
				sqltypes.TestValue(sqltypes.Uint24, "16777215"),
				sqltypes.TestValue(sqltypes.Int32, "-2147483648"),
				sqltypes.TestValue(sqltypes.Uint32, "4294967295"),
				sqltypes.TestValue(sqltypes.Int64, "-9223372036854775808"),
				sqltypes.TestValue(sqltypes.Uint64, "18446744073709551615"),
				sqltypes.TestValue(sqltypes.Year, "2012"),
			},
		},
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", prettyPrint(*qr), prettyPrint(want))
	}
	// This test was added because the following query causes mysql to
	// return flags with both binary and unsigned set. The test ensures
	// that a Uint64 is produced in spite of the stray binary flag.
	qr, err = client.Execute("select max(bigu) from vitess_ints", nil)
	if err != nil {
		t.Fatal(err)
	}
	want = sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:         "max(bigu)",
				Type:         sqltypes.Uint64,
				ColumnLength: 20,
				Charset:      63,
				Flags:        32928,
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.TestValue(sqltypes.Uint64, "18446744073709551615"),
			},
		},
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", prettyPrint(*qr), prettyPrint(want))
	}
}

func TestFractionals(t *testing.T) {
	client := framework.NewClient()
	defer client.Execute("delete from vitess_fracts", nil)

	_, err := client.Execute(
		"insert into vitess_fracts values(:id, :deci, :num, :f, :d)",
		map[string]*querypb.BindVariable{
			"id":   sqltypes.Int64BindVariable(1),
			"deci": sqltypes.StringBindVariable("1.99"),
			"num":  sqltypes.StringBindVariable("2.99"),
			"f":    sqltypes.Float64BindVariable(3.99),
			"d":    sqltypes.Float64BindVariable(4.99),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	qr, err := client.Execute("select * from vitess_fracts where id = 1", nil)
	if err != nil {
		t.Fatal(err)
	}
	want := sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:         "id",
				Type:         sqltypes.Int32,
				Table:        "vitess_fracts",
				OrgTable:     "vitess_fracts",
				Database:     "vttest",
				OrgName:      "id",
				ColumnLength: 11,
				Charset:      63,
				Flags:        49155,
			}, {
				Name:         "deci",
				Type:         sqltypes.Decimal,
				Table:        "vitess_fracts",
				OrgTable:     "vitess_fracts",
				Database:     "vttest",
				OrgName:      "deci",
				ColumnLength: 7,
				Charset:      63,
				Decimals:     2,
				Flags:        32768,
			}, {
				Name:         "num",
				Type:         sqltypes.Decimal,
				Table:        "vitess_fracts",
				OrgTable:     "vitess_fracts",
				Database:     "vttest",
				OrgName:      "num",
				ColumnLength: 7,
				Charset:      63,
				Decimals:     2,
				Flags:        32768,
			}, {
				Name:         "f",
				Type:         sqltypes.Float32,
				Table:        "vitess_fracts",
				OrgTable:     "vitess_fracts",
				Database:     "vttest",
				OrgName:      "f",
				ColumnLength: 12,
				Charset:      63,
				Decimals:     31,
				Flags:        32768,
			}, {
				Name:         "d",
				Type:         sqltypes.Float64,
				Table:        "vitess_fracts",
				OrgTable:     "vitess_fracts",
				Database:     "vttest",
				OrgName:      "d",
				ColumnLength: 22,
				Charset:      63,
				Decimals:     31,
				Flags:        32768,
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.TestValue(sqltypes.Int32, "1"),
				sqltypes.TestValue(sqltypes.Decimal, "1.99"),
				sqltypes.TestValue(sqltypes.Decimal, "2.99"),
				sqltypes.TestValue(sqltypes.Float32, "3.99"),
				sqltypes.TestValue(sqltypes.Float64, "4.99"),
			},
		},
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", prettyPrint(*qr), prettyPrint(want))
	}
}

func TestStrings(t *testing.T) {
	client := framework.NewClient()
	defer client.Execute("delete from vitess_strings", nil)

	_, err := client.Execute(
		"insert into vitess_strings values "+
			"(:vb, :c, :vc, :b, :tb, :bl, :ttx, :tx, :en, :s)",
		map[string]*querypb.BindVariable{
			"vb":  sqltypes.StringBindVariable("a"),
			"c":   sqltypes.StringBindVariable("b"),
			"vc":  sqltypes.StringBindVariable("c"),
			"b":   sqltypes.StringBindVariable("d"),
			"tb":  sqltypes.StringBindVariable("e"),
			"bl":  sqltypes.StringBindVariable("f"),
			"ttx": sqltypes.StringBindVariable("g"),
			"tx":  sqltypes.StringBindVariable("h"),
			"en":  sqltypes.StringBindVariable("a"),
			"s":   sqltypes.StringBindVariable("a,b"),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	qr, err := client.Execute("select * from vitess_strings where vb = 'a'", nil)
	if err != nil {
		t.Fatal(err)
	}
	want := sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:         "vb",
				Type:         sqltypes.VarBinary,
				Table:        "vitess_strings",
				OrgTable:     "vitess_strings",
				Database:     "vttest",
				OrgName:      "vb",
				ColumnLength: 16,
				Charset:      63,
				Flags:        16515,
			}, {
				Name:         "c",
				Type:         sqltypes.Char,
				Table:        "vitess_strings",
				OrgTable:     "vitess_strings",
				Database:     "vttest",
				OrgName:      "c",
				ColumnLength: 48,
				Charset:      33,
			}, {
				Name:         "vc",
				Type:         sqltypes.VarChar,
				Table:        "vitess_strings",
				OrgTable:     "vitess_strings",
				Database:     "vttest",
				OrgName:      "vc",
				ColumnLength: 48,
				Charset:      33,
			}, {
				Name:         "b",
				Type:         sqltypes.Binary,
				Table:        "vitess_strings",
				OrgTable:     "vitess_strings",
				Database:     "vttest",
				OrgName:      "b",
				ColumnLength: 4,
				Charset:      63,
				Flags:        128,
			}, {
				Name:         "tb",
				Type:         sqltypes.Blob,
				Table:        "vitess_strings",
				OrgTable:     "vitess_strings",
				Database:     "vttest",
				OrgName:      "tb",
				ColumnLength: 255,
				Charset:      63,
				Flags:        144,
			}, {
				Name:         "bl",
				Type:         sqltypes.Blob,
				Table:        "vitess_strings",
				OrgTable:     "vitess_strings",
				Database:     "vttest",
				OrgName:      "bl",
				ColumnLength: 65535,
				Charset:      63,
				Flags:        144,
			}, {
				Name:         "ttx",
				Type:         sqltypes.Text,
				Table:        "vitess_strings",
				OrgTable:     "vitess_strings",
				Database:     "vttest",
				OrgName:      "ttx",
				ColumnLength: 765,
				Charset:      33,
				Flags:        16,
			}, {
				Name:         "tx",
				Type:         sqltypes.Text,
				Table:        "vitess_strings",
				OrgTable:     "vitess_strings",
				Database:     "vttest",
				OrgName:      "tx",
				ColumnLength: 196605,
				Charset:      33,
				Flags:        16,
			}, {
				Name:         "en",
				Type:         sqltypes.Enum,
				Table:        "vitess_strings",
				OrgTable:     "vitess_strings",
				Database:     "vttest",
				OrgName:      "en",
				ColumnLength: 3,
				Charset:      33,
				Flags:        256,
			}, {
				Name:         "s",
				Type:         sqltypes.Set,
				Table:        "vitess_strings",
				OrgTable:     "vitess_strings",
				Database:     "vttest",
				OrgName:      "s",
				ColumnLength: 9,
				Charset:      33,
				Flags:        2048,
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.TestValue(sqltypes.VarBinary, "a"),
				sqltypes.TestValue(sqltypes.Char, "b"),
				sqltypes.TestValue(sqltypes.VarChar, "c"),
				sqltypes.TestValue(sqltypes.Binary, "d\x00\x00\x00"),
				sqltypes.TestValue(sqltypes.Blob, "e"),
				sqltypes.TestValue(sqltypes.Blob, "f"),
				sqltypes.TestValue(sqltypes.Text, "g"),
				sqltypes.TestValue(sqltypes.Text, "h"),
				sqltypes.TestValue(sqltypes.Enum, "a"),
				sqltypes.TestValue(sqltypes.Set, "a,b"),
			},
		},
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", prettyPrint(*qr), prettyPrint(want))
	}
}

func TestMiscTypes(t *testing.T) {
	client := framework.NewClient()
	defer client.Execute("delete from vitess_misc", nil)

	_, err := client.Execute(
		"insert into vitess_misc values(:id, :b, :d, :dt, :t, point(1, 2))",
		map[string]*querypb.BindVariable{
			"id": sqltypes.Int64BindVariable(1),
			"b":  sqltypes.StringBindVariable("\x01"),
			"d":  sqltypes.StringBindVariable("2012-01-01"),
			"dt": sqltypes.StringBindVariable("2012-01-01 15:45:45"),
			"t":  sqltypes.StringBindVariable("15:45:45"),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	qr, err := client.Execute("select * from vitess_misc where id = 1", nil)
	if err != nil {
		t.Fatal(err)
	}
	want := sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:         "id",
				Type:         sqltypes.Int32,
				Table:        "vitess_misc",
				OrgTable:     "vitess_misc",
				Database:     "vttest",
				OrgName:      "id",
				ColumnLength: 11,
				Charset:      63,
				Flags:        49155,
			}, {
				Name:         "b",
				Type:         sqltypes.Bit,
				Table:        "vitess_misc",
				OrgTable:     "vitess_misc",
				Database:     "vttest",
				OrgName:      "b",
				ColumnLength: 8,
				Charset:      63,
				Flags:        32,
			}, {
				Name:         "d",
				Type:         sqltypes.Date,
				Table:        "vitess_misc",
				OrgTable:     "vitess_misc",
				Database:     "vttest",
				OrgName:      "d",
				ColumnLength: 10,
				Charset:      63,
				Flags:        128,
			}, {
				Name:         "dt",
				Type:         sqltypes.Datetime,
				Table:        "vitess_misc",
				OrgTable:     "vitess_misc",
				Database:     "vttest",
				OrgName:      "dt",
				ColumnLength: 19,
				Charset:      63,
				Flags:        128,
			}, {
				Name:         "t",
				Type:         sqltypes.Time,
				Table:        "vitess_misc",
				OrgTable:     "vitess_misc",
				Database:     "vttest",
				OrgName:      "t",
				ColumnLength: 10,
				Charset:      63,
				Flags:        128,
			}, {
				Name:         "g",
				Type:         sqltypes.Geometry,
				Table:        "vitess_misc",
				OrgTable:     "vitess_misc",
				Database:     "vttest",
				OrgName:      "g",
				ColumnLength: 4294967295,
				Charset:      63,
				Flags:        144,
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.TestValue(sqltypes.Int32, "1"),
				sqltypes.TestValue(sqltypes.Bit, "\x01"),
				sqltypes.TestValue(sqltypes.Date, "2012-01-01"),
				sqltypes.TestValue(sqltypes.Datetime, "2012-01-01 15:45:45"),
				sqltypes.TestValue(sqltypes.Time, "15:45:45"),
				sqltypes.TestValue(sqltypes.Geometry, point12),
			},
		},
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", prettyPrint(*qr), prettyPrint(want))
	}
}

func TestNull(t *testing.T) {
	client := framework.NewClient()
	qr, err := client.Execute("select null from dual", nil)
	if err != nil {
		t.Fatal(err)
	}
	want := sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:    "NULL",
				Type:    sqltypes.Null,
				Charset: 63,
				Flags:   32896,
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
		t.Errorf("Execute: \n%#v, want \n%#v", prettyPrint(*qr), prettyPrint(want))
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
			t.Fatal(err)
		}
	}

	mismatchCases := []struct {
		query string
		bv    map[string]*querypb.BindVariable
		out   string
	}{{
		query: "insert into vitess_ints(tiny) values('str')",
		bv:    nil,
		out:   "strconv.ParseInt",
	}, {
		query: "insert into vitess_ints(tiny) values(:str)",
		bv:    map[string]*querypb.BindVariable{"str": sqltypes.StringBindVariable("str")},
		out:   "strconv.ParseInt",
	}, {
		query: "insert into vitess_ints(tiny) values(1.2)",
		bv:    nil,
		out:   "unsupported: cannot identify primary key of statement",
	}, {
		query: "insert into vitess_ints(tiny) values(:fl)",
		bv:    map[string]*querypb.BindVariable{"fl": sqltypes.Float64BindVariable(1.2)},
		out:   "invalid syntax",
	}, {
		query: "insert into vitess_strings(vb) select tiny from vitess_ints",
		bv:    nil,
		out:   "type mismatch",
	}, {
		query: "insert into vitess_ints(tiny) select num from vitess_fracts",
		bv:    nil,
		out:   "type mismatch",
	}, {
		query: "insert into vitess_ints(tiny) select vb from vitess_strings",
		bv:    nil,
		out:   "type mismatch",
	}}
	for _, tcase := range mismatchCases {
		_, err := client.Execute(tcase.query, tcase.bv)
		if err == nil || !strings.Contains(err.Error(), tcase.out) {
			t.Errorf("Error(%s): %v, want %s", tcase.query, err, tcase.out)
		}
	}

	want := "Out of range"
	for _, query := range []string{
		"insert into vitess_ints(tiny) values(-129)",
		"insert into vitess_ints(tiny) select medium from vitess_ints",
	} {
		_, err := client.Execute(query, nil)
		if err == nil || !strings.HasPrefix(err.Error(), want) {
			t.Errorf("Error(%s): %v, want %s", query, err, want)
		}
	}

	want = "Data too long"
	_, err := client.Execute("insert into vitess_strings(vb) values('12345678901234567')", nil)
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("Error: %v, want %s", err, want)
	}
}

func TestJSONType(t *testing.T) {
	// JSON is supported only after mysql57.
	client := framework.NewClient()
	if _, err := client.Execute("create table vitess_json(id int default 1, val json, primary key(id))", nil); err != nil {
		// If it's a syntax error, MySQL is an older version. Skip this test.
		if strings.Contains(err.Error(), "syntax") {
			return
		}
		t.Fatal(err)
	}
	defer client.Execute("drop table vitess_json", nil)

	if _, err := client.Execute(`insert into vitess_json values(1, '{"foo": "bar"}')`, nil); err != nil {
		t.Fatal(err)
	}

	qr, err := client.Execute("select id, val from vitess_json", nil)
	if err != nil {
		t.Fatal(err)
	}
	want := sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:         "id",
				Type:         sqltypes.Int32,
				Table:        "vitess_json",
				OrgTable:     "vitess_json",
				Database:     "vttest",
				OrgName:      "id",
				ColumnLength: 11,
				Charset:      63,
				Flags:        49155,
			}, {
				Name:         "val",
				Type:         sqltypes.TypeJSON,
				Table:        "vitess_json",
				OrgTable:     "vitess_json",
				Database:     "vttest",
				OrgName:      "val",
				ColumnLength: 4294967295,
				Charset:      63,
				Flags:        144,
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.TestValue(sqltypes.Int32, "1"),
				sqltypes.TestValue(sqltypes.TypeJSON, "{\"foo\": \"bar\"}"),
			},
		},
	}
	if !reflect.DeepEqual(*qr, want) {
		// MariaDB 10.3 has different behavior.
		want2 := want.Copy()
		want2.Fields[1].Type = sqltypes.Blob
		want2.Fields[1].Charset = 33
		want2.Rows[0][1] = sqltypes.TestValue(sqltypes.Blob, "{\"foo\": \"bar\"}")
		if !reflect.DeepEqual(*qr, *want2) {
			t.Errorf("Execute:\n%v, want\n%v or\n%v", prettyPrint(*qr), prettyPrint(want), prettyPrint(*want2))
		}
	}

}
