// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package endtoend

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/mysql"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
)

func TestCharaterSet(t *testing.T) {
	qr, err := newQueryClient().Execute("select * from vtocc_test limit 2", nil)
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
		RowsAffected: 2,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{
				sqltypes.Value{Inner: sqltypes.Numeric("1")},
				sqltypes.Value{Inner: sqltypes.Fractional("1.12345")},
				sqltypes.Value{Inner: sqltypes.String("\xc2\xa2")},
				sqltypes.Value{Inner: sqltypes.String("\x00\xff")},
			},
			[]sqltypes.Value{
				sqltypes.Value{Inner: sqltypes.Numeric("2")},
				sqltypes.Value{},
				sqltypes.Value{Inner: sqltypes.String("")},
				sqltypes.Value{},
			},
		},
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", *qr, want)
	}
}

func TestInts(t *testing.T) {
	client := newQueryClient()
	qr, err := client.Execute(
		"insert into vtocc_ints values(:tiny, :tinyu, :small, "+":smallu, :medium, :mediumu, :normal, :normalu, :big, :bigu, :year)",
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
	qr, err = client.Execute("select * from vtocc_ints where tiny = -128", nil)
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
}
