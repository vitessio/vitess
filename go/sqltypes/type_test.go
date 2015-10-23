// Copyright 2015| Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqltypes

import (
	"testing"

	"github.com/youtube/vitess/go/vt/proto/query"
)

func TestTypeValues(t *testing.T) {
	testcases := []struct {
		defined  query.Type
		expected int
	}{{
		defined:  Null,
		expected: 0,
	}, {
		defined:  Int8,
		expected: 1 | IsNumber,
	}, {
		defined:  Uint8,
		expected: 2 | IsNumber | IsUnsigned,
	}, {
		defined:  Int16,
		expected: 3 | IsNumber,
	}, {
		defined:  Uint16,
		expected: 4 | IsNumber | IsUnsigned,
	}, {
		defined:  Int24,
		expected: 5 | IsNumber,
	}, {
		defined:  Uint24,
		expected: 6 | IsNumber | IsUnsigned,
	}, {
		defined:  Int32,
		expected: 7 | IsNumber,
	}, {
		defined:  Uint32,
		expected: 8 | IsNumber | IsUnsigned,
	}, {
		defined:  Int64,
		expected: 9 | IsNumber,
	}, {
		defined:  Uint64,
		expected: 10 | IsNumber | IsUnsigned,
	}, {
		defined:  Float,
		expected: 11 | IsFloat,
	}, {
		defined:  Double,
		expected: 12 | IsFloat,
	}, {
		defined:  Timestamp,
		expected: 13 | IsQuoted,
	}, {
		defined:  Date,
		expected: 14 | IsQuoted,
	}, {
		defined:  Time,
		expected: 15 | IsQuoted,
	}, {
		defined:  Datetime,
		expected: 16 | IsQuoted,
	}, {
		defined:  Year,
		expected: 17 | IsNumber | IsUnsigned,
	}, {
		defined:  Decimal,
		expected: 18,
	}, {
		defined:  Text,
		expected: 19 | IsQuoted | IsText,
	}, {
		defined:  Blob,
		expected: 20 | IsQuoted | IsBinary,
	}, {
		defined:  VarChar,
		expected: 21 | IsQuoted | IsText,
	}, {
		defined:  VarBinary,
		expected: 22 | IsQuoted | IsBinary,
	}, {
		defined:  Char,
		expected: 23 | IsQuoted | IsText,
	}, {
		defined:  Binary,
		expected: 24 | IsQuoted | IsBinary,
	}, {
		defined:  Bit,
		expected: 25 | IsQuoted,
	}, {
		defined:  Enum,
		expected: 26 | IsQuoted,
	}, {
		defined:  Set,
		expected: 27 | IsQuoted,
	}}
	for _, tcase := range testcases {
		if int(tcase.defined) != tcase.expected {
			t.Errorf("Type %s: %d, want: %d", tcase.defined, int(tcase.defined), tcase.expected)
		}
	}
}

func TestTypeToMySQL(t *testing.T) {
	v, f := TypeToMySQL(Bit)
	if v != 16 {
		t.Errorf("Bit: %d, want 16", v)
	}
	if f != mysqlUnsigned>>8 {
		t.Errorf("Bit flag: %x, want %x", f, mysqlUnsigned>>8)
	}
	v, f = TypeToMySQL(Date)
	if v != 10 {
		t.Errorf("Bit: %d, want 10", v)
	}
	if f != mysqlBinary>>8 {
		t.Errorf("Bit flag: %x, want %x", f, mysqlBinary>>8)
	}
}

func TestTypeFlexibility(t *testing.T) {
	v, err := MySQLToType(1, mysqlBinary>>8)
	if err != nil {
		t.Error(err)
		return
	}
	if v != Int8 {
		t.Errorf("conversion: %v, want %v", v, Int8)
	}
}
