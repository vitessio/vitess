// Copyright 2015| Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqltypes

import (
	"testing"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

func TestTypeValues(t *testing.T) {
	testcases := []struct {
		defined  querypb.Type
		expected int
	}{{
		defined:  Null,
		expected: 0,
	}, {
		defined:  Int8,
		expected: 1 | flagIsIntegral,
	}, {
		defined:  Uint8,
		expected: 2 | flagIsIntegral | flagIsUnsigned,
	}, {
		defined:  Int16,
		expected: 3 | flagIsIntegral,
	}, {
		defined:  Uint16,
		expected: 4 | flagIsIntegral | flagIsUnsigned,
	}, {
		defined:  Int24,
		expected: 5 | flagIsIntegral,
	}, {
		defined:  Uint24,
		expected: 6 | flagIsIntegral | flagIsUnsigned,
	}, {
		defined:  Int32,
		expected: 7 | flagIsIntegral,
	}, {
		defined:  Uint32,
		expected: 8 | flagIsIntegral | flagIsUnsigned,
	}, {
		defined:  Int64,
		expected: 9 | flagIsIntegral,
	}, {
		defined:  Uint64,
		expected: 10 | flagIsIntegral | flagIsUnsigned,
	}, {
		defined:  Float32,
		expected: 11 | flagIsFloat,
	}, {
		defined:  Float64,
		expected: 12 | flagIsFloat,
	}, {
		defined:  Timestamp,
		expected: 13 | flagIsQuoted,
	}, {
		defined:  Date,
		expected: 14 | flagIsQuoted,
	}, {
		defined:  Time,
		expected: 15 | flagIsQuoted,
	}, {
		defined:  Datetime,
		expected: 16 | flagIsQuoted,
	}, {
		defined:  Year,
		expected: 17 | flagIsIntegral | flagIsUnsigned,
	}, {
		defined:  Decimal,
		expected: 18,
	}, {
		defined:  Text,
		expected: 19 | flagIsQuoted | flagIsText,
	}, {
		defined:  Blob,
		expected: 20 | flagIsQuoted | flagIsBinary,
	}, {
		defined:  VarChar,
		expected: 21 | flagIsQuoted | flagIsText,
	}, {
		defined:  VarBinary,
		expected: 22 | flagIsQuoted | flagIsBinary,
	}, {
		defined:  Char,
		expected: 23 | flagIsQuoted | flagIsText,
	}, {
		defined:  Binary,
		expected: 24 | flagIsQuoted | flagIsBinary,
	}, {
		defined:  Bit,
		expected: 25 | flagIsQuoted,
	}, {
		defined:  Enum,
		expected: 26 | flagIsQuoted,
	}, {
		defined:  Set,
		expected: 27 | flagIsQuoted,
	}, {
		defined:  Tuple,
		expected: 28,
	}}
	for _, tcase := range testcases {
		if int(tcase.defined) != tcase.expected {
			t.Errorf("Type %s: %d, want: %d", tcase.defined, int(tcase.defined), tcase.expected)
		}
	}
}

func TestIsFunctions(t *testing.T) {
	if IsIntegral(Null) {
		t.Error("Null: IsIntegral, must be false")
	}
	if !IsIntegral(Int64) {
		t.Error("Int64: !IsIntegral, must be true")
	}
	if IsSigned(Uint64) {
		t.Error("Uint64: IsSigned, must be false")
	}
	if !IsSigned(Int64) {
		t.Error("Int64: !IsSigned, must be true")
	}
	if IsUnsigned(Int64) {
		t.Error("Int64: IsUnsigned, must be false")
	}
	if !IsUnsigned(Uint64) {
		t.Error("Uint64: !IsUnsigned, must be true")
	}
	if IsFloat(Int64) {
		t.Error("Int64: IsFloat, must be false")
	}
	if !IsFloat(Float64) {
		t.Error("Uint64: !IsFloat, must be true")
	}
	if IsQuoted(Int64) {
		t.Error("Int64: IsQuoted, must be false")
	}
	if !IsQuoted(Binary) {
		t.Error("Binary: !IsQuoted, must be true")
	}
	if IsText(Int64) {
		t.Error("Int64: IsText, must be false")
	}
	if !IsText(Char) {
		t.Error("Char: !IsText, must be true")
	}
	if IsBinary(Int64) {
		t.Error("Int64: IsBinary, must be false")
	}
	if !IsBinary(Binary) {
		t.Error("Char: !IsBinary, must be true")
	}
}

func TestTypeToMySQL(t *testing.T) {
	v, f := TypeToMySQL(Bit)
	if v != 16 {
		t.Errorf("Bit: %d, want 16", v)
	}
	if f != mysqlUnsigned>>16 {
		t.Errorf("Bit flag: %x, want %x", f, mysqlUnsigned>>16)
	}
	v, f = TypeToMySQL(Date)
	if v != 10 {
		t.Errorf("Bit: %d, want 10", v)
	}
	if f != mysqlBinary>>16 {
		t.Errorf("Bit flag: %x, want %x", f, mysqlBinary>>16)
	}
}

func TestTypeFlexibility(t *testing.T) {
	v := MySQLToType(1, mysqlBinary>>16)
	if v != Int8 {
		t.Errorf("conversion: %v, want %v", v, Int8)
	}
}

func TestTypePanic(t *testing.T) {
	defer func() {
		err := recover().(error)
		want := "Could not map: 15 to a vitess type"
		if err == nil || err.Error() != want {
			t.Errorf("Error: %v, want %v", err, want)
		}
	}()
	_ = MySQLToType(15, 0)
}
