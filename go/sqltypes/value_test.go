// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqltypes

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
	"time"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

func TestMake(t *testing.T) {
	v := MakeTrusted(Null, []byte("abcd"))
	if !reflect.DeepEqual(v, NULL) {
		t.Errorf("MakeTrusted(Null...) = %v, want null", makePretty(v))
	}
	v = MakeTrusted(Int64, []byte("1"))
	want := testVal(Int64, "1")
	if !reflect.DeepEqual(v, want) {
		t.Errorf("MakeTrusted(Int64, \"1\") = %v, want %v", makePretty(v), makePretty(want))
	}
	v = MakeString([]byte("a"))
	want = testVal(VarBinary, "a")
	if !reflect.DeepEqual(v, want) {
		t.Errorf("MakeString(\"a\") = %v, want %v", makePretty(v), makePretty(want))
	}
}

func TestBuildValue(t *testing.T) {
	testcases := []struct {
		in  interface{}
		out Value
	}{{
		in:  nil,
		out: NULL,
	}, {
		in:  []byte("a"),
		out: testVal(VarBinary, "a"),
	}, {
		in:  int64(1),
		out: testVal(Int64, "1"),
	}, {
		in:  uint64(1),
		out: testVal(Uint64, "1"),
	}, {
		in:  float64(1.2),
		out: testVal(Float64, "1.2"),
	}, {
		in:  int(1),
		out: testVal(Int64, "1"),
	}, {
		in:  int8(1),
		out: testVal(Int8, "1"),
	}, {
		in:  int16(1),
		out: testVal(Int16, "1"),
	}, {
		in:  int32(1),
		out: testVal(Int32, "1"),
	}, {
		in:  uint(1),
		out: testVal(Uint64, "1"),
	}, {
		in:  uint8(1),
		out: testVal(Uint8, "1"),
	}, {
		in:  uint16(1),
		out: testVal(Uint16, "1"),
	}, {
		in:  uint32(1),
		out: testVal(Uint32, "1"),
	}, {
		in:  float32(1),
		out: testVal(Float32, "1"),
	}, {
		in:  "a",
		out: testVal(VarBinary, "a"),
	}, {
		in:  time.Date(2012, time.February, 24, 23, 19, 43, 10, time.UTC),
		out: testVal(Datetime, "2012-02-24 23:19:43"),
	}, {
		in:  testVal(VarBinary, "a"),
		out: testVal(VarBinary, "a"),
	}}
	for _, tcase := range testcases {
		v, err := BuildValue(tcase.in)
		if err != nil {
			t.Errorf("BuildValue(%#v) error: %v", tcase.in, err)
			continue
		}
		if !reflect.DeepEqual(v, tcase.out) {
			t.Errorf("BuildValue(%#v) = %v, want %v", tcase.in, makePretty(v), makePretty(tcase.out))
		}
	}

	_, err := BuildValue(make(chan bool))
	want := "unexpected"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("BuildValue(chan): %v, want %v", err, want)
	}
}

func TestBuildConverted(t *testing.T) {
	testcases := []struct {
		typ querypb.Type
		val interface{}
		out Value
	}{{
		typ: Int64,
		val: 123,
		out: testVal(Int64, "123"),
	}, {
		typ: Int64,
		val: "123",
		out: testVal(Int64, "123"),
	}, {
		typ: Uint64,
		val: "123",
		out: testVal(Uint64, "123"),
	}, {
		typ: Int64,
		val: []byte("123"),
		out: testVal(Int64, "123"),
	}, {
		typ: Int64,
		val: testVal(VarBinary, "123"),
		out: testVal(Int64, "123"),
	}, {
		typ: Int64,
		val: testVal(Float32, "123"),
		out: testVal(Float32, "123"),
	}, {
		typ: Int64,
		val: &querypb.BindVariable{
			Type:  querypb.Type_VARCHAR,
			Value: []byte("123"),
		},
		out: testVal(Int64, "123"),
	}}
	for _, tcase := range testcases {
		v, err := BuildConverted(tcase.typ, tcase.val)
		if err != nil {
			t.Errorf("BuildValue(%v, %#v) error: %v", tcase.typ, tcase.val, err)
			continue
		}
		if !reflect.DeepEqual(v, tcase.out) {
			t.Errorf("BuildValue(%v, %#v) = %v, want %v", tcase.typ, tcase.val, makePretty(v), makePretty(tcase.out))
		}
	}
}

const (
	InvalidNeg = "-9223372036854775809"
	MinNeg     = "-9223372036854775808"
	MinPos     = "18446744073709551615"
	InvalidPos = "18446744073709551616"
)

func TestValueFromBytes(t *testing.T) {
	testcases := []struct {
		inType querypb.Type
		inVal  string
		outVal Value
		outErr string
	}{{
		inType: Null,
		inVal:  "",
		outVal: NULL,
	}, {
		inType: Int8,
		inVal:  "1",
		outVal: testVal(Int8, "1"),
	}, {
		inType: Int16,
		inVal:  "1",
		outVal: testVal(Int16, "1"),
	}, {
		inType: Int24,
		inVal:  "1",
		outVal: testVal(Int24, "1"),
	}, {
		inType: Int32,
		inVal:  "1",
		outVal: testVal(Int32, "1"),
	}, {
		inType: Int64,
		inVal:  "1",
		outVal: testVal(Int64, "1"),
	}, {
		inType: Uint8,
		inVal:  "1",
		outVal: testVal(Uint8, "1"),
	}, {
		inType: Uint16,
		inVal:  "1",
		outVal: testVal(Uint16, "1"),
	}, {
		inType: Uint24,
		inVal:  "1",
		outVal: testVal(Uint24, "1"),
	}, {
		inType: Uint32,
		inVal:  "1",
		outVal: testVal(Uint32, "1"),
	}, {
		inType: Uint64,
		inVal:  "1",
		outVal: testVal(Uint64, "1"),
	}, {
		inType: Float32,
		inVal:  "1.00",
		outVal: testVal(Float32, "1.00"),
	}, {
		inType: Float64,
		inVal:  "1.00",
		outVal: testVal(Float64, "1.00"),
	}, {
		inType: Decimal,
		inVal:  "1.00",
		outVal: testVal(Decimal, "1.00"),
	}, {
		inType: Timestamp,
		inVal:  "2012-02-24 23:19:43",
		outVal: testVal(Timestamp, "2012-02-24 23:19:43"),
	}, {
		inType: Date,
		inVal:  "2012-02-24",
		outVal: testVal(Date, "2012-02-24"),
	}, {
		inType: Time,
		inVal:  "23:19:43",
		outVal: testVal(Time, "23:19:43"),
	}, {
		inType: Datetime,
		inVal:  "2012-02-24 23:19:43",
		outVal: testVal(Datetime, "2012-02-24 23:19:43"),
	}, {
		inType: Year,
		inVal:  "1",
		outVal: testVal(Year, "1"),
	}, {
		inType: Text,
		inVal:  "a",
		outVal: testVal(Text, "a"),
	}, {
		inType: Blob,
		inVal:  "a",
		outVal: testVal(Blob, "a"),
	}, {
		inType: VarChar,
		inVal:  "a",
		outVal: testVal(VarChar, "a"),
	}, {
		inType: Binary,
		inVal:  "a",
		outVal: testVal(Binary, "a"),
	}, {
		inType: Char,
		inVal:  "a",
		outVal: testVal(Char, "a"),
	}, {
		inType: Bit,
		inVal:  "1",
		outVal: testVal(Bit, "1"),
	}, {
		inType: Enum,
		inVal:  "a",
		outVal: testVal(Enum, "a"),
	}, {
		inType: Set,
		inVal:  "a",
		outVal: testVal(Set, "a"),
	}, {
		inType: VarBinary,
		inVal:  "a",
		outVal: testVal(VarBinary, "a"),
	}, {
		inType: Int64,
		inVal:  InvalidNeg,
		outErr: "out of range",
	}, {
		inType: Int64,
		inVal:  InvalidPos,
		outErr: "out of range",
	}, {
		inType: Uint64,
		inVal:  "-1",
		outErr: "invalid syntax",
	}, {
		inType: Uint64,
		inVal:  InvalidPos,
		outErr: "out of range",
	}, {
		inType: Float64,
		inVal:  "a",
		outErr: "invalid syntax",
	}, {
		inType: Tuple,
		inVal:  "a",
		outErr: "not allowed",
	}}
	for _, tcase := range testcases {
		v, err := ValueFromBytes(tcase.inType, []byte(tcase.inVal))
		if tcase.outErr != "" {
			if err == nil || !strings.Contains(err.Error(), tcase.outErr) {
				t.Errorf("ValueFromBytes(%v, %v) error: %v, must contain %v", tcase.inType, tcase.inVal, err, tcase.outErr)
			}
			continue
		}
		if err != nil {
			t.Errorf("ValueFromBytes(%v, %v) error: %v", tcase.inType, tcase.inVal, err)
			continue
		}
		if !reflect.DeepEqual(v, tcase.outVal) {
			t.Errorf("ValueFromBytes(%v, %v) = %v, want %v", tcase.inType, tcase.inVal, makePretty(v), makePretty(tcase.outVal))
		}
	}
}

func TestBuildIntegral(t *testing.T) {
	testcases := []struct {
		in     string
		outVal Value
		outErr string
	}{{
		in:     MinNeg,
		outVal: testVal(Int64, MinNeg),
	}, {
		in:     "1",
		outVal: testVal(Int64, "1"),
	}, {
		in:     MinPos,
		outVal: testVal(Uint64, MinPos),
	}, {
		in:     InvalidPos,
		outErr: "out of range",
	}}
	for _, tcase := range testcases {
		v, err := BuildIntegral(tcase.in)
		if tcase.outErr != "" {
			if err == nil || !strings.Contains(err.Error(), tcase.outErr) {
				t.Errorf("BuildIntegral(%v) error: %v, must contain %v", tcase.in, err, tcase.outErr)
			}
			continue
		}
		if err != nil {
			t.Errorf("BuildIntegral(%v) error: %v", tcase.in, err)
			continue
		}
		if !reflect.DeepEqual(v, tcase.outVal) {
			t.Errorf("BuildIntegral(%v) = %v, want %v", tcase.in, makePretty(v), makePretty(tcase.outVal))
		}
	}
}

func TestAccessors(t *testing.T) {
	v := testVal(Int64, "1")
	if v.Type() != Int64 {
		t.Errorf("v.Type=%v, want Int64", v.Type())
	}
	if !bytes.Equal(v.Raw(), []byte("1")) {
		t.Errorf("v.Raw=%s, want 1", v.Raw())
	}
	if v.Len() != 1 {
		t.Errorf("v.Len=%d, want 1", v.Len())
	}
	if v.String() != "1" {
		t.Errorf("v.String=%s, want 1", v.String())
	}
	if v.IsNull() {
		t.Error("v.IsNull: true, want false")
	}
	if !v.IsIntegral() {
		t.Error("v.IsIntegral: false, want true")
	}
	if !v.IsSigned() {
		t.Error("v.IsSigned: false, want true")
	}
	if v.IsUnsigned() {
		t.Error("v.IsUnsigned: true, want false")
	}
	if v.IsFloat() {
		t.Error("v.IsFloat: true, want false")
	}
	if v.IsQuoted() {
		t.Error("v.IsQuoted: true, want false")
	}
	if v.IsText() {
		t.Error("v.IsText: true, want false")
	}
	if v.IsBinary() {
		t.Error("v.IsBinary: true, want false")
	}
}

func TestToNative(t *testing.T) {
	testcases := []struct {
		in  Value
		out interface{}
	}{{
		in:  NULL,
		out: nil,
	}, {
		in:  testVal(Int8, "1"),
		out: int64(1),
	}, {
		in:  testVal(Int16, "1"),
		out: int64(1),
	}, {
		in:  testVal(Int24, "1"),
		out: int64(1),
	}, {
		in:  testVal(Int32, "1"),
		out: int64(1),
	}, {
		in:  testVal(Int64, "1"),
		out: int64(1),
	}, {
		in:  testVal(Uint8, "1"),
		out: uint64(1),
	}, {
		in:  testVal(Uint16, "1"),
		out: uint64(1),
	}, {
		in:  testVal(Uint24, "1"),
		out: uint64(1),
	}, {
		in:  testVal(Uint32, "1"),
		out: uint64(1),
	}, {
		in:  testVal(Uint64, "1"),
		out: uint64(1),
	}, {
		in:  testVal(Float32, "1"),
		out: float64(1),
	}, {
		in:  testVal(Float64, "1"),
		out: float64(1),
	}, {
		in:  testVal(Timestamp, "2012-02-24 23:19:43"),
		out: []byte("2012-02-24 23:19:43"),
	}, {
		in:  testVal(Date, "2012-02-24"),
		out: []byte("2012-02-24"),
	}, {
		in:  testVal(Time, "23:19:43"),
		out: []byte("23:19:43"),
	}, {
		in:  testVal(Datetime, "2012-02-24 23:19:43"),
		out: []byte("2012-02-24 23:19:43"),
	}, {
		in:  testVal(Year, "1"),
		out: uint64(1),
	}, {
		in:  testVal(Decimal, "1"),
		out: []byte("1"),
	}, {
		in:  testVal(Text, "a"),
		out: []byte("a"),
	}, {
		in:  testVal(Blob, "a"),
		out: []byte("a"),
	}, {
		in:  testVal(VarChar, "a"),
		out: []byte("a"),
	}, {
		in:  testVal(VarBinary, "a"),
		out: []byte("a"),
	}, {
		in:  testVal(Char, "a"),
		out: []byte("a"),
	}, {
		in:  testVal(Binary, "a"),
		out: []byte("a"),
	}, {
		in:  testVal(Bit, "1"),
		out: []byte("1"),
	}, {
		in:  testVal(Enum, "a"),
		out: []byte("a"),
	}, {
		in:  testVal(Set, "a"),
		out: []byte("a"),
	}}
	for _, tcase := range testcases {
		v := tcase.in.ToNative()
		if !reflect.DeepEqual(v, tcase.out) {
			t.Errorf("%v.ToNative = %#v, want %#v", makePretty(tcase.in), v, tcase.out)
		}
	}
}

func TestToProtoValue(t *testing.T) {
	got := testVal(Int64, "1").ToProtoValue()
	want := &querypb.Value{
		Type:  Int64,
		Value: []byte("1"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("bindvar: %v, want %v", got, want)
	}
}

func TestPanics(t *testing.T) {
	testcases := []struct {
		in  Value
		out string
	}{{
		in:  testVal(Int64, InvalidNeg),
		out: "out of range",
	}, {
		in:  testVal(Uint64, InvalidPos),
		out: "out of range",
	}, {
		in:  testVal(Uint64, "-1"),
		out: "invalid syntax",
	}, {
		in:  testVal(Float64, "a"),
		out: "invalid syntax",
	}, {
		in:  testVal(Tuple, "a"),
		out: "unexpected",
	}}
	for _, tcase := range testcases {
		func() {
			defer func() {
				x := recover()
				if x == nil {
					t.Errorf("%v.ToNative did not panic", makePretty(tcase.in))
				}
				err, ok := x.(error)
				if !ok {
					t.Errorf("%v.ToNative did not panic with an error", makePretty(tcase.in))
				}
				if !strings.Contains(err.Error(), tcase.out) {
					t.Errorf("%v.ToNative error: %v, must contain; %v ", makePretty(tcase.in), err, tcase.out)
				}
			}()
			_ = tcase.in.ToNative()
		}()
	}
	for _, tcase := range testcases {
		func() {
			defer func() {
				x := recover()
				if x == nil {
					t.Errorf("%v.EncodeSQL did not panic", makePretty(tcase.in))
				}
				err, ok := x.(error)
				if !ok {
					t.Errorf("%v.EncodeSQL did not panic with an error", makePretty(tcase.in))
				}
				if !strings.Contains(err.Error(), tcase.out) {
					t.Errorf("%v.EncodeSQL error: %v, must contain; %v ", makePretty(tcase.in), err, tcase.out)
				}
			}()
			tcase.in.EncodeSQL(&bytes.Buffer{})
		}()
	}
	for _, tcase := range testcases {
		func() {
			defer func() {
				x := recover()
				if x == nil {
					t.Errorf("%v.EncodeASCII did not panic", makePretty(tcase.in))
				}
				err, ok := x.(error)
				if !ok {
					t.Errorf("%v.EncodeASCII did not panic with an error", makePretty(tcase.in))
				}
				if !strings.Contains(err.Error(), tcase.out) {
					t.Errorf("%v.EncodeASCII error: %v, must contain; %v ", makePretty(tcase.in), err, tcase.out)
				}
			}()
			tcase.in.EncodeASCII(&bytes.Buffer{})
		}()
	}
}

func TestParseNumbers(t *testing.T) {
	v := testVal(VarChar, "1")
	sval, err := v.ParseInt64()
	if err != nil {
		t.Error(err)
	}
	if sval != 1 {
		t.Errorf("v.ParseInt64 = %d, want 1", sval)
	}
	uval, err := v.ParseUint64()
	if err != nil {
		t.Error(err)
	}
	if uval != 1 {
		t.Errorf("v.ParseUint64 = %d, want 1", uval)
	}
	fval, err := v.ParseFloat64()
	if err != nil {
		t.Error(err)
	}
	if fval != 1 {
		t.Errorf("v.ParseFloat64 = %f, want 1", fval)
	}
}

func TestEncode(t *testing.T) {
	testcases := []struct {
		in       Value
		outSQL   string
		outASCII string
	}{{
		in:       NULL,
		outSQL:   "null",
		outASCII: "null",
	}, {
		in:       testVal(Int64, "1"),
		outSQL:   "1",
		outASCII: "1",
	}, {
		in:       testVal(VarChar, "foo"),
		outSQL:   "'foo'",
		outASCII: "'Zm9v'",
	}, {
		in:       testVal(VarChar, "\x00'\"\b\n\r\t\x1A\\"),
		outSQL:   "'\\0\\'\\\"\\b\\n\\r\\t\\Z\\\\'",
		outASCII: "'ACciCAoNCRpc'",
	}}
	for _, tcase := range testcases {
		buf := &bytes.Buffer{}
		tcase.in.EncodeSQL(buf)
		if tcase.outSQL != buf.String() {
			t.Errorf("%v.EncodeSQL = %q, want %q", makePretty(tcase.in), buf.String(), tcase.outSQL)
		}
		buf = &bytes.Buffer{}
		tcase.in.EncodeASCII(buf)
		if tcase.outASCII != buf.String() {
			t.Errorf("%v.EncodeASCII = %q, want %q", makePretty(tcase.in), buf.String(), tcase.outASCII)
		}
	}
}

// TestEncodeMap ensures DontEscape is not escaped
func TestEncodeMap(t *testing.T) {
	if SQLEncodeMap[DontEscape] != DontEscape {
		t.Errorf("SQLEncodeMap[DontEscape] = %v, want %v", SQLEncodeMap[DontEscape], DontEscape)
	}
	if SQLDecodeMap[DontEscape] != DontEscape {
		t.Errorf("SQLDecodeMap[DontEscape] = %v, want %v", SQLEncodeMap[DontEscape], DontEscape)
	}
}

// testVal makes it easy to build a Value for testing.
func testVal(typ querypb.Type, val string) Value {
	return Value{typ: typ, val: []byte(val)}
}

type prettyVal struct {
	Type  querypb.Type
	Value string
}

// makePretty converts Value to a struct that's readable when printed.
func makePretty(v Value) prettyVal {
	return prettyVal{v.typ, string(v.val)}
}
