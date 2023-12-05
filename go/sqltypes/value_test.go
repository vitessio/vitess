/*
Copyright 2019 The Vitess Authors.

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

package sqltypes

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

const (
	InvalidNeg = "-9223372036854775809"
	MinNeg     = "-9223372036854775808"
	MinPos     = "18446744073709551615"
	InvalidPos = "18446744073709551616"
)

func TestNewValue(t *testing.T) {
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
		outVal: TestValue(Int8, "1"),
	}, {
		inType: Int16,
		inVal:  "1",
		outVal: TestValue(Int16, "1"),
	}, {
		inType: Int24,
		inVal:  "1",
		outVal: TestValue(Int24, "1"),
	}, {
		inType: Int32,
		inVal:  "1",
		outVal: TestValue(Int32, "1"),
	}, {
		inType: Int64,
		inVal:  "1",
		outVal: TestValue(Int64, "1"),
	}, {
		inType: Uint8,
		inVal:  "1",
		outVal: TestValue(Uint8, "1"),
	}, {
		inType: Uint16,
		inVal:  "1",
		outVal: TestValue(Uint16, "1"),
	}, {
		inType: Uint24,
		inVal:  "1",
		outVal: TestValue(Uint24, "1"),
	}, {
		inType: Uint32,
		inVal:  "1",
		outVal: TestValue(Uint32, "1"),
	}, {
		inType: Uint64,
		inVal:  "1",
		outVal: TestValue(Uint64, "1"),
	}, {
		inType: Uint64,
		inVal:  "01",
		outVal: TestValue(Uint64, "01"),
	}, {
		inType: Int64,
		inVal:  "01",
		outVal: TestValue(Int64, "01"),
	}, {
		inType: Float32,
		inVal:  "1.00",
		outVal: TestValue(Float32, "1.00"),
	}, {
		inType: Float64,
		inVal:  "1.00",
		outVal: TestValue(Float64, "1.00"),
	}, {
		inType: Decimal,
		inVal:  "1.00",
		outVal: TestValue(Decimal, "1.00"),
	}, {
		inType: Timestamp,
		inVal:  "2012-02-24 23:19:43",
		outVal: TestValue(Timestamp, "2012-02-24 23:19:43"),
	}, {
		inType: Date,
		inVal:  "2012-02-24",
		outVal: TestValue(Date, "2012-02-24"),
	}, {
		inType: Time,
		inVal:  "23:19:43",
		outVal: TestValue(Time, "23:19:43"),
	}, {
		inType: Datetime,
		inVal:  "2012-02-24 23:19:43",
		outVal: TestValue(Datetime, "2012-02-24 23:19:43"),
	}, {
		inType: Year,
		inVal:  "1",
		outVal: TestValue(Year, "1"),
	}, {
		inType: Text,
		inVal:  "a",
		outVal: TestValue(Text, "a"),
	}, {
		inType: Blob,
		inVal:  "a",
		outVal: TestValue(Blob, "a"),
	}, {
		inType: VarChar,
		inVal:  "a",
		outVal: TestValue(VarChar, "a"),
	}, {
		inType: Binary,
		inVal:  "a",
		outVal: TestValue(Binary, "a"),
	}, {
		inType: Char,
		inVal:  "a",
		outVal: TestValue(Char, "a"),
	}, {
		inType: Bit,
		inVal:  "1",
		outVal: TestValue(Bit, "1"),
	}, {
		inType: Enum,
		inVal:  "a",
		outVal: TestValue(Enum, "a"),
	}, {
		inType: Set,
		inVal:  "a",
		outVal: TestValue(Set, "a"),
	}, {
		inType: VarBinary,
		inVal:  "a",
		outVal: TestValue(VarBinary, "a"),
	}, {
		inType: Int64,
		inVal:  InvalidNeg,
		outErr: `cannot parse int64 from "-9223372036854775809": overflow`,
	}, {
		inType: Int64,
		inVal:  InvalidPos,
		outErr: `cannot parse int64 from "18446744073709551616": overflow`,
	}, {
		inType: Uint64,
		inVal:  "-1",
		outErr: `cannot parse uint64 from "-1"`,
	}, {
		inType: Uint64,
		inVal:  InvalidPos,
		outErr: `cannot parse uint64 from "18446744073709551616": overflow`,
	}, {
		inType: Float64,
		inVal:  "a",
		outErr: `unparsed tail left after parsing float64 from "a"`,
	}, {
		inType: Expression,
		inVal:  "a",
		outErr: "invalid type specified for MakeValue: EXPRESSION",
	}}
	for _, tcase := range testcases {
		v, err := NewValue(tcase.inType, []byte(tcase.inVal))
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
			t.Errorf("ValueFromBytes(%v, %v) = %v, want %v", tcase.inType, tcase.inVal, v, tcase.outVal)
		}
	}
}

// TestNew tests 'New' functions that are not tested
// through other code paths.
func TestNew(t *testing.T) {
	got := NewInt32(1)
	want := MakeTrusted(Int32, []byte("1"))
	if !reflect.DeepEqual(got, want) {
		t.Errorf("NewInt32(aa): %v, want %v", got, want)
	}

	got = NewVarBinary("aa")
	want = MakeTrusted(VarBinary, []byte("aa"))
	if !reflect.DeepEqual(got, want) {
		t.Errorf("NewVarBinary(aa): %v, want %v", got, want)
	}
}

func TestMakeTrusted(t *testing.T) {
	v := MakeTrusted(Null, []byte("abcd"))
	if !reflect.DeepEqual(v, NULL) {
		t.Errorf("MakeTrusted(Null...) = %v, want null", v)
	}
	v = MakeTrusted(Int64, []byte("1"))
	want := TestValue(Int64, "1")
	if !reflect.DeepEqual(v, want) {
		t.Errorf("MakeTrusted(Int64, \"1\") = %v, want %v", v, want)
	}
}

func TestIntegralValue(t *testing.T) {
	testcases := []struct {
		in     string
		outVal Value
		outErr string
	}{{
		in:     MinNeg,
		outVal: TestValue(Int64, MinNeg),
	}, {
		in:     "1",
		outVal: TestValue(Int64, "1"),
	}, {
		in:     MinPos,
		outVal: TestValue(Uint64, MinPos),
	}, {
		in:     InvalidPos,
		outErr: "out of range",
	}}
	for _, tcase := range testcases {
		v, err := NewIntegral(tcase.in)
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
			t.Errorf("BuildIntegral(%v) = %v, want %v", tcase.in, v, tcase.outVal)
		}
	}
}

func TestInterfaceValue(t *testing.T) {
	testcases := []struct {
		in  any
		out Value
	}{{
		in:  nil,
		out: NULL,
	}, {
		in:  []byte("a"),
		out: TestValue(VarBinary, "a"),
	}, {
		in:  int64(1),
		out: TestValue(Int64, "1"),
	}, {
		in:  uint64(1),
		out: TestValue(Uint64, "1"),
	}, {
		in:  float64(1.2),
		out: TestValue(Float64, "1.2"),
	}, {
		in:  "a",
		out: TestValue(VarChar, "a"),
	}}
	for _, tcase := range testcases {
		v, err := InterfaceToValue(tcase.in)
		if err != nil {
			t.Errorf("BuildValue(%#v) error: %v", tcase.in, err)
			continue
		}
		if !reflect.DeepEqual(v, tcase.out) {
			t.Errorf("BuildValue(%#v) = %v, want %v", tcase.in, v, tcase.out)
		}
	}

	_, err := InterfaceToValue(make(chan bool))
	want := "unexpected"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("BuildValue(chan): %v, want %v", err, want)
	}
}

func TestAccessors(t *testing.T) {
	v := TestValue(Int64, "1")
	if v.Type() != Int64 {
		t.Errorf("v.Type=%v, want Int64", v.Type())
	}
	if !bytes.Equal(v.Raw(), []byte("1")) {
		t.Errorf("v.Raw=%s, want 1", v.Raw())
	}
	if v.Len() != 1 {
		t.Errorf("v.Len=%d, want 1", v.Len())
	}
	if v.ToString() != "1" {
		t.Errorf("v.String=%s, want 1", v.ToString())
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
	{
		i, err := v.ToInt64()
		if err != nil {
			t.Errorf("v.ToInt64: got error: %+v, want no error", err)
		}
		if i != 1 {
			t.Errorf("v.ToInt64=%+v, want 1", i)
		}
	}
	{
		i, err := v.ToUint64()
		if err != nil {
			t.Errorf("v.ToUint64: got error: %+v, want no error", err)
		}
		if i != 1 {
			t.Errorf("v.ToUint64=%+v, want 1", i)
		}
	}
	{
		b, err := v.ToBool()
		if err != nil {
			t.Errorf("v.ToBool: got error: %+v, want no error", err)
		}
		if !b {
			t.Errorf("v.ToBool=%+v, want true", b)
		}
	}
}

func TestAccessorsNegative(t *testing.T) {
	v := TestValue(Int64, "-1")
	if v.ToString() != "-1" {
		t.Errorf("v.String=%s, want -1", v.ToString())
	}
	if v.IsNull() {
		t.Error("v.IsNull: true, want false")
	}
	if !v.IsIntegral() {
		t.Error("v.IsIntegral: false, want true")
	}
	{
		i, err := v.ToInt64()
		if err != nil {
			t.Errorf("v.ToInt64: got error: %+v, want no error", err)
		}
		if i != -1 {
			t.Errorf("v.ToInt64=%+v, want -1", i)
		}
	}
	{
		if _, err := v.ToUint64(); err == nil {
			t.Error("v.ToUint64: got no error, want error")
		}
	}
	{
		if _, err := v.ToBool(); err == nil {
			t.Error("v.ToUint64: got no error, want error")
		}
	}
}

func TestToBytesAndString(t *testing.T) {
	for _, v := range []Value{
		NULL,
		TestValue(Int64, "1"),
		TestValue(Int64, "12"),
	} {
		vBytes, err := v.ToBytes()
		require.NoError(t, err)
		if b := vBytes; !bytes.Equal(b, v.Raw()) {
			t.Errorf("%v.ToBytes: %s, want %s", v, b, v.Raw())
		}
		if s := v.ToString(); s != string(v.Raw()) {
			t.Errorf("%v.ToString: %s, want %s", v, s, v.Raw())
		}
	}

	tv := TestValue(Expression, "aa")
	tvBytes, err := tv.ToBytes()
	require.EqualError(t, err, "expression cannot be converted to bytes")
	if b := tvBytes; b != nil {
		t.Errorf("%v.ToBytes: %s, want nil", tv, b)
	}
	if s := tv.ToString(); s != "" {
		t.Errorf("%v.ToString: %s, want \"\"", tv, s)
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
		in:       TestValue(Int64, "1"),
		outSQL:   "1",
		outASCII: "1",
	}, {
		in:       TestValue(VarChar, "foo"),
		outSQL:   "'foo'",
		outASCII: "'Zm9v'",
	}, {
		in:       TestValue(VarChar, "\x00'\"\b\n\r\t\x1A\\"),
		outSQL:   "'\\0\\'\\\"\\b\\n\\r\\t\\Z\\\\'",
		outASCII: "'ACciCAoNCRpc'",
	}, {
		in:       TestValue(Bit, "a"),
		outSQL:   "b'01100001'",
		outASCII: "'YQ=='",
	}}
	for _, tcase := range testcases {
		var buf strings.Builder
		tcase.in.EncodeSQL(&buf)
		if tcase.outSQL != buf.String() {
			t.Errorf("%v.EncodeSQL = %q, want %q", tcase.in, buf.String(), tcase.outSQL)
		}
		buf.Reset()
		tcase.in.EncodeASCII(&buf)
		if tcase.outASCII != buf.String() {
			t.Errorf("%v.EncodeASCII = %q, want %q", tcase.in, buf.String(), tcase.outASCII)
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

func TestHexAndBitToBytes(t *testing.T) {
	tcases := []struct {
		in  Value
		out []byte
	}{{
		in:  MakeTrusted(HexNum, []byte("0x1234")),
		out: []byte{0x12, 0x34},
	}, {
		in:  MakeTrusted(HexVal, []byte("X'1234'")),
		out: []byte{0x12, 0x34},
	}, {
		in:  MakeTrusted(BitNum, []byte("0b1001000110100")),
		out: []byte{0x12, 0x34},
	}, {
		in:  MakeTrusted(BitNum, []byte("0b11101010100101010010101010101010101010101000100100100100100101001101010101010101000001")),
		out: []byte{0x3a, 0xa5, 0x4a, 0xaa, 0xaa, 0xa2, 0x49, 0x25, 0x35, 0x55, 0x41},
	}}

	for _, tcase := range tcases {
		t.Run(tcase.in.String(), func(t *testing.T) {
			out, err := tcase.in.ToBytes()
			require.NoError(t, err)
			assert.Equal(t, tcase.out, out)
		})
	}
}
