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
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/bytes2"
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
	}, {
		// SQL text declared as a hex literal.
		inType: HexVal,
		inVal:  "1+1",
		outErr: `invalid HEXVAL literal: "1+1", expected x'<hex digits>' with an even digit count`,
	}, {
		inType: HexVal,
		inVal:  "x'41'",
		outVal: TestValue(HexVal, "x'41'"),
	}, {
		inType: HexVal,
		inVal:  "x'4'",
		outErr: `invalid HEXVAL literal: "x'4'", expected x'<hex digits>' with an even digit count`,
	}, {
		inType: HexNum,
		inVal:  "0xAB",
		outVal: TestValue(HexNum, "0xAB"),
	}, {
		inType: HexNum,
		inVal:  "1; drop table x #",
		outErr: `invalid HEXNUM literal: "1; drop table x #", expected 0x<hex digits>`,
	}, {
		inType: BitNum,
		inVal:  "0b101",
		outVal: TestValue(BitNum, "0b101"),
	}, {
		inType: BitNum,
		inVal:  "(select user())",
		outErr: `invalid BITNUM literal: "(select user())", expected 0b<binary digits>`,
	}, {
		// decimal.NewFromMySQL sizes the value from digit counts and truncates
		// an over-long integral or fractional part instead of scanning past it,
		// so trailing SQL text passed the parser.
		inType: Decimal,
		inVal:  strings.Repeat("9", 80) + "; drop table x #",
		outErr: `invalid DECIMAL literal:`,
	}, {
		inType: Decimal,
		inVal:  "1." + strings.Repeat("9", 80) + "; drop table x #",
		outErr: `invalid DECIMAL literal:`,
	}, {
		// Go float words that MySQL has no literal for.
		inType: Float64,
		inVal:  "NaN",
		outErr: `invalid FLOAT64 literal: "NaN", expected a numeric literal`,
	}, {
		inType: Float64,
		inVal:  "Infinity",
		outErr: `invalid FLOAT64 literal: "Infinity", expected a numeric literal`,
	}, {
		// The parsers' whitespace tolerance is preserved.
		inType: Float64,
		inVal:  " 1.5",
		outVal: TestValue(Float64, " 1.5"),
	}, {
		inType: Int64,
		inVal:  " 42\t",
		outVal: TestValue(Int64, " 42\t"),
	}, {
		// The echoed payload is capped so a hostile payload cannot inflate
		// the error and its log line.
		inType: HexVal,
		inVal:  strings.Repeat("a", 100),
		outErr: `invalid HEXVAL literal: "` + strings.Repeat("a", 64) + `…", expected x'<hex digits>' with an even digit count`,
	}}
	for _, tcase := range testcases {
		v, err := NewValue(tcase.inType, []byte(tcase.inVal))
		if tcase.outErr != "" {
			require.ErrorContains(t, err, tcase.outErr)
			continue
		}

		require.NoError(t, err)
		assert.Equal(t, tcase.outVal, v)
	}
}

// TestNew tests 'New' functions that are not tested
// through other code paths.
func TestNew(t *testing.T) {
	got := NewInt32(1)
	want := MakeTrusted(Int32, []byte("1"))
	assert.Equal(t, want, got)

	got = NewVarBinary("aa")
	want = MakeTrusted(VarBinary, []byte("aa"))
	assert.Equal(t, want, got)

	got, err := NewJSON("invalid-json")
	assert.Empty(t, got)
	assert.ErrorContains(t, err, "invalid JSON value")
}

func TestNewFromBytes(t *testing.T) {
	assert.Equal(t, NewDate("2021-02-03"), NewDateFromBytes([]byte("2021-02-03")))
	assert.Equal(t, NewTime("12:34:56"), NewTimeFromBytes([]byte("12:34:56")))
	assert.Equal(t, NewDatetime("2021-02-03 12:34:56"), NewDatetimeFromBytes([]byte("2021-02-03 12:34:56")))
	assert.Equal(t, NewDecimal("3.14"), NewDecimalFromBytes([]byte("3.14")))
	assert.Equal(t, NewVarBinary("\x00\x01"), NewVarBinaryFromBytes([]byte("\x00\x01")))
}

func TestMakeTrusted(t *testing.T) {
	v := MakeTrusted(Null, []byte("abcd"))
	assert.Equal(t, NULL, v)

	v = MakeTrusted(Int64, []byte("1"))
	want := TestValue(Int64, "1")
	assert.Equal(t, want, v)
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
			require.ErrorContains(t, err, tcase.outErr)
			continue
		}

		require.NoError(t, err)
		assert.Equal(t, tcase.outVal, v)
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

		require.NoError(t, err)
		assert.Equal(t, tcase.out, v)
	}

	_, err := InterfaceToValue(make(chan bool))
	want := "unexpected"
	assert.ErrorContains(t, err, want)
}

func TestAccessors(t *testing.T) {
	v := TestValue(Int64, "1")
	assert.Equal(t, Int64, v.Type())
	assert.Equal(t, []byte("1"), v.Raw())
	assert.Equal(t, 1, v.Len())
	assert.Equal(t, "1", v.ToString())
	assert.False(t, v.IsNull())
	assert.True(t, v.IsIntegral())
	assert.True(t, v.IsSigned())
	assert.False(t, v.IsUnsigned())
	assert.False(t, v.IsFloat())
	assert.False(t, v.IsQuoted())
	assert.False(t, v.IsText())
	assert.False(t, v.IsBinary())

	{
		i, err := v.ToInt64()
		require.NoError(t, err)
		assert.Equal(t, int64(1), i)
	}
	{
		i, err := v.ToUint64()
		require.NoError(t, err)
		assert.Equal(t, uint64(1), i)
	}
	{
		b, err := v.ToBool()
		require.NoError(t, err)
		assert.True(t, b)
	}
}

func TestAccessorsNegative(t *testing.T) {
	v := TestValue(Int64, "-1")
	assert.Equal(t, "-1", v.ToString())
	assert.False(t, v.IsNull())
	assert.True(t, v.IsIntegral())

	{
		i, err := v.ToInt64()
		require.NoError(t, err)
		assert.Equal(t, int64(-1), i)
	}
	{
		_, err := v.ToUint64()
		require.Error(t, err)
	}
	{
		_, err := v.ToBool()
		assert.Error(t, err)
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
		assert.Equal(t, v.Raw(), vBytes)
		assert.Equal(t, string(v.Raw()), v.ToString())
	}

	tv := TestValue(Expression, "aa")
	tvBytes, err := tv.ToBytes()
	require.EqualError(t, err, "expression cannot be converted to bytes")
	assert.Nil(t, tvBytes)
	assert.Empty(t, tv.ToString())
}

func TestEncode(t *testing.T) {
	testcases := []struct {
		in       Value
		outSQL   string
		outASCII string
	}{{
		in:     NULL,
		outSQL: "null",
	}, {
		in:     TestValue(Int64, "1"),
		outSQL: "1",
	}, {
		in:     TestValue(VarChar, "foo"),
		outSQL: "'foo'",
	}, {
		in:     TestValue(VarChar, "\x00'\"\b\n\r\t\x1A\\"),
		outSQL: "'\\0\\'\"\\b\\n\\r\\t\\Z\\\\'",
	}, {
		in:     TestValue(VarBinary, "\x00'\"\b\n\r\t\x1A\\"),
		outSQL: "_binary'\\0\\'\"\\b\\n\\r\\t\\Z\\\\'",
	}, {
		in:     TestValue(Bit, "a"),
		outSQL: "b'01100001'",
	}, {
		in:     TestTuple(TestValue(Int64, "1"), TestValue(VarChar, "foo")),
		outSQL: "(1, 'foo')",
	}}
	for _, tcase := range testcases {
		var buf strings.Builder
		tcase.in.EncodeSQL(&buf)
		assert.Equal(t, tcase.outSQL, buf.String())
	}
}

// TestEncodeMap ensures DontEscape is not escaped
func TestEncodeMap(t *testing.T) {
	assert.Equal(t, DontEscape, SQLEncodeMap[DontEscape])
	assert.Equal(t, DontEscape, SQLDecodeMap[DontEscape])
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

func TestEncodeStringSQL(t *testing.T) {
	testcases := []struct {
		in  string
		out string
	}{
		{
			in:  "",
			out: "''",
		},
		{
			in:  "\x00'\"\b\n\r\t\x1A\\",
			out: "'\\0\\'\"\\b\\n\\r\\t\\Z\\\\'",
		},
	}
	for _, tcase := range testcases {
		out := EncodeStringSQL(tcase.in)
		assert.Equal(t, tcase.out, out)
	}
}

func TestDecodeStringSQL(t *testing.T) {
	testcases := []struct {
		in  string
		out string
		err string
	}{
		{
			in:  "",
			err: ": invalid SQL encoded string",
		},
		{
			in:  "''",
			err: "",
		},
		{
			in:  "'\\0\\'\\\"\\b\\n\\r\\t\\Z\\\\'",
			out: "\x00'\"\b\n\r\t\x1A\\",
		},
		{
			in:  "'light ''green\\r\\n, \\nfoo'",
			out: "light 'green\r\n, \nfoo",
		},
		{
			in:  "'foo \\\\ % _bar'",
			out: "foo \\ % _bar",
		},
	}
	for _, tcase := range testcases {
		out, err := DecodeStringSQL(tcase.in)
		if tcase.err != "" {
			assert.EqualError(t, err, tcase.err)
		} else {
			require.NoError(t, err)
			assert.Equal(t, tcase.out, out)
		}
	}
}

func TestTinyWeightCmp(t *testing.T) {
	val1 := TestValue(Int64, "12")
	val2 := TestValue(VarChar, "aa")

	val1.SetTinyWeight(10)

	// Test TinyWeight
	assert.Equal(t, uint32(10), val1.TinyWeight())

	cmp := val1.TinyWeightCmp(val2)
	assert.Equal(t, 0, cmp)

	val2.SetTinyWeight(10)
	cmp = val1.TinyWeightCmp(val2)
	assert.Equal(t, 0, cmp)

	val2.SetTinyWeight(20)
	cmp = val1.TinyWeightCmp(val2)
	assert.Equal(t, -1, cmp)

	val2.SetTinyWeight(5)
	cmp = val1.TinyWeightCmp(val2)
	assert.Equal(t, 1, cmp)
}

func TestToCastInt64(t *testing.T) {
	tcases := []struct {
		in   Value
		want int64
		err  string
	}{
		{TestValue(Int64, "213"), 213, ""},
		{TestValue(Int64, "-213"), -213, ""},
		{TestValue(VarChar, "9223372036854775808a"), math.MaxInt64, `cannot parse int64 from "9223372036854775808a": overflow`},
		{TestValue(Time, "12:23:59"), 12, `unparsed tail left after parsing int64 from "12:23:59": ":23:59"`},
	}

	for _, tcase := range tcases {
		t.Run(tcase.in.String(), func(t *testing.T) {
			got, err := tcase.in.ToCastInt64()
			assert.Equal(t, tcase.want, got)

			if tcase.err != "" {
				assert.ErrorContains(t, err, tcase.err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestToCastUint64(t *testing.T) {
	tcases := []struct {
		in   Value
		want uint64
		err  string
	}{
		{TestValue(Int64, "213"), 213, ""},
		{TestValue(Int64, "-213"), 0, `cannot parse uint64 from "-213"`},
		{TestValue(VarChar, "9223372036854775808a"), 9223372036854775808, `unparsed tail left after parsing uint64 from "9223372036854775808a": "a"`},
		{TestValue(Time, "12:23:59"), 12, `unparsed tail left after parsing uint64 from "12:23:59": ":23:59"`},
	}

	for _, tcase := range tcases {
		t.Run(tcase.in.String(), func(t *testing.T) {
			got, err := tcase.in.ToCastUint64()
			assert.Equal(t, tcase.want, got)

			if tcase.err != "" {
				assert.ErrorContains(t, err, tcase.err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestToUint16(t *testing.T) {
	tcases := []struct {
		in   Value
		want uint16
		err  string
	}{
		{TestValue(Int64, "213"), 213, ""},
		{TestValue(Int64, "-213"), 0, `parsing "-213": invalid syntax`},
		{TestValue(VarChar, "9223372036854775808a"), 0, ErrIncompatibleTypeCast.Error()},
		{TestValue(Time, "12:23:59"), 0, ErrIncompatibleTypeCast.Error()},
	}

	for _, tcase := range tcases {
		t.Run(tcase.in.String(), func(t *testing.T) {
			got, err := tcase.in.ToUint16()
			assert.Equal(t, tcase.want, got)

			if tcase.err != "" {
				assert.ErrorContains(t, err, tcase.err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestToUint32(t *testing.T) {
	tcases := []struct {
		in   Value
		want uint32
		err  string
	}{
		{TestValue(Int64, "213"), 213, ""},
		{TestValue(Int64, "-213"), 0, `parsing "-213": invalid syntax`},
		{TestValue(VarChar, "9223372036854775808a"), 0, ErrIncompatibleTypeCast.Error()},
		{TestValue(Time, "12:23:59"), 0, ErrIncompatibleTypeCast.Error()},
	}

	for _, tcase := range tcases {
		t.Run(tcase.in.String(), func(t *testing.T) {
			got, err := tcase.in.ToUint32()
			assert.Equal(t, tcase.want, got)

			if tcase.err != "" {
				assert.ErrorContains(t, err, tcase.err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestEncodeSQLStringBuilder(t *testing.T) {
	testcases := []struct {
		in     Value
		outSQL string
	}{{
		in:     NULL,
		outSQL: "null",
	}, {
		in:     TestValue(Int64, "1"),
		outSQL: "1",
	}, {
		in:     TestValue(VarChar, "foo"),
		outSQL: "'foo'",
	}, {
		in:     TestValue(VarChar, "\x00'\"\b\n\r\t\x1A\\"),
		outSQL: "'\\0\\'\"\\b\\n\\r\\t\\Z\\\\'",
	}, {
		in:     TestValue(Bit, "a"),
		outSQL: "b'01100001'",
	}, {
		in:     TestTuple(TestValue(Int64, "1"), TestValue(VarChar, "foo")),
		outSQL: "(1, 'foo')",
	}, {
		in:     TestValue(VarBinary, "foo"),
		outSQL: "_binary'foo'",
	}}
	for _, tcase := range testcases {
		var buf strings.Builder

		tcase.in.EncodeSQLStringBuilder(&buf)
		assert.Equal(t, tcase.outSQL, buf.String())
	}
}

func TestEncodeSQLBytes2(t *testing.T) {
	testcases := []struct {
		in     Value
		outSQL string
	}{{
		in:     NULL,
		outSQL: "null",
	}, {
		in:     TestValue(Int64, "1"),
		outSQL: "1",
	}, {
		in:     TestValue(VarChar, "foo"),
		outSQL: "'foo'",
	}, {
		in:     TestValue(VarChar, "\x00'\"\b\n\r\t\x1A\\"),
		outSQL: "'\\0\\'\"\\b\\n\\r\\t\\Z\\\\'",
	}, {
		in:     TestValue(Bit, "a"),
		outSQL: "b'01100001'",
	}, {
		in:     TestTuple(TestValue(Int64, "1"), TestValue(VarChar, "foo")),
		outSQL: "(1, 'foo')",
	}}
	for _, tcase := range testcases {
		var buf bytes2.Buffer

		tcase.in.EncodeSQLBytes2(&buf)
		assert.Equal(t, tcase.outSQL, buf.String())
	}
}

func TestIsComparable(t *testing.T) {
	testcases := []struct {
		in    Value
		isCmp bool
	}{{
		in:    NULL,
		isCmp: true,
	}, {
		in:    TestValue(Int64, "1"),
		isCmp: true,
	}, {
		in: TestValue(VarChar, "foo"),
	}, {
		in: TestValue(VarChar, "\x00'\"\b\n\r\t\x1A\\"),
	}, {
		in:    TestValue(Bit, "a"),
		isCmp: true,
	}, {
		in:    TestValue(Time, "12:21:11"),
		isCmp: true,
	}, {
		in: TestTuple(TestValue(Int64, "1"), TestValue(VarChar, "foo")),
	}}
	for _, tcase := range testcases {
		isCmp := tcase.in.IsComparable()
		assert.Equal(t, tcase.isCmp, isCmp)
	}
}

// TestIsRawSQLLiteral pins the one legal payload grammar per type that the
// encoders write into generated SQL verbatim. The grammar follows the lexer
// and MySQL: hex digits in either case, 0x/0b prefixes lowercase only, an even
// digit count inside x'...', and the space/tab padding the numeric parsers
// already tolerate.
func TestIsRawSQLLiteral(t *testing.T) {
	testcases := []struct {
		typ querypb.Type
		val string
		ok  bool
	}{
		// signed integral
		{Int64, "0", true},
		{Int64, "42", true},
		{Int64, "-42", true},
		{Int8, " 42\t", true},
		{Int64, "", false},
		{Int64, "-", false},
		{Int64, "+42", false},
		{Int64, "4 2", false},
		{Int64, "1; drop table x #", false},
		{Int64, "1 or 1=1", false},
		{Int64, "--1", false},
		{Int64, " \t", false},
		{Int64, "1\n", false},
		// unsigned integral (Year is unsigned integral)
		{Uint64, "42", true},
		{Year, "2026", true},
		{Uint64, "-1", false},
		{Uint64, "", false},
		// float
		{Float64, "1.5", true},
		{Float64, "+1.5", true},
		{Float64, "-1.5", true},
		{Float64, ".5", true},
		{Float64, "1.", true},
		{Float64, "1e5", true},
		{Float64, "1.5E-3", true},
		{Float32, " 1.5 ", true},
		{Float64, "NaN", false},
		{Float64, "Inf", false},
		{Float64, "Infinity", false},
		{Float64, "-inf", false},
		{Float64, "1e", false},
		{Float64, "e5", false},
		{Float64, ".", false},
		{Float64, "1.5.2", false},
		{Float64, "1.5 or 1=1", false},
		{Float64, "1.e5", true},
		{Float64, "1e+", false},
		{Float64, "1..5", false},
		{Float64, "1.5\n-- x", false},
		// decimal: no exponent
		{Decimal, "1.5", true},
		{Decimal, "-0", true},
		{Decimal, "+.5", true},
		{Decimal, " 1.5 ", true},
		{Decimal, "1e5", false},
		{Decimal, strings.Repeat("9", 80) + "; drop table x #", false},
		// HexNum: 0x + one or more hex digits, lowercase prefix only
		{HexNum, "0x0", true},
		{HexNum, "0xAB", true},
		{HexNum, "0xab", true},
		{HexNum, "0xA", true},
		{HexNum, "0x", false},
		{HexNum, "0X1", false},
		{HexNum, "0xG", false},
		{HexNum, "1+1", false},
		{HexNum, "", false},
		{HexNum, "0x1 ", false},
		// HexVal: x'..' or X'..' with an even number of hex digits (zero allowed)
		{HexVal, "x''", true},
		{HexVal, "x'41'", true},
		{HexVal, "X'aB'", true},
		{HexVal, "x'ABCD'", true},
		{HexVal, "x'4'", false},
		{HexVal, "x'41", false},
		{HexVal, "'41'", false},
		{HexVal, "x'4G'", false},
		{HexVal, "1+1", false},
		{HexVal, "x", false},
		{HexVal, "", false},
		{HexVal, "x'41' or 1=1", false},
		{HexVal, "x'''", false},
		{HexVal, "''", false},
		// BitNum: 0b + one or more binary digits, lowercase prefix only
		{BitNum, "0b0", true},
		{BitNum, "0b101", true},
		{BitNum, "0b", false},
		{BitNum, "0B1", false},
		{BitNum, "0b102", false},
		{BitNum, "1+1", false},
		// never raw
		{VarChar, "1", false},
		{VarBinary, "1", false},
		{Bit, "1", false},
		{Null, "", false},
		{Expression, "1", false},
		{Tuple, "", false},
		{querypb.Type_RAW, "1", false},
		{Datetime, "2026-01-01 00:00:00", false},
	}
	for _, tc := range testcases {
		t.Run(fmt.Sprintf("%s/%q", tc.typ, tc.val), func(t *testing.T) {
			assert.Equal(t, tc.ok, isRawSQLLiteral(tc.typ, []byte(tc.val)))
		})
	}
}

// TestForEachValueMalformedEncoding pins that a tuple payload whose member
// length prefix exceeds the buffer reports ErrBadTupleEncoding instead of
// panicking: the tablet validator decodes wire-supplied tuple payloads.
func TestForEachValueMalformedEncoding(t *testing.T) {
	// A well-formed single-member tuple, then the same bytes with the member
	// length inflated past the end of the buffer.
	good := TestTuple(TestValue(Int64, "1"))
	var n int
	require.NoError(t, good.ForEachValue(func(Value) { n++ }))
	require.Equal(t, 1, n)

	bad := make([]byte, len(good.val))
	copy(bad, good.val)
	// protowire layout from encodeTuple: varint type, varint len, bytes.
	// The length byte precedes the single payload byte '1'; inflate it.
	idx := bytes.LastIndexByte(bad, '1') - 1
	require.GreaterOrEqual(t, idx, 0)
	require.Equal(t, byte(1), bad[idx], "expected the member length byte")
	bad[idx] = 0x7f
	malformed := Value{typ: uint16(Tuple), val: bad}
	require.NotPanics(t, func() {
		err := malformed.ForEachValue(func(Value) {})
		assert.ErrorIs(t, err, ErrBadTupleEncoding)
	})
}
