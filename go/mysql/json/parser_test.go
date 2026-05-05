/*
Copyright 2018 Aliaksandr Valialkin
Copyright 2023 The Vitess Authors.

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

package json

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/hack"
)

func TestParseRawNumber(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		f := func(s, expectedRN, expectedTail string) {
			t.Helper()

			flen, ok := readFloat(s)
			require.Truef(t, ok, "unexpected error when parsing '%s'", s)

			rn, tail := s[:flen], s[flen:]
			require.Equalf(t, expectedRN, rn, "unexpected raw number; got %q; want %q", rn, expectedRN)
			require.Equalf(t, expectedTail, tail, "unexpected tail; got %q; want %q", tail, expectedTail)
		}

		f("0", "0", "")
		f("0tail", "0", "tail")
		f("123", "123", "")
		f("123tail", "123", "tail")
		f("-123tail", "-123", "tail")
		f("-12.345tail", "-12.345", "tail")
		f("-12.345e67tail", "-12.345e67", "tail")
		f("-12.345E+67 tail", "-12.345E+67", " tail")
		f("-12.345E-67,tail", "-12.345E-67", ",tail")
		f("-1234567.8e+90tail", "-1234567.8e+90", "tail")
		f("12.tail", "12.", "tail")
		f(".2tail", ".2", "tail")
		f("-.2tail", "-.2", "tail")
	})

	t.Run("error", func(t *testing.T) {
		f := func(s, expectedTail string) {
			t.Helper()

			flen, ok := readFloat(s)
			require.False(t, ok, "expecting non-nil error")
			require.Equalf(t, expectedTail, s[flen:], "unexpected tail; got %q; want %q", s[flen:], expectedTail)
		}

		f("xyz", "xyz")
		f(" ", " ")
		f("[", "[")
		f(",", ",")
		f("{", "{")
		f("\"", "\"")
	})
}

func TestUnescapeStringBestEffort(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		testUnescapeStringBestEffort(t, ``, ``)
		testUnescapeStringBestEffort(t, `\"`, `"`)
		testUnescapeStringBestEffort(t, `\\`, `\`)
		testUnescapeStringBestEffort(t, `\\\"`, `\"`)
		testUnescapeStringBestEffort(t, `\\\"абв`, `\"абв`)
		testUnescapeStringBestEffort(t, `йцук\n\"\\Y`, "йцук\n\"\\Y")
		testUnescapeStringBestEffort(t, `q\u1234we`, "q\u1234we")
		testUnescapeStringBestEffort(t, `п\ud83e\udd2dи`, "п🤭и")
	})

	t.Run("error", func(t *testing.T) {
		testUnescapeStringBestEffort(t, `\`, ``)
		testUnescapeStringBestEffort(t, `foo\qwe`, `foo\qwe`)
		testUnescapeStringBestEffort(t, `\"x\uyz\"`, `"x\uyz"`)
		testUnescapeStringBestEffort(t, `\u12\"пролw`, `\u12"пролw`)
		testUnescapeStringBestEffort(t, `п\ud83eи`, "п\\ud83eи")
	})
}

func testUnescapeStringBestEffort(t *testing.T, s, expectedS string) {
	t.Helper()

	// unescapeString modifies the original s, so call it
	// on a byte slice copy.
	b := append([]byte{}, s...)
	us := unescapeStringBestEffort(hack.String(b))
	require.Equalf(t, expectedS, us, "unexpected unescaped string; got %q; want %q", us, expectedS)
}

func TestParseRawString(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		f := func(s, expectedRS, expectedTail string) {
			t.Helper()

			rs, tail, err := parseRawString(s[1:])
			require.NoError(t, err)
			require.Equalf(t, expectedRS, rs, "unexpected string on parseRawString; got %q; want %q", rs, expectedRS)
			require.Equalf(t, expectedTail, tail, "unexpected tail on parseRawString; got %q; want %q", tail, expectedTail)

			// parseRawKey results must be identical to parseRawString.
			rs, tail, _, err = parseRawKey(s[1:])
			require.NoError(t, err)
			require.Equalf(t, expectedRS, rs, "unexpected string on parseRawKey; got %q; want %q", rs, expectedRS)
			require.Equalf(t, expectedTail, tail, "unexpected tail on parseRawKey; got %q; want %q", tail, expectedTail)
		}

		f(`""`, "", "")
		f(`""xx`, "", "xx")
		f(`"foobar"`, "foobar", "")
		f(`"foobar"baz`, "foobar", "baz")
		f(`"\""`, `\"`, "")
		f(`"\""tail`, `\"`, "tail")
		f(`"\\"`, `\\`, "")
		f(`"\\"tail`, `\\`, "tail")
		f(`"x\\"`, `x\\`, "")
		f(`"x\\"tail`, `x\\`, "tail")
		f(`"x\\y"`, `x\\y`, "")
		f(`"x\\y"tail`, `x\\y`, "tail")
		f(`"\\\"й\n\"я"tail`, `\\\"й\n\"я`, "tail")
		f(`"\\\\\\\\"tail`, `\\\\\\\\`, "tail")
	})

	t.Run("error", func(t *testing.T) {
		f := func(s, expectedTail string) {
			t.Helper()

			_, tail, err := parseRawString(s[1:])
			require.Error(t, err, "expecting non-nil error on parseRawString")
			require.Equalf(t, expectedTail, tail, "unexpected tail on parseRawString; got %q; want %q", tail, expectedTail)

			// parseRawKey results must be identical to parseRawString.
			_, tail, _, err = parseRawKey(s[1:])
			require.Error(t, err, "expecting non-nil error on parseRawKey")
			require.Equalf(t, expectedTail, tail, "unexpected tail on parseRawKey; got %q; want %q", tail, expectedTail)
		}

		f(`"`, "")
		f(`"unclosed string`, "")
		f(`"\"`, "")
		f(`"\"unclosed`, "")
		f(`"foo\\\\\"тест\n\r\t`, "")
	})
}

func TestValueInvalidTypeConversion(t *testing.T) {
	var p Parser

	v, err := p.Parse(`[{},[],"",123.45,true,null]`)
	require.NoError(t, err)
	a, _ := v.Array()

	// object
	_, ok := a[0].Object()
	require.True(t, ok, "unexpected error when obtaining object")
	_, ok = a[0].Array()
	require.False(t, ok, "expecting non-nil error when trying to obtain array from object")

	// array
	_, ok = a[1].Array()
	require.True(t, ok, "unexpected error when obtaining array")
	_, ok = a[1].Object()
	require.False(t, ok, "expecting non-nil error when trying to obtain object from array")

	// string
	_, ok = a[2].StringBytes()
	require.True(t, ok, "unexpected error when obtaining string")
}

func TestParserParse(t *testing.T) {
	var p Parser

	t.Run("invalid-string-escape", func(t *testing.T) {
		v, err := p.Parse(`"fo\u"`)
		require.NoError(t, err, "unexpected error when parsing string")
		// Make sure only valid string part remains
		sb, ok := v.StringBytes()
		require.True(t, ok, "cannot obtain string")
		require.Equalf(t, "fo\\u", string(sb), "unexpected string; got %q; want %q", sb, "fo\\u")

		v, err = p.Parse(`"foo\ubarz2134"`)
		require.NoError(t, err, "unexpected error when parsing string")
		sb, ok = v.StringBytes()
		require.True(t, ok, "cannot obtain string")
		require.Equalf(t, "foo\\ubarz2134", string(sb), "unexpected string; got %q; want %q", sb, "foo")

		v, err = p.Parse(`"fo` + "\x19" + `\u"`)
		require.NoError(t, err, "unexpected error when parsing string")
		sb, ok = v.StringBytes()
		require.True(t, ok, "cannot obtain string")
		require.Equalf(t, "fo\x19\\u", string(sb), "unexpected string; got %q; want %q", sb, "fo\x19\\u")
	})

	t.Run("empty-json", func(t *testing.T) {
		_, err := p.Parse("")
		require.Error(t, err, "expecting non-nil error when parsing empty json")
		_, err = p.Parse("\n\t    \n")
		require.Error(t, err, "expecting non-nil error when parsing empty json")
	})

	t.Run("invalid-tail", func(t *testing.T) {
		_, err := p.Parse("123 456")
		require.Error(t, err, "expecting non-nil error when parsing invalid tail")
		_, err = p.Parse("[] 1223")
		require.Error(t, err, "expecting non-nil error when parsing invalid tail")
	})

	t.Run("invalid-json", func(t *testing.T) {
		f := func(s string) {
			t.Helper()
			_, err := p.Parse(s)
			require.Errorf(t, err, "expecting non-nil error when parsing invalid json %q", s)
		}

		f("free")
		f("tree")
		f("\x00\x10123")
		f("1 \n\x01")
		f("{\x00}")
		f("[\x00]")
		f("\"foo\"\x00")
		f("{\"foo\"\x00:123}")
		f("nil")
		f("[foo]")
		f("{foo}")
		f("[123 34]")
		f(`{"foo" "bar"}`)
		f(`{"foo":123 "bar":"baz"}`)
		f("-2134.453eec+43")

		_, err := p.Parse("-2134.453E+43")
		require.NoError(t, err)

		// Incomplete object key key.
		f(`{"foo: 123}`)

		// Incomplete string.
		f(`"{\"foo\": 123}`)

		v, err := p.Parse(`"{\"foo\": 123}"`)
		require.NoError(t, err)
		sb, _ := v.StringBytes()
		require.Equalf(t, `{"foo": 123}`, string(sb), "unexpected string value; got %q; want %q", sb, `{"foo": 123}`)
	})

	t.Run("incomplete-object", func(t *testing.T) {
		f := func(s string) {
			t.Helper()
			_, err := p.Parse(s)
			require.Errorf(t, err, "expecting non-nil error when parsing incomplete object %q", s)
		}

		f(" {  ")
		f(`{"foo"`)
		f(`{"foo":`)
		f(`{"foo":null`)
		f(`{"foo":null,`)
		f(`{"foo":null,}`)
		f(`{"foo":null,"bar"}`)

		_, err := p.Parse(`{"foo":null,"bar":"baz"}`)
		require.NoError(t, err)
	})

	t.Run("incomplete-array", func(t *testing.T) {
		f := func(s string) {
			t.Helper()
			_, err := p.Parse(s)
			require.Errorf(t, err, "expecting non-nil error when parsing incomplete array %q", s)
		}

		f("  [ ")
		f("[123")
		f("[123,")
		f("[123,]")
		f("[123,{}")
		f("[123,{},]")

		_, err := p.Parse("[123,{},[]]")
		require.NoError(t, err)
	})

	t.Run("incomplete-string", func(t *testing.T) {
		f := func(s string) {
			t.Helper()
			_, err := p.Parse(s)
			require.Errorf(t, err, "expecting non-nil error when parsing incomplete string %q", s)
		}

		f(`  "foo`)
		f(`"foo\`)
		f(`"foo\"`)
		f(`"foo\\\"`)
		f(`"foo'`)
		f(`"foo'bar'`)

		_, err := p.Parse(`"foo\\\""`)
		require.NoError(t, err)
	})

	t.Run("empty-object", func(t *testing.T) {
		v, err := p.Parse("{}")
		require.NoError(t, err)
		tp := v.Type()
		require.Equalf(t, TypeObject, tp, "unexpected value obtained for empty object: %#v", v)
		require.Equal(t, "object", tp.String())
		o, ok := v.Object()
		require.True(t, ok, "cannot obtain object")
		require.Zerof(t, o.Len(), "unexpected number of items in empty object: %d; want 0", o.Len())
		require.Equalf(t, "{}", v.String(), "unexpected string representation of empty object")
	})

	t.Run("empty-array", func(t *testing.T) {
		v, err := p.Parse("[]")
		require.NoError(t, err)
		tp := v.Type()
		require.Equalf(t, TypeArray, tp, "unexpected value obtained for empty array: %#v", v)
		require.Equal(t, "array", tp.String())
		a, ok := v.Array()
		require.True(t, ok, "unexpected error")
		require.Zerof(t, len(a), "unexpected number of items in empty array: %d; want 0", len(a))
		require.Equalf(t, "[]", v.String(), "unexpected string representation of empty array")
	})

	t.Run("null", func(t *testing.T) {
		v, err := p.Parse("null")
		require.NoError(t, err)
		tp := v.Type()
		require.Equalf(t, TypeNull, tp, "unexpected value obtained for null: %#v", v)
		require.Equal(t, "null", tp.String())
		require.Equalf(t, "null", v.String(), "unexpected string representation of null")
	})

	t.Run("true", func(t *testing.T) {
		v, err := p.Parse("true")
		require.NoError(t, err)
		require.Equalf(t, ValueTrue, v, "unexpected value obtained for true: %#v", v)
		b, ok := v.Bool()
		require.True(t, ok, "unexpected error")
		require.True(t, b, "expecting true; got false")
		require.Equalf(t, "true", v.String(), "unexpected string representation of true")
	})

	t.Run("false", func(t *testing.T) {
		v, err := p.Parse("false")
		require.NoError(t, err)
		require.Equalf(t, ValueFalse, v, "unexpected value obtained for false: %#v", v)
		b, ok := v.Bool()
		require.True(t, ok, "unexpected error")
		require.False(t, b, "expecting false; got true")
		require.Equalf(t, "false", v.String(), "unexpected string representation of false")
	})

	t.Run("integer", func(t *testing.T) {
		v, err := p.Parse("12345")
		require.NoError(t, err)
		tp := v.Type()
		require.Equalf(t, TypeNumber, tp, "unexpected type obtained for integer: %#v", v)
		require.Equal(t, "number", tp.String())
		require.Equalf(t, NumberTypeSigned, v.NumberType(), "unexpected non integer value: %#v", v)
		require.Equalf(t, "12345", v.String(), "unexpected string representation of integer")
	})

	t.Run("int64", func(t *testing.T) {
		v, err := p.Parse("-8838840643388017390")
		require.NoError(t, err)
		tp := v.Type()
		require.Equalf(t, TypeNumber, tp, "unexpected type obtained for int64: %#v", v)
		require.Equal(t, "number", tp.String())
		require.Equalf(t, "-8838840643388017390", v.String(), "unexpected string representation of int64")
	})

	t.Run("uint", func(t *testing.T) {
		v, err := p.Parse("18446744073709551615")
		require.NoError(t, err)
		tp := v.Type()
		require.Equalf(t, TypeNumber, tp, "unexpected type obtained for uint: %#v", v)
		require.Equal(t, "number", tp.String())
		require.Equalf(t, "18446744073709551615", v.String(), "unexpected string representation of uint")
	})

	t.Run("uint64", func(t *testing.T) {
		v, err := p.Parse("18446744073709551615")
		require.NoError(t, err)
		tp := v.Type()
		require.Equalf(t, TypeNumber, tp, "unexpected type obtained for uint64: %#v", v)
		require.Equal(t, "number", tp.String())
		require.Equalf(t, "18446744073709551615", v.String(), "unexpected string representation of uint64")
	})

	t.Run("float", func(t *testing.T) {
		v, err := p.Parse("-12.345")
		require.NoError(t, err)
		tp := v.Type()
		require.Equalf(t, TypeNumber, tp, "unexpected type obtained for integer: %#v", v)
		require.Equal(t, "number", tp.String())
		require.Equalf(t, NumberTypeFloat, v.NumberType(), "unexpected integer value: %#v", v)
		require.Equalf(t, "-12.345", v.String(), "unexpected string representation of integer")
	})

	t.Run("float with zero", func(t *testing.T) {
		v, err := p.Parse("12.0")
		require.NoError(t, err)
		tp := v.Type()
		require.Equalf(t, TypeNumber, tp, "unexpected type obtained for number: %#v", v)
		require.Equal(t, "number", tp.String())
		require.Equalf(t, NumberTypeFloat, v.NumberType(), "unexpected integer value: %#v", v)
		require.Equalf(t, "12.0", v.String(), "unexpected string representation of float")
	})

	t.Run("float with large exponent", func(t *testing.T) {
		v, err := p.Parse("1e100")
		require.NoError(t, err)
		tp := v.Type()
		require.Equalf(t, TypeNumber, tp, "unexpected type obtained for number: %#v", v)
		require.Equal(t, "number", tp.String())
		require.Equalf(t, NumberTypeFloat, v.NumberType(), "unexpected integer value: %#v", v)
		require.Equalf(t, "1e100", v.String(), "unexpected string representation of float")
	})

	t.Run("string", func(t *testing.T) {
		v, err := p.Parse(`"foo bar"`)
		require.NoError(t, err)
		tp := v.Type()
		require.Equalf(t, TypeString, tp, "unexpected type obtained for string: %#v", v)
		require.Equal(t, "string", tp.String())
		sb, ok := v.StringBytes()
		require.True(t, ok, "cannot obtain string")
		require.Equalf(t, "foo bar", string(sb), "unexpected value obtained for string")
		require.Equalf(t, `"foo bar"`, v.String(), "unexpected string representation of string")
	})

	t.Run("string-escaped", func(t *testing.T) {
		v, err := p.Parse(`"\n\t\\foo\"bar\u3423x\/\b\f\r\\"`)
		require.NoError(t, err)
		require.Equalf(t, TypeString, v.Type(), "unexpected type obtained for string: %#v", v)
		sb, ok := v.StringBytes()
		require.True(t, ok, "cannot obtain string")
		require.Equalf(t, "\n\t\\foo\"bar\u3423x/\b\f\r\\", string(sb), "unexpected value obtained for string")
		require.Equalf(t, `"\n\t\\foo\"bar㐣x/\b\f\r\\"`, v.String(), "unexpected string representation of string")
	})

	t.Run("object-one-element", func(t *testing.T) {
		v, err := p.Parse(`  {
	"foo"   : "bar"  }	 `)
		require.NoError(t, err)
		require.Equalf(t, TypeObject, v.Type(), "unexpected type obtained for object: %#v", v)
		o, ok := v.Object()
		require.True(t, ok, "cannot obtain object")
		vv := o.Get("foo")
		require.Equalf(t, TypeString, vv.Type(), "unexpected type for foo item")
		vv = o.Get("non-existing key")
		require.Nilf(t, vv, "unexpected value obtained for non-existing key: %#v", vv)

		require.Equalf(t, `{"foo": "bar"}`, v.String(), "unexpected string representation for object")
	})

	t.Run("object-multi-elements", func(t *testing.T) {
		v, err := p.Parse(`{"foo": [1,2,3  ]  ,"bar":{},"baz":123.456}`)
		require.NoError(t, err)
		require.Equalf(t, TypeObject, v.Type(), "unexpected type obtained for object: %#v", v)
		o, ok := v.Object()
		require.True(t, ok, "cannot obtain object")
		vv := o.Get("foo")
		require.Equalf(t, TypeArray, vv.Type(), "unexpected type for foo item")
		vv = o.Get("bar")
		require.Equalf(t, TypeObject, vv.Type(), "unexpected type for bar item")
		vv = o.Get("baz")
		require.Equalf(t, TypeNumber, vv.Type(), "unexpected type for baz item")
		vv = o.Get("non-existing-key")
		require.Nilf(t, vv, "unexpected value obtained for non-existing key: %#v", vv)

		require.Equal(t, "{\"bar\": {}, \"baz\": 123.456, \"foo\": [1, 2, 3]}", v.String(), "unexpected string representation for object")
	})

	t.Run("array-one-element", func(t *testing.T) {
		v, err := p.Parse(`   [{"bar":[  [],[[]]   ]} ]  `)
		require.NoError(t, err)
		require.Equalf(t, TypeArray, v.Type(), "unexpected type obtained for array: %#v", v)
		a, ok := v.Array()
		require.True(t, ok, "unexpected error")
		require.Lenf(t, a, 1, "unexpected array len")
		require.Equalf(t, TypeObject, a[0].Type(), "unexpected type for a[0]")

		require.Equalf(t, `[{"bar": [[], [[]]]}]`, v.String(), "unexpected string representation for array")
	})

	t.Run("array-multi-elements", func(t *testing.T) {
		v, err := p.Parse(`   [1,"foo",{"bar":[     ],"baz":""}    ,[  "x" ,	"y"   ]     ]   `)
		require.NoError(t, err)
		require.Equalf(t, TypeArray, v.Type(), "unexpected type obtained for array: %#v", v)
		a, ok := v.Array()
		require.True(t, ok, "unexpected error")
		require.Lenf(t, a, 4, "unexpected array len")
		require.Equalf(t, TypeNumber, a[0].Type(), "unexpected type for a[0]")
		require.Equalf(t, TypeString, a[1].Type(), "unexpected type for a[1]")
		require.Equalf(t, TypeObject, a[2].Type(), "unexpected type for a[2]")
		require.Equalf(t, TypeArray, a[3].Type(), "unexpected type for a[3]")

		require.Equalf(t, `[1, "foo", {"bar": [], "baz": ""}, ["x", "y"]]`, v.String(), "unexpected string representation for array")
	})

	t.Run("complex-object", func(t *testing.T) {
		s := `{"foo":[-1.345678,[[[[[]]]],{}],"bar"],"baz":{"bbb":123}}`
		want := `{"baz": {"bbb": 123}, "foo": [-1.345678, [[[[[]]]], {}], "bar"]}`
		v, err := p.Parse(s)
		require.NoError(t, err)
		require.Equalf(t, TypeObject, v.Type(), "unexpected type obtained for object: %#v", v)

		require.Equalf(t, want, v.String(), "unexpected string representation for object")
	})
}
