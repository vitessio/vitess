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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/hack"
)

func TestParseRawNumber(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		f := func(s, expectedRN, expectedTail string) {
			t.Helper()

			flen, _, ok := readFloat(s)
			if !ok {
				t.Fatalf("unexpected error when parsing '%s'", s)
			}

			rn, tail := s[:flen], s[flen:]

			if rn != expectedRN {
				t.Fatalf("unexpected raw number; got %q; want %q", rn, expectedRN)
			}
			if tail != expectedTail {
				t.Fatalf("unexpected tail; got %q; want %q", tail, expectedTail)
			}
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
		f("0.2tail", "0.2", "tail")
		f("-0.2tail", "-0.2", "tail")
	})

	t.Run("error", func(t *testing.T) {
		f := func(s, expectedTail string) {
			t.Helper()

			flen, _, ok := readFloat(s)
			if ok {
				t.Fatalf("expecting non-nil error")
			}
			if s[flen:] != expectedTail {
				t.Fatalf("unexpected tail; got %q; want %q", s[flen:], expectedTail)
			}
		}

		f("xyz", "xyz")
		f(" ", " ")
		f("[", "[")
		f(",", ",")
		f("{", "{")
		f("\"", "\"")

		// A decimal point needs a digit on either side of it, and a number
		// opens with a minus or a digit.
		f("12.tail", "tail")
		f(".2tail", ".2tail")
		f("-.2tail", ".2tail")
		f("+1tail", "+1tail")
	})
}

// TestParseNumberTooBigForDouble covers the boundary MySQL puts on JSON
// numbers: a number it cannot store as a double makes the whole document
// invalid, rather than being kept at the precision it was written to.
// Underflow is not rejected — it flushes to zero.
func TestParseNumberTooBigForDouble(t *testing.T) {
	tooManyDigits := "1" + strings.Repeat("0", 309)

	t.Run("accepted", func(t *testing.T) {
		for _, doc := range []string{
			"1e308",
			"-1e308",
			"1.7976931348623157e308",
			"-1.7976931348623157e308",
			"99999999999999999999999999999999999999999",
			"1" + strings.Repeat("0", 307),
			// Underflow keeps the document valid and reads as zero.
			"1e-400",
			"1e-1000",
			"1e-1024",
			"0." + strings.Repeat("0", 400) + "1",
			// Digits a double has room for, moved out of the way by a negative
			// exponent.
			"1" + strings.Repeat("0", 307) + "e-1",
			"1" + strings.Repeat("0", 307) + "e-400",
			"-1" + strings.Repeat("0", 307) + "e-400",
			"0." + strings.Repeat("0", 400) + "1e-400",
			// A written sign and a padded exponent are spellings, not
			// magnitudes, and none of these is anywhere near the limit.
			"1e+0",
			"1e-0",
			"1e0000000000",
			"1e-0000000000",
			"1e+308",
			"1e00000000000000000308",
			// A written exponent is bounded by where it puts the decimal
			// point, so digits after the point buy the same number of places
			// back. Zero is subject to the bound like anything else.
			"0e308",
			"-0e308",
			"0.0e309",
			"0.00e310",
			"0.1e309",
			"0.01e310",
			"0." + strings.Repeat("0", 400) + "1e700",
		} {
			t.Run(startEndString(doc), func(t *testing.T) {
				var p Parser
				v, err := p.Parse(doc)
				require.NoError(t, err)
				require.Equal(t, TypeNumber, v.Type())
			})
		}
	})

	t.Run("rejected", func(t *testing.T) {
		for _, doc := range []string{
			"1e309",
			"-1e309",
			"1e1025",
			"1.7976931348623159e308",
			"1e+309",
			tooManyDigits,
			// One place past what the digits after the point buy back. These
			// all convert to zero, so only the exponent as written rules them
			// out.
			"0e309",
			"-0e309",
			"0e+309",
			"0e1000",
			"0.0e310",
			"0.00e311",
			"0.1e310",
			"0.01e311",
			"0." + strings.Repeat("0", 400) + "1e710",
			// Within the written bound, but too big once converted.
			"10e308",
			"1" + strings.Repeat("0", 30) + "e279",
			// More digits than a double has room for. The digits are read before
			// the exponent is applied, so a negative exponent does not buy the
			// room back however far it moves the decimal point afterwards.
			"1" + strings.Repeat("0", 320) + "e-20",
			"1" + strings.Repeat("0", 350) + "e-50",
			"1" + strings.Repeat("0", 400) + "e-400",
			"-1" + strings.Repeat("0", 400) + "e-400",
			"1" + strings.Repeat("0", 400) + ".5e-400",
			strings.Repeat("9", 400) + "e-100",
			// A number anywhere in the document invalidates all of it.
			"[1, 1e309]",
			`{"a": 1e309}`,
			"[[1e309]]",
		} {
			t.Run(startEndString(doc), func(t *testing.T) {
				var p Parser
				_, err := p.Parse(doc)
				require.ErrorContains(t, err, "number too big to be stored in double")
			})
		}
	})

	// A negative exponent is not bounded, only stopped before it overflows the
	// int it accumulates into, and what sends a number through the conversion at
	// all is being written to more digits than a double holds. These cross the
	// two, so the exponent is read into an int that cannot hold it and then
	// scales a significand: written past that stop, it lands wherever the
	// overflow leaves it, which can be a power of ten the table does not go up
	// to. Each of these stays valid and reads as zero. MySQL 8.0.46 accepts all
	// three and reads them as zero too.
	t.Run("a negative exponent written past what an int holds", func(t *testing.T) {
		for _, doc := range []string{
			strings.Repeat("9", 400) + "e-" + strings.Repeat("2", 306),
			"-" + strings.Repeat("9", 400) + "e-" + strings.Repeat("2", 306),
			strings.Repeat("1", 400) + "." + strings.Repeat("5", 20) + "e-" + strings.Repeat("2", 306),
		} {
			t.Run(startEndString(doc), func(t *testing.T) {
				var p Parser
				v, err := p.Parse(doc)
				require.NoError(t, err)

				f, ok := v.Float64()
				require.True(t, ok)
				require.Zero(t, f)
			})
		}
	})

	// The significand accumulates one digit at a time, and each step rounds
	// the multiplication and the addition separately, the way MySQL's builds
	// run the loop. Fusing the two into one rounding — which the Go compiler
	// may do on arm64 unless the conversion in mysqlNumberFits stops it —
	// moves the accumulation an ULP for these documents, and that is enough
	// to push them over the largest double. MySQL 8.0.45, 8.4.11 and 9.4.0
	// accept all of them.
	t.Run("each accumulation step rounds on its own", func(t *testing.T) {
		for _, doc := range []string{
			"17976931348623154547712857878e280",
			"179769313486231559524062337652e279",
			"179769313486231577704643761e282",
			"1797693134862315724800793889e281",
		} {
			t.Run(startEndString(doc), func(t *testing.T) {
				var p Parser
				_, err := p.Parse(doc)
				require.NoError(t, err)
			})
		}
	})

	// Right at the top of the range the answer turns on how a number was
	// written rather than on what it is worth. The digits are split between a
	// significand and a power of ten to scale it by, and where that split falls
	// decides which way the last place rounds — so writing the same value to one
	// more digit moves the split and can move the answer with it.
	t.Run("spelling at the largest double", func(t *testing.T) {
		for _, tc := range []struct {
			doc  string
			fits bool
		}{
			{"1.7976931348623157e308", true},
			{"1.7976931348623158e308", false},
			{"1.79769313486231580e308", true},
			{"1.797693134862315800e308", true},
			{"1.79769313486231581e308", true},
			{"1.79769313486231585e308", true},
			{"1.7976931348623159e308", false},
			{"1.7976931348623157081e308", true},
			{"17976931348623157e292", true},
			{"17976931348623158e292", false},
			{"179769313486231580e291", true},
			{"1797693134862315800e290", false},
			{"17976931348623158000000e286", false},
		} {
			t.Run(tc.doc, func(t *testing.T) {
				var p Parser
				_, err := p.Parse(tc.doc)
				if tc.fits {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, "number too big to be stored in double")
				}
			})
		}
	})
}

// TestParseNumberGrammar covers the shapes JSON's grammar allows a number to
// take. A number opens with a minus or a digit, an integer part of more than
// one digit does not open with a zero, and a decimal point has digits on both
// sides of it. MySQL holds documents to the same grammar, and nan is not a
// number to either of them.
func TestParseNumberGrammar(t *testing.T) {
	t.Run("accepted", func(t *testing.T) {
		for _, doc := range []string{
			"0", "-0", "0.5", "-0.5", "1", "-1", "1.2", "0e0", "-0e0",
			"1e5", "1E5", "1e007", "1e+007", "1e-5", "0.0",
			"[0,1,2]", `{"a":-0.5,"b":[1e5]}`,
		} {
			t.Run(doc, func(t *testing.T) {
				var p Parser
				_, err := p.Parse(doc)
				require.NoError(t, err)
			})
		}
	})

	t.Run("rejected", func(t *testing.T) {
		for _, doc := range []string{
			// An integer part that opens with a zero.
			"007", "-003", "01", "00", "00.5", "01.5", "[007]",
			// A decimal point missing a digit on one side.
			".2", "-.2", "12.", "-12.", "1.e5", `{"a": .2}`, "[12.]",
			// A written plus.
			"+1", "+1.5", "+0", "[+1]",
			// Not a number at all.
			"nan", "NaN", "NAN", "[nan]", `{"a": nan}`, "-nan", ".", "-",
		} {
			t.Run(doc, func(t *testing.T) {
				var p Parser
				_, err := p.Parse(doc)
				require.Error(t, err)
			})
		}
	})
}

// TestParseErrorAbbreviatesTheDocument covers how much of a rejected document
// its error names. Nothing bounds how long a document may be, and Parse copies
// the message it wraps, so naming the text in full hands a client its own
// document back twice over. Each rejection abbreviates what it names, as the
// unparsed tail alongside it already did.
func TestParseErrorAbbreviatesTheDocument(t *testing.T) {
	long := strings.Repeat("9", 100000)

	for _, tc := range []struct {
		name string
		doc  string
	}{
		{name: "a number too big for a double", doc: "1" + long},
		{name: "a written plus", doc: "+" + long},
		{name: "a decimal point with nothing before it", doc: "." + long},
		{name: "a decimal point with nothing after it", doc: long + "."},
		{name: "nan", doc: "nan" + long},
		{name: "nothing the grammar has a shape for", doc: "q" + long},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var p Parser
			_, err := p.Parse(tc.doc)
			require.Error(t, err)
			require.NotContains(t, err.Error(), strings.Repeat("9", 200),
				"the error carries the document it is reporting on")
		})
	}
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
	if us != expectedS {
		t.Fatalf("unexpected unescaped string; got %q; want %q", us, expectedS)
	}
}

func TestParseRawString(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		f := func(s, expectedRS, expectedTail string) {
			t.Helper()

			rs, tail, err := parseRawString(s[1:])
			if err != nil {
				t.Fatalf("unexpected error on parseRawString: %s", err)
			}
			if rs != expectedRS {
				t.Fatalf("unexpected string on parseRawString; got %q; want %q", rs, expectedRS)
			}
			if tail != expectedTail {
				t.Fatalf("unexpected tail on parseRawString; got %q; want %q", tail, expectedTail)
			}

			// parseRawKey results must be identical to parseRawString.
			rs, tail, _, err = parseRawKey(s[1:])
			if err != nil {
				t.Fatalf("unexpected error on parseRawKey: %s", err)
			}
			if rs != expectedRS {
				t.Fatalf("unexpected string on parseRawKey; got %q; want %q", rs, expectedRS)
			}
			if tail != expectedTail {
				t.Fatalf("unexpected tail on parseRawKey; got %q; want %q", tail, expectedTail)
			}
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
			if err == nil {
				t.Fatalf("expecting non-nil error on parseRawString")
			}
			if tail != expectedTail {
				t.Fatalf("unexpected tail on parseRawString; got %q; want %q", tail, expectedTail)
			}

			// parseRawKey results must be identical to parseRawString.
			_, tail, _, err = parseRawKey(s[1:])
			if err == nil {
				t.Fatalf("expecting non-nil error on parseRawKey")
			}
			if tail != expectedTail {
				t.Fatalf("unexpected tail on parseRawKey; got %q; want %q", tail, expectedTail)
			}
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
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	a, _ := v.Array()

	// object
	_, ok := a[0].Object()
	if !ok {
		t.Fatalf("unexpected error when obtaining object")
	}
	_, ok = a[0].Array()
	if ok {
		t.Fatalf("expecting non-nil error when trying to obtain array from object")
	}

	// array
	_, ok = a[1].Array()
	if !ok {
		t.Fatalf("unexpected error when obtaining array")
	}
	_, ok = a[1].Object()
	if ok {
		t.Fatalf("expecting non-nil error when trying to obtain object from array")
	}

	// string
	_, ok = a[2].StringBytes()
	if !ok {
		t.Fatalf("unexpected error when obtaining string")
	}
}

func TestParserParse(t *testing.T) {
	var p Parser

	t.Run("invalid-string-escape", func(t *testing.T) {
		v, err := p.Parse(`"fo\u"`)
		if err != nil {
			t.Fatalf("unexpected error when parsing string")
		}
		// Make sure only valid string part remains
		sb, ok := v.StringBytes()
		if !ok {
			t.Fatalf("cannot obtain string")
		}
		if string(sb) != "fo\\u" {
			t.Fatalf("unexpected string; got %q; want %q", sb, "fo\\u")
		}

		v, err = p.Parse(`"foo\ubarz2134"`)
		if err != nil {
			t.Fatalf("unexpected error when parsing string")
		}
		sb, ok = v.StringBytes()
		if !ok {
			t.Fatalf("cannot obtain string")
		}
		if string(sb) != "foo\\ubarz2134" {
			t.Fatalf("unexpected string; got %q; want %q", sb, "foo")
		}

		v, err = p.Parse(`"fo` + "\x19" + `\u"`)
		if err != nil {
			t.Fatalf("unexpected error when parsing string")
		}
		sb, ok = v.StringBytes()
		if !ok {
			t.Fatalf("cannot obtain string")
		}
		if string(sb) != "fo\x19\\u" {
			t.Fatalf("unexpected string; got %q; want %q", sb, "fo\x19\\u")
		}
	})

	t.Run("empty-json", func(t *testing.T) {
		_, err := p.Parse("")
		if err == nil {
			t.Fatalf("expecting non-nil error when parsing empty json")
		}
		_, err = p.Parse("\n\t    \n")
		if err == nil {
			t.Fatalf("expecting non-nil error when parsing empty json")
		}
	})

	t.Run("invalid-tail", func(t *testing.T) {
		_, err := p.Parse("123 456")
		if err == nil {
			t.Fatalf("expecting non-nil error when parsing invalid tail")
		}
		_, err = p.Parse("[] 1223")
		if err == nil {
			t.Fatalf("expecting non-nil error when parsing invalid tail")
		}
	})

	t.Run("invalid-json", func(t *testing.T) {
		f := func(s string) {
			t.Helper()
			if _, err := p.Parse(s); err == nil {
				t.Fatalf("expecting non-nil error when parsing invalid json %q", s)
			}
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

		if _, err := p.Parse("-2134.453E+43"); err != nil {
			t.Fatalf("unexpected error when parsing number: %s", err)
		}

		// Incomplete object key key.
		f(`{"foo: 123}`)

		// Incomplete string.
		f(`"{\"foo\": 123}`)

		v, err := p.Parse(`"{\"foo\": 123}"`)
		if err != nil {
			t.Fatalf("unexpected error when parsing json string: %s", err)
		}
		sb, _ := v.StringBytes()
		if string(sb) != `{"foo": 123}` {
			t.Fatalf("unexpected string value; got %q; want %q", sb, `{"foo": 123}`)
		}
	})

	t.Run("incomplete-object", func(t *testing.T) {
		f := func(s string) {
			t.Helper()
			if _, err := p.Parse(s); err == nil {
				t.Fatalf("expecting non-nil error when parsing incomplete object %q", s)
			}
		}

		f(" {  ")
		f(`{"foo"`)
		f(`{"foo":`)
		f(`{"foo":null`)
		f(`{"foo":null,`)
		f(`{"foo":null,}`)
		f(`{"foo":null,"bar"}`)

		if _, err := p.Parse(`{"foo":null,"bar":"baz"}`); err != nil {
			t.Fatalf("unexpected error when parsing object: %s", err)
		}
	})

	t.Run("incomplete-array", func(t *testing.T) {
		f := func(s string) {
			t.Helper()
			if _, err := p.Parse(s); err == nil {
				t.Fatalf("expecting non-nil error when parsing incomplete array %q", s)
			}
		}

		f("  [ ")
		f("[123")
		f("[123,")
		f("[123,]")
		f("[123,{}")
		f("[123,{},]")

		if _, err := p.Parse("[123,{},[]]"); err != nil {
			t.Fatalf("unexpected error when parsing array: %s", err)
		}
	})

	t.Run("incomplete-string", func(t *testing.T) {
		f := func(s string) {
			t.Helper()
			if _, err := p.Parse(s); err == nil {
				t.Fatalf("expecting non-nil error when parsing incomplete string %q", s)
			}
		}

		f(`  "foo`)
		f(`"foo\`)
		f(`"foo\"`)
		f(`"foo\\\"`)
		f(`"foo'`)
		f(`"foo'bar'`)

		if _, err := p.Parse(`"foo\\\""`); err != nil {
			t.Fatalf("unexpected error when parsing string: %s", err)
		}
	})

	t.Run("empty-object", func(t *testing.T) {
		v, err := p.Parse("{}")
		if err != nil {
			t.Fatalf("cannot parse empty object: %s", err)
		}
		tp := v.Type()
		if tp != TypeObject || tp.String() != "object" {
			t.Fatalf("unexpected value obtained for empty object: %#v", v)
		}
		o, ok := v.Object()
		if !ok {
			t.Fatalf("cannot obtain object")
		}
		n := o.Len()
		if n != 0 {
			t.Fatalf("unexpected number of items in empty object: %d; want 0", n)
		}
		s := v.String()
		if s != "{}" {
			t.Fatalf("unexpected string representation of empty object: got %q; want %q", s, "{}")
		}
	})

	t.Run("empty-array", func(t *testing.T) {
		v, err := p.Parse("[]")
		if err != nil {
			t.Fatalf("cannot parse empty array: %s", err)
		}
		tp := v.Type()
		if tp != TypeArray || tp.String() != "array" {
			t.Fatalf("unexpected value obtained for empty array: %#v", v)
		}
		a, ok := v.Array()
		if !ok {
			t.Fatalf("unexpected error")
		}
		n := len(a)
		if n != 0 {
			t.Fatalf("unexpected number of items in empty array: %d; want 0", n)
		}
		s := v.String()
		if s != "[]" {
			t.Fatalf("unexpected string representation of empty array: got %q; want %q", s, "[]")
		}
	})

	t.Run("null", func(t *testing.T) {
		v, err := p.Parse("null")
		if err != nil {
			t.Fatalf("cannot parse null: %s", err)
		}
		tp := v.Type()
		if tp != TypeNull || tp.String() != "null" {
			t.Fatalf("unexpected value obtained for null: %#v", v)
		}
		s := v.String()
		if s != "null" {
			t.Fatalf("unexpected string representation of null; got %q; want %q", s, "null")
		}
	})

	t.Run("true", func(t *testing.T) {
		v, err := p.Parse("true")
		if err != nil {
			t.Fatalf("cannot parse true: %s", err)
		}
		if v != ValueTrue {
			t.Fatalf("unexpected value obtained for true: %#v", v)
		}
		b, ok := v.Bool()
		if !ok {
			t.Fatalf("unexpected error")
		}
		if !b {
			t.Fatalf("expecting true; got false")
		}
		s := v.String()
		if s != "true" {
			t.Fatalf("unexpected string representation of true; got %q; want %q", s, "true")
		}
	})

	t.Run("false", func(t *testing.T) {
		v, err := p.Parse("false")
		if err != nil {
			t.Fatalf("cannot parse false: %s", err)
		}
		if v != ValueFalse {
			t.Fatalf("unexpected value obtained for false: %#v", v)
		}
		b, ok := v.Bool()
		if !ok {
			t.Fatalf("unexpected error")
		}
		if b {
			t.Fatalf("expecting false; got true")
		}
		s := v.String()
		if s != "false" {
			t.Fatalf("unexpected string representation of false; got %q; want %q", s, "false")
		}
	})

	t.Run("integer", func(t *testing.T) {
		v, err := p.Parse("12345")
		if err != nil {
			t.Fatalf("cannot parse integer: %s", err)
		}
		tp := v.Type()
		if tp != TypeNumber || tp.String() != "number" {
			t.Fatalf("unexpected type obtained for integer: %#v", v)
		}
		if v.NumberType() != NumberTypeSigned {
			t.Fatalf("unexpected non integer value: %#v", v)
		}
		s := v.String()
		if s != "12345" {
			t.Fatalf("unexpected string representation of integer; got %q; want %q", s, "12345")
		}
	})

	t.Run("int64", func(t *testing.T) {
		v, err := p.Parse("-8838840643388017390")
		if err != nil {
			t.Fatalf("cannot parse int64: %s", err)
		}
		tp := v.Type()
		if tp != TypeNumber || tp.String() != "number" {
			t.Fatalf("unexpected type obtained for int64: %#v", v)
		}
		s := v.String()
		if s != "-8838840643388017390" {
			t.Fatalf("unexpected string representation of int64; got %q; want %q", s, "-8838840643388017390")
		}
	})

	t.Run("uint", func(t *testing.T) {
		v, err := p.Parse("18446744073709551615")
		if err != nil {
			t.Fatalf("cannot parse uint: %s", err)
		}
		tp := v.Type()
		if tp != TypeNumber || tp.String() != "number" {
			t.Fatalf("unexpected type obtained for uint: %#v", v)
		}
		s := v.String()
		if s != "18446744073709551615" {
			t.Fatalf("unexpected string representation of uint; got %q; want %q", s, "18446744073709551615")
		}
	})

	t.Run("uint64", func(t *testing.T) {
		v, err := p.Parse("18446744073709551615")
		if err != nil {
			t.Fatalf("cannot parse uint64: %s", err)
		}
		tp := v.Type()
		if tp != TypeNumber || tp.String() != "number" {
			t.Fatalf("unexpected type obtained for uint64: %#v", v)
		}
		s := v.String()
		if s != "18446744073709551615" {
			t.Fatalf("unexpected string representation of uint64; got %q; want %q", s, "18446744073709551615")
		}
	})

	t.Run("float", func(t *testing.T) {
		v, err := p.Parse("-12.345")
		if err != nil {
			t.Fatalf("cannot parse float: %s", err)
		}
		tp := v.Type()
		if tp != TypeNumber || tp.String() != "number" {
			t.Fatalf("unexpected type obtained for integer: %#v", v)
		}
		if v.NumberType() != NumberTypeFloat {
			t.Fatalf("unexpected integer value: %#v", v)
		}
		s := v.String()
		if s != "-12.345" {
			t.Fatalf("unexpected string representation of integer; got %q; want %q", s, "-12.345")
		}
	})

	t.Run("float with zero", func(t *testing.T) {
		v, err := p.Parse("12.0")
		if err != nil {
			t.Fatalf("cannot parse float: %s", err)
		}
		tp := v.Type()
		if tp != TypeNumber || tp.String() != "number" {
			t.Fatalf("unexpected type obtained for number: %#v", v)
		}
		if v.NumberType() != NumberTypeFloat {
			t.Fatalf("unexpected integer value: %#v", v)
		}
		s := v.String()
		if s != "12.0" {
			t.Fatalf("unexpected string representation of float; got %q; want %q", s, "12.0")
		}
	})

	t.Run("float with large exponent", func(t *testing.T) {
		v, err := p.Parse("1e100")
		if err != nil {
			t.Fatalf("cannot parse float: %s", err)
		}
		tp := v.Type()
		if tp != TypeNumber || tp.String() != "number" {
			t.Fatalf("unexpected type obtained for number: %#v", v)
		}
		if v.NumberType() != NumberTypeFloat {
			t.Fatalf("unexpected integer value: %#v", v)
		}
		s := v.String()
		if s != "1e100" {
			t.Fatalf("unexpected string representation of float; got %q; want %q", s, "1e100")
		}
	})

	t.Run("string", func(t *testing.T) {
		v, err := p.Parse(`"foo bar"`)
		if err != nil {
			t.Fatalf("cannot parse string: %s", err)
		}
		tp := v.Type()
		if tp != TypeString || tp.String() != "string" {
			t.Fatalf("unexpected type obtained for string: %#v", v)
		}
		sb, ok := v.StringBytes()
		if !ok {
			t.Fatalf("cannot obtain string")
		}
		if string(sb) != "foo bar" {
			t.Fatalf("unexpected value obtained for string; got %q; want %q", sb, "foo bar")
		}
		ss := v.String()
		if ss != `"foo bar"` {
			t.Fatalf("unexpected string representation of string; got %q; want %q", ss, `"foo bar"`)
		}
	})

	t.Run("string-escaped", func(t *testing.T) {
		v, err := p.Parse(`"\n\t\\foo\"bar\u3423x\/\b\f\r\\"`)
		if err != nil {
			t.Fatalf("cannot parse string: %s", err)
		}
		tp := v.Type()
		if tp != TypeString {
			t.Fatalf("unexpected type obtained for string: %#v", v)
		}
		sb, ok := v.StringBytes()
		if !ok {
			t.Fatalf("cannot obtain string")
		}
		if string(sb) != "\n\t\\foo\"bar\u3423x/\b\f\r\\" {
			t.Fatalf("unexpected value obtained for string; got %q; want %q", sb, "\n\t\\foo\"bar\u3423x/\b\f\r\\")
		}
		ss := v.String()
		if ss != `"\n\t\\foo\"bar㐣x/\b\f\r\\"` {
			t.Fatalf("unexpected string representation of string; got %q; want %q", ss, `"\n\t\\foo\"bar㐣x/\b\f\r\\"`)
		}
	})

	t.Run("object-one-element", func(t *testing.T) {
		v, err := p.Parse(`  {
	"foo"   : "bar"  }	 `)
		if err != nil {
			t.Fatalf("cannot parse object: %s", err)
		}
		tp := v.Type()
		if tp != TypeObject {
			t.Fatalf("unexpected type obtained for object: %#v", v)
		}
		o, ok := v.Object()
		if !ok {
			t.Fatalf("cannot obtain object")
		}
		vv := o.Get("foo")
		if vv.Type() != TypeString {
			t.Fatalf("unexpected type for foo item: got %d; want %d", vv.Type(), TypeString)
		}
		vv = o.Get("non-existing key")
		if vv != nil {
			t.Fatalf("unexpected value obtained for non-existing key: %#v", vv)
		}

		s := v.String()
		if s != `{"foo": "bar"}` {
			t.Fatalf("unexpected string representation for object; got %q; want %q", s, `{"foo":"bar"}`)
		}
	})

	t.Run("object-multi-elements", func(t *testing.T) {
		v, err := p.Parse(`{"foo": [1,2,3  ]  ,"bar":{},"baz":123.456}`)
		if err != nil {
			t.Fatalf("cannot parse object: %s", err)
		}
		tp := v.Type()
		if tp != TypeObject {
			t.Fatalf("unexpected type obtained for object: %#v", v)
		}
		o, ok := v.Object()
		if !ok {
			t.Fatalf("cannot obtain object")
		}
		vv := o.Get("foo")
		if vv.Type() != TypeArray {
			t.Fatalf("unexpected type for foo item; got %d; want %d", vv.Type(), TypeArray)
		}
		vv = o.Get("bar")
		if vv.Type() != TypeObject {
			t.Fatalf("unexpected type for bar item; got %d; want %d", vv.Type(), TypeObject)
		}
		vv = o.Get("baz")
		if vv.Type() != TypeNumber {
			t.Fatalf("unexpected type for baz item; got %d; want %d", vv.Type(), TypeNumber)
		}
		vv = o.Get("non-existing-key")
		if vv != nil {
			t.Fatalf("unexpected value obtained for non-existing key: %#v", vv)
		}

		s := v.String()
		if s != "{\"bar\": {}, \"baz\": 123.456, \"foo\": [1, 2, 3]}" {
			t.Fatalf("unexpected string representation for object; got %q; want %q", s, "{\"bar\": {}, \"baz\": 123.456, \"foo\": [1, 2, 3]}")
		}
	})

	t.Run("array-one-element", func(t *testing.T) {
		v, err := p.Parse(`   [{"bar":[  [],[[]]   ]} ]  `)
		if err != nil {
			t.Fatalf("cannot parse array: %s", err)
		}
		tp := v.Type()
		if tp != TypeArray {
			t.Fatalf("unexpected type obtained for array: %#v", v)
		}
		a, ok := v.Array()
		if !ok {
			t.Fatalf("unexpected error")
		}
		if len(a) != 1 {
			t.Fatalf("unexpected array len; got %d; want %d", len(a), 1)
		}
		if a[0].Type() != TypeObject {
			t.Fatalf("unexpected type for a[0]; got %d; want %d", a[0].Type(), TypeObject)
		}

		s := v.String()
		if s != `[{"bar": [[], [[]]]}]` {
			t.Fatalf("unexpected string representation for array; got %q; want %q", s, `[{"bar":[[],[[]]]}]`)
		}
	})

	t.Run("array-multi-elements", func(t *testing.T) {
		v, err := p.Parse(`   [1,"foo",{"bar":[     ],"baz":""}    ,[  "x" ,	"y"   ]     ]   `)
		if err != nil {
			t.Fatalf("cannot parse array: %s", err)
		}
		tp := v.Type()
		if tp != TypeArray {
			t.Fatalf("unexpected type obtained for array: %#v", v)
		}
		a, ok := v.Array()
		if !ok {
			t.Fatalf("unexpected error")
		}
		if len(a) != 4 {
			t.Fatalf("unexpected array len; got %d; want %d", len(a), 4)
		}
		if a[0].Type() != TypeNumber {
			t.Fatalf("unexpected type for a[0]; got %d; want %d", a[0].Type(), TypeNumber)
		}
		if a[1].Type() != TypeString {
			t.Fatalf("unexpected type for a[1]; got %d; want %d", a[1].Type(), TypeString)
		}
		if a[2].Type() != TypeObject {
			t.Fatalf("unexpected type for a[2]; got %d; want %d", a[2].Type(), TypeObject)
		}
		if a[3].Type() != TypeArray {
			t.Fatalf("unexpected type for a[3]; got %d; want %d", a[3].Type(), TypeArray)
		}

		s := v.String()
		if s != `[1, "foo", {"bar": [], "baz": ""}, ["x", "y"]]` {
			t.Fatalf("unexpected string representation for array; got %q; want %q", s, `[1,"foo",{"bar":[],"baz":""},["x","y"]]`)
		}
	})

	t.Run("complex-object", func(t *testing.T) {
		s := `{"foo":[-1.345678,[[[[[]]]],{}],"bar"],"baz":{"bbb":123}}`
		want := `{"baz": {"bbb": 123}, "foo": [-1.345678, [[[[[]]]], {}], "bar"]}`
		v, err := p.Parse(s)
		if err != nil {
			t.Fatalf("cannot parse complex object: %s", err)
		}
		if v.Type() != TypeObject {
			t.Fatalf("unexpected type obtained for object: %#v", v)
		}

		ss := v.String()
		if ss != want {
			t.Fatalf("unexpected string representation for object; got %q; want %q", ss, want)
		}
	})
}

// TestMarshalToBlob verifies that marshaling a blob or bit value appends to
// the caller's buffer instead of discarding previously accumulated output.
func TestMarshalToBlob(t *testing.T) {
	// base64("foo") == "Zm9v".
	const encoded = `"base64:type15:Zm9v"`

	t.Run("bare", func(t *testing.T) {
		require.Equal(t, encoded, string(NewBlob("foo").MarshalTo(nil)))
		require.Equal(t, encoded, string(NewBit("foo").MarshalTo(nil)))
	})

	t.Run("inside-array", func(t *testing.T) {
		v := NewArray([]*Value{NewString("a"), NewBlob("foo")})
		require.Equal(t, `["a", `+encoded+`]`, string(v.MarshalTo(nil)))

		v = NewArray([]*Value{NewString("a"), NewBit("foo")})
		require.Equal(t, `["a", `+encoded+`]`, string(v.MarshalTo(nil)))
	})

	t.Run("inside-object", func(t *testing.T) {
		var obj Object
		obj.Add("k", NewBlob("foo"))
		require.Equal(t, `{"k": `+encoded+`}`, string(NewObject(obj).MarshalTo(nil)))
	})
}
