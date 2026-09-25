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
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/decimal"
)

func TestParseRawNumber(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		f := func(s, expectedRN, expectedTail string) {
			t.Helper()

			flen, _, ok := readFloat(s)
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
		f("0.2tail", "0.2", "tail")
		f("-0.2tail", "-0.2", "tail")
	})

	t.Run("error", func(t *testing.T) {
		f := func(s, expectedTail string) {
			t.Helper()

			flen, _, ok := readFloat(s)
			require.False(t, ok, "expecting non-nil error")
			require.Equalf(t, expectedTail, s[flen:], "unexpected tail; got %q; want %q", s[flen:], expectedTail)
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
	// may do on arm64 unless the conversion in mysqlDouble stops it —
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

// TestParseReadsDoublesAsMySQL pins numbers whose double MySQL's JSON parser
// lands somewhere other than the correctly rounded one. Each mysql value is
// what MySQL printed for CAST('[text]' AS JSON), which a correctly rounded read
// turns back into the double MySQL holds.
func TestParseReadsDoublesAsMySQL(t *testing.T) {
	testCases := []struct {
		text  string
		mysql string
	}{
		// Sixteen and seventeen significant digits, no exponent.
		{"9.373401039503115", "9.373401039503117"},
		{"907820456.6878871", "907820456.6878872"},
		{"-97850197.21336927", "-97850197.21336928"},
		{"9918.029753268321", "9918.02975326832"},
		{"22043578.934931774", "22043578.934931777"},
		{"22323.780221271709", "22323.780221271707"},
		// More digits than a double holds.
		{"7.952458273698010123097", "7.952458273698009"},
		{"-5001678.8730277932907349979", "-5001678.873027794"},
		{"32682596125924014.99384040696476537", "3.268259612592402e16"},
		{"0.00000000000056378173515265162148", "0.0000000000005637817351526517"},
		// Integers too long for 64 bits, which MySQL keeps as doubles.
		{"-85542944950666963514162118608", "-8.554294495066698e28"},
		{"9495784086192452298075156", "9.495784086192454e24"},
		// Few digits, but a power of ten that is not exact.
		{"685276831e210", "6.8527683099999996e218"},
		{"4.54827886204e-225", "4.5482788620399995e-225"},
		{"9.755003974708891e271", "9.755003974708892e271"},
		{"0.00000000000000000000021059834276", "2.1059834275999999e-22"},
		{"-682093.3194e-224", "-6.820933194000001e-219"},
		{"0.00000000000009739818150763633668228E+293", "9.739818150763633e279"},
		{"4107408810066.08607026258e-01", "410740881006.60864"},
		// Subnormals, where the scaling underflows before the significand.
		{"2.2250738585072011e-308", "2.2250738585072014e-308"},
		{"2.4703282292062328e-324", "0.0"},
	}

	for _, tc := range testCases {
		t.Run(tc.text, func(t *testing.T) {
			want, err := strconv.ParseFloat(tc.mysql, 64)
			require.NoError(t, err)

			var p Parser
			v, err := p.ParseBytes([]byte(tc.text))
			require.NoError(t, err)
			require.Equal(t, NumberTypeFloat, v.NumberType())
			got, ok := v.Float64()
			require.True(t, ok)
			assert.Equal(t, math.Float64bits(want), math.Float64bits(got), "got %v, MySQL holds %v", got, want)
			assert.Equal(t, tc.mysql, v.String())

			d, isDouble, fits := mysqlDouble(tc.text)
			assert.True(t, isDouble)
			assert.True(t, fits)
			assert.Equal(t, math.Float64bits(want), math.Float64bits(d))
			assert.False(t, mysqlDoubleIsExact(tc.text, writtenExponent(t, tc.text)))

			// Had MySQL printed this spelling, it would have stored the
			// correctly rounded double, which is what ParseStored reads.
			v, err = p.ParseStored([]byte(tc.text))
			require.NoError(t, err)
			got, _ = v.Float64()
			exact, err := strconv.ParseFloat(tc.text, 64)
			require.NoError(t, err)
			assert.Equal(t, math.Float64bits(exact), math.Float64bits(got))
		})
	}
}

// TestParseSpellsDoublesAsPrinted checks that a double MySQL reads to another
// value is spelled as MySQL prints that value, so a reader of the text, such as
// UNHEX, sees the spelling MySQL would hand it: an integral double keeps its
// fraction rather than passing for an integer.
func TestParseSpellsDoublesAsPrinted(t *testing.T) {
	testCases := []struct {
		text string
		want string
	}{
		{"0.9999999999999999", "1.0"},
		{"-0.9999999999999999", "-1.0"},
		{"0.9999999999999999e5", "100000.0"},
		{"0.9999999999999999e15", "1e15"},
		{"0.99999999999999999", "1.0000000000000002"},
		{"9999999999999999.9", "1.0000000000000002e16"},
		{"0.9999999999999999e-5", "0.00001"},
	}

	for _, tc := range testCases {
		t.Run(tc.text, func(t *testing.T) {
			var p Parser
			v, err := p.Parse(tc.text)
			require.NoError(t, err)
			assert.Equal(t, NumberTypeFloat, v.NumberType())
			assert.Equal(t, tc.want, v.Raw())
			assert.Equal(t, tc.want, v.String())
		})
	}
}

// TestParseStored pins doubles as MySQL prints them from documents it holds,
// where its own conversion of the printed form lands on another double. A
// correctly rounded read recovers the double MySQL holds, and Parse would not.
func TestParseStored(t *testing.T) {
	for _, printed := range []string{
		"1.557263309126091e-58",
		"4.679235127060472e-54",
		"9.540364558481608e-24",
		"2.2095423932793299e21",
		"-9.392380146584453e41",
		"2.1651550741616174e34",
		"0.00000000000004480737565052",
		"0.00000000000001985614047401509",
		"92851060.59457423",
	} {
		t.Run(printed, func(t *testing.T) {
			stored, err := strconv.ParseFloat(printed, 64)
			require.NoError(t, err)

			var p Parser
			v, err := p.ParseStored([]byte("[" + printed + "]"))
			require.NoError(t, err)
			a, _ := v.Array()
			got, ok := a[0].Float64()
			require.True(t, ok)
			assert.Equal(t, math.Float64bits(stored), math.Float64bits(got))

			v, err = p.ParseBytes([]byte("[" + printed + "]"))
			require.NoError(t, err)
			a, _ = v.Array()
			got, _ = a[0].Float64()
			assert.NotEqual(t, math.Float64bits(stored), math.Float64bits(got))
		})
	}
}

// TestParseStoredDecimals checks that ParseStored tells a decimal MySQL printed
// from a double it printed. MySQL prints a decimal digit for digit and never
// with an exponent, prints a double in its shortest form, always with a
// fraction or an exponent and never with a fractional zero beyond a lone .0,
// and keeps an integer only while it fits 64 bits. Text only a decimal prints
// reads back as one, with every digit it was stored with.
func TestParseStoredDecimals(t *testing.T) {
	testCases := []struct {
		printed string
		typ     NumberType
	}{
		{"9007199254740993.0", NumberTypeDecimal},
		{"12345678901234567.89", NumberTypeDecimal},
		{"0.30000000000000003", NumberTypeDecimal},
		{"1.500", NumberTypeDecimal},
		{"0.00", NumberTypeDecimal},
		{"-1.50", NumberTypeDecimal},
		{"99999999999999999999", NumberTypeDecimal},
		{"100000000000000000000", NumberTypeDecimal},
		{"9007199254740992.0", NumberTypeFloat},
		{"3922024541026610.5", NumberTypeFloat},
		{"0.30000000000000004", NumberTypeFloat},
		{"0.1", NumberTypeFloat},
		{"-0.0", NumberTypeFloat},
		{"1e20", NumberTypeFloat},
		{"1e-16", NumberTypeFloat},
		{"1.50e+5", NumberTypeFloat},
		{"0.000000000000001", NumberTypeFloat},
		{"100", NumberTypeSigned},
		{"-9223372036854775808", NumberTypeSigned},
		{"18446744073709551615", NumberTypeUnsigned},
	}

	for _, tc := range testCases {
		t.Run(tc.printed, func(t *testing.T) {
			var p Parser
			v, err := p.ParseStored([]byte("[" + tc.printed + "]"))
			require.NoError(t, err)
			a, _ := v.Array()
			require.Len(t, a, 1)
			assert.Equal(t, tc.typ, a[0].NumberType())
			if tc.typ == NumberTypeDecimal {
				assert.Equal(t, "["+tc.printed+"]", v.String())
				got, ok := a[0].NumericValue()
				require.True(t, ok)
				want, err := decimal.NewFromString(tc.printed)
				require.NoError(t, err)
				assert.Equal(t, 0, got.Cmp(want), "%s compares as %s", tc.printed, got)
			}

			// The same text handed to MySQL as JSON is never a decimal.
			v, err = p.ParseBytes([]byte("[" + tc.printed + "]"))
			require.NoError(t, err)
			a, _ = v.Array()
			assert.NotEqual(t, NumberTypeDecimal, a[0].NumberType())
		})
	}
}

// TestParseKeepsForms checks that reading doubles MySQL's way leaves alone
// what MySQL keeps exact, and what it reads to the correctly rounded double
// anyway.
func TestParseKeepsForms(t *testing.T) {
	testCases := []struct {
		text string
		typ  NumberType
		out  string
	}{
		{"1", NumberTypeSigned, "1"},
		{"-9223372036854775808", NumberTypeSigned, "-9223372036854775808"},
		{"9223372036854775808", NumberTypeUnsigned, "9223372036854775808"},
		{"18446744073709551615", NumberTypeUnsigned, "18446744073709551615"},
		{"18446744073709551616", NumberTypeFloat, "1.8446744073709552e19"},
		{"1.0", NumberTypeFloat, "1.0"},
		{"1e2", NumberTypeFloat, "100.0"},
		{"-0.0", NumberTypeFloat, "-0.0"},
		{"0.1", NumberTypeFloat, "0.1"},
		{"9007199254740992.1", NumberTypeFloat, "9.007199254740992e15"},
	}

	for _, tc := range testCases {
		t.Run(tc.text, func(t *testing.T) {
			var p Parser
			v, err := p.ParseBytes([]byte("[" + tc.text + "]"))
			require.NoError(t, err)
			a, _ := v.Array()
			require.Len(t, a, 1)
			assert.Equal(t, tc.typ, a[0].NumberType())
			assert.Equal(t, "["+tc.out+"]", v.String())
		})
	}

	var p Parser
	_, err := p.ParseBytes([]byte("1.7976931348623158e308"))
	require.ErrorContains(t, err, "number too big to be stored in double")
	_, err = p.ParseBytes([]byte("1.79769313486231580e308"))
	require.NoError(t, err)
	// The correctly rounded read of this spelling overflows; MySQL's lands on the largest double.
	v, err := p.ParseBytes([]byte("1.79769313486231581e308"))
	require.NoError(t, err)
	assert.Equal(t, "1.7976931348623157e308", v.String())
}

// TestMySQLDoubleIsExact checks the shortcut that lets Parse skip
// MySQL's conversion: wherever it claims the conversion lands on the correctly
// rounded double, it must.
func TestMySQLDoubleIsExact(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	var checked int
	for range 200000 {
		var b strings.Builder
		if r.IntN(2) == 0 {
			b.WriteByte('-')
		}
		intDigits, fracDigits := 1+r.IntN(16), r.IntN(18)
		if r.IntN(4) == 0 {
			b.WriteByte('0')
		} else {
			b.WriteByte(byte('1' + r.IntN(9)))
			for range intDigits - 1 {
				b.WriteByte(byte('0' + r.IntN(10)))
			}
		}
		if fracDigits > 0 {
			b.WriteByte('.')
			for range fracDigits {
				b.WriteByte(byte('0' + r.IntN(10)))
			}
		}
		if r.IntN(2) == 0 {
			fmt.Fprintf(&b, "e%d", r.IntN(61)-30)
		}
		num := b.String()

		if !mysqlDoubleIsExact(num, writtenExponent(t, num)) {
			continue
		}
		checked++
		d, isDouble, fits := mysqlDouble(num)
		require.True(t, fits, num)
		if !isDouble {
			continue
		}
		exact, err := strconv.ParseFloat(num, 64)
		require.NoError(t, err)
		require.Equal(t, math.Float64bits(exact), math.Float64bits(d), "%s: MySQL lands on %v, correctly rounded is %v", num, d, exact)
	}
	require.Greater(t, checked, 50000)
}

func writtenExponent(t *testing.T, num string) int {
	n, exponent, ok := readFloat(num)
	require.True(t, ok, num)
	require.Equal(t, len(num), n, num)
	return exponent
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
		require.Emptyf(t, a, "unexpected number of items in empty array: %d; want 0", len(a))
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

// TestResolve verifies that resolving a parsed document settles every lazily
// computed field in place: raw strings are unescaped and numbers classified
// at every depth, so later Type and NumberType calls never write, and that
// explicitly constructed values keep their declared kinds.
func TestResolve(t *testing.T) {
	t.Run("parsed", func(t *testing.T) {
		v := MustParse(`{"i": -1, "u": 18446744073709551615, "f": 1.5e300, "s": "\u0070lain", "a": [7, {"n": 2.5, "t": "text"}]}`)

		var unsettled func(v *Value) int
		unsettled = func(v *Value) int {
			switch v.t {
			case TypeObject:
				var n int
				for _, item := range v.o.kvs {
					n += unsettled(item.v)
				}
				return n
			case TypeArray:
				var n int
				for _, item := range v.a {
					n += unsettled(item)
				}
				return n
			case typeRawString:
				return 1
			case TypeNumber:
				if v.n == numberTypeRaw {
					return 1
				}
				return 0
			default:
				return 0
			}
		}
		// Five numbers and two strings: the parser leaves every string raw,
		// escaped or not.
		require.Equal(t, 7, unsettled(v), "parsed strings and numbers must start out lazily settled")

		v.Resolve()
		assert.Zero(t, unsettled(v))

		obj, ok := v.Object()
		require.True(t, ok)
		assert.Equal(t, NumberTypeSigned, obj.Get("i").NumberType())
		assert.Equal(t, NumberTypeUnsigned, obj.Get("u").NumberType())
		assert.Equal(t, NumberTypeFloat, obj.Get("f").NumberType())
		assert.Equal(t, "plain", obj.Get("s").Raw())
	})

	t.Run("constructed", func(t *testing.T) {
		v := NewArray([]*Value{NewNumber("1.5", NumberTypeDecimal), NewString("as is")})
		v.Resolve()
		assert.Equal(t, NumberTypeDecimal, v.a[0].NumberType())
		assert.Equal(t, TypeString, v.a[1].Type())
	})
}
