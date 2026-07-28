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
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/decimal"
	"vitess.io/vitess/go/mysql/format"
	"vitess.io/vitess/go/vt/vthash"
)

func TestParseRawNumber(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		f := func(s, expectedRN, expectedTail string) {
			t.Helper()

			flen, _, _, ok := readFloat(s)
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

			flen, _, _, ok := readFloat(s)
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

// TestParseCastDoubles pins the double a number is worth to the double MySQL
// stores for the same text. MySQL parses JSON at normal precision — the
// significand accumulates into a double and one multiplication scales it —
// so for long significands and large exponents its double sits an ULP or two
// from the nearest one, and an expression evaluated here has to land on
// MySQL's. The expected bits below are what MySQL 8.0.45, 8.4.11 and 9.4.0
// store, confirmed by comparing JSON_EXTRACT of each document against a
// correctly-rounded CAST of the expected value inside the server.
func TestParseCastDoubles(t *testing.T) {
	docs := []struct {
		doc  string
		want uint64 // IEEE 754 bits of MySQL's double
	}{
		{"99999999999999999999999999999999999999999", 0x48725dfa371a19e6},  // 9.999999999999998e40, nearest is 1e41
		{"-99999999999999999999999999999999999999999", 0xc8725dfa371a19e6}, // sign rides on the same conversion
		{"100000000000000000000000000000000000000000", 0x48725dfa371a19e6}, // meets the nines at the same double
		{"1e41", 0x48725dfa371a19e7},                                       // one table lookup, lands on the nearest double
		{"10e40", 0x48725dfa371a19e7},
		{"1.7976931348623157081e308", 0x7feffffffffffffe}, // one below the largest double; nearest is the largest
		{"1.79769313486231580e308", 0x7fefffffffffffff},
		{"17976931348623157e292", 0x7fefffffffffffff},
		{"291.276103743955106997454", 0x4072346aebc26973},
		{"41.451230056623148274", 0x4044b9c1e8101597},
		{"2.2250738585072014e-308", 0x0010000000000000}, // smallest normal double
		{"1e-320", 0x00000000000007e8},                  // subnormal, scaled in two steps
		{"1e-400", 0x0000000000000000},                  // flushes to zero rather than the smallest subnormal
		{"123456789012345678901234567890", 0x45f8ee90ff6c373d},
		{"3.14159265358979323846264338327950288", 0x400921fb54442d18},
		{"18446744073709551616", 0x43f0000000000000},      // one past the largest integer, forced onto the double path
		{"0.99999999999999999999999", 0x3ff0000000000001}, // one above 1.0; the nearest double is 1.0
		{"1234567890.12345678901234567890", 0x41d26580b487e6b8},
		{"0.30000000000000000000000000001", 0x3fd3333333333333},
		{"0.5", 0x3fe0000000000000},
	}

	t.Run("character data reads as MySQL's double", func(t *testing.T) {
		for _, tc := range docs {
			t.Run(tc.doc, func(t *testing.T) {
				var p Parser
				v, err := p.ParseCast(tc.doc)
				require.NoError(t, err)
				require.Equal(t, NumberTypeFloat, v.NumberType())
				f, ok := v.Float64()
				require.True(t, ok)
				require.Equal(t, tc.want, math.Float64bits(f),
					"got %v (%016x), want %v (%016x)",
					f, math.Float64bits(f), math.Float64frombits(tc.want), tc.want)
			})
		}
	})

	// A printed JSON value spells each double exactly — whoever printed it
	// chose text that reads back as the double it held — so reading it as the
	// nearest double reconstructs that double, and MySQL's conversion must
	// stay out of it.
	t.Run("printed JSON reads as the nearest double", func(t *testing.T) {
		for _, tc := range docs {
			t.Run(tc.doc, func(t *testing.T) {
				var p Parser
				v, err := p.Parse(tc.doc)
				require.NoError(t, err)
				f, ok := v.Float64()
				require.True(t, ok)
				nearest, err := strconv.ParseFloat(tc.doc, 64)
				require.NoError(t, err)
				require.Equal(t, math.Float64bits(nearest), math.Float64bits(f))
			})
		}
	})

	// A number built around an existing double — a float column, a binary log
	// entry — carries that double's exact spelling and has to keep holding the
	// same double, even where MySQL's document conversion would read the very
	// same text into a different one.
	t.Run("numbers built from a double keep that double", func(t *testing.T) {
		f := math.Float64frombits(0xd64d4ae72831c4f2) // -5.3746011104623175e107
		text := string(format.FormatFloat(f))

		v := NewNumber(text, NumberTypeFloat)
		got, ok := v.Float64()
		require.True(t, ok)
		require.Equal(t, math.Float64bits(f), math.Float64bits(got))

		var p Parser
		doc, err := p.ParseCast(text)
		require.NoError(t, err)
		converted, ok := doc.Float64()
		require.True(t, ok)
		require.Equal(t, uint64(0xd64d4ae72831c4f3), math.Float64bits(converted),
			"MySQL reads this spelling into the double one ULP over")
	})

	// Two spellings that MySQL stores as the same double are the same value
	// everywhere a value reaches: they convert, weigh and therefore hash
	// alike, while a spelling stored as a different double weighs apart.
	t.Run("spellings weigh by their double", func(t *testing.T) {
		var p Parser
		nines, err := p.ParseCast("99999999999999999999999999999999999999999")
		require.NoError(t, err)
		ninesWeight := nines.WeightString(nil)

		var q Parser
		ten41, err := q.ParseCast("100000000000000000000000000000000000000000")
		require.NoError(t, err)
		require.Equal(t, ninesWeight, ten41.WeightString(nil))

		var r Parser
		e41, err := r.ParseCast("1e41")
		require.NoError(t, err)
		require.NotEqual(t, ninesWeight, e41.WeightString(nil))
	})
}

// TestDecimalOfFloat pins how a float becomes a decimal: through the shortest
// text of its double, which is what MySQL's double2decimal prints and reads
// back. The digits the document spelled beyond the double's precision are
// gone by then, while a number that is a decimal to begin with keeps all of
// them.
func TestDecimalOfFloat(t *testing.T) {
	text := "0.30000000000000000000000000001"

	var p Parser
	v, err := p.ParseCast(text)
	require.NoError(t, err)
	dec, ok := v.Decimal()
	require.True(t, ok)
	require.True(t, dec.Equal(decimal.NewFromFloat(0.3)), "got %s", dec.String())

	exact, err := decimal.NewFromString(text)
	require.NoError(t, err)
	kept, ok := NewNumber(text, NumberTypeDecimal).Decimal()
	require.True(t, ok)
	require.True(t, kept.Equal(exact), "got %s", kept.String())
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

// TestNumberKindMatchesParsing is the safety net under deciding a number's kind
// from its shape: the shape rule exists to avoid the conversions, so it has to
// reach the same answer they would.
func TestNumberKindMatchesParsing(t *testing.T) {
	var spellings []string
	for digits := 1; digits <= 25; digits++ {
		for _, lead := range []string{"1", "9"} {
			run := lead + strings.Repeat("7", digits-1)
			spellings = append(spellings, run, "-"+run, "0"+run, "-0"+run, strings.Repeat("0", digits)+run)
		}
	}
	// The values either side of every limit the rule has to respect.
	spellings = append(spellings,
		"0", "-0", "00", "9223372036854775806", "9223372036854775807", "9223372036854775808",
		"-9223372036854775807", "-9223372036854775808", "-9223372036854775809",
		"18446744073709551614", "18446744073709551615", "18446744073709551616",
	)

	for _, spelling := range spellings {
		t.Run(spelling, func(t *testing.T) {
			want := parseNumberType(spelling)
			got := numberKind(spelling, false)
			if want == NumberTypeUnknown {
				// Too long for any of them to hold; the parser rejects it as a
				// document, and until then it is a double like any other.
				require.Equal(t, NumberTypeFloat, got)
				return
			}
			require.Equalf(t, want, got, "%q", spelling)
		})
	}
}

// TestParseSettlesValues checks that parsing leaves nothing for a reader to
// work out later. A parsed document is shared by every goroutine running a
// cached plan, so a read that rewrites the value it read is a data race.
func TestParseSettlesValues(t *testing.T) {
	var p Parser
	v, err := p.Parse(`{"k": "a\u0062", "n": [1, 2.5, 3e4, 18446744073709551615], "s": "plain"}`)
	require.NoError(t, err)

	var walk func(*Value)
	walk = func(v *Value) {
		switch v.t {
		case TypeArray:
			for _, elem := range v.a {
				walk(elem)
			}
		case TypeObject:
			for _, kv := range v.o.kvs {
				walk(kv.v)
			}
		case TypeString:
			require.NotContains(t, v.s, `\u`, "string still carries an escape")
		case TypeNumber:
			require.NotEqual(t, NumberTypeUnknown, v.n, "number kind left undecided")
		}
		// Reading a value must not change it.
		before := *v
		require.Equal(t, before.t, v.Type())
		require.Equal(t, before.n, v.NumberType())
		require.Equal(t, before.s, v.s)
	}
	walk(v)
}

// TestParseUnescapesStrings pins that a string reads and renders the same
// whether or not anything has looked at it, which is what MySQL does: it
// resolves an escape when it parses the document, so \u0061 is stored and
// printed as a.
func TestParseUnescapesStrings(t *testing.T) {
	for doc, want := range map[string]string{
		`"\u0061"`:   `"a"`,
		`["\u0061"]`: `["a"]`,
		`"\u00e9"`:   `"é"`,
		`"a\tb"`:     `"a\tb"`,
		`"plain"`:    `"plain"`,
	} {
		t.Run(doc, func(t *testing.T) {
			var p Parser
			v, err := p.Parse(doc)
			require.NoError(t, err)
			require.Equal(t, want, v.String())
		})
	}
}

// TestParseConcurrentReads is the regression test for the races this settling
// removes: one parsed document, read by several goroutines at once, which is
// how a folded JSON literal in a cached plan is used.
func TestParseConcurrentReads(t *testing.T) {
	for _, doc := range []string{
		`["a", "b", "c", "d"]`,
		`["\u0061", "\u0062"]`,
		`[1, 2.5, 3e4, 18446744073709551615]`,
		`{"ka": "vb", "n": [1, 2.5, "x"]}`,
		`2.5`,
		`"a"`,
	} {
		t.Run(doc, func(t *testing.T) {
			var p Parser
			v, err := p.Parse(doc)
			require.NoError(t, err)

			const readers = 8
			fingerprints := make([]vthash.Hash, readers)
			renders := make([]string, readers)

			var start sync.WaitGroup
			var readersDone sync.WaitGroup
			start.Add(1)
			for i := range readers {
				readersDone.Go(func() {
					start.Wait()
					h := vthash.New()
					v.Hash(&h)
					fingerprints[i] = h.Sum128()
					renders[i] = v.String()
				})
			}
			start.Done()
			readersDone.Wait()

			for i := 1; i < readers; i++ {
				require.Equal(t, fingerprints[0], fingerprints[i])
				require.Equal(t, renders[0], renders[i])
			}
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
