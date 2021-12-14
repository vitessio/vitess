package decimal

import (
	"fmt"
	"testing"
)

func TestRoundString(t *testing.T) {
	type roundStringTest struct {
		input  string
		mode   RoundingMode
		prec   int
		expect string
	}

	// From https://en.wikipedia.org/wiki/IEEE_floating_point#Rounding_rules
	// Formatted with https://ozh.github.io/ascii-tables/
	//
	// +---------------------------------+-------+-------+-------+-------+
	// |      Mode / Example Value       | +11.5 | +12.5 | −11.5 | −12.5 |
	// +---------------------------------+-------+-------+-------+-------+
	// | to nearest, ties to even        | +12.0 | +12.0 | −12.0 | −12.0 |
	// | to nearest, ties away from zero | +12.0 | +13.0 | −12.0 | −13.0 |
	// | toward 0                        | +11.0 | +12.0 | −11.0 | −12.0 |
	// | toward +∞                       | +12.0 | +13.0 | −11.0 | −12.0 |
	// | toward −∞                       | +11.0 | +12.0 | −12.0 | −13.0 |
	// +---------------------------------+-------+-------+-------+-------+

	makeWikiTests := func(mode RoundingMode, out ...string) []roundStringTest {
		var tests [4]roundStringTest
		for i, inp := range [...]string{"+115", "+125", "-115", "-125"} {
			tests[i] = roundStringTest{inp, mode, 2, out[i]}
		}
		return tests[:]
	}

	even := makeWikiTests(ToNearestEven, "12", "12", "12", "12")
	away := makeWikiTests(ToNearestAway, "12", "13", "12", "13")
	zero := makeWikiTests(ToZero, "11", "12", "11", "12")
	pinf := makeWikiTests(ToPositiveInf, "12", "13", "11", "12")
	ninf := makeWikiTests(ToNegativeInf, "11", "12", "12", "13")

	tests := []roundStringTest{
		{"+12345", ToNearestEven, 4, "1234"},
		{"+12349", ToNearestEven, 4, "1235"},
		{"+12395", ToNearestEven, 4, "1240"},
		{"+99", ToNearestEven, 1, "10"},
		{"+400", ToZero /* mode is irrelevant */, 1, "4"},
	}
	tests = append(tests, even...)
	tests = append(tests, away...)
	tests = append(tests, zero...)
	tests = append(tests, pinf...)
	tests = append(tests, ninf...)

	for i, test := range tests {
		pos := test.input[0] == '+'
		inp := test.input[1:]
		got := roundString([]byte(inp), test.mode, pos, test.prec)
		if string(got) != test.expect {
			t.Fatalf(`#%d:
[round(%q, %s, %d)]
got   : %q
wanted: %q
`, i, test.input, test.mode, test.prec, got, test.expect)
		}
	}
}

func TestDecimal_Format(t *testing.T) {
	for i, s := range [...]struct {
		format string
		input  string
		want   string
	}{
		{"%.10f", "0.1234567891", "0.1234567891"},
		{"%.10f", "0.01", "0.0100000000"},
		{"%.10f", "0.0000000000000000000000000000000000000000000000000000000000001", "0.0000000000"},
	} {
		z, _ := new(Big).SetString(s.input)
		got := fmt.Sprintf(s.format, z)
		if got != s.want {
			t.Fatalf(`#%d: printf(%s)
got   : %s
wanted: %s
`, i, s.format, got, s.want)
		}
	}
}
