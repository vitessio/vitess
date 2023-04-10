/*
Copyright 2022 The Vitess Authors.
Copyright (c) 2015 Spring, Inc.
Copyright (c) 2013 Oguz Bilgic

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

package decimal

import (
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"testing/quick"
)

type testEnt struct {
	float   float64
	short   string
	exact   string
	inexact string
}

var testTable = []*testEnt{
	{3.141592653589793, "3.141592653589793", "", "3.14159265358979300000000000000000000000000000000000004"},
	{3, "3", "", "3.0000000000000000000000002"},
	{1234567890123456, "1234567890123456", "", "1234567890123456.00000000000000002"},
	{1234567890123456000, "1234567890123456000", "", "1234567890123456000.0000000000000008"},
	{1234.567890123456, "1234.567890123456", "", "1234.5678901234560000000000000009"},
	{.1234567890123456, "0.1234567890123456", "", "0.12345678901234560000000000006"},
	{0, "0", "", "0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001"},
	{.1111111111111110, "0.111111111111111", "", "0.111111111111111000000000000000009"},
	{.1111111111111111, "0.1111111111111111", "", "0.111111111111111100000000000000000000023423545644534234"},
	{.1111111111111119, "0.1111111111111119", "", "0.111111111111111900000000000000000000000000000000000134123984192834"},
	{.000000000000000001, "0.000000000000000001", "", "0.00000000000000000100000000000000000000000000000000012341234"},
	{.000000000000000002, "0.000000000000000002", "", "0.0000000000000000020000000000000000000012341234123"},
	{.000000000000000003, "0.000000000000000003", "", "0.00000000000000000299999999999999999999999900000000000123412341234"},
	{.000000000000000005, "0.000000000000000005", "", "0.00000000000000000500000000000000000023412341234"},
	{.000000000000000008, "0.000000000000000008", "", "0.0000000000000000080000000000000000001241234432"},
	{.1000000000000001, "0.1000000000000001", "", "0.10000000000000010000000000000012341234"},
	{.1000000000000002, "0.1000000000000002", "", "0.10000000000000020000000000001234123412"},
	{.1000000000000003, "0.1000000000000003", "", "0.1000000000000003000000000000001234123412"},
	{.1000000000000005, "0.1000000000000005", "", "0.1000000000000005000000000000000006441234"},
	{.1000000000000008, "0.1000000000000008", "", "0.100000000000000800000000000000000009999999999999999999999999999"},
	{1e25, "10000000000000000000000000", "", ""},
	{1.5e14, "150000000000000", "", ""},
	{1.5e15, "1500000000000000", "", ""},
	{1.5e16, "15000000000000000", "", ""},
	{1.0001e25, "10001000000000000000000000", "", ""},
	{1.0001000000000000033e25, "10001000000000000000000000", "", ""},
	{2e25, "20000000000000000000000000", "", ""},
	{4e25, "40000000000000000000000000", "", ""},
	{8e25, "80000000000000000000000000", "", ""},
	{1e250, "10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", "", ""},
	{2e250, "20000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", "", ""},
	{math.MaxInt64, strconv.FormatFloat(float64(math.MaxInt64), 'f', -1, 64), "", strconv.FormatInt(math.MaxInt64, 10)},
	{1.29067116156722e-309, "0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000129067116156722", "", "0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001290671161567218558822290567835270536800098852722416870074139002112543896676308448335063375297788379444685193974290737962187240854947838776604607190387984577130572928111657710645015086812756013489109884753559084166516937690932698276436869274093950997935137476803610007959500457935217950764794724766740819156974617155861568214427828145972181876775307023388139991104942469299524961281641158436752347582767153796914843896176260096039358494077706152272661453132497761307744086665088096215425146090058519888494342944692629602847826300550628670375451325582843627504604013541465361435761965354140678551369499812124085312128659002910905639984075064968459581691226705666561364681985266583563078466180095375402399087817404368974165082030458595596655868575908243656158447265625000000000000000000000000000000000000004440000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"},
	// go Issue 29491.
	{498484681984085570, "498484681984085570", "", ""},
	{5.8339553793802237e+23, "583395537938022370000000", "", ""},
}

var testTableScientificNotation = map[string]string{
	"1e9":        "1000000000",
	"2.41E-3":    "0.00241",
	"24.2E-4":    "0.00242",
	"243E-5":     "0.00243",
	"1e-5":       "0.00001",
	"245E3":      "245000",
	"1.2345E-1":  "0.12345",
	"0e5":        "0",
	"0e-5":       "0",
	"0.e0":       "0",
	".0e0":       "0",
	"123.456e0":  "123.456",
	"123.456e2":  "12345.6",
	"123.456e10": "1234560000000",
}

func init() {
	for _, s := range testTable {
		s.exact = strconv.FormatFloat(s.float, 'f', 1500, 64)
		if strings.ContainsRune(s.exact, '.') {
			s.exact = strings.TrimRight(s.exact, "0")
			s.exact = strings.TrimRight(s.exact, ".")
		}
	}

	// add negatives
	withNeg := testTable[:]
	for _, s := range testTable {
		if s.float > 0 && s.short != "0" && s.exact != "0" {
			withNeg = append(withNeg, &testEnt{-s.float, "-" + s.short, "-" + s.exact, "-" + s.inexact})
		}
	}
	testTable = withNeg

	for e, s := range testTableScientificNotation {
		if string(e[0]) != "-" && s != "0" {
			testTableScientificNotation["-"+e] = "-" + s
		}
	}
}

func TestNewFromFloat(t *testing.T) {
	for _, x := range testTable {
		s := x.short
		d := NewFromFloat(x.float)
		if d.String() != s {
			t.Errorf("expected %s, got %s (float: %v) (%s, %d)",
				s, d.String(), x.float,
				d.value.String(), d.exp)
		}
	}

	shouldPanicOn := []float64{
		math.NaN(),
		math.Inf(1),
		math.Inf(-1),
	}

	for _, n := range shouldPanicOn {
		var d Decimal
		if !didPanic(func() { d = NewFromFloat(n) }) {
			t.Fatalf("Expected panic when creating a Decimal from %v, got %v instead", n, d.String())
		}
	}
}

func TestNewFromFloatRandom(t *testing.T) {
	n := 0
	rng := rand.New(rand.NewSource(0xdead1337))
	for {
		n++
		if n == 10 {
			break
		}
		in := (rng.Float64() - 0.5) * math.MaxFloat64 * 2
		want, err := NewFromString(strconv.FormatFloat(in, 'f', -1, 64))
		if err != nil {
			t.Error(err)
			continue
		}
		got := NewFromFloat(in)
		if !want.Equal(got) {
			t.Errorf("in: %v, expected %s (%s, %d), got %s (%s, %d) ",
				in, want.String(), want.value.String(), want.exp,
				got.String(), got.value.String(), got.exp)
		}
	}
}

func TestNewFromFloatQuick(t *testing.T) {
	err := quick.Check(func(f float64) bool {
		want, werr := NewFromString(strconv.FormatFloat(f, 'f', -1, 64))
		if werr != nil {
			return true
		}
		got := NewFromFloat(f)
		return got.Equal(want)
	}, &quick.Config{})
	if err != nil {
		t.Error(err)
	}
}

func TestNewFromFloat32Random(t *testing.T) {
	n := 0
	rng := rand.New(rand.NewSource(0xdead1337))
	for {
		n++
		if n == 10 {
			break
		}
		in := float32((rng.Float64() - 0.5) * math.MaxFloat32 * 2)
		want, err := NewFromString(strconv.FormatFloat(float64(in), 'f', -1, 32))
		if err != nil {
			t.Error(err)
			continue
		}
		got := NewFromFloat32(in)
		if !want.Equal(got) {
			t.Errorf("in: %v, expected %s (%s, %d), got %s (%s, %d) ",
				in, want.String(), want.value.String(), want.exp,
				got.String(), got.value.String(), got.exp)
		}
	}
}

func TestNewFromFloat32Quick(t *testing.T) {
	err := quick.Check(func(f float32) bool {
		want, werr := NewFromString(strconv.FormatFloat(float64(f), 'f', -1, 32))
		if werr != nil {
			return true
		}
		got := NewFromFloat32(f)
		return got.Equal(want)
	}, &quick.Config{})
	if err != nil {
		t.Error(err)
	}
}

func TestNewFromString(t *testing.T) {
	for _, x := range testTable {
		s := x.short
		d, err := NewFromString(s)
		if err != nil {
			t.Errorf("error while parsing %s", s)
		} else if d.String() != s {
			t.Errorf("expected %s, got %s (%s, %d)",
				s, d.String(),
				d.value.String(), d.exp)
		}
	}

	for _, x := range testTable {
		s := x.exact
		d, err := NewFromString(s)
		if err != nil {
			t.Errorf("error while parsing %s", s)
		} else if d.String() != s {
			t.Errorf("expected %s, got %s (%s, %d)",
				s, d.String(),
				d.value.String(), d.exp)
		}
	}

	for e, s := range testTableScientificNotation {
		d, err := NewFromString(e)
		if err != nil {
			t.Errorf("error while parsing %s", e)
		} else if d.String() != s {
			t.Errorf("expected %s, got %s (%s, %d)",
				s, d.String(),
				d.value.String(), d.exp)
		}
	}
}

func TestFloat64(t *testing.T) {
	t.Skipf("Float64 does not check for exact")

	for _, x := range testTable {
		if x.inexact == "" || x.inexact == "-" {
			continue
		}
		s := x.exact
		d, err := NewFromString(s)
		if err != nil {
			t.Errorf("error while parsing %s", s)
		} else if f, exact := d.Float64(); !exact || f != x.float {
			t.Errorf("cannot represent exactly %s", s)
		}
		s = x.inexact
		d, err = NewFromString(s)
		if err != nil {
			t.Errorf("error while parsing %s", s)
		} else if f, exact := d.Float64(); exact || f != x.float {
			t.Errorf("%s should be represented inexactly", s)
		}
	}
}

func TestNewFromStringErrs(t *testing.T) {
	tests := []string{
		"",
		"qwert",
		"-",
		".",
		"-.",
		".-",
		"234-.56",
		"234-56",
		"2-",
		"..",
		"2..",
		"..2",
		".5.2",
		"8..2",
		"8.1.",
		"1e",
		"1-e",
		"1e9e",
		"1ee9",
		"1ee",
		"1eE",
		"1e-",
		"1e-.",
		"1e1.2",
		"123.456e1.3",
		"1e-1.2",
		"123.456e-1.3",
		"123.456Easdf",
		"123.456e" + strconv.FormatInt(math.MinInt64, 10),
		"123.456e" + strconv.FormatInt(math.MinInt32, 10),
		"512.99 USD",
		"$99.99",
		"51,850.00",
		"20_000_000.00",
		"$20_000_000.00",
	}

	for _, s := range tests {
		_, err := NewFromString(s)

		if err == nil {
			t.Errorf("error expected when parsing %s", s)
		}
	}
}

func TestNewFromStringDeepEquals(t *testing.T) {
	type StrCmp struct {
		str1     string
		str2     string
		expected bool
	}
	tests := []StrCmp{
		{"1", "1", true},
		{"1.0", "1.0", true},
		{"10", "10.0", false},
		{"1.1", "1.10", false},
		{"1.001", "1.01", false},
	}

	for _, cmp := range tests {
		d1, err1 := NewFromString(cmp.str1)
		d2, err2 := NewFromString(cmp.str2)

		if err1 != nil || err2 != nil {
			t.Errorf("error parsing strings to decimals")
		}

		if reflect.DeepEqual(d1, d2) != cmp.expected {
			t.Errorf("comparison result is different from expected results for %s and %s",
				cmp.str1, cmp.str2)
		}
	}
}

func TestRequireFromString(t *testing.T) {
	s := "1.23"
	defer func() {
		err := recover()
		if err != nil {
			t.Errorf("error while parsing %s", s)
		}
	}()

	d := RequireFromString(s)
	if d.String() != s {
		t.Errorf("expected %s, got %s (%s, %d)",
			s, d.String(),
			d.value.String(), d.exp)
	}
}

func TestRequireFromStringErrs(t *testing.T) {
	s := "qwert"
	var d Decimal
	var err any

	func(d Decimal) {
		defer func() {
			err = recover()
		}()

		RequireFromString(s)
	}(d)

	if err == nil {
		t.Errorf("panic expected when parsing %s", s)
	}
}

func TestNewFromInt(t *testing.T) {
	tests := map[int64]string{
		0:                   "0",
		1:                   "1",
		323412345:           "323412345",
		9223372036854775807: "9223372036854775807",
	}

	// add negatives
	for p, s := range tests {
		if p > 0 {
			tests[-p] = "-" + s
		}
	}

	for input, s := range tests {
		d := NewFromInt(input)
		if d.String() != s {
			t.Errorf("expected %s, got %s (%s, %d)",
				s, d.String(),
				d.value.String(), d.exp)
		}
	}
}

func TestCopy(t *testing.T) {
	origin := New(1, 0)
	cpy := origin.Copy()

	if cpy.Cmp(origin) != 0 {
		t.Error("expecting copy and origin to be equals, but they are not")
	}

	//change value
	cpy = cpy.Add(New(1, 0))

	if cpy.Cmp(origin) == 0 {
		t.Error("expecting copy and origin to have different values, but they are equal")
	}
}

func TestDecimal_RoundAndStringFixed(t *testing.T) {
	type testData struct {
		input         string
		places        int32
		expected      string
		expectedFixed string
	}
	tests := []testData{
		{"1.454", 0, "1", ""},
		{"1.454", 1, "1.5", ""},
		{"1.454", 2, "1.45", ""},
		{"1.454", 3, "1.454", ""},
		{"1.454", 4, "1.454", "1.4540"},
		{"1.454", 5, "1.454", "1.45400"},
		{"1.554", 0, "2", ""},
		{"1.554", 1, "1.6", ""},
		{"1.554", 2, "1.55", ""},
		{"0.554", 0, "1", ""},
		{"0.454", 0, "0", ""},
		{"0.454", 5, "0.454", "0.45400"},
		{"0", 0, "0", ""},
		{"0", 1, "0", "0.0"},
		{"0", 2, "0", "0.00"},
		{"0", -1, "0", ""},
		{"5", 2, "5", "5.00"},
		{"5", 1, "5", "5.0"},
		{"5", 0, "5", ""},
		{"500", 2, "500", "500.00"},
		{"545", -1, "550", ""},
		{"545", -2, "500", ""},
		{"545", -3, "1000", ""},
		{"545", -4, "0", ""},
		{"499", -3, "0", ""},
		{"499", -4, "0", ""},
	}

	// add negative number tests
	for _, test := range tests {
		expected := test.expected
		if expected != "0" {
			expected = "-" + expected
		}
		expectedStr := test.expectedFixed
		if strings.ContainsAny(expectedStr, "123456789") && expectedStr != "" {
			expectedStr = "-" + expectedStr
		}
		tests = append(tests,
			testData{"-" + test.input, test.places, expected, expectedStr})
	}

	for _, test := range tests {
		d, err := NewFromString(test.input)
		if err != nil {
			t.Fatal(err)
		}

		// test Round
		expected, err := NewFromString(test.expected)
		if err != nil {
			t.Fatal(err)
		}
		got := d.Round(test.places)
		if !got.Equal(expected) {
			t.Errorf("Rounding %s to %d places, got %s, expected %s",
				d, test.places, got, expected)
		}

		// test StringFixed
		if test.expectedFixed == "" {
			test.expectedFixed = test.expected
		}
		gotStr := d.StringFixed(test.places)
		if gotStr != test.expectedFixed {
			t.Errorf("(%s).StringFixed(%d): got %s, expected %s",
				d, test.places, gotStr, test.expectedFixed)
		}
	}
}

func TestDecimal_Add(t *testing.T) {
	type Inp struct {
		a string
		b string
	}

	inputs := map[Inp]string{
		{"2", "3"}:                     "5",
		{"2454495034", "3451204593"}:   "5905699627",
		{"24544.95034", ".3451204593"}: "24545.2954604593",
		{".1", ".1"}:                   "0.2",
		{".1", "-.1"}:                  "0",
		{"0", "1.001"}:                 "1.001",
	}

	for inp, res := range inputs {
		a, err := NewFromString(inp.a)
		if err != nil {
			t.FailNow()
		}
		b, err := NewFromString(inp.b)
		if err != nil {
			t.FailNow()
		}
		c := a.Add(b)
		if c.String() != res {
			t.Errorf("expected %s, got %s", res, c.String())
		}
	}
}

func TestDecimal_Sub(t *testing.T) {
	type Inp struct {
		a string
		b string
	}

	inputs := map[Inp]string{
		{"2", "3"}:                     "-1",
		{"12", "3"}:                    "9",
		{"-2", "9"}:                    "-11",
		{"2454495034", "3451204593"}:   "-996709559",
		{"24544.95034", ".3451204593"}: "24544.6052195407",
		{".1", "-.1"}:                  "0.2",
		{".1", ".1"}:                   "0",
		{"0", "1.001"}:                 "-1.001",
		{"1.001", "0"}:                 "1.001",
		{"2.3", ".3"}:                  "2",
	}

	for inp, res := range inputs {
		a, err := NewFromString(inp.a)
		if err != nil {
			t.FailNow()
		}
		b, err := NewFromString(inp.b)
		if err != nil {
			t.FailNow()
		}
		c := a.sub(b)
		if c.String() != res {
			t.Errorf("expected %s, got %s", res, c.String())
		}
	}
}

func TestDecimal_Neg(t *testing.T) {
	inputs := map[string]string{
		"0":     "0",
		"10":    "-10",
		"5.56":  "-5.56",
		"-10":   "10",
		"-5.56": "5.56",
	}

	for inp, res := range inputs {
		a, err := NewFromString(inp)
		if err != nil {
			t.FailNow()
		}
		b := a.Neg()
		if b.String() != res {
			t.Errorf("expected %s, got %s", res, b.String())
		}
	}
}

func TestDecimal_NegFromEmpty(t *testing.T) {
	a := Decimal{}
	b := a.Neg()
	if b.String() != "0" {
		t.Errorf("expected %s, got %s", "0", b)
	}
}

func TestDecimal_Mul(t *testing.T) {
	type Inp struct {
		a string
		b string
	}

	inputs := map[Inp]string{
		{"2", "3"}:                     "6",
		{"2454495034", "3451204593"}:   "8470964534836491162",
		{"24544.95034", ".3451204593"}: "8470.964534836491162",
		{".1", ".1"}:                   "0.01",
		{"0", "1.001"}:                 "0",
	}

	for inp, res := range inputs {
		a, err := NewFromString(inp.a)
		if err != nil {
			t.FailNow()
		}
		b, err := NewFromString(inp.b)
		if err != nil {
			t.FailNow()
		}
		c := a.mul(b)
		if c.String() != res {
			t.Errorf("expected %s, got %s", res, c.String())
		}
	}

	// positive scale
	c := New(1234, 5).mul(New(45, -1))
	if c.String() != "555300000" {
		t.Errorf("Expected %s, got %s", "555300000", c.String())
	}
}

func TestDecimal_Div(t *testing.T) {
	type Inp struct {
		a string
		b string
	}

	inputs := map[Inp]string{
		{"6", "3"}:                            "2",
		{"10", "2"}:                           "5",
		{"2.2", "1.1"}:                        "2",
		{"-2.2", "-1.1"}:                      "2",
		{"12.88", "5.6"}:                      "2.3",
		{"1023427554493", "43432632"}:         "23563.5628642767953828", // rounded
		{"1", "434324545566634"}:              "0.0000000000000023",
		{"1", "3"}:                            "0.3333333333333333",
		{"2", "3"}:                            "0.6666666666666667", // rounded
		{"10000", "3"}:                        "3333.3333333333333333",
		{"10234274355545544493", "-3"}:        "-3411424785181848164.3333333333333333",
		{"-4612301402398.4753343454", "23.5"}: "-196268144782.9138440146978723",
	}

	for inp, expectedStr := range inputs {
		num, err := NewFromString(inp.a)
		if err != nil {
			t.FailNow()
		}
		denom, err := NewFromString(inp.b)
		if err != nil {
			t.FailNow()
		}
		got := num.div(denom)
		expected, _ := NewFromString(expectedStr)
		if !got.Equal(expected) {
			t.Errorf("expected %v when dividing %v by %v, got %v",
				expected, num, denom, got)
		}
		got2 := num.divRound(denom, int32(divisionPrecision))
		if !got2.Equal(expected) {
			t.Errorf("expected %v on divRound (%v,%v), got %v", expected, num, denom, got2)
		}
	}

	type Inp2 struct {
		n    int64
		exp  int32
		n2   int64
		exp2 int32
	}

	// test code path where exp > 0
	inputs2 := map[Inp2]string{
		{124, 10, 3, 1}: "41333333333.3333333333333333",
		{124, 10, 3, 0}: "413333333333.3333333333333333",
		{124, 10, 6, 1}: "20666666666.6666666666666667",
		{124, 10, 6, 0}: "206666666666.6666666666666667",
		{10, 10, 10, 1}: "1000000000",
	}

	for inp, expectedAbs := range inputs2 {
		for i := -1; i <= 1; i += 2 {
			for j := -1; j <= 1; j += 2 {
				n := inp.n * int64(i)
				n2 := inp.n2 * int64(j)
				num := New(n, inp.exp)
				denom := New(n2, inp.exp2)
				expected := expectedAbs
				if i != j {
					expected = "-" + expectedAbs
				}
				got := num.div(denom)
				if got.String() != expected {
					t.Errorf("expected %s when dividing %v by %v, got %v",
						expected, num, denom, got)
				}
			}
		}
	}
}

func TestDecimal_QuoRem(t *testing.T) {
	type Inp4 struct {
		d   string
		d2  string
		exp int32
		q   string
		r   string
	}
	cases := []Inp4{
		{"10", "1", 0, "10", "0"},
		{"1", "10", 0, "0", "1"},
		{"1", "4", 2, "0.25", "0"},
		{"1", "8", 2, "0.12", "0.04"},
		{"10", "3", 1, "3.3", "0.1"},
		{"100", "3", 1, "33.3", "0.1"},
		{"1000", "10", -3, "0", "1000"},
		{"1e-3", "2e-5", 0, "50", "0"},
		{"1e-3", "2e-3", 1, "0.5", "0"},
		{"4e-3", "0.8", 4, "5e-3", "0"},
		{"4.1e-3", "0.8", 3, "5e-3", "1e-4"},
		{"-4", "-3", 0, "1", "-1"},
		{"-4", "3", 0, "-1", "-1"},
	}

	for _, inp4 := range cases {
		d, _ := NewFromString(inp4.d)
		d2, _ := NewFromString(inp4.d2)
		prec := inp4.exp
		q, r := d.QuoRem(d2, prec)
		expectedQ, _ := NewFromString(inp4.q)
		expectedR, _ := NewFromString(inp4.r)
		if !q.Equal(expectedQ) || !r.Equal(expectedR) {
			t.Errorf("bad QuoRem division %s , %s , %d got %v, %v expected %s , %s",
				inp4.d, inp4.d2, prec, q, r, inp4.q, inp4.r)
		}
		if !d.Equal(d2.mul(q).Add(r)) {
			t.Errorf("not fitting: d=%v, d2= %v, prec=%d, q=%v, r=%v",
				d, d2, prec, q, r)
		}
		if !q.Equal(q.Truncate(prec)) {
			t.Errorf("quotient wrong precision: d=%v, d2= %v, prec=%d, q=%v, r=%v",
				d, d2, prec, q, r)
		}
		if r.Abs().Cmp(d2.Abs().mul(New(1, -prec))) >= 0 {
			t.Errorf("remainder too large: d=%v, d2= %v, prec=%d, q=%v, r=%v",
				d, d2, prec, q, r)
		}
		if r.value.Sign()*d.value.Sign() < 0 {
			t.Errorf("signum of divisor and rest do not match: d=%v, d2= %v, prec=%d, q=%v, r=%v",
				d, d2, prec, q, r)
		}
	}
}

type DivTestCase struct {
	d    Decimal
	d2   Decimal
	prec int32
}

func createDivTestCases() []DivTestCase {
	res := make([]DivTestCase, 0)
	var n int32 = 5
	a := []int{1, 2, 3, 6, 7, 10, 100, 14, 5, 400, 0, 1000000, 1000000 + 1, 1000000 - 1}
	for s := -1; s < 2; s = s + 2 { // 2
		for s2 := -1; s2 < 2; s2 = s2 + 2 { // 2
			for e1 := -n; e1 <= n; e1++ { // 2n+1
				for e2 := -n; e2 <= n; e2++ { // 2n+1
					var prec int32
					for prec = -n; prec <= n; prec++ { // 2n+1
						for _, v1 := range a { // 11
							for _, v2 := range a { // 11, even if 0 is skipped
								sign1 := New(int64(s), 0)
								sign2 := New(int64(s2), 0)
								d := sign1.mul(New(int64(v1), e1))
								d2 := sign2.mul(New(int64(v2), e2))
								res = append(res, DivTestCase{d, d2, prec})
							}
						}
					}
				}
			}
		}
	}
	return res
}

func TestDecimal_QuoRem2(t *testing.T) {
	for _, tc := range createDivTestCases() {
		d := tc.d
		if sign(tc.d2) == 0 {
			continue
		}
		d2 := tc.d2
		prec := tc.prec
		q, r := d.QuoRem(d2, prec)
		// rule 1: d = d2*q +r
		if !d.Equal(d2.mul(q).Add(r)) {
			t.Errorf("not fitting, d=%v, d2=%v, prec=%d, q=%v, r=%v",
				d, d2, prec, q, r)
		}
		// rule 2: q is integral multiple of 10^(-prec)
		if !q.Equal(q.Truncate(prec)) {
			t.Errorf("quotient wrong precision, d=%v, d2=%v, prec=%d, q=%v, r=%v",
				d, d2, prec, q, r)
		}
		// rule 3: abs(r)<abs(d) * 10^(-prec)
		if r.Abs().Cmp(d2.Abs().mul(New(1, -prec))) >= 0 {
			t.Errorf("remainder too large, d=%v, d2=%v, prec=%d, q=%v, r=%v",
				d, d2, prec, q, r)
		}
		// rule 4: r and d have the same sign
		if r.value.Sign()*d.value.Sign() < 0 {
			t.Errorf("signum of divisor and rest do not match, "+
				"d=%v, d2=%v, prec=%d, q=%v, r=%v",
				d, d2, prec, q, r)
		}
	}
}

func sign(d Decimal) int {
	return d.value.Sign()
}

// rules for rounded divide, rounded to integer
// rounded_divide(d,d2) = q
// sign q * sign (d/d2) >= 0
// for d and d2 >0 :
// q is already rounded
// q = d/d2 + r , with r > -0.5 and r <= 0.5
// thus q-d/d2 = r, with r > -0.5 and r <= 0.5
// and d2 q -d = r d2 with r d2 > -d2/2 and r d2 <= d2/2
// and 2 (d2 q -d) = x with x > -d2 and x <= d2
// if we factor in precision then x > -d2 * 10^(-precision) and x <= d2 * 10(-precision)

func TestDecimal_DivRound(t *testing.T) {
	cases := []struct {
		d      string
		d2     string
		prec   int32
		result string
	}{
		{"2", "2", 0, "1"},
		{"1", "2", 0, "1"},
		{"-1", "2", 0, "-1"},
		{"-1", "-2", 0, "1"},
		{"1", "-2", 0, "-1"},
		{"1", "-20", 1, "-0.1"},
		{"1", "-20", 2, "-0.05"},
		{"1", "20.0000000000000000001", 1, "0"},
		{"1", "19.9999999999999999999", 1, "0.1"},
	}
	for _, s := range cases {
		d, _ := NewFromString(s.d)
		d2, _ := NewFromString(s.d2)
		result, _ := NewFromString(s.result)
		prec := s.prec
		q := d.divRound(d2, prec)
		if sign(q)*sign(d)*sign(d2) < 0 {
			t.Errorf("sign of quotient wrong, got: %v/%v is about %v", d, d2, q)
		}
		x := q.mul(d2).Abs().sub(d.Abs()).mul(New(2, 0))
		if x.Cmp(d2.Abs().mul(New(1, -prec))) > 0 {
			t.Errorf("wrong rounding, got: %v/%v prec=%d is about %v", d, d2, prec, q)
		}
		if x.Cmp(d2.Abs().mul(New(-1, -prec))) <= 0 {
			t.Errorf("wrong rounding, got: %v/%v prec=%d is about %v", d, d2, prec, q)
		}
		if !q.Equal(result) {
			t.Errorf("rounded division wrong %s / %s scale %d = %s, got %v", s.d, s.d2, prec, s.result, q)
		}
	}
}

func TestDecimal_DivRound2(t *testing.T) {
	for _, tc := range createDivTestCases() {
		d := tc.d
		if sign(tc.d2) == 0 {
			continue
		}
		d2 := tc.d2
		prec := tc.prec
		q := d.divRound(d2, prec)
		if sign(q)*sign(d)*sign(d2) < 0 {
			t.Errorf("sign of quotient wrong, got: %v/%v is about %v", d, d2, q)
		}
		x := q.mul(d2).Abs().sub(d.Abs()).mul(New(2, 0))
		if x.Cmp(d2.Abs().mul(New(1, -prec))) > 0 {
			t.Errorf("wrong rounding, got: %v/%v prec=%d is about %v", d, d2, prec, q)
		}
		if x.Cmp(d2.Abs().mul(New(-1, -prec))) <= 0 {
			t.Errorf("wrong rounding, got: %v/%v prec=%d is about %v", d, d2, prec, q)
		}
	}
}

func TestDecimal_Mod(t *testing.T) {
	type Inp struct {
		a string
		b string
	}

	inputs := map[Inp]string{
		{"3", "2"}:                           "1",
		{"3451204593", "2454495034"}:         "996709559",
		{"9999999999", "1275"}:               "324",
		{"9999999999.9999998", "1275.49"}:    "239.2399998",
		{"24544.95034", "0.3451204593"}:      "0.3283950433",
		{"0.499999999999999999", "0.25"}:     "0.249999999999999999",
		{"0.989512958912895912", "0.000001"}: "0.000000958912895912",
		{"0.1", "0.1"}:                       "0",
		{"0", "1.001"}:                       "0",
		{"-7.5", "2"}:                        "-1.5",
		{"7.5", "-2"}:                        "1.5",
		{"-7.5", "-2"}:                       "-1.5",
	}

	for inp, res := range inputs {
		a, err := NewFromString(inp.a)
		if err != nil {
			t.FailNow()
		}
		b, err := NewFromString(inp.b)
		if err != nil {
			t.FailNow()
		}
		c := a.mod(b)
		if c.String() != res {
			t.Errorf("expected %s, got %s", res, c.String())
		}
	}
}

func TestDecimal_Overflow(t *testing.T) {
	if !didPanic(func() { New(1, math.MinInt32).mul(New(1, math.MinInt32)) }) {
		t.Fatalf("should have gotten an overflow panic")
	}
	if !didPanic(func() { New(1, math.MaxInt32).mul(New(1, math.MaxInt32)) }) {
		t.Fatalf("should have gotten an overflow panic")
	}
}

// old tests after this line

func TestDecimal_Scale(t *testing.T) {
	a := New(1234, -3)
	if a.Exponent() != -3 {
		t.Errorf("error")
	}
}

func TestDecimal_Abs1(t *testing.T) {
	a := New(-1234, -4)
	b := New(1234, -4)

	c := a.Abs()
	if c.Cmp(b) != 0 {
		t.Errorf("error")
	}
}

func TestDecimal_Abs2(t *testing.T) {
	a := New(-1234, -4)
	b := New(1234, -4)

	c := b.Abs()
	if c.Cmp(a) == 0 {
		t.Errorf("error")
	}
}

func TestDecimal_ScalesNotEqual(t *testing.T) {
	a := New(1234, 2)
	b := New(1234, 3)
	if a.Equal(b) {
		t.Errorf("%q should not equal %q", a, b)
	}
}

func TestDecimal_Cmp1(t *testing.T) {
	a := New(123, 3)
	b := New(-1234, 2)

	if a.Cmp(b) != 1 {
		t.Errorf("Error")
	}
}

func TestDecimal_Cmp2(t *testing.T) {
	a := New(123, 3)
	b := New(1234, 2)

	if a.Cmp(b) != -1 {
		t.Errorf("Error")
	}
}

func TestDecimal_IsInteger(t *testing.T) {
	for _, testCase := range []struct {
		Dec       string
		IsInteger bool
	}{
		{"0", true},
		{"0.0000", true},
		{"0.01", false},
		{"0.01010101010000", false},
		{"12.0", true},
		{"12.00000000000000", true},
		{"12.10000", false},
		{"9999.0000", true},
		{"99999999.000000000", true},
		{"-656323444.0000000000000", true},
		{"-32768.01234", false},
		{"-32768.0123423562623600000", false},
	} {
		d, err := NewFromString(testCase.Dec)
		if err != nil {
			t.Fatal(err)
		}
		if d.isInteger() != testCase.IsInteger {
			t.Errorf("expect %t, got %t, for %s", testCase.IsInteger, d.isInteger(), testCase.Dec)
		}
	}
}

func TestDecimal_Sign(t *testing.T) {
	if Zero.Sign() != 0 {
		t.Errorf("%q should have sign 0", Zero)
	}

	one := New(1, 0)
	if one.Sign() != 1 {
		t.Errorf("%q should have sign 1", one)
	}

	mone := New(-1, 0)
	if mone.Sign() != -1 {
		t.Errorf("%q should have sign -1", mone)
	}
}

func didPanic(f func()) bool {
	ret := false
	func() {

		defer func() {
			if message := recover(); message != nil {
				ret = true
			}
		}()

		// call the target function
		f()

	}()

	return ret

}
