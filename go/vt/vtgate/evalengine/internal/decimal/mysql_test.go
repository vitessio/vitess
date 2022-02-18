/*
Copyright 2022 The Vitess Authors.

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
	"strings"
	"testing"
)

func TestDecimalAdd(t *testing.T) {
	for _, tc := range []struct {
		lhs, rhs, expected string
	}{
		{".00012345000098765", "123.45", "123.45012345000098765"},
		{".1", ".45", "0.55"},
		{"1234500009876.5", ".00012345000098765", "1234500009876.50012345000098765"},
		{"9999909999999.5", ".555", "9999910000000.055"},
		{"99999999", "1", "100000000"},
		{"989999999", "1", "990000000"},
		{"999999999", "1", "1000000000"},
		{"12345", "123.45", "12468.45"},
		{"-12345", "-123.45", "-12468.45"},
		{"-12345", "123.45", "-12221.55"},
		{"12345", "-123.45", "12221.55"},
		{"123.45", "-12345", "-12221.55"},
		{"-123.45", "12345", "12221.55"},
		{"5", "-6.0", "-1.0"},
	} {
		left := RequireFromString(tc.lhs)
		right := RequireFromString(tc.rhs)
		out := left.Add(right)
		if out.StringMySQL() != tc.expected {
			t.Errorf("expected %q + %q = %q\nprocessed: %q + %q = %q",
				tc.lhs, tc.rhs, tc.expected, left.StringMySQL(), right.StringMySQL(), out.StringMySQL())
		}
	}
}

func TestDecimalSub(t *testing.T) {
	for _, tc := range []struct {
		lhs, rhs, expected string
	}{
		{".00012345000098765", "123.45", "-123.44987654999901235"},
		{"1234500009876.5", ".00012345000098765", "1234500009876.49987654999901235"},
		{"9999900000000.5", ".555", "9999899999999.945"},
		{"1111.5551", "1111.555", "0.0001"},
		{".555", ".555", "0"},
		{"10000000", "1", "9999999"},
		{"1000001000", ".1", "1000000999.9"},
		{"1000000000", ".1", "999999999.9"},
		{"12345", "123.45", "12221.55"},
		{"-12345", "-123.45", "-12221.55"},
		{"-12345", "123.45", "-12468.45"},
		{"12345", "-123.45", "12468.45"},
		{"123.45", "12345", "-12221.55"},
		{"-123.45", "-12345", "12221.55"},
	} {
		left := RequireFromString(tc.lhs)
		right := RequireFromString(tc.rhs)
		out := left.Sub(right)
		if out.StringMySQL() != tc.expected {
			t.Errorf("expected %q - %q = %q\nprocessed: %q - %q = %q",
				tc.lhs, tc.rhs, tc.expected, left.StringMySQL(), right.StringMySQL(), out.StringMySQL())
		}
	}
}

func TestDecimalMul(t *testing.T) {
	for _, tc := range []struct {
		lhs, rhs, expected string
	}{
		{"12", "10", "120"},
		{"-123.456", "98765.4321", "-12193185.1853376"},
		{"-123456000000", "98765432100000", "-12193185185337600000000000"},
		{"123456", "987654321", "121931851853376"},
		{"123456", "9876543210", "1219318518533760"},
		{"123", "0.01", "1.23"},
		{"123", "0", "0"},
	} {
		left := RequireFromString(tc.lhs)
		right := RequireFromString(tc.rhs)
		out := left.Mul(right)
		if out.StringMySQL() != tc.expected {
			t.Errorf("expected %q * %q = %q\nprocessed: %q * %q = %q",
				tc.lhs, tc.rhs, tc.expected, left.StringMySQL(), right.StringMySQL(), out.StringMySQL())
		}
	}
}

func TestDecimalDiv(t *testing.T) {
	for _, tc := range []struct {
		lhs, rhs, expected string
		scaleIncr          int32
	}{
		{"120", "10", "12.000000000", 5},
		{"123", "0.01", "12300.000000000", 5},
		{"120", "100000000000.00000", "0.000000001200000000", 5},
		{"-12193185.1853376", "98765.4321", "-123.456000000000000000", 5},
		{"121931851853376", "987654321", "123456.000000000", 5},
		{"0", "987", "0", 5},
		{"1", "3", "0.333333333", 5},
		{"1.000000000000", "3", "0.333333333333333333", 5},
		{"1", "1", "1.000000000", 5},
		{"0.0123456789012345678912345", "9999999999", "0.000000000001234567890246913578148141", 5},
		{"10.333000000", "12.34500", "0.837019036046982584042122316", 5},
		{"10.000000000060", "2", "5.000000000030000000", 5},
	} {
		left := RequireFromString(tc.lhs)
		right := RequireFromString(tc.rhs)
		out := left.Div(right, tc.scaleIncr)
		if out.StringMySQL() != tc.expected {
			t.Errorf("expected %q / %q = %q\nprocessed: %q / %q = %q",
				tc.lhs, tc.rhs, tc.expected, left.StringMySQL(), right.StringMySQL(), out.StringMySQL())
		}
	}
}

func TestOpRoundings(t *testing.T) {
	t.Run("NestedDivisions", func(t *testing.T) {
		const Expected = "0.60288653"

		// (14620 / 9432456) / (24250 / 9432456) = 0.60288653
		a := NewFromInt(14620)
		b := NewFromInt(24250)
		d := NewFromInt(9432456)

		xx := a.Div(d, 4)
		yy := b.Div(d, 4)
		zz := xx.Div(yy, 4)
		got := string(zz.FormatMySQL(8))

		if got != Expected {
			t.Fatalf("expected %s got %s", Expected, got)
		}
	})

	t.Run("HighPrecision", func(t *testing.T) {
		const Expected = "0.837019036046982584042122316"

		// 10.333000000 / 12.34500 = 0.837019036046982584042122316
		aa := RequireFromString("10.333000000")
		bb := RequireFromString("12.34500")
		xx := aa.Div(bb, 5)
		got := xx.StringMySQL()
		if got != Expected {
			t.Fatalf("expected %s got %s", Expected, got)
		}
	})
}

func TestLargestForm(t *testing.T) {
	var cases = []struct {
		integral, fractional int32
		result               string
	}{
		{1, 1, "9.9"},
		{1, 0, "9"},
		{10, 10, "9999999999.9999999999"},
		{5, 5, "99999.99999"},
		{8, 0, "99999999"},
		{0, 5, "0.99999"},
	}

	for _, tc := range cases {
		var b = largestForm(tc.integral, tc.fractional)
		if b.String() != tc.result {
			t.Errorf("LargestForm(%d, %d) = %q (expected %q)", tc.integral, tc.fractional, b.String(), tc.result)
		}
	}
}

var decimals = []string{
	"120", "10", "12.000000000",
	"123", "0.01", "12300.000000000",
	"120", "100000000000.00000", "0.000000001200000000",
	"-12193185.1853376", "98765.4321", "-123.456000000000000000",
	"121931851853376", "987654321", "123456.000000000",
	"0", "987", "0",
	"1", "3", "0.333333333",
	"1.000000000000", "3", "0.333333333333333333",
	"1", "1", "1.000000000",
	"0.0123456789012345678912345", "9999999999", "0.000000000001234567890246913578148141",
	"10.333000000", "12.34500", "0.837019036046982584042122316",
	"10.000000000060", "2", "5.000000000030000000",
	"12", "10", "120",
	"-123.456", "98765.4321", "-12193185.1853376",
	"-123456000000", "98765432100000", "-12193185185337600000000000",
	"123456", "987654321", "121931851853376",
	"123456", "9876543210", "1219318518533760",
	"123", "0.01", "1.23",
	"123", "0", "0",
	".00012345000098765", "123.45", "-123.44987654999901235",
	"1234500009876.5", ".00012345000098765", "1234500009876.49987654999901235",
	"9999900000000.5", ".555", "9999899999999.945",
	"1111.5551", "1111.555", "0.0001",
	".555", ".555", "0",
	"10000000", "1", "9999999",
	"1000001000", ".1", "1000000999.9",
	"1000000000", ".1", "999999999.9",
	"12345", "123.45", "12221.55",
	"-12345", "-123.45", "-12221.55",
	"-12345", "123.45", "-12468.45",
	"12345", "-123.45", "12468.45",
	"123.45", "12345", "-12221.55",
	"-123.45", "-12345", "12221.55",
	".00012345000098765", "123.45", "123.45012345000098765",
	".1", ".45", "0.55",
	"1234500009876.5", ".00012345000098765", "1234500009876.50012345000098765",
	"9999909999999.5", ".555", "9999910000000.055",
	"99999999", "1", "100000000",
	"989999999", "1", "990000000",
	"999999999", "1", "1000000000",
	"12345", "123.45", "12468.45",
	"-12345", "-123.45", "-12468.45",
	"-12345", "123.45", "-12221.55",
	"12345", "-123.45", "12221.55",
	"123.45", "-12345", "-12221.55",
	"-123.45", "12345", "12221.55",
	"5", "-6.0", "-1.0",
	"", "+", "-",
	"0.", "1.",
	"99999999999999999999.99999999999999999999",
	"999999999999999999",
	"111111111111111111",
	"111111111111111111",
	"-999999999999999999",
	"-999999999999999999.",
	"999999999999999999.0",
}

func BenchmarkDecimalParsing(b *testing.B) {
	b.Run("Naive", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			for _, dec := range decimals {
				_, _ = NewFromString(dec)
			}
		}
	})

	var decimalBytes = make([][]byte, 0, len(decimals))
	for _, dec := range decimals {
		decimalBytes = append(decimalBytes, []byte(dec))
	}

	b.Run("MySQL", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			for _, dec := range decimalBytes {
				_, _ = NewFromMySQL(dec)
			}
		}
	})
}

func TestRoundtrip(t *testing.T) {
	for _, in := range decimals {
		d, err1 := NewFromString(in)
		d2, err2 := NewFromMySQL([]byte(in))

		if err1 != nil || err2 != nil {
			if err1 != nil && err2 != nil {
				continue
			}
			t.Fatalf("mismatch in errors: %v vs %v", err1, err2)
		}

		expected := in
		if strings.HasPrefix(expected, ".") {
			expected = "0" + expected
		}
		if strings.HasSuffix(expected, ".") {
			expected = expected[:len(expected)-1]
		}
		if d.StringMySQL() != expected {
			t.Errorf("roundtrip(1) %q -> %q", expected, d.StringMySQL())
		}
		if d2.StringMySQL() != expected {
			t.Errorf("roundtrip(2) %q -> %q", expected, d2.StringMySQL())
		}
	}
}
