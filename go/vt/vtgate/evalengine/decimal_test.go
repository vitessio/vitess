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

package evalengine

import (
	"testing"

	"vitess.io/vitess/go/vt/vtgate/evalengine/decimal"
)

func newDecimal(val ...string) *decimal.Big {
	dec := decimal.WithContext(decimalContextSQL)
	switch len(val) {
	case 0:
	case 1:
		dec.SetString(val[0])
		if dec.Context.Conditions != 0 {
			panic(dec.Context.Conditions)
		}
	default:
		panic("newDecimal([val]?)")
	}
	return dec
}

func checkAdd(t *testing.T, left, right, result string) {
	t.Helper()
	l, r, o := newDecimal(left), newDecimal(right), newDecimal()
	o.Add(l, r)
	if o.String() != result {
		t.Errorf("expected %q + %q = %q\nprocessed: %q + %q = %q", left, right, result, l.String(), r.String(), o.String())
	}
}

func checkSub(t *testing.T, left, right, result string) {
	t.Helper()
	l, r, o := newDecimal(left), newDecimal(right), newDecimal()
	o.Sub(l, r)
	if o.String() != result {
		t.Errorf("expected %q - %q = %q\nprocessed: %q - %q = %q", left, right, result, l.String(), r.String(), o.String())
	}
}

func checkMul(t *testing.T, left, right, result string) {
	t.Helper()
	l, r, o := newDecimal(left), newDecimal(right), newDecimal()
	o.Mul(l, r)
	if o.String() != result {
		t.Errorf("expected %q * %q = %q\nprocessed: %q * %q = %q", left, right, result, l.String(), r.String(), o.String())
	}
}

func checkDiv(t *testing.T, left, right, result string, scaleIncr int) {
	t.Helper()
	l := newDecimal(left)
	r := newDecimal(right)
	o := newDecimal()
	o.Div(l, r, scaleIncr)
	if o.String() != result {
		t.Errorf("expected %q / %q = %q\nprocessed: %q / %q = %q", left, right, result, l.String(), r.String(), o.String())
	}
}

func TestDecimalAdd(t *testing.T) {
	checkAdd(t, ".00012345000098765", "123.45", "123.45012345000098765")
	checkAdd(t, ".1", ".45", "0.55")
	checkAdd(t, "1234500009876.5", ".00012345000098765", "1234500009876.50012345000098765")
	checkAdd(t, "9999909999999.5", ".555", "9999910000000.055")
	checkAdd(t, "99999999", "1", "100000000")
	checkAdd(t, "989999999", "1", "990000000")
	checkAdd(t, "999999999", "1", "1000000000")
	checkAdd(t, "12345", "123.45", "12468.45")
	checkAdd(t, "-12345", "-123.45", "-12468.45")
	checkSub(t, "-12345", "123.45", "-12468.45")
	checkSub(t, "12345", "-123.45", "12468.45")
}

func TestDecimalSub(t *testing.T) {
	checkSub(t, ".00012345000098765", "123.45", "-123.44987654999901235")
	checkSub(t, "1234500009876.5", ".00012345000098765", "1234500009876.49987654999901235")
	checkSub(t, "9999900000000.5", ".555", "9999899999999.945")
	checkSub(t, "1111.5551", "1111.555", "0.0001")
	checkSub(t, ".555", ".555", "0")
	checkSub(t, "10000000", "1", "9999999")
	checkSub(t, "1000001000", ".1", "1000000999.9")
	checkSub(t, "1000000000", ".1", "999999999.9")
	checkSub(t, "12345", "123.45", "12221.55")
	checkSub(t, "-12345", "-123.45", "-12221.55")
	checkAdd(t, "-12345", "123.45", "-12221.55")
	checkAdd(t, "12345", "-123.45", "12221.55")
	checkSub(t, "123.45", "12345", "-12221.55")
	checkSub(t, "-123.45", "-12345", "12221.55")
	checkAdd(t, "123.45", "-12345", "-12221.55")
	checkAdd(t, "-123.45", "12345", "12221.55")
	checkAdd(t, "5", "-6.0", "-1.0")
}

func TestDecimalMul(t *testing.T) {
	checkMul(t, "12", "10", "120")
	checkMul(t, "-123.456", "98765.4321", "-12193185.1853376")
	checkMul(t, "-123456000000", "98765432100000", "-12193185185337600000000000")
	checkMul(t, "123456", "987654321", "121931851853376")
	checkMul(t, "123456", "9876543210", "1219318518533760")
	checkMul(t, "123", "0.01", "1.23")
	checkMul(t, "123", "0", "0")
}

func TestDecimalDiv(t *testing.T) {
	checkDiv(t, "120", "10", "12.000000000", 5)
	checkDiv(t, "123", "0.01", "12300.000000000", 5)
	checkDiv(t, "120", "100000000000.00000", "0.000000001200000000", 5)
	checkDiv(t, "-12193185.1853376", "98765.4321", "-123.456000000000000000", 5)
	checkDiv(t, "121931851853376", "987654321", "123456.000000000", 5)
	checkDiv(t, "0", "987", "0", 5)
	checkDiv(t, "1", "3", "0.333333333", 5)
	checkDiv(t, "1.000000000000", "3", "0.333333333333333333", 5)
	checkDiv(t, "1", "1", "1.000000000", 5)
	checkDiv(t, "0.0123456789012345678912345", "9999999999", "0.000000000001234567890246913578148141", 5)
	checkDiv(t, "10.333000000", "12.34500", "0.837019036046982584042122316", 5)
	checkDiv(t, "10.000000000060", "2", "5.000000000030000000", 5)
}

func TestDecimalRoundings(t *testing.T) {
	defer func() {
		decimalContextSQL.RoundingMode = roundingModeArithmetic
	}()

	// (14620 / 9432456) / (24250 / 9432456)
	found := false
	a := newEvalInt64(14620)
	b := newEvalInt64(24250)
	d := newEvalInt64(9432456)

	for i := 0; i < 7; i++ {
		var ri = decimal.RoundingMode(i)
		decimalContextSQL.RoundingMode = ri

		x, _ := decimalDivide(a, d, 4)
		y, _ := decimalDivide(b, d, 4)
		z, _ := decimalDivide(x, y, 4)

		aa, _ := newDecimalString("10.333000000")
		bb, _ := newDecimalString("12.34500")
		xx, _ := decimalDivide(newEvalDecimal(aa), newEvalDecimal(bb), 5)

		for j := 0; j < 7; j++ {
			var ok1, ok2 bool
			var rj = decimal.RoundingMode(j)

			str := string(z.decimal.num.FormatCustom(z.decimal.frac, rj))
			if str == "0.60288653" {
				ok1 = true
			}

			str = string(xx.decimal.num.FormatCustom(xx.decimal.num.Precision(), rj))
			if str == "0.837019036046982584042122316" {
				ok2 = true
			}

			if ok1 && ok2 {
				t.Logf("i=%s j=%s => %v", ri, rj, ok1 && ok2)
				found = true
			}
		}
	}

	if !found {
		t.Fatalf("did not find any valid combinations for arithmetic + format rounding modes")
	}
}
