package evalengine

import (
	"fmt"
	"testing"

	"github.com/ericlagergren/decimal"
)

func checkAdd(t *testing.T, left, right, result string) {
	t.Helper()
	l := decimal.WithContext(decimalContextSQL)
	r := decimal.WithContext(decimalContextSQL)
	o := decimal.WithContext(decimalContextSQL)

	l.SetString(left)
	r.SetString(right)
	o.Add(l, r)
	if o.Sign() == 0 {
		o.SetUint64(0)
	}

	if o.String() != result {
		t.Errorf("expected %q + %q = %q\nprocessed: %q + %q = %q", left, right, result, l.String(), r.String(), o.String())
	}
}

func checkSub(t *testing.T, left, right, result string) {
	t.Helper()
	l := decimal.WithContext(decimalContextSQL)
	r := decimal.WithContext(decimalContextSQL)
	o := decimal.WithContext(decimalContextSQL)

	l.SetString(left)
	r.SetString(right)
	o.Sub(l, r)
	if o.Sign() == 0 {
		o.SetUint64(0)
	}

	if o.String() != result {
		t.Errorf("expected %q - %q = %q\nprocessed: %q - %q = %q", left, right, result, l.String(), r.String(), o.String())
	}
}

func checkMul(t *testing.T, left, right, result string) {
	t.Helper()
	l := decimal.WithContext(decimalContextSQL)
	r := decimal.WithContext(decimalContextSQL)
	o := decimal.WithContext(decimalContextSQL)

	l.SetString(left)
	r.SetString(right)
	o.Mul(l, r)
	if o.Sign() == 0 {
		o.SetUint64(0)
	}

	if o.String() != result {
		t.Errorf("expected %q * %q = %q\nprocessed: %q * %q = %q", left, right, result, l.String(), r.String(), o.String())
	}
}

func checkDiv(t *testing.T, left, right, result string, scaleIncr int) {
	t.Helper()
	l := decimal.WithContext(decimalContextSQL)
	r := decimal.WithContext(decimalContextSQL)

	l.SetString(left)
	r.SetString(right)

	o := decimal.WithContext(decimalContextSQL)
	o.Quo(l, r)

	frac1 := myround(l.Scale()) * 9
	frac2 := myround(r.Scale()) * 9

	scaleIncr -= frac1 - l.Scale() + frac2 - r.Scale()
	if scaleIncr < 0 {
		scaleIncr = 0
	}

	frac0 := myround(frac1 + frac2 + scaleIncr)
	scale := frac0 * 9
	if o.Scale() != scale {
		o.Quantize(scale)
	}
	if o.Sign() == 0 {
		o.SetUint64(0)
	}

	ostr := fmt.Sprintf("%f", o)

	if ostr != result {
		t.Errorf("expected %q / %q = %q\nprocessed: %q / %q = %q", left, right, result, l.String(), r.String(), ostr)
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
