package decimal

import "testing"

func TestCondition_String(t *testing.T) {
	for i, test := range [...]struct {
		c Condition
		s string
	}{
		{Clamped, "clamped"},
		{Clamped | Underflow, "clamped, underflow"},
		{Inexact | Rounded | Subnormal, "inexact, rounded, subnormal"},
		{1 << 31, "unknown(2147483648)"},
	} {
		s := test.c.String()
		if s != test.s {
			t.Fatalf("#%d: wanted %q, got %q", i, test.s, s)
		}
	}
}

func TestToNearestAwayQuoRounding(t *testing.T) {
	for _, test := range [...]struct {
		x        string
		y        string
		expected string // x div y round 20
	}{
		{"1", "9", "0.11111111111111111111"},
		{"2", "9", "0.22222222222222222222"},
		{"3", "9", "0.33333333333333333333"},
		{"4", "9", "0.44444444444444444444"},
		{"5", "9", "0.55555555555555555556"},
		{"6", "9", "0.66666666666666666667"},
		{"7", "9", "0.77777777777777777778"},
		{"8", "9", "0.88888888888888888889"},
	} {

		x, _ := WithContext(Context128).SetString(test.x)
		y, _ := WithContext(Context128).SetString(test.y)
		z, _ := WithContext(Context128).SetString("0")

		z.Context.Precision = 20
		z.Context.RoundingMode = ToNearestAway

		actual := z.Quo(x, y).String()
		expected := test.expected

		if actual != expected {
			t.Errorf("Quo(%s,%s) result %s, expected %s", test.x, test.y, actual, expected)
		}
	}
}

func TestNonStandardRoundingModes(t *testing.T) {
	for i, test := range [...]struct {
		value    int64
		mode     RoundingMode
		expected int64
	}{
		{55, ToNearestTowardZero, 5},
		{25, ToNearestTowardZero, 2},
		{16, ToNearestTowardZero, 2},
		{11, ToNearestTowardZero, 1},
		{10, ToNearestTowardZero, 1},
		{-10, ToNearestTowardZero, -1},
		{-11, ToNearestTowardZero, -1},
		{-16, ToNearestTowardZero, -2},
		{-25, ToNearestTowardZero, -2},
		{-55, ToNearestTowardZero, -5},
		{55, AwayFromZero, 6},
		{25, AwayFromZero, 3},
		{16, AwayFromZero, 2},
		{11, AwayFromZero, 2},
		{10, AwayFromZero, 1},
		{-10, AwayFromZero, -1},
		{-11, AwayFromZero, -2},
		{-16, AwayFromZero, -2},
		{-25, AwayFromZero, -3},
		{-55, AwayFromZero, -6},
	} {
		v := New(test.value, 1)
		v.Context.RoundingMode = test.mode
		r, ok := v.RoundToInt().Int64()
		if !ok {
			t.Fatalf("#%d: failed to convert result to int64", i)
		}
		if test.expected != r {
			t.Fatalf("#%d: wanted %d, got %d", i, test.expected, r)
		}
	}
}
