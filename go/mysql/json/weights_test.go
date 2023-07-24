package json

import (
	"bytes"
	"testing"

	"vitess.io/vitess/go/mysql/format"
)

func TestWeightStrings(t *testing.T) {
	var cases = []struct {
		l, r *Value
	}{
		{NewNumber("-2.3742940301417033", NumberTypeFloat), NewNumber("-0.024384053736998118", NumberTypeFloat)},
		{NewNumber("2.3742940301417033", NumberTypeFloat), NewNumber("20.3742940301417033", NumberTypeFloat)},
		{NewNumber(string(format.FormatFloat(1000000000000000.0)), NumberTypeFloat), NewNumber("100000000000000000", NumberTypeDecimal)},
	}

	for _, tc := range cases {
		l := tc.l.WeightString(nil)
		r := tc.r.WeightString(nil)

		if bytes.Compare(l, r) >= 0 {
			t.Errorf("expected %s < %s\nl = %v\n  = %v\nr = %v\n  = %v",
				tc.l.String(), tc.r.String(), l, string(l), r, string(r))
		}
	}
}
