/*
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
