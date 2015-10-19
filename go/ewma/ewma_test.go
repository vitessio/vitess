// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ewma

import (
	"testing"
)

func TestEWMACalc(t *testing.T) {
	input := []float64{200000.0, 210000.0, 190000.0, 201000.0}
	// EWMA result of input when weighting factor = 0.8
	output := 199880.0

	// Test EWMA calculation with weighting factor = 0.8
	e := NewEWMA(0.8)
	if v := e.GetEWMA(); v != 0 {
		t.Errorf("Expect zero EWMA when no input has been give, but got %v", v)
	}
	for _, in := range input {
		e.AddValue(in)
	}
	if r := e.GetEWMA(); r != output {
		t.Errorf("Expect EWMA result to be %v, but got %v", output, r)
	}

	// Test invalid weighting factor
	e = NewEWMA(-0.1)
	if e.weightingFactor != DefaultWeightingFactor {
		t.Errorf("Weighting factor should fall back to defult(%v), but got %v", DefaultWeightingFactor, e.weightingFactor)
	}
	e = NewEWMA(1.01)
	if e.weightingFactor != DefaultWeightingFactor {
		t.Errorf("Weighting factor should fall back to defult(%v), but got %v", DefaultWeightingFactor, e.weightingFactor)
	}
}
