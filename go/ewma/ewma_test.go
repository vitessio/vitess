/*
Copyright 2017 Google Inc.

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
