// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mathstats

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBetaInc(t *testing.T) {
	// Example values from MATLAB betainc documentation.
	testFunc(t, "I_0.5(%v, 3)",
		func(a float64) float64 { return mathBetaInc(0.5, a, 3) },
		map[float64]float64{
			0:  1.00000000000000,
			1:  0.87500000000000,
			2:  0.68750000000000,
			3:  0.50000000000000,
			4:  0.34375000000000,
			5:  0.22656250000000,
			6:  0.14453125000000,
			7:  0.08984375000000,
			8:  0.05468750000000,
			9:  0.03271484375000,
			10: 0.01928710937500,
		})
}

func TestBetaincPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			assert.Contains(t, r, "betainc: a or b too big; failed to converge")
		} else {
			t.Error("Expected panic, but no panic occurred")
		}
	}()

	a := 1e30
	b := 1e30
	x := 0.5

	_ = mathBetaInc(x, a, b)
}

func TestMathBetaIncNaN(t *testing.T) {
	x := -0.1

	result := mathBetaInc(x, 2.0, 3.0)

	assert.True(t, math.IsNaN(result), "Expected NaN for x < 0, got %v", result)
}
