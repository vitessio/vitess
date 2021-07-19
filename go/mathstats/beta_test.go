// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mathstats

import (
	"testing"
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
