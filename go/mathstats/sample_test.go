// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mathstats

import "testing"

func TestSamplePercentile(t *testing.T) {
	s := Sample{Xs: []float64{15, 20, 35, 40, 50}}
	testFunc(t, "Percentile", s.Percentile, map[float64]float64{
		-1:  15,
		0:   15,
		.05: 15,
		.30: 19.666666666666666,
		.40: 27,
		.95: 50,
		1:   50,
		2:   50,
	})
}
