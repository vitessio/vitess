// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mathstats

import "testing"

func TestTTest(t *testing.T) {
	s1 := Sample{Xs: []float64{2, 1, 3, 4}}
	s2 := Sample{Xs: []float64{6, 5, 7, 9}}

	check := func(want, got *TTestResult) {
		if want.N1 != got.N1 || want.N2 != got.N2 ||
			!aeq(want.T, got.T) || !aeq(want.DoF, got.DoF) ||
			want.AltHypothesis != got.AltHypothesis ||
			!aeq(want.P, got.P) {
			t.Errorf("want %+v, got %+v", want, got)
		}
	}
	check3 := func(test func(alt LocationHypothesis) (*TTestResult, error), n1, n2 int, t, dof float64, pless, pdiff, pgreater float64) {
		want := &TTestResult{N1: n1, N2: n2, T: t, DoF: dof}

		want.AltHypothesis = LocationLess
		want.P = pless
		got, _ := test(want.AltHypothesis)
		check(want, got)

		want.AltHypothesis = LocationDiffers
		want.P = pdiff
		got, _ = test(want.AltHypothesis)
		check(want, got)

		want.AltHypothesis = LocationGreater
		want.P = pgreater
		got, _ = test(want.AltHypothesis)
		check(want, got)
	}

	check3(func(alt LocationHypothesis) (*TTestResult, error) {
		return TwoSampleTTest(s1, s1, alt)
	}, 4, 4, 0, 6,
		0.5, 1, 0.5)
	check3(func(alt LocationHypothesis) (*TTestResult, error) {
		return TwoSampleWelchTTest(s1, s1, alt)
	}, 4, 4, 0, 6,
		0.5, 1, 0.5)

	check3(func(alt LocationHypothesis) (*TTestResult, error) {
		return TwoSampleTTest(s1, s2, alt)
	}, 4, 4, -3.9703446152237674, 6,
		0.0036820296121056195, 0.0073640592242113214, 0.9963179703878944)
	check3(func(alt LocationHypothesis) (*TTestResult, error) {
		return TwoSampleWelchTTest(s1, s2, alt)
	}, 4, 4, -3.9703446152237674, 5.584615384615385,
		0.004256431565689112, 0.0085128631313781695, 0.9957435684343109)

	check3(func(alt LocationHypothesis) (*TTestResult, error) {
		return PairedTTest(s1.Xs, s2.Xs, 0, alt)
	}, 4, 4, -17, 3,
		0.0002216717691559955, 0.00044334353831207749, 0.999778328230844)

	check3(func(alt LocationHypothesis) (*TTestResult, error) {
		return OneSampleTTest(s1, 0, alt)
	}, 4, 0, 3.872983346207417, 3,
		0.9847668541689145, 0.030466291662170977, 0.015233145831085482)
	check3(func(alt LocationHypothesis) (*TTestResult, error) {
		return OneSampleTTest(s1, 2.5, alt)
	}, 4, 0, 0, 3,
		0.5, 1, 0.5)
}
