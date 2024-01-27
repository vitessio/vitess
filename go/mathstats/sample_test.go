// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mathstats

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

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

func TestBounds(t *testing.T) {
	tests := []struct {
		xs  []float64
		min float64
		max float64
	}{
		{[]float64{15, 20, 35, 40, 50}, 15, 50},
		{[]float64{}, math.NaN(), math.NaN()},
		{[]float64{10, 20, 5, 30, 15}, 5, 30},
	}

	for _, tt := range tests {
		min, max := Bounds(tt.xs)

		if len(tt.xs) == 0 {
			assert.True(t, math.IsNaN(min), "min value should be NaN")
			assert.True(t, math.IsNaN(max), "max value should be NaN")
		} else {
			assert.Equal(t, tt.min, min, "min value mismatch")
			assert.Equal(t, tt.max, max, "max value mismatch")
		}
	}
}

func TestSampleBounds(t *testing.T) {
	tests := []struct {
		sample Sample
		min    float64
		max    float64
	}{
		{Sample{Xs: []float64{15, 20, 35, 40, 50}, Sorted: false}, 15, 50},
		{Sample{Xs: []float64{}, Sorted: false}, math.NaN(), math.NaN()},
		{Sample{Xs: []float64{15, 20, 35, 40, 50}, Sorted: true}, 15, 50},
	}

	for _, tt := range tests {
		min, max := tt.sample.Bounds()

		if len(tt.sample.Xs) == 0 {
			assert.True(t, math.IsNaN(min), "min value should be NaN")
			assert.True(t, math.IsNaN(max), "max value should be NaN")
		} else {
			assert.Equal(t, tt.min, min, "min value mismatch")
			assert.Equal(t, tt.max, max, "max value mismatch")
		}
	}
}

func TestVecSum(t *testing.T) {
	tests := []struct {
		xs  []float64
		sum float64
	}{
		{[]float64{15, 20, 35, 40, 50}, 160},
		{[]float64{}, 0},
	}

	for _, tt := range tests {
		sum := vecSum(tt.xs)
		assert.Equal(t, tt.sum, sum, "sum value mismatch")
	}
}

func TestSampleSum(t *testing.T) {
	tests := []struct {
		sample Sample
		sum    float64
	}{
		{Sample{Xs: []float64{15, 20, 35, 40, 50}}, 160},
		{Sample{Xs: []float64{}}, 0},
	}

	for _, tt := range tests {
		sum := tt.sample.Sum()
		assert.Equal(t, tt.sum, sum, "sum value mismatch")
	}
}

func TestMean(t *testing.T) {
	tests := []struct {
		xs       []float64
		expected float64
	}{
		{[]float64{1, 2, 3, 4, 5}, 3},
		{[]float64{-1, 0, 1}, 0},
		{[]float64{}, math.NaN()},
		{[]float64{10}, 10},
		{[]float64{-2, 2, -2, 2}, 0},
	}

	for _, tt := range tests {
		mean := Mean(tt.xs)

		if math.IsNaN(tt.expected) {
			assert.True(t, math.IsNaN(mean), "Expected NaN")
		} else {
			assert.Equal(t, tt.expected, mean, "Mean value mismatch")
		}
	}
}
func TestSampleCopy(t *testing.T) {
	s := Sample{Xs: []float64{15, 20, 35, 40, 50}, Sorted: true}
	copySample := s.Copy()

	// Modify the original sample and check if the copy remains unchanged
	s.Xs[0] = 100

	assert.NotEqual(t, s.Xs[0], copySample.Xs[0], "Original and copied samples should not share data")
	assert.Equal(t, len(s.Xs), len(copySample.Xs), "Length of original and copied samples should be the same")
	assert.Equal(t, s.Sorted, copySample.Sorted, "Sorting status should be the same")
}

func TestSampleFilterOutliers(t *testing.T) {
	s := Sample{Xs: []float64{15, 20, 35, 40, 50, 100, 200}}
	s.FilterOutliers()

	expected := []float64{15, 20, 35, 40, 50, 100}
	assert.Equal(t, expected, s.Xs, "FilterOutliers should remove outliers")
}

func TestSampleClear(t *testing.T) {
	s := Sample{Xs: []float64{15, 20, 35, 40, 50}, Sorted: true}
	s.Clear()

	assert.Empty(t, s.Xs, "Clear should reset the sample to contain 0 values")
	assert.False(t, s.Sorted, "Sorting status should be false after clearing")
}

func TestSampleSort(t *testing.T) {
	tests := []struct {
		sample   Sample
		expected []float64
	}{
		{Sample{Xs: []float64{15, 20, 35, 40, 50}, Sorted: false}, []float64{15, 20, 35, 40, 50}},
		{Sample{Xs: []float64{}, Sorted: false}, []float64{}},
		{Sample{Xs: []float64{15, 20, 35, 40, 50}, Sorted: true}, []float64{15, 20, 35, 40, 50}},
		{Sample{Xs: []float64{10, 5, 30, 20, 15}, Sorted: false}, []float64{5, 10, 15, 20, 30}},
	}

	for _, tt := range tests {
		sortedSample := tt.sample.Sort()

		assert.Equal(t, tt.expected, sortedSample.Xs, "Sorted values mismatch")
	}
}
