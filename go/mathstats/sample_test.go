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

func TestSamplePercentileEmpty(t *testing.T) {
	s := Sample{Xs: []float64{}}
	assert.True(t, math.IsNaN(s.Percentile(0.5)), "Percentile should return NaN for empty sample")
}

func TestSampleStdDev(t *testing.T) {
	values := []float64{2, 4, 4, 4, 5, 5, 7, 9}
	expected := 2.138089935299395

	sample := Sample{Xs: values}
	result := sample.StdDev()

	assert.Equal(t, expected, result)
}

func TestBounds(t *testing.T) {
	tt := []struct {
		xs  []float64
		min float64
		max float64
	}{
		{[]float64{15, 20, 35, 40, 50}, 15, 50},
		{[]float64{}, math.NaN(), math.NaN()},
		{[]float64{10, 20, 5, 30, 15}, 5, 30},
	}

	for _, tc := range tt {
		min, max := Bounds(tc.xs)

		if len(tc.xs) == 0 {
			assert.True(t, math.IsNaN(min), "min value should be NaN")
			assert.True(t, math.IsNaN(max), "max value should be NaN")
		} else {
			assert.Equal(t, tc.min, min, "min value mismatch")
			assert.Equal(t, tc.max, max, "max value mismatch")
		}
	}
}

func TestSampleBounds(t *testing.T) {
	tt := []struct {
		sample Sample
		min    float64
		max    float64
	}{
		{Sample{Xs: []float64{15, 20, 35, 40, 50}, Sorted: false}, 15, 50},
		{Sample{Xs: []float64{}, Sorted: false}, math.NaN(), math.NaN()},
		{Sample{Xs: []float64{15, 20, 35, 40, 50}, Sorted: true}, 15, 50},
	}

	for _, tc := range tt {
		min, max := tc.sample.Bounds()

		if len(tc.sample.Xs) == 0 {
			assert.True(t, math.IsNaN(min), "min value should be NaN")
			assert.True(t, math.IsNaN(max), "max value should be NaN")
		} else {
			assert.Equal(t, tc.min, min, "min value mismatch")
			assert.Equal(t, tc.max, max, "max value mismatch")
		}
	}
}

func TestVecSum(t *testing.T) {
	tt := []struct {
		xs  []float64
		sum float64
	}{
		{[]float64{15, 20, 35, 40, 50}, 160},
		{[]float64{}, 0},
	}

	for _, tc := range tt {
		sum := vecSum(tc.xs)
		assert.Equal(t, tc.sum, sum, "sum value mismatch")
	}
}

func TestSampleSum(t *testing.T) {
	tt := []struct {
		sample Sample
		sum    float64
	}{
		{Sample{Xs: []float64{15, 20, 35, 40, 50}}, 160},
		{Sample{Xs: []float64{}}, 0},
	}

	for _, tc := range tt {
		sum := tc.sample.Sum()
		assert.Equal(t, tc.sum, sum, "sum value mismatch")
	}
}

func TestMean(t *testing.T) {
	tt := []struct {
		xs       []float64
		expected float64
	}{
		{[]float64{1, 2, 3, 4, 5}, 3},
		{[]float64{-1, 0, 1}, 0},
		{[]float64{}, math.NaN()},
		{[]float64{10}, 10},
		{[]float64{-2, 2, -2, 2}, 0},
	}

	for _, tc := range tt {
		mean := Mean(tc.xs)

		if math.IsNaN(tc.expected) {
			assert.True(t, math.IsNaN(mean), "Expected NaN")
		} else {
			assert.Equal(t, tc.expected, mean, "Mean value mismatch")
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
	tt := []struct {
		sample   Sample
		expected []float64
	}{
		{Sample{Xs: []float64{15, 20, 35, 40, 50}, Sorted: false}, []float64{15, 20, 35, 40, 50}},
		{Sample{Xs: []float64{}, Sorted: false}, []float64{}},
		{Sample{Xs: []float64{15, 20, 35, 40, 50}, Sorted: true}, []float64{15, 20, 35, 40, 50}},
		{Sample{Xs: []float64{10, 5, 30, 20, 15}, Sorted: false}, []float64{5, 10, 15, 20, 30}},
	}

	for _, tc := range tt {
		sortedSample := tc.sample.Sort()

		assert.Equal(t, tc.expected, sortedSample.Xs, "Sorted values mismatch")
	}
}

func TestGeoMean(t *testing.T) {
	tt := []struct {
		name     string
		values   []float64
		expected float64
	}{
		{
			name:     "Valid_case",
			values:   []float64{2, 4, 8, 16},
			expected: 5.65685424949238,
		},
		{
			name:     "Empty_values",
			values:   []float64{},
			expected: math.NaN(),
		},
		{
			name:     "Zero_value",
			values:   []float64{1, 0, 3},
			expected: math.NaN(),
		},
		{
			name:     "Negative_value",
			values:   []float64{2, -4, 8, 16},
			expected: math.NaN(),
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			sample := Sample{Xs: tc.values}
			result := sample.GeoMean()

			if math.IsNaN(tc.expected) {
				assert.True(t, math.IsNaN(result))
			} else {
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}
