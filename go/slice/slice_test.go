/*
Copyright 2024 The Vitess Authors.
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

package slice

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAll_EmptySlice(t *testing.T) {
	result := All([]int{}, func(i int) bool { return i > 0 })
	assert.True(t, result, "All should return true for empty slice")
}

func TestAll_AllElementsTrue(t *testing.T) {
	result := All([]int{1, 2, 3}, func(i int) bool { return i > 0 })
	assert.True(t, result, "All should return true when all elements are true")
}

func TestAll_SomeElementsFalse(t *testing.T) {
	result := All([]int{1, 2, 0}, func(i int) bool { return i > 0 })
	assert.False(t, result, "All should return false when some elements are false")
}

func TestAll_SingleElementFalse(t *testing.T) {
	result := All([]int{0}, func(i int) bool { return i > 0 })
	assert.False(t, result, "All should return false for a single false element")
}

func TestAnyReturnsTrue(t *testing.T) {
	s := []int{1, 2, 3, 4, 5}
	fn := func(n int) bool {
		return n == 3
	}
	result := Any(s, fn)
	assert.True(t, result, "Any should return true")
}

func TestMapAppliesFunction(t *testing.T) {
	s := []int{1, 2, 3, 4, 5}
	fn := func(n int) int {
		return n * 2
	}
	result := Map(s, fn)
	expected := []int{2, 4, 6, 8, 10}
	assert.Equal(t, expected, result, "Map should apply function to each element")
}

func TestEmptySliceInput(t *testing.T) {
	var s []int
	fn := func(n int) bool {
		return n > 0
	}
	allResult := All(s, fn)
	anyResult := Any(s, fn)
	mapResult := Map(s, func(n int) int { return n })
	mapWithErrorResult, err := MapWithError(s, func(n int) (int, error) { return n, nil })
	filterResult := Filter(s, fn)

	assert.True(t, allResult, "All should return true for empty slice")
	assert.False(t, anyResult, "Any should return false for empty slice")
	assert.Nil(t, mapResult, "Map should return nil for empty slice")
	assert.Nil(t, mapWithErrorResult, "MapWithError should return nil for empty slice")
	assert.Nil(t, err, "MapWithError should not return an error for empty slice")
	assert.Nil(t, filterResult, "Filter should return nil for empty slice")
}

func TestFilter_Filtering(t *testing.T) {
	input := []int{1, 2, 3, 4, 5}
	result := Filter(input, func(i int) bool { return i%2 == 0 })
	assert.Equal(t, []int{2, 4}, result, "Filter should return the correct filtered slice")
}

func TestFilter_AllElementsFilteredOut(t *testing.T) {
	input := []int{1, 3, 5}
	result := Filter(input, func(i int) bool { return i%2 == 0 })
	assert.Equal(t, []int{}, result, "Filter should return an empty slice if all elements are filtered out")
}

func TestNilInputForMapWithError(t *testing.T) {
	var s []int
	result, err := MapWithError(s, func(n int) (int, error) { return n, nil })
	assert.Nil(t, result, "MapWithError should return nil for nil input")
	assert.Nil(t, err, "MapWithError should not return an error for nil input")
}

func TestErrorReturnedByMapWithError(t *testing.T) {
	s := []int{1, 2, 3}
	fn := func(n int) (int, error) {
		if n == 2 {
			return 0, errors.New("error")
		}
		return n, nil
	}
	result, err := MapWithError(s, fn)
	assert.Nil(t, result, "MapWithError should return nil when error occurs")
	assert.NotNil(t, err, "MapWithError should return an error when error occurs")
}

func TestMapWithError_SuccessfulMapping(t *testing.T) {
	input := []int{1, 2, 3}
	result, err := MapWithError(input, func(i int) (int, error) { return i * 2, nil })
	assert.Equal(t, []int{2, 4, 6}, result, "MapWithError should return the correct mapped slice")
	assert.NoError(t, err, "MapWithError should not return an error for successful mapping")
}
