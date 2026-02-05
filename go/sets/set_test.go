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

package sets

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInsert(t *testing.T) {
	testSet := New[int](1, 2, 3)
	testSet.Insert(4, 5, 6)
	compareSet := New[int](1, 2, 3, 4, 5, 6)
	assert.Equal(t, testSet, compareSet)
}

func TestDelete(t *testing.T) {
	testSet := New[int](1, 2, 3, 4, 5, 6)
	testSet.Delete(1, 5)
	compareSet := New[int](2, 3, 4, 6)
	assert.Equal(t, testSet, compareSet)
	testSet.Delete(2, 3, 4, 6)
	assert.Empty(t, testSet)
}

func TestHas(t *testing.T) {
	testSet := New[int](1, 2, 3)
	assert.True(t, testSet.Has(3))
	assert.False(t, testSet.Has(-1))
}

func TestHasAny(t *testing.T) {
	testSet := New[int](1, 2, 3)
	assert.True(t, testSet.HasAny(1, 10, 11))
	assert.False(t, testSet.HasAny(-1, 10, 11))
}

func TestDifference(t *testing.T) {
	testSet := New[int](1, 2, 3)
	compareSet := New[int](-1, -2, 1, 2, 3)
	diffSet := New[int](-1, -2)
	assert.Equal(t, diffSet, compareSet.Difference(testSet))
}

func TestIntersection(t *testing.T) {
	setA := New[int](1, 2, 3)
	setB := New[int](1, 2, 8, 9, 10)
	expectedSet := New[int](1, 2)
	assert.Equal(t, expectedSet, setA.Intersection(setB))
}

func TestEqual(t *testing.T) {
	testSet := New[int](1, 2, 3, 4, 5, 6)
	compareSet := New[int](1, 2, 3, 4, 5, 6)
	assert.True(t, testSet.Equal(compareSet))
	compareSet.Insert(-1, -2)
	assert.False(t, testSet.Equal(compareSet))
}

func TestLen(t *testing.T) {
	testSet := New[int](1, 2, 3)
	assert.Equal(t, testSet.Len(), 3)
}

func TestList(t *testing.T) {
	testSet := New[string]("a string", "testing", "Capital", "34")
	list := List(testSet)
	require.EqualValues(t, []string{"34", "Capital", "a string", "testing"}, list)
}
