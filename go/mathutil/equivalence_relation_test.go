/*
Copyright 2023 The Vitess Authors.

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

package mathutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEquivalenceRelation(t *testing.T) {
	initialElements := []string{"a", "b", "c", "d", "e", "f"}
	trivialExpect := map[int]([]string){
		0: []string{"a"},
		1: []string{"b"},
		2: []string{"c"},
		3: []string{"d"},
		4: []string{"e"},
		5: []string{"f"},
	}
	trivialExpectClasses := []int{0, 1, 2, 3, 4, 5}
	tt := []struct {
		name      string
		relations []string
		expect    map[int]([]string)
		classes   []int
	}{
		{
			name:    "empty",
			expect:  trivialExpect,
			classes: trivialExpectClasses,
		},
		{
			name:      "reflective",
			relations: []string{"aa"},
			expect:    trivialExpect,
			classes:   trivialExpectClasses,
		},
		{
			name:      "reflective2",
			relations: []string{"aa", "bb", "ff", "dd"},
			expect:    trivialExpect,
			classes:   trivialExpectClasses,
		},
		{
			name:      "relate",
			relations: []string{"ab"},
			expect: map[int]([]string){
				0: []string{"a", "b"},
				2: []string{"c"},
				3: []string{"d"},
				4: []string{"e"},
				5: []string{"f"},
			},
			classes: []int{0, 2, 3, 4, 5},
		},
		{
			name:      "relate ef",
			relations: []string{"ef"},
			expect: map[int]([]string){
				0: []string{"a"},
				1: []string{"b"},
				2: []string{"c"},
				3: []string{"d"},
				4: []string{"e", "f"},
			},
			classes: []int{0, 1, 2, 3, 4},
		},
		{
			name:      "relate, reverse",
			relations: []string{"ba"},
			expect: map[int]([]string){
				0: []string{"a", "b"},
				2: []string{"c"},
				3: []string{"d"},
				4: []string{"e"},
				5: []string{"f"},
			},
			classes: []int{0, 2, 3, 4, 5},
		},
		{
			name:      "relate, relate reverse, reflective",
			relations: []string{"ba", "ab", "aa"},
			expect: map[int]([]string){
				0: []string{"a", "b"},
				2: []string{"c"},
				3: []string{"d"},
				4: []string{"e"},
				5: []string{"f"},
			},
			classes: []int{0, 2, 3, 4, 5},
		},
		{
			name:      "relate, ab cd",
			relations: []string{"ba", "cd"},
			expect: map[int]([]string){
				0: []string{"a", "b"},
				2: []string{"c", "d"},
				4: []string{"e"},
				5: []string{"f"},
			},
			classes: []int{0, 2, 4, 5},
		},
		{
			name:      "relate, multi",
			relations: []string{"ba", "cd", "ef"},
			expect: map[int]([]string){
				0: []string{"a", "b"},
				2: []string{"c", "d"},
				4: []string{"e", "f"},
			},
			classes: []int{0, 2, 4},
		},
		{
			name:      "relate, multi join",
			relations: []string{"ba", "cb", "fc"},
			expect: map[int]([]string){
				0: []string{"a", "b", "c", "f"},
				3: []string{"d"},
				4: []string{"e"},
			},
			classes: []int{0, 3, 4},
		},
		{
			name:      "relate, multi, join",
			relations: []string{"ba", "cd", "ef", "eb"},
			expect: map[int]([]string){
				0: []string{"a", "b", "e", "f"},
				2: []string{"c", "d"},
			},
			classes: []int{0, 2},
		},
		{
			name:      "relate, multi, join all",
			relations: []string{"ba", "cd", "ef", "eb", "fc"},
			expect: map[int]([]string){
				0: []string{"a", "b", "e", "f", "c", "d"},
			},
			classes: []int{0},
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			r := NewEquivalenceRelation()
			r.AddAll(initialElements)
			trivialM := r.Map()
			assert.Equal(t, trivialExpect, trivialM)
			require.Equal(t, len(initialElements), len(trivialM))

			for _, relation := range tc.relations {
				require.Equal(t, 2, len(relation))
				_, err := r.Relate(relation[0:1], relation[1:2])
				assert.NoError(t, err)
			}
			m := r.Map()
			assert.Equal(t, tc.expect, m)
			assert.Equal(t, tc.classes, r.OrderedClasses())
		})
	}
}

func TestEquivalenceRelationError(t *testing.T) {
	type ttError struct {
		name        string
		element1    string
		element2    string
		expectedErr string
	}

	testsRelateErrorCases := []ttError{
		{
			name:        "UnknownElementError",
			element1:    "x",
			element2:    "b",
			expectedErr: "unknown element x",
		},
		{
			name:        "UnknownClassError",
			element1:    "a",
			element2:    "y",
			expectedErr: "unknown element y",
		},
	}

	for _, tc := range testsRelateErrorCases {
		t.Run(tc.name, func(t *testing.T) {
			r := NewEquivalenceRelation()
			r.AddAll([]string{"a", "b", "c"})

			_, err := r.Relate(tc.element1, tc.element2)
			assert.Error(t, err)
			assert.EqualError(t, err, tc.expectedErr)
		})
	}
}

func TestUnknownElementError(t *testing.T) {
	err := &UnknownElementError{element: "test_element"}

	assert.EqualError(t, err, "unknown element test_element")
}

func TestUnknownClassError(t *testing.T) {
	err := &UnknownClassError{class: 42}

	assert.EqualError(t, err, "unknown class 42")
}

func TestAdd(t *testing.T) {
	r := NewEquivalenceRelation()
	initialElements := []string{"a", "b", "c"}

	for _, element := range initialElements {
		r.Add(element)
	}

	for _, element := range initialElements {
		class, err := r.ElementClass(element)
		require.NoError(t, err)
		assert.Contains(t, r.classElementsMap[class], element)
	}

	classCounter := r.classCounter
	r.Add("a")
	assert.Equal(t, classCounter, r.classCounter)
}

func TestElementClass(t *testing.T) {
	r := NewEquivalenceRelation()
	element := "test_element"

	_, err := r.ElementClass(element)
	assert.Error(t, err)

	r.Add(element)
	class, err := r.ElementClass(element)
	require.NoError(t, err)
	assert.Greater(t, class, -1)
}

func TestRelated(t *testing.T) {
	type tt struct {
		name      string
		relations []string
		element1  string
		element2  string
		expect    bool
		err       error
	}

	tests := []tt{
		{
			name:      "related, same class",
			relations: []string{"ab"},
			element1:  "a",
			element2:  "b",
			expect:    true,
			err:       nil,
		},
		{
			name:      "related, different classes",
			relations: []string{"ab, cd"},
			element1:  "a",
			element2:  "c",
			expect:    false,
			err:       nil,
		},
		{
			name:      "related, unknown element",
			relations: []string{"ab"},
			element1:  "x",
			element2:  "b",
			expect:    false,
			err:       &UnknownElementError{element: "x"},
		},
		{
			name:      "related, unknown element 2",
			relations: []string{"ab"},
			element1:  "a",
			element2:  "y",
			expect:    false,
			err:       &UnknownElementError{element: "y"},
		},
		{
			name:      "related, both elements unknown",
			relations: []string{"ab"},
			element1:  "x",
			element2:  "y",
			expect:    false,
			err:       &UnknownElementError{element: "x"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := NewEquivalenceRelation()
			r.AddAll([]string{"a", "b", "c", "d"})
			for _, relation := range tc.relations {
				_, err := r.Relate(relation[0:1], relation[1:2])
				require.NoError(t, err)
			}

			result, err := r.Related(tc.element1, tc.element2)
			if tc.err != nil {
				assert.EqualError(t, err, tc.err.Error())
			} else {
				assert.Equal(t, tc.expect, result)
			}
		})
	}
}
