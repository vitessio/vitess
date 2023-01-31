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
