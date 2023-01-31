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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPermutations(t *testing.T) {
	allPerms := map[string]bool{}

	Permutations(4, func(indexes []int) {
		if len(allPerms) == 0 {
			// first perutation. Let's validate that it is the trivial permutation
			assert.Equal(t, []int{0, 1, 2, 3}, indexes)
		}
		allPerms[fmt.Sprintf("%v", indexes)] = true
	})
	// validate all permutations are different:
	assert.Equal(t, 24, len(allPerms))
}
