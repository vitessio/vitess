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

package stats

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRingInt64(t *testing.T) {
	t.Run("Add Values", func(t *testing.T) {
		ri := NewRingInt64(3)
		ri.Add(1)
		ri.Add(2)
		ri.Add(3)

		assert.Equal(t, []int64{1, 2, 3}, ri.Values())

		ri.Add(4)
		ri.Add(5)
		assert.Equal(t, []int64{3, 4, 5}, ri.Values())
	})

	t.Run("Empty Ring", func(t *testing.T) {
		ri := NewRingInt64(3)
		assert.Empty(t, ri.Values())
	})
}
