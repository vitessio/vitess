/*
Copyright 2023 The Vitess Authors.
Copyright 2023 Yiling-J

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

package theine

import (
	"fmt"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
)

func TestEnsureCapacity(t *testing.T) {
	sketch := NewCountMinSketch()
	sketch.EnsureCapacity(1)
	require.Len(t, sketch.Table, 16)
}

func TestSketch(t *testing.T) {
	sketch := NewCountMinSketch()
	sketch.EnsureCapacity(100)
	require.Len(t, sketch.Table, 128)
	require.Equal(t, uint(1000), sketch.SampleSize)
	// override sampleSize so test won't reset
	sketch.SampleSize = 5120

	failed := 0
	for i := range 500 {
		key := fmt.Sprintf("key:%d", i)
		keyh := xxhash.Sum64String(key)
		sketch.Add(keyh)
		sketch.Add(keyh)
		sketch.Add(keyh)
		sketch.Add(keyh)
		sketch.Add(keyh)
		key = fmt.Sprintf("key:%d:b", i)
		keyh2 := xxhash.Sum64String(key)
		sketch.Add(keyh2)
		sketch.Add(keyh2)
		sketch.Add(keyh2)

		es1 := sketch.Estimate(keyh)
		es2 := sketch.Estimate(keyh2)
		if es2 > es1 {
			failed++
		}
		require.GreaterOrEqual(t, es1, uint(5))
		require.GreaterOrEqual(t, es2, uint(3))
	}
	require.Less(t, float32(failed)/4000, float32(0.1))
	require.Greater(t, sketch.Additions, uint(3500))
	a := sketch.Additions
	sketch.reset()
	require.Equal(t, a>>1, sketch.Additions)
}
