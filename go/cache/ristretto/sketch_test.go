/*
 * Copyright 2020 Dgraph Labs, Inc. and Contributors
 * Copyright 2021 The Vitess Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ristretto

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSketch(t *testing.T) {
	defer func() {
		require.NotNil(t, recover())
	}()

	s := newCmSketch(5)
	require.Equal(t, uint64(7), s.mask)
	newCmSketch(0)
}

func TestSketchIncrement(t *testing.T) {
	s := newCmSketch(16)
	s.Increment(1)
	s.Increment(5)
	s.Increment(9)
	for i := 0; i < cmDepth; i++ {
		if s.rows[i].string() != s.rows[0].string() {
			break
		}
		require.False(t, i == cmDepth-1, "identical rows, bad seeding")
	}
}

func TestSketchEstimate(t *testing.T) {
	s := newCmSketch(16)
	s.Increment(1)
	s.Increment(1)
	require.Equal(t, int64(2), s.Estimate(1))
	require.Equal(t, int64(0), s.Estimate(0))
}

func TestSketchReset(t *testing.T) {
	s := newCmSketch(16)
	s.Increment(1)
	s.Increment(1)
	s.Increment(1)
	s.Increment(1)
	s.Reset()
	require.Equal(t, int64(2), s.Estimate(1))
}

func TestSketchClear(t *testing.T) {
	s := newCmSketch(16)
	for i := 0; i < 16; i++ {
		s.Increment(uint64(i))
	}
	s.Clear()
	for i := 0; i < 16; i++ {
		require.Equal(t, int64(0), s.Estimate(uint64(i)))
	}
}

func TestNext2Power(t *testing.T) {
	sz := 12 << 30
	szf := float64(sz) * 0.01
	val := int64(szf)
	t.Logf("szf = %.2f val = %d\n", szf, val)
	pow := next2Power(val)
	t.Logf("pow = %d. mult 4 = %d\n", pow, pow*4)
}

func BenchmarkSketchIncrement(b *testing.B) {
	s := newCmSketch(16)
	b.SetBytes(1)
	for n := 0; n < b.N; n++ {
		s.Increment(1)
	}
}

func BenchmarkSketchEstimate(b *testing.B) {
	s := newCmSketch(16)
	s.Increment(1)
	b.SetBytes(1)
	for n := 0; n < b.N; n++ {
		s.Estimate(1)
	}
}
