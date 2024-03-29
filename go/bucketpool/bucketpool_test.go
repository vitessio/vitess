/*
Copyright 2019 The Vitess Authors

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

package bucketpool

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPool(t *testing.T) {
	maxSize := 16384
	pool := New(1024, maxSize)
	require.Equal(t, maxSize, pool.maxSize, "Invalid max pool size")
	require.Len(t, pool.pools, 5, "Invalid number of pools")

	buf := pool.Get(64)
	require.Len(t, *buf, 64, "unexpected buf length")
	require.Equal(t, 1024, cap(*buf), "unexpected buf cap")

	// get from same pool, check that length is right
	buf = pool.Get(128)
	require.Len(t, *buf, 128, "unexpected buf length")
	require.Equal(t, 1024, cap(*buf), "unexpected buf cap")
	pool.Put(buf)

	// get boundary size
	buf = pool.Get(1024)
	require.Len(t, *buf, 1024, "unexpected buf length")
	require.Equal(t, 1024, cap(*buf), "unexpected buf cap")
	pool.Put(buf)

	// get from the middle
	buf = pool.Get(5000)
	require.Len(t, *buf, 5000, "unexpected buf length")
	require.Equal(t, 8192, cap(*buf), "unexpected buf cap")
	pool.Put(buf)

	// check last pool
	buf = pool.Get(16383)
	require.Len(t, *buf, 16383, "unexpected buf length")
	require.Equal(t, 16384, cap(*buf), "unexpected buf cap")
	pool.Put(buf)

	// get big buffer
	buf = pool.Get(16385)
	require.Len(t, *buf, 16385, "unexpected buf length")
	require.Equal(t, 16385, cap(*buf), "unexpected buf cap")
	pool.Put(buf)
}

func TestPoolOneSize(t *testing.T) {
	maxSize := 1024
	pool := New(1024, maxSize)
	require.Equal(t, maxSize, pool.maxSize, "Invalid max pool size")
	buf := pool.Get(64)
	require.Len(t, *buf, 64, "unexpected buf length")
	require.Equal(t, 1024, cap(*buf), "unexpected buf cap")
	pool.Put(buf)

	buf = pool.Get(1025)
	require.Len(t, *buf, 1025, "unexpected buf length")
	require.Equal(t, 1025, cap(*buf), "unexpected buf cap")
	pool.Put(buf)
}

func TestPoolTwoSizeNotMultiplier(t *testing.T) {
	maxSize := 2000
	pool := New(1024, maxSize)
	require.Equal(t, maxSize, pool.maxSize, "Invalid max pool size")
	buf := pool.Get(64)
	require.Len(t, *buf, 64, "unexpected buf length")
	require.Equal(t, 1024, cap(*buf), "unexpected buf cap")
	pool.Put(buf)

	buf = pool.Get(2001)
	require.Len(t, *buf, 2001, "unexpected buf length")
	require.Equal(t, 2001, cap(*buf), "unexpected buf cap")
	pool.Put(buf)
}

func TestPoolMaxSizeLessThanMinSize(t *testing.T) {
	assert.Panics(t, func() { New(15000, 1024) })
}

func TestPoolWeirdMaxSize(t *testing.T) {
	maxSize := 15000
	pool := New(1024, maxSize)
	require.Equal(t, maxSize, pool.maxSize, "Invalid max pool size")

	buf := pool.Get(14000)
	require.Len(t, *buf, 14000, "unexpected buf length")
	require.Equal(t, 15000, cap(*buf), "unexpected buf cap")
	pool.Put(buf)

	buf = pool.Get(16383)
	require.Len(t, *buf, 16383, "unexpected buf length")
	require.Equal(t, 16383, cap(*buf), "unexpected buf cap")
	pool.Put(buf)
}

func TestFuzz(t *testing.T) {
	maxTestSize := 16384
	for range 20000 {
		minSize := rand.IntN(maxTestSize)
		if minSize == 0 {
			minSize = 1
		}
		maxSize := rand.IntN(maxTestSize-minSize) + minSize
		p := New(minSize, maxSize)
		bufSize := rand.IntN(maxTestSize)
		buf := p.Get(bufSize)
		require.Len(t, *buf, bufSize, "unexpected buf length")
		sPool := p.findPool(bufSize)
		if sPool == nil {
			require.Equal(t, len(*buf), cap(*buf), "unexpected buf cap")
		} else {
			require.Equal(t, sPool.size, cap(*buf), "unexpected buf cap")
		}
		p.Put(buf)
	}
}

func BenchmarkPool(b *testing.B) {
	pool := New(2, 16384)
	b.SetParallelism(16)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			randomSize := rand.IntN(pool.maxSize)
			data := pool.Get(randomSize)
			pool.Put(data)
		}
	})
}

func BenchmarkPoolGet(b *testing.B) {
	pool := New(2, 16384)
	b.SetParallelism(16)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			randomSize := rand.IntN(pool.maxSize)
			data := pool.Get(randomSize)
			_ = data
		}
	})
}
