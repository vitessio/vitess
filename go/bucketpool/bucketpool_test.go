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
	"math/rand"
	"testing"
)

func TestPool(t *testing.T) {
	maxSize := 16384
	pool := New(1024, maxSize)
	if pool.maxSize != maxSize {
		t.Fatalf("Invalid max pool size: %d, expected %d", pool.maxSize, maxSize)
	}
	if len(pool.pools) != 5 {
		t.Fatalf("Invalid number of pools: %d, expected %d", len(pool.pools), 5)
	}

	buf := pool.Get(64)
	if len(*buf) != 64 {
		t.Fatalf("unexpected buf length: %d", len(*buf))
	}
	if cap(*buf) != 1024 {
		t.Fatalf("unexepected buf cap: %d", cap(*buf))
	}

	// get from same pool, check that length is right
	buf = pool.Get(128)
	if len(*buf) != 128 {
		t.Fatalf("unexpected buf length: %d", len(*buf))
	}
	if cap(*buf) != 1024 {
		t.Fatalf("unexepected buf cap: %d", cap(*buf))
	}
	pool.Put(buf)

	// get boundary size
	buf = pool.Get(1024)
	if len(*buf) != 1024 {
		t.Fatalf("unexpected buf length: %d", len(*buf))
	}
	if cap(*buf) != 1024 {
		t.Fatalf("unexepected buf cap: %d", cap(*buf))
	}
	pool.Put(buf)

	// get from the middle
	buf = pool.Get(5000)
	if len(*buf) != 5000 {
		t.Fatalf("unexpected buf length: %d", len(*buf))
	}
	if cap(*buf) != 8192 {
		t.Fatalf("unexepected buf cap: %d", cap(*buf))
	}
	pool.Put(buf)

	// check last pool
	buf = pool.Get(16383)
	if len(*buf) != 16383 {
		t.Fatalf("unexpected buf length: %d", len(*buf))
	}
	if cap(*buf) != 16384 {
		t.Fatalf("unexepected buf cap: %d", cap(*buf))
	}
	pool.Put(buf)

	// get big buffer
	buf = pool.Get(16385)
	if len(*buf) != 16385 {
		t.Fatalf("unexpected buf length: %d", len(*buf))
	}
	if cap(*buf) != 16385 {
		t.Fatalf("unexepected buf cap: %d", cap(*buf))
	}
	pool.Put(buf)
}

func TestPoolOneSize(t *testing.T) {
	maxSize := 1024
	pool := New(1024, maxSize)
	if pool.maxSize != maxSize {
		t.Fatalf("Invalid max pool size: %d, expected %d", pool.maxSize, maxSize)
	}
	buf := pool.Get(64)
	if len(*buf) != 64 {
		t.Fatalf("unexpected buf length: %d", len(*buf))
	}
	if cap(*buf) != 1024 {
		t.Fatalf("unexepected buf cap: %d", cap(*buf))
	}
	pool.Put(buf)

	buf = pool.Get(1025)
	if len(*buf) != 1025 {
		t.Fatalf("unexpected buf length: %d", len(*buf))
	}
	if cap(*buf) != 1025 {
		t.Fatalf("unexepected buf cap: %d", cap(*buf))
	}
	pool.Put(buf)
}

func TestPoolTwoSizeNotMultiplier(t *testing.T) {
	maxSize := 2000
	pool := New(1024, maxSize)
	if pool.maxSize != maxSize {
		t.Fatalf("Invalid max pool size: %d, expected %d", pool.maxSize, maxSize)
	}
	buf := pool.Get(64)
	if len(*buf) != 64 {
		t.Fatalf("unexpected buf length: %d", len(*buf))
	}
	if cap(*buf) != 1024 {
		t.Fatalf("unexepected buf cap: %d", cap(*buf))
	}
	pool.Put(buf)

	buf = pool.Get(2001)
	if len(*buf) != 2001 {
		t.Fatalf("unexpected buf length: %d", len(*buf))
	}
	if cap(*buf) != 2001 {
		t.Fatalf("unexepected buf cap: %d", cap(*buf))
	}
	pool.Put(buf)
}

func TestPoolWeirdMaxSize(t *testing.T) {
	maxSize := 15000
	pool := New(1024, maxSize)
	if pool.maxSize != maxSize {
		t.Fatalf("Invalid max pool size: %d, expected %d", pool.maxSize, maxSize)
	}

	buf := pool.Get(14000)
	if len(*buf) != 14000 {
		t.Fatalf("unexpected buf length: %d", len(*buf))
	}
	if cap(*buf) != 15000 {
		t.Fatalf("unexepected buf cap: %d", cap(*buf))
	}
	pool.Put(buf)

	buf = pool.Get(16383)
	if len(*buf) != 16383 {
		t.Fatalf("unexpected buf length: %d", len(*buf))
	}
	if cap(*buf) != 16383 {
		t.Fatalf("unexepected buf cap: %d", cap(*buf))
	}
	pool.Put(buf)
}

func TestFuzz(t *testing.T) {
	maxTestSize := 16384
	for i := 0; i < 20000; i++ {
		minSize := rand.Intn(maxTestSize)
		maxSize := rand.Intn(maxTestSize-minSize) + minSize
		p := New(minSize, maxSize)
		bufSize := rand.Intn(maxTestSize)
		buf := p.Get(bufSize)
		if len(*buf) != bufSize {
			t.Fatalf("Invalid length %d, expected %d", len(*buf), bufSize)
		}
		sPool := p.findPool(bufSize)
		if sPool == nil {
			if cap(*buf) != len(*buf) {
				t.Fatalf("Invalid cap %d, expected %d", cap(*buf), len(*buf))
			}
		} else {
			if cap(*buf) != sPool.size {
				t.Fatalf("Invalid cap %d, expected %d", cap(*buf), sPool.size)
			}
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
			randomSize := rand.Intn(pool.maxSize)
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
			randomSize := rand.Intn(pool.maxSize)
			data := pool.Get(randomSize)
			_ = data
		}
	})
}
