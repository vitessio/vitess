package grpctabletconn

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkStreamPool_GetPut(b *testing.B) {
	pool := newStreamPool(16, func() (int, context.CancelFunc, error) {
		return 0, func() {}, nil
	})
	s, _ := pool.get()
	pool.put(s)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, _ := pool.get()
		pool.put(s)
	}
}

func BenchmarkStreamPool_GetPut_Parallel(b *testing.B) {
	pool := newStreamPool(64, func() (int, context.CancelFunc, error) {
		return 0, func() {}, nil
	})
	for range 16 {
		s, _ := pool.get()
		pool.put(s)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s, _ := pool.get()
			if s != nil {
				pool.put(s)
			}
		}
	})
}

func BenchmarkStreamPool_Contention_Scarce(b *testing.B) {
	pool := newStreamPool(2, func() (int, context.CancelFunc, error) {
		return 0, func() {}, nil
	})
	for range 2 {
		s, _ := pool.get()
		pool.put(s)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s, _ := pool.get()
			if s != nil {
				pool.put(s)
			}
		}
	})
}

func BenchmarkStreamPool_Contention_WithWork(b *testing.B) {
	pool := newStreamPool(4, func() (int, context.CancelFunc, error) {
		return 0, func() {}, nil
	})
	for range 4 {
		s, _ := pool.get()
		pool.put(s)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s, _ := pool.get()
			if s != nil {
				start := time.Now()
				for time.Since(start) < 100*time.Nanosecond {
				}
				pool.put(s)
			}
		}
	})
}

func BenchmarkStreamPool_Contention_MixedOps(b *testing.B) {
	pool := newStreamPool(8, func() (int, context.CancelFunc, error) {
		return 0, func() {}, nil
	})
	for range 8 {
		s, _ := pool.get()
		pool.put(s)
	}
	b.ResetTimer()
	var iter atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s, _ := pool.get()
			if s == nil {
				continue
			}
			if iter.Add(1)%10 == 0 {
				pool.discard(s)
			} else {
				pool.put(s)
			}
		}
	})
}
