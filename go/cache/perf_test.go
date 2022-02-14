package cache

import (
	"testing"
)

func BenchmarkGet(b *testing.B) {
	cache := NewLRUCache(64*1024*1024, func(val interface{}) int64 {
		return int64(cap(val.([]byte)))
	})
	value := make([]byte, 1000)
	cache.Set("stuff", value)
	for i := 0; i < b.N; i++ {
		val, ok := cache.Get("stuff")
		if !ok {
			panic("error")
		}
		_ = val
	}
}
