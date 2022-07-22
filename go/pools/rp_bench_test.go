package pools

import (
	"context"
	"strconv"
	"testing"
)

func BenchmarkGetPut(b *testing.B) {
	for _, size := range []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024} {
		parallelism := (size + 1) / 2
		pool := NewResourcePool(testResourceFactory, size, size, 0, parallelism, nil, nil, 0)
		b.Run("size="+strconv.Itoa(size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				ctx := context.Background()
				if _, err := pool.Get(ctx); err != nil {
					b.Error(err)
				}
				pool.Put(nil)
			}
		})
		pool.Close()
	}
}

func testResourceFactory(context.Context) (Resource, error) {
	return &TestResource{}, nil
}
