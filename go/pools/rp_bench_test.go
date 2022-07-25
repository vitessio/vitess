package pools

import (
	"context"
	"strconv"
	"testing"
)

func BenchmarkGetPut(b *testing.B) {
	for _, size := range []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024} {
		parallelism := (size + 1) / 2
		var pool IResourcePool
		for _, f := range []func(int, int) IResourcePool{getResourcePool} {
			pool = f(size, parallelism)
			b.Run(pool.Name()+"size="+strconv.Itoa(size), func(b *testing.B) {
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
}

func getResourcePool(size, parallelism int) IResourcePool {
	return NewResourcePool(testResourceFactory, size, size, 0, parallelism, nil, nil, 0)
}

func testResourceFactory(context.Context) (Resource, error) {
	return &TestResource{}, nil
}
