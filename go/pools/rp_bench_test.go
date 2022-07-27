/*
Copyright 2022 The Vitess Authors.

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
