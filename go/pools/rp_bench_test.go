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
	"fmt"
	"testing"
)

func BenchmarkGetPut(b *testing.B) {
	testResourceFactory := func(context.Context) (Resource, error) {
		return &TestResource{}, nil
	}

	tcases := []struct {
		name string
		pool func(int) IResourcePool
	}{{
		name: "static",
		pool: func(cap int) IResourcePool {
			return NewResourcePool(testResourceFactory, cap, cap, 0, nil, nil, 0)
		},
	}}

	for _, size := range []int{64, 128, 512} {
		for _, parallelism := range []int{1, 8, 32, 128} {
			for _, tc := range tcases {
				rName := fmt.Sprintf("%s/x%d-cap%d", tc.name, parallelism, size)
				b.Run(rName, func(b *testing.B) {
					pool := tc.pool(size)
					defer pool.Close()

					b.ReportAllocs()
					b.SetParallelism(parallelism)
					b.RunParallel(func(pb *testing.PB) {
						var ctx = context.Background()
						for pb.Next() {
							if conn, err := pool.Get(ctx); err != nil {
								b.Error(err)
							} else {
								pool.Put(conn)
							}
						}
					})
				})
			}
		}
	}
}
