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
							if conn, err := pool.Get(ctx, nil); err != nil {
								b.Error(err)
							} else {
								pool.Put(conn, 0)
							}
						}
					})
				})
			}
		}
	}
}

func BenchmarkGetPutWithSettings(b *testing.B) {
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

	settings := [][]string{{}, {"a", "b", "c"}}

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
						ctx := context.Background()
						for pb.Next() {
							if conn, err := pool.Get(ctx, settings[1]); err != nil {
								b.Error(err)
							} else {
								pool.Put(conn, conn.SettingHash())
							}
						}
					})
				})
			}
		}
	}
}

func BenchmarkApplySettings(b *testing.B) {
	b.ReportAllocs()
	ctx := context.Background()
	tcases := []struct {
		name     string
		settings []string
	}{{
		name:     "small",
		settings: []string{"a", "b", "c"},
	}, {
		name:     "split",
		settings: []string{"set @@sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'", "set @@sql_safe_updates = false", "set @@read_buffer_size = 9191181919"},
	}, {
		name:     "combined",
		settings: []string{"set @@sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION', @@sql_safe_updates = false, @@read_buffer_size = 9191181919"},
	}}
	for _, tcase := range tcases {
		b.Run(tcase.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				resource := &TestResource{}
				if err := resource.ApplySettings(ctx, tcase.settings); err != nil {
					b.Error(err)
				}
				if resource.SettingHash() == 0 {
					b.Error("setting hash is 0")
				}
			}
		})
	}
}
