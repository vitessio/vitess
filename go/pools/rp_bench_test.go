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
	"sync"
	"testing"
)

func BenchmarkGetPut(b *testing.B) {
	tcases := []struct {
		name string
		pool func(int, int) IResourcePool
	}{
		{
			name: "static",
			pool: getResourcePool,
		},
		{
			name: "dynamic",
			pool: getDynamicPool,
		},
	}

	b.ReportAllocs()
	for _, size := range []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024} {
		parallelism := (size + 1) / 2
		var pool IResourcePool
		for _, tc := range tcases {
			rName := fmt.Sprintf("%s:%d", tc.name, size)
			b.Run(rName, func(b *testing.B) {
				pool = tc.pool(size, parallelism)
				for i := 0; i < b.N; i++ {
					execTest(b, pool)
				}
				pool.Close()
			})
		}
	}
}

func execTest(b *testing.B, pool IResourcePool) {
	ctx := context.Background()
	maxProcs, numReqs := 2, 50

	var wg sync.WaitGroup
	wg.Add(numReqs)

	reqs := make(chan bool)
	defer close(reqs)

	for i := 0; i < maxProcs*2; i++ {
		go func() {
			for range reqs {
				if conn, err := pool.Get(ctx); err != nil {
					b.Error(err)
				} else {
					pool.Put(conn)
				}
				wg.Done()
			}
		}()
	}

	for i := 0; i < numReqs; i++ {
		reqs <- true
	}

	wg.Wait()

}

func getResourcePool(size, parallelism int) IResourcePool {
	return NewResourcePool(testResourceFactory, size, size, 0, parallelism, nil, nil, 0)
}

func getDynamicPool(size, _ int) IResourcePool {
	return NewDynamicResourcePool(testResourceFactory, size, 0)
}

func testResourceFactory(context.Context) (Resource, error) {
	return &TestResource{}, nil
}
