/*
Copyright 2021 The Vitess Authors.

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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRPCPool(t *testing.T) {
	t.Parallel()

	pool := NewRPCPool(1, time.Millisecond*100, nil)

	err := pool.Acquire(context.Background())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*5)
	err = pool.Acquire(ctx)
	cancel()
	assert.Error(t, err)

	pool.Release()
	err = pool.Acquire(context.Background())
	require.NoError(t, err)

	t.Run("context timeout exceeded", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()

		err := pool.Acquire(ctx)
		assert.Error(t, err)

		select {
		case <-ctx.Done():
		default:
			assert.Fail(t, "calling context should be done after failed Acquire")
		}
	})

	t.Run("pool timeout exceeded", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
		defer cancel()

		err := pool.Acquire(ctx)
		assert.Error(t, err)

		select {
		case <-ctx.Done():
			assert.Fail(t, "calling context should not be done after failed Acquire")
		default:
		}
	})
}

func BenchmarkRPCPoolAllocs(b *testing.B) {
	for i := 0; i < b.N; i++ {
		NewRPCPool(1000, 0, nil)
		// p.Close()
	}
}
