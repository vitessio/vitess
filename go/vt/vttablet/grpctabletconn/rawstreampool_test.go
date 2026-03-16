/*
Copyright 2026 The Vitess Authors.

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

package grpctabletconn

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamPool_GetCreatesNew(t *testing.T) {
	created := 0
	pool := newStreamPool(2, func() (int, context.CancelFunc, error) {
		created++
		return created, func() {}, nil
	})

	s, err := pool.get()
	require.NoError(t, err)
	assert.Equal(t, 1, s.stream)
	assert.Equal(t, 1, created)
}

func TestStreamPool_PutAndReuse(t *testing.T) {
	created := 0
	pool := newStreamPool(2, func() (int, context.CancelFunc, error) {
		created++
		return created, func() {}, nil
	})

	s1, err := pool.get()
	require.NoError(t, err)
	assert.Equal(t, 1, s1.stream)

	pool.put(s1)

	s2, err := pool.get()
	require.NoError(t, err)
	assert.Equal(t, 1, s2.stream, "should reuse the pooled stream")
	assert.Equal(t, 1, created, "should not create a new stream")
}

func TestStreamPool_MaxIdle(t *testing.T) {
	var cancelled atomic.Int32
	pool := newStreamPool(1, func() (int, context.CancelFunc, error) {
		return 0, func() { cancelled.Add(1) }, nil
	})

	s1, err := pool.get()
	require.NoError(t, err)
	s2, err := pool.get()
	require.NoError(t, err)

	pool.put(s1)
	pool.put(s2) // exceeds maxIdle=1, should be discarded

	assert.Equal(t, int32(1), cancelled.Load(), "second put should cancel excess stream")
}

func TestStreamPool_Discard(t *testing.T) {
	var cancelled atomic.Int32
	pool := newStreamPool(2, func() (int, context.CancelFunc, error) {
		return 0, func() { cancelled.Add(1) }, nil
	})

	s, err := pool.get()
	require.NoError(t, err)

	pool.discard(s)
	assert.Equal(t, int32(1), cancelled.Load())
}

func TestStreamPool_Close(t *testing.T) {
	var cancelled atomic.Int32
	pool := newStreamPool(2, func() (int, context.CancelFunc, error) {
		return 0, func() { cancelled.Add(1) }, nil
	})

	s1, _ := pool.get()
	s2, _ := pool.get()
	pool.put(s1)
	pool.put(s2)

	pool.close()
	assert.Equal(t, int32(2), cancelled.Load(), "close should cancel all pooled streams")

	_, err := pool.get()
	assert.ErrorIs(t, err, ErrPoolClosed)
}

func TestStreamPool_PutAfterClose(t *testing.T) {
	var cancelled atomic.Int32
	pool := newStreamPool(2, func() (int, context.CancelFunc, error) {
		return 0, func() { cancelled.Add(1) }, nil
	})

	s, _ := pool.get()
	pool.close()

	pool.put(s)
	assert.Equal(t, int32(1), cancelled.Load(), "put after close should cancel the stream")
}
