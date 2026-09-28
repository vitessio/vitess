/*
Copyright 2019 The Vitess Authors.

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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (pool *IDPool) want(want *IDPool, t *testing.T) {
	assert.Equal(t, want.maxUsed, pool.maxUsed)
	assert.Equal(t, want.used, pool.used)
}

func TestIDPoolFirstGet(t *testing.T) {
	pool := NewIDPool(0)
	got := pool.Get()
	assert.EqualValues(t, 1, got)
	pool.want(&IDPool{used: map[uint32]bool{}, maxUsed: 1}, t)
}

func TestIDPoolSecondGet(t *testing.T) {
	pool := NewIDPool(0)
	pool.Get()
	got := pool.Get()
	assert.EqualValues(t, 2, got)
	pool.want(&IDPool{used: map[uint32]bool{}, maxUsed: 2}, t)
}

func TestIDPoolPutToUsedSet(t *testing.T) {
	pool := NewIDPool(0)
	id1 := pool.Get()
	pool.Get()
	pool.Put(id1)
	pool.want(&IDPool{used: map[uint32]bool{1: true}, maxUsed: 2}, t)
}

func TestIDPoolPutMaxUsed1(t *testing.T) {
	pool := NewIDPool(0)
	id1 := pool.Get()
	pool.Put(id1)
	pool.want(&IDPool{used: map[uint32]bool{}, maxUsed: 0}, t)
}

func TestIDPoolPutMaxUsed2(t *testing.T) {
	pool := NewIDPool(0)
	pool.Get()
	id2 := pool.Get()
	pool.Put(id2)
	pool.want(&IDPool{used: map[uint32]bool{}, maxUsed: 1}, t)
}

func TestIDPoolGetFromUsedSet(t *testing.T) {
	pool := NewIDPool(0)
	id1 := pool.Get()
	pool.Get()
	pool.Put(id1)
	got := pool.Get()
	assert.EqualValues(t, 1, got)
	pool.want(&IDPool{used: map[uint32]bool{}, maxUsed: 2}, t)
}

func wantError(want string, t *testing.T) {
	rec := recover()
	require.NotNil(t, rec, "expected panic, but there wasn't one")
	err, ok := rec.(error)
	assert.True(t, ok)
	assert.Contains(t, err.Error(), want)
}

func TestIDPoolPut0(t *testing.T) {
	pool := NewIDPool(0)
	pool.Get()

	defer wantError("invalid value", t)
	pool.Put(0)
}

func TestIDPoolPutInvalid(t *testing.T) {
	pool := NewIDPool(0)
	pool.Get()

	defer wantError("invalid value", t)
	pool.Put(5)
}

func TestIDPoolPutDuplicate(t *testing.T) {
	pool := NewIDPool(0)
	pool.Get()
	pool.Get()
	pool.Put(1)

	defer wantError("already recycled", t)
	pool.Put(1)
}
