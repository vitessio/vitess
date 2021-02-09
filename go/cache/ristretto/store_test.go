/*
 * Copyright 2020 Dgraph Labs, Inc. and Contributors
 * Copyright 2021 The Vitess Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ristretto

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStoreSetGet(t *testing.T) {
	s := newStore()
	key, conflict := defaultStringHash("1")
	i := Item{
		Key:      key,
		Conflict: conflict,
		Value:    2,
	}
	s.Set(&i)
	val, ok := s.Get(key, conflict)
	require.True(t, ok)
	require.Equal(t, 2, val.(int))

	i.Value = 3
	s.Set(&i)
	val, ok = s.Get(key, conflict)
	require.True(t, ok)
	require.Equal(t, 3, val.(int))

	key, conflict = defaultStringHash("2")
	i = Item{
		Key:      key,
		Conflict: conflict,
		Value:    2,
	}
	s.Set(&i)
	val, ok = s.Get(key, conflict)
	require.True(t, ok)
	require.Equal(t, 2, val.(int))
}

func TestStoreDel(t *testing.T) {
	s := newStore()
	key, conflict := defaultStringHash("1")
	i := Item{
		Key:      key,
		Conflict: conflict,
		Value:    1,
	}
	s.Set(&i)
	s.Del(key, conflict)
	val, ok := s.Get(key, conflict)
	require.False(t, ok)
	require.Nil(t, val)

	s.Del(2, 0)
}

func TestStoreClear(t *testing.T) {
	s := newStore()
	for i := 0; i < 1000; i++ {
		key, conflict := defaultStringHash(strconv.Itoa(i))
		it := Item{
			Key:      key,
			Conflict: conflict,
			Value:    i,
		}
		s.Set(&it)
	}
	s.Clear(nil)
	for i := 0; i < 1000; i++ {
		key, conflict := defaultStringHash(strconv.Itoa(i))
		val, ok := s.Get(key, conflict)
		require.False(t, ok)
		require.Nil(t, val)
	}
}

func TestStoreUpdate(t *testing.T) {
	s := newStore()
	key, conflict := defaultStringHash("1")
	i := Item{
		Key:      key,
		Conflict: conflict,
		Value:    1,
	}
	s.Set(&i)
	i.Value = 2
	_, ok := s.Update(&i)
	require.True(t, ok)

	val, ok := s.Get(key, conflict)
	require.True(t, ok)
	require.NotNil(t, val)

	val, ok = s.Get(key, conflict)
	require.True(t, ok)
	require.Equal(t, 2, val.(int))

	i.Value = 3
	_, ok = s.Update(&i)
	require.True(t, ok)

	val, ok = s.Get(key, conflict)
	require.True(t, ok)
	require.Equal(t, 3, val.(int))

	key, conflict = defaultStringHash("2")
	i = Item{
		Key:      key,
		Conflict: conflict,
		Value:    2,
	}
	_, ok = s.Update(&i)
	require.False(t, ok)
	val, ok = s.Get(key, conflict)
	require.False(t, ok)
	require.Nil(t, val)
}

func TestStoreCollision(t *testing.T) {
	s := newShardedMap()
	s.shards[1].Lock()
	s.shards[1].data[1] = storeItem{
		key:      1,
		conflict: 0,
		value:    1,
	}
	s.shards[1].Unlock()
	val, ok := s.Get(1, 1)
	require.False(t, ok)
	require.Nil(t, val)

	i := Item{
		Key:      1,
		Conflict: 1,
		Value:    2,
	}
	s.Set(&i)
	val, ok = s.Get(1, 0)
	require.True(t, ok)
	require.NotEqual(t, 2, val.(int))

	_, ok = s.Update(&i)
	require.False(t, ok)
	val, ok = s.Get(1, 0)
	require.True(t, ok)
	require.NotEqual(t, 2, val.(int))

	s.Del(1, 1)
	val, ok = s.Get(1, 0)
	require.True(t, ok)
	require.NotNil(t, val)
}

func BenchmarkStoreGet(b *testing.B) {
	s := newStore()
	key, conflict := defaultStringHash("1")
	i := Item{
		Key:      key,
		Conflict: conflict,
		Value:    1,
	}
	s.Set(&i)
	b.SetBytes(1)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Get(key, conflict)
		}
	})
}

func BenchmarkStoreSet(b *testing.B) {
	s := newStore()
	key, conflict := defaultStringHash("1")
	b.SetBytes(1)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := Item{
				Key:      key,
				Conflict: conflict,
				Value:    1,
			}
			s.Set(&i)
		}
	})
}

func BenchmarkStoreUpdate(b *testing.B) {
	s := newStore()
	key, conflict := defaultStringHash("1")
	i := Item{
		Key:      key,
		Conflict: conflict,
		Value:    1,
	}
	s.Set(&i)
	b.SetBytes(1)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Update(&Item{
				Key:      key,
				Conflict: conflict,
				Value:    2,
			})
		}
	})
}
