/*
 * Copyright 2019 Dgraph Labs, Inc. and Contributors
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
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var wait = time.Millisecond * 10

func TestCacheKeyToHash(t *testing.T) {
	keyToHashCount := 0
	c, err := NewCache(&Config{
		NumCounters:        10,
		MaxCost:            1000,
		BufferItems:        64,
		IgnoreInternalCost: true,
		KeyToHash: func(key string) (uint64, uint64) {
			keyToHashCount++
			return defaultStringHash(key)
		},
	})
	require.NoError(t, err)
	if c.SetWithCost("1", 1, 1) {
		time.Sleep(wait)
		val, ok := c.Get("1")
		require.True(t, ok)
		require.NotNil(t, val)
		c.Delete("1")
	}
	require.Equal(t, 3, keyToHashCount)
}

func TestCacheMaxCost(t *testing.T) {
	charset := "abcdefghijklmnopqrstuvwxyz0123456789"
	key := func() string {
		k := make([]byte, 2)
		for i := range k {
			k[i] = charset[rand.Intn(len(charset))]
		}
		return string(k)
	}
	c, err := NewCache(&Config{
		NumCounters: 12960, // 36^2 * 10
		MaxCost:     1e6,   // 1mb
		BufferItems: 64,
		Metrics:     true,
	})
	require.NoError(t, err)
	stop := make(chan struct{}, 8)
	for i := 0; i < 8; i++ {
		go func() {
			for {
				select {
				case <-stop:
					return
				default:
					time.Sleep(time.Millisecond)

					k := key()
					if _, ok := c.Get(k); !ok {
						val := ""
						if rand.Intn(100) < 10 {
							val = "test"
						} else {
							val = strings.Repeat("a", 1000)
						}
						c.SetWithCost(key(), val, int64(2+len(val)))
					}
				}
			}
		}()
	}
	for i := 0; i < 20; i++ {
		time.Sleep(time.Second)
		cacheCost := c.Metrics.CostAdded() - c.Metrics.CostEvicted()
		t.Logf("total cache cost: %d\n", cacheCost)
		require.True(t, float64(cacheCost) <= float64(1e6*1.05))
	}
	for i := 0; i < 8; i++ {
		stop <- struct{}{}
	}
}

func TestUpdateMaxCost(t *testing.T) {
	c, err := NewCache(&Config{
		NumCounters: 10,
		MaxCost:     10,
		BufferItems: 64,
	})
	require.NoError(t, err)
	require.Equal(t, int64(10), c.MaxCapacity())
	require.True(t, c.SetWithCost("1", 1, 1))
	time.Sleep(wait)
	_, ok := c.Get("1")
	// Set is rejected because the cost of the entry is too high
	// when accounting for the internal cost of storing the entry.
	require.False(t, ok)

	// Update the max cost of the cache and retry.
	c.SetCapacity(1000)
	require.Equal(t, int64(1000), c.MaxCapacity())
	require.True(t, c.SetWithCost("1", 1, 1))
	time.Sleep(wait)
	val, ok := c.Get("1")
	require.True(t, ok)
	require.NotNil(t, val)
	c.Delete("1")
}

func TestNewCache(t *testing.T) {
	_, err := NewCache(&Config{
		NumCounters: 0,
	})
	require.Error(t, err)

	_, err = NewCache(&Config{
		NumCounters: 100,
		MaxCost:     0,
	})
	require.Error(t, err)

	_, err = NewCache(&Config{
		NumCounters: 100,
		MaxCost:     10,
		BufferItems: 0,
	})
	require.Error(t, err)

	c, err := NewCache(&Config{
		NumCounters: 100,
		MaxCost:     10,
		BufferItems: 64,
		Metrics:     true,
	})
	require.NoError(t, err)
	require.NotNil(t, c)
}

func TestNilCache(t *testing.T) {
	var c *Cache
	val, ok := c.Get("1")
	require.False(t, ok)
	require.Nil(t, val)

	require.False(t, c.SetWithCost("1", 1, 1))
	c.Delete("1")
	c.Clear()
	c.Close()
}

func TestMultipleClose(t *testing.T) {
	var c *Cache
	c.Close()

	var err error
	c, err = NewCache(&Config{
		NumCounters: 100,
		MaxCost:     10,
		BufferItems: 64,
		Metrics:     true,
	})
	require.NoError(t, err)
	c.Close()
	c.Close()
}

func TestSetAfterClose(t *testing.T) {
	c, err := newTestCache()
	require.NoError(t, err)
	require.NotNil(t, c)

	c.Close()
	require.False(t, c.SetWithCost("1", 1, 1))
}

func TestClearAfterClose(t *testing.T) {
	c, err := newTestCache()
	require.NoError(t, err)
	require.NotNil(t, c)

	c.Close()
	c.Clear()
}

func TestGetAfterClose(t *testing.T) {
	c, err := newTestCache()
	require.NoError(t, err)
	require.NotNil(t, c)

	require.True(t, c.SetWithCost("1", 1, 1))
	c.Close()

	_, ok := c.Get("2")
	require.False(t, ok)
}

func TestDelAfterClose(t *testing.T) {
	c, err := newTestCache()
	require.NoError(t, err)
	require.NotNil(t, c)

	require.True(t, c.SetWithCost("1", 1, 1))
	c.Close()

	c.Delete("1")
}

func TestCacheProcessItems(t *testing.T) {
	m := &sync.Mutex{}
	evicted := make(map[uint64]struct{})
	c, err := NewCache(&Config{
		NumCounters:        100,
		MaxCost:            10,
		BufferItems:        64,
		IgnoreInternalCost: true,
		Cost: func(value interface{}) int64 {
			return int64(value.(int))
		},
		OnEvict: func(item *Item) {
			m.Lock()
			defer m.Unlock()
			evicted[item.Key] = struct{}{}
		},
	})
	require.NoError(t, err)

	var key uint64
	var conflict uint64

	key, conflict = defaultStringHash("1")
	c.setBuf <- &Item{
		flag:     itemNew,
		Key:      key,
		Conflict: conflict,
		Value:    1,
		Cost:     0,
	}
	time.Sleep(wait)
	require.True(t, c.policy.Has(key))
	require.Equal(t, int64(1), c.policy.Cost(key))

	key, conflict = defaultStringHash("1")
	c.setBuf <- &Item{
		flag:     itemUpdate,
		Key:      key,
		Conflict: conflict,
		Value:    2,
		Cost:     0,
	}
	time.Sleep(wait)
	require.Equal(t, int64(2), c.policy.Cost(key))

	key, conflict = defaultStringHash("1")
	c.setBuf <- &Item{
		flag:     itemDelete,
		Key:      key,
		Conflict: conflict,
	}
	time.Sleep(wait)
	key, conflict = defaultStringHash("1")
	val, ok := c.store.Get(key, conflict)
	require.False(t, ok)
	require.Nil(t, val)
	require.False(t, c.policy.Has(1))

	key, conflict = defaultStringHash("2")
	c.setBuf <- &Item{
		flag:     itemNew,
		Key:      key,
		Conflict: conflict,
		Value:    2,
		Cost:     3,
	}
	key, conflict = defaultStringHash("3")
	c.setBuf <- &Item{
		flag:     itemNew,
		Key:      key,
		Conflict: conflict,
		Value:    3,
		Cost:     3,
	}
	key, conflict = defaultStringHash("4")
	c.setBuf <- &Item{
		flag:     itemNew,
		Key:      key,
		Conflict: conflict,
		Value:    3,
		Cost:     3,
	}
	key, conflict = defaultStringHash("5")
	c.setBuf <- &Item{
		flag:     itemNew,
		Key:      key,
		Conflict: conflict,
		Value:    3,
		Cost:     5,
	}
	time.Sleep(wait)
	m.Lock()
	require.NotEqual(t, 0, len(evicted))
	m.Unlock()

	defer func() {
		require.NotNil(t, recover())
	}()
	c.Close()
	c.setBuf <- &Item{flag: itemNew}
}

func TestCacheGet(t *testing.T) {
	c, err := NewCache(&Config{
		NumCounters:        100,
		MaxCost:            10,
		BufferItems:        64,
		IgnoreInternalCost: true,
		Metrics:            true,
	})
	require.NoError(t, err)

	key, conflict := defaultStringHash("1")
	i := Item{
		Key:      key,
		Conflict: conflict,
		Value:    1,
	}
	c.store.Set(&i)
	val, ok := c.Get("1")
	require.True(t, ok)
	require.NotNil(t, val)

	val, ok = c.Get("2")
	require.False(t, ok)
	require.Nil(t, val)

	// 0.5 and not 1.0 because we tried Getting each item twice
	require.Equal(t, 0.5, c.Metrics.Ratio())

	c = nil
	val, ok = c.Get("0")
	require.False(t, ok)
	require.Nil(t, val)
}

// retrySet calls SetWithCost until the item is accepted by the cache.
func retrySet(t *testing.T, c *Cache, key string, value int, cost int64) {
	for {
		if set := c.SetWithCost(key, value, cost); !set {
			time.Sleep(wait)
			continue
		}

		time.Sleep(wait)
		val, ok := c.Get(key)
		require.True(t, ok)
		require.NotNil(t, val)
		require.Equal(t, value, val.(int))
		return
	}
}

func TestCacheSet(t *testing.T) {
	c, err := NewCache(&Config{
		NumCounters:        100,
		MaxCost:            10,
		IgnoreInternalCost: true,
		BufferItems:        64,
		Metrics:            true,
	})
	require.NoError(t, err)

	retrySet(t, c, "1", 1, 1)

	c.SetWithCost("1", 2, 2)
	val, ok := c.store.Get(defaultStringHash("1"))
	require.True(t, ok)
	require.Equal(t, 2, val.(int))

	c.stop <- struct{}{}
	for i := 0; i < setBufSize; i++ {
		key, conflict := defaultStringHash("1")
		c.setBuf <- &Item{
			flag:     itemUpdate,
			Key:      key,
			Conflict: conflict,
			Value:    1,
			Cost:     1,
		}
	}
	require.False(t, c.SetWithCost("2", 2, 1))
	require.Equal(t, uint64(1), c.Metrics.SetsDropped())
	close(c.setBuf)
	close(c.stop)

	c = nil
	require.False(t, c.SetWithCost("1", 1, 1))
}

func TestCacheInternalCost(t *testing.T) {
	c, err := NewCache(&Config{
		NumCounters: 100,
		MaxCost:     10,
		BufferItems: 64,
		Metrics:     true,
	})
	require.NoError(t, err)

	// Get should return false because the cache's cost is too small to store the item
	// when accounting for the internal cost.
	c.SetWithCost("1", 1, 1)
	time.Sleep(wait)
	_, ok := c.Get("1")
	require.False(t, ok)
}

func TestCacheDel(t *testing.T) {
	c, err := NewCache(&Config{
		NumCounters: 100,
		MaxCost:     10,
		BufferItems: 64,
	})
	require.NoError(t, err)

	c.SetWithCost("1", 1, 1)
	c.Delete("1")
	// The deletes and sets are pushed through the setbuf. It might be possible
	// that the delete is not processed before the following get is called. So
	// wait for a millisecond for things to be processed.
	time.Sleep(time.Millisecond)
	val, ok := c.Get("1")
	require.False(t, ok)
	require.Nil(t, val)

	c = nil
	defer func() {
		require.Nil(t, recover())
	}()
	c.Delete("1")
}

func TestCacheClear(t *testing.T) {
	c, err := NewCache(&Config{
		NumCounters:        100,
		MaxCost:            10,
		IgnoreInternalCost: true,
		BufferItems:        64,
		Metrics:            true,
	})
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		c.SetWithCost(strconv.Itoa(i), i, 1)
	}
	time.Sleep(wait)
	require.Equal(t, uint64(10), c.Metrics.KeysAdded())

	c.Clear()
	require.Equal(t, uint64(0), c.Metrics.KeysAdded())

	for i := 0; i < 10; i++ {
		val, ok := c.Get(strconv.Itoa(i))
		require.False(t, ok)
		require.Nil(t, val)
	}
}

func TestCacheMetrics(t *testing.T) {
	c, err := NewCache(&Config{
		NumCounters:        100,
		MaxCost:            10,
		IgnoreInternalCost: true,
		BufferItems:        64,
		Metrics:            true,
	})
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		c.SetWithCost(strconv.Itoa(i), i, 1)
	}
	time.Sleep(wait)
	m := c.Metrics
	require.Equal(t, uint64(10), m.KeysAdded())
}

func TestMetrics(t *testing.T) {
	newMetrics()
}

func TestNilMetrics(t *testing.T) {
	var m *Metrics
	for _, f := range []func() uint64{
		m.Hits,
		m.Misses,
		m.KeysAdded,
		m.KeysEvicted,
		m.CostEvicted,
		m.SetsDropped,
		m.SetsRejected,
		m.GetsDropped,
		m.GetsKept,
	} {
		require.Equal(t, uint64(0), f())
	}
}

func TestMetricsAddGet(t *testing.T) {
	m := newMetrics()
	m.add(hit, 1, 1)
	m.add(hit, 2, 2)
	m.add(hit, 3, 3)
	require.Equal(t, uint64(6), m.Hits())

	m = nil
	m.add(hit, 1, 1)
	require.Equal(t, uint64(0), m.Hits())
}

func TestMetricsRatio(t *testing.T) {
	m := newMetrics()
	require.Equal(t, float64(0), m.Ratio())

	m.add(hit, 1, 1)
	m.add(hit, 2, 2)
	m.add(miss, 1, 1)
	m.add(miss, 2, 2)
	require.Equal(t, 0.5, m.Ratio())

	m = nil
	require.Equal(t, float64(0), m.Ratio())
}

func TestMetricsString(t *testing.T) {
	m := newMetrics()
	m.add(hit, 1, 1)
	m.add(miss, 1, 1)
	m.add(keyAdd, 1, 1)
	m.add(keyUpdate, 1, 1)
	m.add(keyEvict, 1, 1)
	m.add(costAdd, 1, 1)
	m.add(costEvict, 1, 1)
	m.add(dropSets, 1, 1)
	m.add(rejectSets, 1, 1)
	m.add(dropGets, 1, 1)
	m.add(keepGets, 1, 1)
	require.Equal(t, uint64(1), m.Hits())
	require.Equal(t, uint64(1), m.Misses())
	require.Equal(t, 0.5, m.Ratio())
	require.Equal(t, uint64(1), m.KeysAdded())
	require.Equal(t, uint64(1), m.KeysUpdated())
	require.Equal(t, uint64(1), m.KeysEvicted())
	require.Equal(t, uint64(1), m.CostAdded())
	require.Equal(t, uint64(1), m.CostEvicted())
	require.Equal(t, uint64(1), m.SetsDropped())
	require.Equal(t, uint64(1), m.SetsRejected())
	require.Equal(t, uint64(1), m.GetsDropped())
	require.Equal(t, uint64(1), m.GetsKept())

	require.NotEqual(t, 0, len(m.String()))

	m = nil
	require.Equal(t, 0, len(m.String()))

	require.Equal(t, "unidentified", stringFor(doNotUse))
}

func TestCacheMetricsClear(t *testing.T) {
	c, err := NewCache(&Config{
		NumCounters: 100,
		MaxCost:     10,
		BufferItems: 64,
		Metrics:     true,
	})
	require.NoError(t, err)

	c.SetWithCost("1", 1, 1)
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				c.Get("1")
			}
		}
	}()
	time.Sleep(wait)
	c.Clear()
	stop <- struct{}{}
	c.Metrics = nil
	c.Metrics.Clear()
}

// Regression test for bug https://github.com/dgraph-io/ristretto/issues/167
func TestDropUpdates(t *testing.T) {
	originalSetBugSize := setBufSize
	defer func() { setBufSize = originalSetBugSize }()

	test := func() {
		// dropppedMap stores the items dropped from the cache.
		droppedMap := make(map[int]struct{})
		lastEvictedSet := int64(-1)

		var err error
		handler := func(_ interface{}, value interface{}) {
			v := value.(string)
			lastEvictedSet, err = strconv.ParseInt(string(v), 10, 32)
			require.NoError(t, err)

			_, ok := droppedMap[int(lastEvictedSet)]
			if ok {
				panic(fmt.Sprintf("val = %+v was dropped but it got evicted. Dropped items: %+v\n",
					lastEvictedSet, droppedMap))
			}
		}

		// This is important. The race condition shows up only when the setBuf
		// is full and that's why we reduce the buf size here. The test will
		// try to fill up the setbuf to it's capacity and then perform an
		// update on a key.
		setBufSize = 10

		c, err := NewCache(&Config{
			NumCounters: 100,
			MaxCost:     10,
			BufferItems: 64,
			Metrics:     true,
			OnEvict: func(item *Item) {
				if item.Value != nil {
					handler(nil, item.Value)
				}
			},
		})
		require.NoError(t, err)

		for i := 0; i < 5*setBufSize; i++ {
			v := fmt.Sprintf("%0100d", i)
			// We're updating the same key.
			if !c.SetWithCost("0", v, 1) {
				// The race condition doesn't show up without this sleep.
				time.Sleep(time.Microsecond)
				droppedMap[i] = struct{}{}
			}
		}
		// Wait for all the items to be processed.
		c.Wait()
		// This will cause eviction from the cache.
		require.True(t, c.SetWithCost("1", nil, 10))
		c.Close()
	}

	// Run the test 100 times since it's not reliable.
	for i := 0; i < 100; i++ {
		test()
	}
}

func newTestCache() (*Cache, error) {
	return NewCache(&Config{
		NumCounters: 100,
		MaxCost:     10,
		BufferItems: 64,
		Metrics:     true,
	})
}
