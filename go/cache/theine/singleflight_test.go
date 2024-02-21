/*
Copyright 2023 The Vitess Authors.
Copyright 2023 Yiling-J
Copyright 2013 The Go Authors. All rights reserved.

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

// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package theine

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDo(t *testing.T) {
	g := NewGroup[string, string]()
	v, err, _ := g.Do("key", func() (string, error) {
		return "bar", nil
	})

	assert.Equal(t, "bar (string)", fmt.Sprintf("%v (%T)", v, v))
	assert.NoError(t, err)
}

func TestDoErr(t *testing.T) {
	g := NewGroup[string, string]()
	someErr := errors.New("Some error")
	v, err, _ := g.Do("key", func() (string, error) {
		return "", someErr
	})

	assert.ErrorIs(t, err, someErr, "incorrect Do error")
	assert.Empty(t, v, "unexpected non-empty value")
}

func TestDoDupSuppress(t *testing.T) {
	g := NewGroup[string, string]()
	var wg1, wg2 sync.WaitGroup
	c := make(chan string, 1)
	var calls int32
	fn := func() (string, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			// First invocation.
			wg1.Done()
		}
		v := <-c
		c <- v // pump; make available for any future calls

		time.Sleep(10 * time.Millisecond) // let more goroutines enter Do

		return v, nil
	}

	const n = 10
	wg1.Add(1)
	for range n {
		wg1.Add(1)
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			wg1.Done()
			v, err, _ := g.Do("key", fn)
			if !assert.NoError(t, err) {
				return
			}

			assert.Equal(t, "bar", v)
		}()
	}
	wg1.Wait()
	// At least one goroutine is in fn now and all of them have at
	// least reached the line before the Do.
	c <- "bar"
	wg2.Wait()
	got := atomic.LoadInt32(&calls)
	assert.Greater(t, got, int32(0))
	assert.Less(t, got, int32(n))
}

// Test singleflight behaves correctly after Do panic.
// See https://github.com/golang/go/issues/41133
func TestPanicDo(t *testing.T) {
	g := NewGroup[string, string]()
	fn := func() (string, error) {
		panic("invalid memory address or nil pointer dereference")
	}

	const n = 5
	waited := int32(n)
	panicCount := int32(0)
	done := make(chan struct{})
	for range n {
		go func() {
			defer func() {
				if err := recover(); err != nil {
					atomic.AddInt32(&panicCount, 1)
				}

				if atomic.AddInt32(&waited, -1) == 0 {
					close(done)
				}
			}()

			_, _, _ = g.Do("key", fn)
		}()
	}

	select {
	case <-done:
		assert.EqualValues(t, n, panicCount)
	case <-time.After(time.Second):
		require.Fail(t, "Do hangs")
	}
}

func TestGoexitDo(t *testing.T) {
	g := NewGroup[string, int]()
	fn := func() (int, error) {
		runtime.Goexit()
		return 0, nil
	}

	const n = 5
	waited := int32(n)
	done := make(chan struct{})
	for range n {
		go func() {
			var err error
			defer func() {
				assert.NoError(t, err)

				if atomic.AddInt32(&waited, -1) == 0 {
					close(done)
				}
			}()
			_, err, _ = g.Do("key", fn)
		}()
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		require.Fail(t, "Do hangs")
	}
}

func BenchmarkDo(b *testing.B) {
	keys := randKeys(b, 10240, 10)
	benchDo(b, NewGroup[string, int](), keys)

}

func benchDo(b *testing.B, g *Group[string, int], keys []string) {
	keyc := len(keys)
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			_, _, _ = g.Do(keys[i%keyc], func() (int, error) {
				return 0, nil
			})
		}
	})
}

func randKeys(b *testing.B, count, length uint) []string {
	keys := make([]string, 0, count)
	key := make([]byte, length)

	for i := range uint(count) {
		_, err := io.ReadFull(rand.Reader, key)
		require.NoError(b, err, "Failed to generate random key %d of %d length %d", i+1, count, length)
		keys = append(keys, string(key))
	}
	return keys
}
